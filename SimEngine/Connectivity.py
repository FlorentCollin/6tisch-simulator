#!/usr/bin/python
"""
Creates a connectivity matrix and provide methods to get the connectivity
between two motes.

The connectivity matrix is index by source id, destination id and channel.
Each cell of the matrix is a dict with the fields `pdr` and `rssi`

The connectivity matrix can be filled statically at startup or be updated along
time if a connectivity trace is given.

The propagate() method is called at every slot. It loops through the
transmissions occurring during that slot and checks if the transmission fails or
succeeds.
"""

# =========================== imports =========================================

import sys
import random
import math
from abc import abstractmethod

import SimSettings
import SimEngine
from Mote.Mote import Mote
from Mote import MoteDefines as d

# =========================== defines =========================================

CONN_TYPE_TRACE         = "trace"

# =========================== helpers =========================================

# =========================== classes =========================================

class Connectivity(object):
    def __new__(cls):
        settings    = SimEngine.SimSettings.SimSettings()
        class_name  = 'Connectivity{0}'.format(settings.conn_class)
        return getattr(sys.modules[__name__], class_name)()

class ConnectivityBase(object):

    # ===== start singleton
    _instance = None
    _init = False

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(ConnectivityBase, cls).__new__(cls, *args, **kwargs)
        return cls._instance
    # ===== end singleton

    def __init__(self):

        # ==== start singleton
        cls = type(self)
        if cls._init:
            return
        cls._init = True
        # ==== end singleton
        
        # store params
        
        # singletons (quicker access, instead of recreating every time)
        self.settings = SimSettings.SimSettings()
        self.engine   = SimEngine.SimEngine()
        self.log      = SimEngine.SimLog.SimLog().log

        # local variables
        self.connectivity_matrix = {} # described at the top of the file
        self.connectivity_matrix_timestamp = 0
        
        # at the beginning, connectivity matrix indicates no connectivity at all
        for source in self.engine.motes:
            self.connectivity_matrix[source.id] = {}
            for destination in self.engine.motes:
                self.connectivity_matrix[source.id][destination.id] = {}
                for channel in range(self.settings.phy_numChans):
                    self.connectivity_matrix[source.id][destination.id][channel] = {
                        "pdr":      0,
                        "rssi": -1000,
                    }
        
        # introduce some connectivity in the matrix
        self._init_connectivity_matrix()

        # schedule propagation task
        self._schedule_propagate()

    def destroy(self):
        cls           = type(self)
        cls._instance = None
        cls._init     = False
    
    # ======================== abstract =======================================
    
    @abstractmethod
    def _init_connectivity_matrix(self):
        raise NotImplementedError() # abstractmethod
    
    # ======================== public =========================================
    
    # === getters
    
    def get_pdr(self, source, destination, channel):
        
        assert type(source)==int
        assert type(destination)==int
        assert type(channel)==int
        
        return self.connectivity_matrix[source][destination][channel]["pdr"]

    def get_rssi(self, source, destination, channel):
        
        assert type(source)==int
        assert type(destination)==int
        assert type(channel)==int
        
        return self.connectivity_matrix[source][destination][channel]["rssi"]
    
    # === propagation
    
    def propagate(self):
        """ Simulate the propagation of frames in a slot. """
        
        # local shorthands
        asn        = self.engine.getAsn()
        slotOffset = asn % self.settings.tsch_slotframeLength
        
        # repeat propagation for each channel
        for channel in range(self.settings.phy_numChans):
            
            # === accounting
            
            # list all transmissions at that frequency
            alltransmissions = []
            for mote in self.engine.motes:
                if mote.radio.onGoingTransmission:
                    assert mote.radio.state == d.RADIO_STATE_TX
                    if mote.radio.onGoingTransmission['channel'] == channel:
                        thisTran = {}
                        
                        # channel
                        thisTran['channel'] = channel
                        
                        # packet
                        thisTran['packet']  = mote.radio.onGoingTransmission['packet']
                        srcMac              = thisTran['packet']['mac']['srcMac']
                        srcMote             = self.engine.motes[srcMac]
                        
                        # time at which the packet starts transmitting
                        thisTran['txTime']  = srcMote.tsch.computeTimeOffsetToDagRoot()
                        
                        # number of ACKs received by this packet
                        thisTran['numACKs'] = 0
                        
                        alltransmissions   += [thisTran]
            
            # === decide which listener gets which packet (rxDone)
            
            for listener in self._get_listeners(channel):
                
                # list the transmissions that listener can hear
                transmissions = []
                for t in alltransmissions:
                    rssi = self.get_rssi(
                        source      = t['packet']['mac']['srcMac'],
                        destination = listener,
                        channel     = channel,
                    )
                    if self.settings.phy_minRssi < rssi:
                        transmissions += [t]
                
                if transmissions==[]:
                    # no transmissions
                    
                    # idle listen
                    sentAnAck = self.engine.motes[listener].radio.rxDone(
                        packet = None,
                    )
                    assert sentAnAck==False
                else:
                    # there are transmissions
                    
                    # listener locks onto the earliest transmission
                    lockon_transmission = None
                    for t in transmissions:
                        if lockon_transmission==None or t['txTime']<lockon_transmission['txTime']:
                            lockon_transmission = t
                    
                    # all other transmissions are now intereferers
                    interfering_transmissions = [t for t in transmissions if t!=lockon_transmission]
                    assert len(transmissions) == len(interfering_transmissions)+1
                    
                    # log
                    if interfering_transmissions:
                        self.log(
                            SimEngine.SimLog.LOG_PROP_INTERFERENCE,
                            {
                                '_mote_id':                    lockon_transmission['packet']['mac']['srcMac'],
                                'channel':                     lockon_transmission['channel'],
                                'lockon_transmission':         lockon_transmission['packet'],
                                'interfering_transmissions':   [t['packet'] for t in interfering_transmissions],
                            }
                        )

                    # calculate the resulting pdr when taking interferers into account
                    pdr = self._compute_pdr_with_interference(
                        listener                     = listener,
                        lockon_transmission          = lockon_transmission,
                        interfering_transmissions    = interfering_transmissions,
                    )

                    # decide whether listener receives lockon_transmission or not
                    if random.random() < pdr:
                        # listener receives!
                        
                        # lockon_transmission received correctly
                        sentAnAck = self.engine.motes[listener].radio.rxDone(
                            packet = lockon_transmission['packet'],
                        )
                        
                        # keep track of the number of ACKs received by that transmission
                        if sentAnAck:
                            lockon_transmission['numACKs'] += 1

                    else:
                        # lockon_transmission NOT received correctly (interference)
                        sentAnAck = self.engine.motes[listener].radio.rxDone(
                            packet = None,
                        )
                        assert sentAnAck==False
            
            assert self._get_listeners(channel)==[]
            
            # === decide whether transmitters get an ACK (txDone)
            
            for t in alltransmissions:
                
                # decide whether transmitter received an ACK
                if   t['numACKs']==0:
                    isACKed = False
                elif t['numACKs']==1:
                    isACKed = True
                else:
                    # we do not expect multiple ACKs (would indicate duplicate MAC addresses)
                    raise SystemError()
                
                # indicate to source packet was sent
                self.engine.motes[t['packet']['mac']['srcMac']].radio.txDone(isACKed)
            
            # verify all radios OFF
            for mote in self.engine.motes:
                assert mote.radio.state   == d.RADIO_STATE_OFF
                assert mote.radio.channel == None

        # schedule next propagation
        self._schedule_propagate()

    # ======================= private =========================================
    
    # === schedule

    def _schedule_propagate(self):
        '''
        schedule a propagation task in the middle of the next slot.
        FIXME: only schedule for next active slot.
        '''
        self.engine.scheduleAtAsn(
            asn              = self.engine.getAsn() + 1,
            cb               = self.propagate,
            uniqueTag        = (None, 'Connectivity.propagate'),
            intraSlotOrder   = d.INTRASLOTORDER_PROPAGATE,
        )

    # === listeners

    def _get_listeners(self, channel):
        returnVal = []
        for mote in self.engine.motes:
            if (mote.radio.state == d.RADIO_STATE_RX) and (mote.radio.channel == channel):
                returnVal.append(mote.id)
        return returnVal
    
    # === wireless
    
    def _compute_pdr_with_interference(self, listener, lockon_transmission, interfering_transmissions):
    
        # shorthand
        channel = lockon_transmission['channel']
        for t in interfering_transmissions:
            assert t['channel'] == channel
        lockon_srcMac  = lockon_transmission['packet']['mac']['srcMac']
        
        # === compute the SINR
        
        noise_mW   = self._dBm_to_mW(self.engine.motes[listener].radio.noisepower)
        
        # S = RSSI - N
        
        signal_mW = self._dBm_to_mW(self.get_rssi(lockon_srcMac, listener, channel)) - noise_mW
        if signal_mW < 0.0:
            # RSSI has not to be below the noise level.
            # If this happens, return very low SINR (-10.0dB)
            return -10.0

        # I = RSSI - N
        
        totalInterference_mW = 0.0
        for interfering_tran in interfering_transmissions:
            interfering_srcMac = interfering_tran['packet']['mac']['srcMac']
            interference_mW = self._dBm_to_mW(self.get_rssi(interfering_srcMac, listener, channel)) - noise_mW
            if interference_mW < 0.0:
                # RSSI has not to be below noise level.
                # If this happens, set interference to 0.0
                interference_mW = 0.0
            totalInterference_mW += interference_mW

        sinr_dB = self._mW_to_dBm( signal_mW / (totalInterference_mW + noise_mW) )
        
        # === compute the interference PDR
        
        # shorthand
        noise_dBm = self.engine.motes[listener].radio.noisepower
        
        # RSSI of the interfering transmissions
        interference_rssi = self._mW_to_dBm(
            self._dBm_to_mW(sinr_dB + noise_dBm) +
            self._dBm_to_mW(noise_dBm)
        )
        
        # PDR of the interfering transmissions
        interference_pdr = self._rssi_to_pdr(interference_rssi)
        
        # === compute the resulting PDR
        
        lockon_pdr = self.get_pdr(
            source      = lockon_srcMac,
            destination = listener,
            channel     = channel)
        returnVal  = lockon_pdr * interference_pdr
        
        return returnVal
    
    # === helpers
    
    def _dBm_to_mW(self,dBm):
        return math.pow(10.0, dBm / 10.0)
    
    def _mW_to_dBm(self,mW):
        return 10 * math.log10(mW)
    
    def _rssi_to_pdr(self,rssi):
        """
        rssi and pdr relationship obtained by experiment below
        http://wsn.eecs.berkeley.edu/connectivity/?dataset=dust
        """

        rssi_pdr_table = {
            -97:    0.0000,  # this value is not from experiment
            -96:    0.1494,
            -95:    0.2340,
            -94:    0.4071,
            # <-- 50% PDR is here, at RSSI=-93.6
            -93:    0.6359,
            -92:    0.6866,
            -91:    0.7476,
            -90:    0.8603,
            -89:    0.8702,
            -88:    0.9324,
            -87:    0.9427,
            -86:    0.9562,
            -85:    0.9611,
            -84:    0.9739,
            -83:    0.9745,
            -82:    0.9844,
            -81:    0.9854,
            -80:    0.9903,
            -79:    1.0000,  # this value is not from experiment
        }

        minRssi = min(rssi_pdr_table.keys())
        maxRssi = max(rssi_pdr_table.keys())

        if  rssi < minRssi:
            pdr = 0.0
        elif rssi > maxRssi:
            pdr = 1.0
        else:
            floorRssi = int(math.floor(rssi))
            pdrLow    = rssi_pdr_table[floorRssi]
            pdrHigh   = rssi_pdr_table[floorRssi+1]
            # linear interpolation
            pdr       = (pdrHigh - pdrLow) * (rssi - float(floorRssi)) + pdrLow

        assert 0 <= pdr <= 1.0

        return pdr

class ConnectivityFullyMeshed(ConnectivityBase):
    """
    All nodes can hear all nodes with PDR=100%.
    """
    
    def _init_connectivity_matrix(self):
        for source in self.engine.motes:
            for destination in self.engine.motes:
                for channel in range(self.settings.phy_numChans):
                    self.connectivity_matrix[source.id][destination.id][channel] = {
                        "pdr":    1.00,
                        "rssi":    -10,
                    }

class ConnectivityLinear(ConnectivityBase):
    """
    Perfect linear topology.
           100%     100%     100%       100%
        0 <----> 1 <----> 2 <----> ... <----> num_motes-1
    """
    
    def _init_connectivity_matrix(self):
        parent = None
        for mote in self.engine.motes:
            if parent is not None:
                for channel in range(self.settings.phy_numChans):
                    self.connectivity_matrix[mote.id][parent.id][channel] = {
                        "pdr": 1.00,
                        "rssi": -10,
                    }
                    self.connectivity_matrix[parent.id][mote.id][channel] = {
                        "pdr": 1.00,
                        "rssi": -10,
                    }
            parent = mote

class ConnectivityK7(ConnectivityBase):
    """
    Replay K7 connectivity trace.
    """
    
    # ======================= inheritance =====================================
    
    # definitions of abstract methods
    
    def _init_connectivity_matrix(self):
        """ Fill the matrix using the connectivity trace"""
        raise NotImplementedError() # TODO
    
    # overloaded methods
    
    def get_pdr(self, source, destination, channel):
        
        # update PDR matrix if we are a new row in our K7 file
        if  self.connectivity_matrix_timestamp < self.engine.asn:
            self._update_connectivity_matrix_from_trace()
        
        # then call the parent's method
        return super(ConnectivityK7, self).get_pdr(source, destination, channel)
    
    def get_rssi(self, source, destination, channel):
        
        # update PDR matrix if we are a new row in our K7 file
        if  self.connectivity_matrix_timestamp < self.engine.asn:
            self._update_connectivity_matrix_from_trace()
        
        # then call the parent's method
        return super(ConnectivityK7, self).get_rssi(source, destination, channel)
    
    # ======================= private =========================================
    
    def _update_connectivity_matrix_from_trace(self):
        """
        :return: Timestamp when to update the matrix again
        """

        first_line = None
        with open(self.settings.conn_trace, 'r') as trace:
            trace.readline()  # ignore header
            self.csv_header = trace.readline().split(',')
            for line in trace:
                # read and parse line
                vals = line.split(',')
                row = dict(zip(self.csv_header, vals))

                if first_line is None:
                    first_line = line
                else:
                    if line == first_line:
                        return row['datetime']

                # update matrix value
                self.connectivity_matrix[row['src']][row['dst']][row['channel']] = row['pdr']

class ConnectivityPisterHack(ConnectivityBase):
    """
    Pister-Hack connectivity.
    """
    
    PISTER_HACK_LOWER_SHIFT =         40 # dB
    TWO_DOT_FOUR_GHZ        = 2400000000 # Hz
    SPEED_OF_LIGHT          =  299792458 # m/s
    
    def _init_connectivity_matrix(self):
        
        for source in self.engine.motes:
            for destination in self.engine.motes:
                for channel in range(self.settings.phy_numChans):
                    rssi = self._compute_rssi_pisterhack(source, destination)
                    pdr  = self._rssi_to_pdr(rssi)
                    self.connectivity_matrix[source.id][destination.id][channel] = {
                        "pdr": pdr,
                        "rssi": rssi,
                    }
    
    def _compute_rssi_pisterhack(mote, neighbor):
        """
        computes RSSI between any two nodes (not only neighbors)
        according to the Pister-hack model.
        """

        # distance in m
        distance = self._get_distance(mote, neighbor)

        # sqrt and inverse of the free space path loss
        fspl = self.SPEED_OF_LIGHT / (4 * math.pi * distance * self.TWO_DOT_FOUR_GHZ)

        # simple friis equation in Pr=Pt+Gt+Gr+20log10(c/4piR)
        pr = (mote.txPower + mote.antennaGain + neighbor.antennaGain +
              (20 * math.log10(fspl)))

        # according to the receiver power (RSSI) we can apply the Pister hack
        # model.
        rssi = pr - random.uniform(0, self.PISTER_HACK_LOWER_SHIFT)

        return rssi
    
    def _get_distance(mote, neighbor):
        """
        mote.x and mote.y are in km. This function returns the distance in m.
        """

        return 1000*math.sqrt((mote.x - neighbor.x)**2 +
                              (mote.y - neighbor.y)**2)
