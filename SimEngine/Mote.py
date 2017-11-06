#!/usr/bin/python
'''
\brief Model of a 6TiSCH mote.

\author Thomas Watteyne <watteyne@eecs.berkeley.edu>
\author Kazushi Muraoka <k-muraoka@eecs.berkeley.edu>
\author Nicola Accettura <nicola.accettura@eecs.berkeley.edu>
\author Xavier Vilajosana <xvilajosana@eecs.berkeley.edu>
\author Malisa Vucinic <malisa.vucinic@inria.fr>
\author Esteban Municio <esteban.municio@uantwerpen.be>
\author Glenn Daneels <glenn.daneels@uantwerpen.be>
'''

#============================ logging =========================================

import logging
class NullHandler(logging.Handler):
    def emit(self, record):
        pass
log = logging.getLogger('Mote')
log.setLevel(logging.DEBUG)
log.addHandler(NullHandler())

#============================ imports =========================================

import copy
import random
import threading
import math

import SimEngine
import SimSettings
import Propagation
import Topology

#============================ defines =========================================

#============================ body ============================================

class Mote(object):
    
    # sufficient num. of tx to estimate pdr by ACK
    NUM_SUFFICIENT_TX                  = 10
    # maximum number of tx for history
    NUM_MAX_HISTORY                    = 32
    
    DIR_TX                             = 'TX'
    DIR_RX                             = 'RX'
    DIR_TXRX_SHARED                    = 'SHARED'
    
    
    DEBUG                              = 'DEBUG'
    INFO                               = 'INFO'
    WARNING                            = 'WARNING'
    ERROR                              = 'ERROR'
    
    #=== app
    APP_TYPE_DATA                      = 'DATA'
    APP_TYPE_ACK                       = 'ACK'  # end to end ACK
    APP_TYPE_JOIN                      = 'JOIN' # join traffic
    RPL_TYPE_DIO                       = 'DIO'
    RPL_TYPE_DAO                       = 'DAO'
    TSCH_TYPE_EB                       = 'EB'
    #=== 6top message types
    IANA_6TOP_TYPE_REQUEST              = '6TOP_REQUEST'
    IANA_6TOP_TYPE_RESPONSE             = '6TOP_RESPONSE'
    IANA_6TOP_TYPE_CONFIRMATION         = '6TOP_CONFIRMATION'

    #=== rpl
    RPL_PARENT_SWITCH_THRESHOLD        = 768 # corresponds to 1.5 hops. 6tisch minimal draft use 384 for 2*ETX.
    RPL_MIN_HOP_RANK_INCREASE          = 256
    RPL_MAX_ETX                        = 4
    RPL_MAX_RANK_INCREASE              = RPL_MAX_ETX*RPL_MIN_HOP_RANK_INCREASE*2 # 4 transmissions allowed for rank increase for parents
    RPL_MAX_TOTAL_RANK                 = 256*RPL_MIN_HOP_RANK_INCREASE*2 # 256 transmissions allowed for total path cost for parents
    RPL_PARENT_SET_SIZE                = 3
    DEFAULT_DIO_INTERVAL_MIN           = 3 # log2(DIO_INTERVAL_MIN), with DIO_INTERVAL_MIN expressed in ms
    DEFAULT_DIO_INTERVAL_DOUBLINGS     = 20 # maximum number of doublings of DIO_INTERVAL_MIN (DIO_INTERVAL_MAX = 2^(DEFAULT_DIO_INTERVAL_MIN+DEFAULT_DIO_INTERVAL_DOUBLINGS) ms)
    DEFAULT_DIO_REDUNDANCY_CONSTANT    = 10 # number of hearings to suppress next transmission in the current interval
    
    #=== 6top states
    SIX_STATE_IDLE                              = 0x00
    # sending
    SIX_STATE_SENDING_REQUEST                   = 0x01
    # waiting for SendDone confirmation
    SIX_STATE_WAIT_ADDREQUEST_SENDDONE          = 0x02
    SIX_STATE_WAIT_DELETEREQUEST_SENDDONE       = 0x03
    SIX_STATE_WAIT_RELOCATEREQUEST_SENDDONE     = 0x04
    SIX_STATE_WAIT_COUNTREQUEST_SENDDONE        = 0x05
    SIX_STATE_WAIT_LISTREQUEST_SENDDONE         = 0x06
    SIX_STATE_WAIT_CLEARREQUEST_SENDDONE        = 0x07
    # waiting for response from the neighbor
    SIX_STATE_WAIT_ADDRESPONSE                  = 0x08
    SIX_STATE_WAIT_DELETERESPONSE               = 0x09
    SIX_STATE_WAIT_RELOCATERESPONSE             = 0x0a
    SIX_STATE_WAIT_COUNTRESPONSE                = 0x0b
    SIX_STATE_WAIT_LISTRESPONSE                 = 0x0c
    SIX_STATE_WAIT_CLEARRESPONSE                = 0x0d
    #response
    SIX_STATE_REQUEST_ADD_RECEIVED              = 0x0e
    SIX_STATE_WAIT_ADD_RESPONSE_SENDDONE        = 0x0f
    SIX_STATE_REQUEST_DELETE_RECEIVED           = 0x10
    SIX_STATE_WAIT_DELETE_RESPONSE_SENDDONE     = 0x11
    
    #=== 6top commands
    IANA_6TOP_CMD_ADD                           = 0x01 # add one or more cells
    IANA_6TOP_CMD_DELETE                        = 0x02 # delete one or more cells
    IANA_6TOP_CMD_RELOCATE                      = 0x03 # relocate one or more cells
    IANA_6TOP_CMD_COUNT                         = 0x04 # count scheduled cells
    IANA_6TOP_CMD_LIST                          = 0x05 # list the scheduled cells
    IANA_6TOP_CMD_CLEAR                         = 0x06 # clear all cells

    #=== 6P return code
    IANA_6TOP_RC_SUCCESS                        = 0x00 # operation succeeded
    IANA_6TOP_RC_ERROR                          = 0x01 # generic error
    IANA_6TOP_RC_EOL                            = 0x02 # end of list
    IANA_6TOP_RC_RESET                          = 0x03 # critical error, reset
    IANA_6TOP_RC_VER_ERR                        = 0x04 # unsupported 6P version
    IANA_6TOP_RC_SFID_ERR                       = 0x05 # unsupported SFID
    IANA_6TOP_RC_GEN_ERR                        = 0x06 # wrong schedule generation
    IANA_6TOP_RC_BUSY                           = 0x07 # busy
    IANA_6TOP_RC_NORES                          = 0x08 # not enough resources
    IANA_6TOP_RC_CELLLIST_ERR                   = 0x09 # cellList error
    
    #=== 6P default timeout value
    SIXTOP_TIMEOUT                     = 15
    
    #=== otf
    OTF_TRAFFIC_SMOOTHING              = 0.5
    #=== 6top
    #=== tsch
    TSCH_QUEUE_SIZE                    = 10
    TSCH_MAXTXRETRIES                  = 5
    TSCH_MIN_BACKOFF_EXPONENT          = 2
    TSCH_MAX_BACKOFF_EXPONENT          = 4
    #=== radio
    RADIO_MAXDRIFT                     = 30 # in ppm
    #=== battery
    # see A Realistic Energy Consumption Model for TSCH Networks.
    # Xavier Vilajosana, Qin Wang, Fabien Chraim, Thomas Watteyne, Tengfei
    # Chang, Kris Pister. IEEE Sensors, Vol. 14, No. 2, February 2014.
    CHARGE_Idle_uC                     = 6.4
    CHARGE_TxDataRxAck_uC              = 54.5
    CHARGE_TxData_uC                   = 49.5
    CHARGE_RxDataTxAck_uC              = 32.6
    CHARGE_RxData_uC                   = 22.6
    CHARGE_IdleNotSync_uC              = 45.0
    
    BROADCAST_ADDRESS                  = 0xffff
    
    def __init__(self,id):
        
        # store params
        self.id                        = id
        # local variables
        self.dataLock                  = threading.RLock()
        
        self.engine                    = SimEngine.SimEngine()
        self.settings                  = SimSettings.SimSettings()
        self.propagation               = Propagation.Propagation()
        
        # join process
        self.isJoined                  = False if self.settings.withJoin else True
        self.joinRetransmissionPayload = 0
        self.joinAsn                   = 0                     # ASN at the time node successfully joined
        self.firstBeaconAsn            = 0
        #bootstrap   ->   nodes have a parent and at least 1 dedicated cell                       
        self.isBootstrapped               = False if self.settings.withBootstrap else True
        self.firstIsBootstrapped       = 0                      # ASN at the time node becomes ready (have at least 1 dedicated cell)
        # app
        self.pkPeriod                  = self.settings.pkPeriod        
        # role
        self.dagRoot                   = False
        # rpl
        self.rank                      = None
        self.dagRank                   = None
        self.parentSet                 = []
        self.preferredParent           = None
        self.rplRxDIO                  = {}                    # indexed by neighbor, contains int
        self.neighborRank              = {}                    # indexed by neighbor
        self.neighborDagRank           = {}                    # indexed by neighbor
        self.trafficPortionPerParent   = {}                    # indexed by parent, portion of outgoing traffic
        self.dagRootAddress            = None
        # otf
        self.asnOTFevent               = None
        self.otfHousekeepingPeriod     = self.settings.otfHousekeepingPeriod
        self.timeBetweenOTFevents      = []
        self.inTraffic                 = {}                    # indexed by neighbor
        self.inTrafficMovingAve        = {}                    # indexed by neighbor
        # 6top
        self.numCellsToNeighbors       = {}                    # indexed by neighbor, contains int
        self.numCellsFromNeighbors     = {}                    # indexed by neighbor, contains int
        # changing this threshold the detection of a bad cell can be
        # tuned, if as higher the slower to detect a wrong cell but the more prone
        # to avoid churn as lower the faster but with some chances to introduces
        # churn due to unstable medium
        self.sixtopPdrThreshold           = self.settings.sixtopPdrThreshold
        self.sixtopHousekeepingPeriod  = self.settings.sixtopHousekeepingPeriod

        # 6top protocol
        self.sixtopStates               = {}
        # a dictionary that stores the different 6p states for each neighbor
        # in each entry the key is the neighbor.id
        # the values are:
        #                 'state', used for tracking the transaction state for each neighbor
        #                 'responseCode', used in the receiver node to act differently when a responseACK is received
        #                 'blockedCells', candidates cell pending for an operation

        # tsch
        self.txQueue                   = []
        self.pktToSend                 = None
        self.schedule                  = {}                    # indexed by ts, contains cell
        self.waitingFor                = None
        self.timeCorrectedSlot         = None
        self.isSync                    = False
        self.firstEB                   = True                  # flag to indicate first received enhanced beacon
        self._tsch_resetBackoff()
        # radio
        self.txPower                   = 0                     # dBm
        self.antennaGain               = 0                     # dBi
        self.minRssi                   = self.settings.minRssi # dBm
        self.noisepower                = -105                  # dBm
        self.drift                     = random.uniform(-self.RADIO_MAXDRIFT, self.RADIO_MAXDRIFT)
        # wireless
        self.RSSI                      = {}                    # indexed by neighbor
        self.PDR                       = {}                    # indexed by neighbor
        # location
        # battery
        self.chargeConsumed            = 0
        
        # stats
        self._stats_resetMoteStats()
        self._stats_resetQueueStats()
        self._stats_resetLatencyStats()
        self._stats_resetHopsStats()
        self._stats_resetRadioStats()
    
        self.pktGen=0                                      #DATA packets generated during the experiment (without warming up)
        self.pktReceived=0                                 #DATA packets received during the experiment (without warming up)
        self.pktDropMac=0                                  #DATA packets drop during the experiment (without warming up) due to MAC retransmissions
        self.pktDropQueue=0                                #DATA packets received during the experiment (without warming up) to Queue full
        self.tsSixTopReqRecv           = {}                #for every neighbor, it tracks the 6top transaction latency
        self.avgsixtopLatency          = []                # it tracks the average 6P transaction latency in a given frame
    #======================== stack ===========================================
    
    #===== role
    
    def role_setDagRoot(self):
        self.dagRoot              = True
        self.rank                 = 0
        self.dagRank              = 0
        self.packetLatencies      = [] # in slots
        self.packetHops           = []
        self.parents              = {} # dictionary containing parents of each node from whom DAG root received a DAO
        self.isJoined             = True
        self.isSync               = True
        self.isBootstrapped          = True

        # imprint DAG root's ID at each mote
        for mote in self.engine.motes:
            mote.dagRootAddress = self
    
    # ===== join process
    def join_scheduleJoinProcess(self):
        delay = self.settings.slotDuration + self.settings.joinAttemptTimeout * random.random()

        # schedule
        self.engine.scheduleIn(
            delay=delay,
            cb=self.join_initiateJoinProcess,
            uniqueTag=(self.id, '_join_action_initiateJoinProcess'),
            priority=2,
        )

    def join_setJoined(self):
        assert self.settings.withJoin
        if not self.isJoined:
            self.isJoined = True
            self._tsch_schedule_sendEB(firstEB=True)
            self.joinAsn  = self.engine.getAsn()
            #start scheduling packets once you have joined
            self._app_schedule_sendSinglePacket(firstPacket=True)
            # log
            self._log(
                self.INFO,
                "[join] Mote joined",
            )

            #if there is not bootstrap phase, check if end of simulation has to be scheduled
            if not self.settings.withBootstrap:
                # check if all motes have joined, if so end the simulation
                if all(mote.isJoined == True for mote in self.engine.motes):
                    if self.settings.numCyclesPerRun!=0:
                        #experiment time in ASNs
                        simTime=self.settings.numCyclesPerRun*self.settings.slotframeLength
                        #offset until the end of the current cycle
                        offset=self.settings.slotframeLength-(self.engine.asn%self.settings.slotframeLength)
                        #experiment time + offset
                        delay=simTime+offset
                    else:
                        #simulation will finish in the next asn
                        delay=1
                    # end the simulation
                    self.engine.terminateSimulation(delay)
                    #setting init experiment
                    self.engine.asnInitExperiment=self.engine.asn+self.settings.slotframeLength-(self.engine.asn%self.settings.slotframeLength)

    def join_initiateJoinProcess(self):
        if not self.dagRoot:
            if self.preferredParent:
                if not self.isJoined:
                    self.join_sendJoinPacket(token = self.settings.joinNumExchanges - 1, destination=self.dagRootAddress)
            else:#node doesn't have a parent yet, re-scheduling
                self.join_scheduleJoinProcess()

    def join_sendJoinPacket(self, token, destination):
        # send join packet with payload equal to the number of exchanges
        # create new packet
        sourceRoute = []
        if self.dagRoot:
            sourceRoute = self._rpl_getSourceRoute([destination.id])

        if sourceRoute or not self.dagRoot:
            # create new packet
            newPacket = {
                'asn': self.engine.getAsn(),
                'type': self.APP_TYPE_JOIN,
                'code': None,
                'payload': token,
                'retriesLeft': self.TSCH_MAXTXRETRIES,
                'srcIp': self,  # DAG root
                'dstIp': destination,
                'sourceRoute': sourceRoute
            }

            # enqueue packet in TSCH queue
            isEnqueued = self._tsch_enqueue(newPacket)

            if isEnqueued:
                # increment traffic
                self._otf_incrementIncomingTraffic(self)
            else:
                # update mote stats
                self._stats_incrementMoteStats('droppedFailedEnqueue')

            # save last token sent
            self.joinRetransmissionPayload = token

            # schedule retransmission
            if not self.dagRoot:
                self.engine.scheduleIn(
                    delay=self.settings.slotDuration + self.settings.joinAttemptTimeout,
                    cb=self.join_retransmitJoinPacket,
                    uniqueTag=(self.id, '_join_action_retransmission'),
                    priority=2,
                )

    def join_retransmitJoinPacket(self):
        if not self.dagRoot and not self.isJoined:
            self.join_sendJoinPacket(self.joinRetransmissionPayload, self.dagRootAddress)

    def join_receiveJoinPacket(self, srcIp, payload, timestamp):
        self.engine.removeEvent((self.id, '_join_action_retransmission')) # remove the pending retransmission event

        if payload != 0:
            newPayload = payload - 1

            self.join_sendJoinPacket(token=newPayload, destination=srcIp)

        else:
            self.join_setJoined()

    def join_joinedNeighbors(self):
        return [nei for nei in self._myNeigbors() if nei.isJoined == True]
    #===== application
    
    def _app_schedule_sendSinglePacket(self,firstPacket=False):
        '''
        create an event that is inserted into the simulator engine to send the data according to the traffic
        '''
        
        if not firstPacket:
            # compute random delay
            delay            = self.pkPeriod*(1+random.uniform(-self.settings.pkPeriodVar,self.settings.pkPeriodVar))            
        else:
            # compute initial time within the range of [next asn, next asn+pkPeriod]
            delay            = self.settings.slotDuration + self.pkPeriod*random.random()
            
        assert delay>0    
        
        # schedule
        self.engine.scheduleIn(
            delay            = delay,
            cb               = self._app_action_sendSinglePacket,
            uniqueTag        = (self.id, '_app_action_sendSinglePacket'),
            priority         = 2,
        )
    
    def _app_schedule_sendPacketBurst(self):
        ''' create an event that is inserted into the simulator engine to send a data burst'''
        
        # schedule numPacketsBurst packets at burstTimestamp
        for i in xrange(self.settings.numPacketsBurst):
            self.engine.scheduleIn(
                delay        = self.settings.burstTimestamp,
                cb           = self._app_action_enqueueData,
                uniqueTag    = (self.id, '_app_action_enqueueData_burst1'),
                priority     = 2,
            )
            self.engine.scheduleIn(
                delay        = 3*self.settings.burstTimestamp,
                cb           = self._app_action_enqueueData,
                uniqueTag    = (self.id, '_app_action_enqueueData_burst2'),
                priority     = 2,
            )
    
    def _app_action_sendSinglePacket(self):
        ''' actual send data function. Evaluates queue length too '''
        
        # enqueue data
        self._app_action_enqueueData()
        
        # schedule next _app_action_sendSinglePacket
        self._app_schedule_sendSinglePacket()

    def _app_action_receiveAck(self, srcIp, payload, timestamp):
        assert not self.dagRoot

    def _app_action_receivePacket(self, srcIp, payload, timestamp):
        assert self.dagRoot

        if payload[1] > self.engine.asnInitExperiment and self.engine.asn <= self.engine.asnEndExperiment:
            self.pktReceived+=1

        # update mote stats
        self._stats_incrementMoteStats('appReachesDagroot')

        # calculate end-to-end latency
        self._stats_logLatencyStat(timestamp - payload[1])

        # log the number of hops
        self._stats_logHopsStat(payload[2])

        if self.settings.downwardAcks:  # Downward End-to-end ACKs
            destination = srcIp

            sourceRoute = self._rpl_getSourceRoute([destination.id])


            if sourceRoute: # if DAO was received from this node

                # send an ACK
                # create new packet
                newPacket = {
                    'asn': self.engine.getAsn(),
                    'type': self.APP_TYPE_ACK,
                    'code': None,
                    'payload': [],
                    'retriesLeft': self.TSCH_MAXTXRETRIES,
                    'srcIp': self, # DAG root
                    'dstIp': destination,
                    'sourceRoute' : sourceRoute

                }

                # enqueue packet in TSCH queue
                isEnqueued = self._tsch_enqueue(newPacket)
          
    def _app_action_enqueueData(self):
        ''' enqueue data packet into stack '''
        
        # only start sending data if I have some TX cells
        if self.getTxCells():
            
            if self.isBootstrapped==False:        
                self.isBootstrapped=True
                self.firstIsBootstrapped=self.engine.asn
                # check if all motes are ready, if so, schedule end the simulation
                if all(mote.isBootstrapped == True for mote in self.engine.motes):
                    if self.settings.numCyclesPerRun!=0:
                        #experiment time in ASNs
                        simTime=self.settings.numCyclesPerRun*self.settings.slotframeLength
                        #offset until the end of the current cycle
                        offset=self.settings.slotframeLength-(self.engine.asn%self.settings.slotframeLength)
                        #experiment time + offset
                        delay=simTime+offset
                    else:
                        #simulation will finish in the next asn
                        delay=1
                    # end the simulation
                    self.engine.terminateSimulation(delay)
                    #setting init experiment
                    self.engine.asnInitExperiment=self.engine.asn+self.settings.slotframeLength-(self.engine.asn%self.settings.slotframeLength)

            if self.engine.asn > self.engine.asnInitExperiment:
                self.pktGen+=1

            # create new packet
            newPacket = {
                'asn':            self.engine.getAsn(),
                'type':           self.APP_TYPE_DATA,
                'code':           None,
                'payload':        [self.id,self.engine.getAsn(),1], # the payload is used for latency and number of hops calculation
                'retriesLeft':    self.TSCH_MAXTXRETRIES,
                'srcIp':          self,
                'dstIp':          self.dagRootAddress,
                'sourceRoute':    [],
            }
            
            # update mote stats
            self._stats_incrementMoteStats('appGenerated')
            
            # enqueue packet in TSCH queue
            isEnqueued = self._tsch_enqueue(newPacket)
            
            if isEnqueued:
                # increment traffic
                self._otf_incrementIncomingTraffic(self)
            else:
                if self.engine.asn > self.engine.asnInitExperiment:
                    self.pktDropQueue+=1
                # update mote stats
                self._stats_incrementMoteStats('droppedDataFailedEnqueue')

    def _tsch_action_enqueueEB(self):
        ''' enqueue EB packet into stack '''

        # only start sending EBs if I have Shared cells
        if self.getSharedCells():

            # create new packet
            newPacket = {
                'asn': self.engine.getAsn(),
                'type': self.TSCH_TYPE_EB,
                'code': None,
                'payload': [self.dagRank],  # the payload is the rpl rank
                'retriesLeft': 1,  # do not retransmit broadcast
                'srcIp': self,
                'dstIp': self.BROADCAST_ADDRESS,
                'sourceRoute': []
            }

            # enqueue packet in TSCH queue
            isEnqueued = self._tsch_enqueue(newPacket)

            if not isEnqueued:
                # update mote stats
                self._stats_incrementMoteStats('droppedFailedEnqueue')

    def _tsch_schedule_sendEB(self, firstEB=False):

        with self.dataLock:

            asn = self.engine.getAsn()

            if self.settings.bayesianBroadcast:
                futureAsn = int(self.settings.slotframeLength)
            else:
                futureAsn = int(math.ceil(
                    random.uniform(0.8 * self.settings.beaconPeriod, 1.2 * self.settings.beaconPeriod) / (
                    self.settings.slotDuration)))

            # schedule at start of next cycle
            self.engine.scheduleAtAsn(
                asn=asn + futureAsn,
                cb=self._tsch_action_sendEB,
                uniqueTag=(self.id, '_tsch_action_sendEB'),
                priority=3,
            )

    def _tsch_action_sendEB(self):

        with self.dataLock:

            if self.settings.bayesianBroadcast:
                beaconProb = float(self.settings.beaconProbability) / float(len(self.join_joinedNeighbors())) if len(self.join_joinedNeighbors()) else float(self.settings.beaconProbability)
                sendBeacon = True if random.random() < beaconProb else False
            else:
                sendBeacon = True
            if self.preferredParent or self.dagRoot:
                if self.isJoined or not self.settings.withJoin:
                    if sendBeacon:
                        self._tsch_action_enqueueEB()
                        self._stats_incrementMoteStats('tschTxEB')

            self._tsch_schedule_sendEB()  # schedule next EB

    def _tsch_action_receiveEB(self, type, smac, payload):

        if self.dagRoot:
            return

        # got an EB, increment stats
        self._stats_incrementMoteStats('tschRxEB')
        if self.firstEB and not self.isSync:
            self.firstBeaconAsn = self.engine.getAsn()
            self.firstEB = False
            self.isSync = True
            self._init_stack()

    #===== rpl

    def _rpl_action_enqueueDIO(self):
        ''' enqueue DIO packet into stack '''
        
        # only start sending data if I have Shared cells
        if self.getSharedCells():
            
            # create new packet
            newPacket = {
                'asn':            self.engine.getAsn(),
                'type':           self.RPL_TYPE_DIO,
                'code':           None,
                'payload':        [self.rank], # the payload is the rpl rank
                'retriesLeft':    1, # do not retransmit broadcast
                'srcIp':          self,
                'dstIp':          self.BROADCAST_ADDRESS,
                'sourceRoute':    []
            }
            
            # enqueue packet in TSCH queue
            isEnqueued = self._tsch_enqueue(newPacket)
            
            if not isEnqueued:
                # update mote stats
                self._stats_incrementMoteStats('droppedFailedEnqueue')

    def _rpl_action_enqueueDAO(self):
        ''' enqueue DAO packet into stack '''

        # only start sending data if I have Shared cells
        if self.getSharedCells() or self.getTxCells():

            # create new packet
            newPacket = {
                'asn': self.engine.getAsn(),
                'type': self.RPL_TYPE_DAO,
                'code': None,
                'payload': [self.id, self.preferredParent.id],
                'retriesLeft': self.TSCH_MAXTXRETRIES,
                'srcIp': self,
                'dstIp': self.dagRootAddress,
                'sourceRoute': []
            }

            # enqueue packet in TSCH queue
            isEnqueued = self._tsch_enqueue(newPacket)

            if not isEnqueued:
                # update mote stats
                self._stats_incrementMoteStats('droppedFailedEnqueue')

    
    def _rpl_schedule_sendDIO(self,firstDIO=False):
        
        with self.dataLock:

            asn    = self.engine.getAsn()

            if self.settings.bayesianBroadcast:
                futureAsn = int(self.settings.slotframeLength)
            else:
                futureAsn = int(math.ceil(
                    random.uniform(0.8 * self.settings.dioPeriod, 1.2 * self.settings.dioPeriod) / (self.settings.slotDuration)))

            # schedule at start of next cycle
            self.engine.scheduleAtAsn(
                asn         = asn + futureAsn,
                cb          = self._rpl_action_sendDIO,
                uniqueTag   = (self.id,'_rpl_action_sendDIO'),
                priority    = 3,
            )

    def _rpl_schedule_sendDAO(self, firstDAO=False):

        with self.dataLock:

            asn = self.engine.getAsn()

            if not firstDAO:
                futureAsn = int(math.ceil(
                    random.uniform(0.8 * self.settings.daoPeriod, 1.2 * self.settings.daoPeriod) / (
                    self.settings.slotDuration)))
            else:
                futureAsn = 1

            # schedule at start of next cycle
            self.engine.scheduleAtAsn(
                asn=asn + futureAsn,
                cb=self._rpl_action_sendDAO,
                uniqueTag=(self.id, '_rpl_action_sendDAO'),
                priority=3,
            )

    def _rpl_action_receiveDIO(self,type,smac,payload):

        with self.dataLock:

            if self.dagRoot:
                return

            if not self.isSync:
                return

            # update my mote stats
            self._stats_incrementMoteStats('rplRxDIO')

            sender = smac

            rank = payload[0]

            # don't update poor link
            if self._rpl_calcRankIncrease(sender)>self.RPL_MAX_RANK_INCREASE:
                return

            # update rank/DAGrank with sender
            self.neighborDagRank[sender]    = rank / self.RPL_MIN_HOP_RANK_INCREASE
            self.neighborRank[sender]       = rank

            # update number of DIOs received from sender
            if sender not in self.rplRxDIO:
                    self.rplRxDIO[sender]   = 0
            self.rplRxDIO[sender]          += 1

            # housekeeping
            self._rpl_housekeeping()

            # update time correction
            if self.preferredParent == sender:
                asn                         = self.engine.getAsn()
                self.timeCorrectedSlot      = asn

    def _rpl_action_receiveDAO(self, type, smac, payload):
        with self.dataLock:

            assert self.dagRoot

            self._stats_incrementMoteStats('rplRxDAO')

            self.parents.update({tuple([payload[0]]) : [[payload[1]]]})
    def _rpl_action_sendDIO(self):
        
        with self.dataLock:

            if self.settings.bayesianBroadcast:
                dioProb = float(self.settings.dioProbability) / float(len(self.join_joinedNeighbors())) if len(self.join_joinedNeighbors()) else float(self.settings.dioProbability)
                sendDio = True if random.random() < dioProb else False
            else:
                sendDio = True

            if self.preferredParent or self.dagRoot:
                if self.isJoined or not self.settings.withJoin:
                    if sendDio:
                        self._rpl_action_enqueueDIO()
                        self._stats_incrementMoteStats('rplTxDIO')
            
            self._rpl_schedule_sendDIO() # schedule next DIO

    def _rpl_action_sendDAO(self):

        with self.dataLock:
            if self.preferredParent and not self.dagRoot:
                self._rpl_action_enqueueDAO()
                self._stats_incrementMoteStats('rplTxDAO')

            self._rpl_schedule_sendDAO()  # schedule next DIO

    def _rpl_housekeeping(self):
        with self.dataLock:
            
            #===
            # refresh the following parameters:
            # - self.preferredParent
            # - self.rank
            # - self.dagRank
            # - self.parentSet
            
            # calculate my potential rank with each of the motes I have heard a DIO from
            potentialRanks = {}
            for (neighbor,neighborRank) in self.neighborRank.items():
                # calculate the rank increase to that neighbor
                rankIncrease = self._rpl_calcRankIncrease(neighbor)
                if rankIncrease!=None and rankIncrease<=min([self.RPL_MAX_RANK_INCREASE, self.RPL_MAX_TOTAL_RANK-neighborRank]):                
                    # record this potential rank
                    potentialRanks[neighbor] = neighborRank+rankIncrease
            
            # sort potential ranks
            sorted_potentialRanks = sorted(potentialRanks.iteritems(), key=lambda x:x[1])
            
            # switch parents only when rank difference is large enough
            for i in range(1,len(sorted_potentialRanks)):
                if sorted_potentialRanks[i][0] in self.parentSet:
                    # compare the selected current parent with motes who have lower potential ranks 
                    # and who are not in the current parent set 
                    for j in range(i):                    
                        if sorted_potentialRanks[j][0] not in self.parentSet:
                            if sorted_potentialRanks[i][1]-sorted_potentialRanks[j][1]<self.RPL_PARENT_SWITCH_THRESHOLD:
                                mote_rank = sorted_potentialRanks.pop(i)
                                sorted_potentialRanks.insert(j,mote_rank)
                                break
                    
            # pick my preferred parent and resulting rank
            if sorted_potentialRanks:
                oldParentSet = set([parent.id for parent in self.parentSet])
                
                (newPreferredParent,newrank) = sorted_potentialRanks[0]
                
                # compare a current preferred parent with new one
                if self.preferredParent and newPreferredParent!=self.preferredParent:
                    for (mote,rank) in sorted_potentialRanks[:self.RPL_PARENT_SET_SIZE]:
                        
                        if mote == self.preferredParent:                      
                            # switch preferred parent only when rank difference is large enough
                            if rank-newrank<self.RPL_PARENT_SWITCH_THRESHOLD:
                                (newPreferredParent,newrank) = (mote,rank)
                            
                    # update mote stats
                    self._stats_incrementMoteStats('rplChurnPrefParent')
                    # log
                    self._log(
                        self.INFO,
                        "[rpl] churn: preferredParent {0}->{1}",
                        (self.preferredParent.id,newPreferredParent.id),
                    )
                
                # update mote stats
                if self.rank and newrank!=self.rank:
                    self._stats_incrementMoteStats('rplChurnRank')
                    # log
                    self._log(
                        self.INFO,
                        "[rpl] churn: rank {0}->{1}",
                        (self.rank,newrank),
                    )
                
                # store new preferred parent and rank
                (self.preferredParent,self.rank) = (newPreferredParent,newrank)
                
                # calculate DAGrank
                self.dagRank = int(self.rank/self.RPL_MIN_HOP_RANK_INCREASE)
            
                # pick my parent set
                self.parentSet = [n for (n,_) in sorted_potentialRanks if self.neighborRank[n]<self.rank][:self.RPL_PARENT_SET_SIZE]
                assert self.preferredParent in self.parentSet
                
                if oldParentSet!=set([parent.id for parent in self.parentSet]):
                    self._stats_incrementMoteStats('rplChurnParentSet')
                
            #===
            # refresh the following parameters:
            # - self.trafficPortionPerParent
            
            etxs        = dict([(p, 1.0/(self.neighborRank[p]+self._rpl_calcRankIncrease(p))) for p in self.parentSet])
            sumEtxs     = float(sum(etxs.values()))
            self.trafficPortionPerParent = dict([(p, etxs[p]/sumEtxs) for p in self.parentSet])
            
            # remove TX cells to neighbor who are not in parent set
            for neighbor in self.numCellsToNeighbors.keys():
                if neighbor not in self.parentSet:
                
                    # log
                    self._log(
                        self.INFO,
                        "[otf] removing cell to {0}, since not in parentSet {1}",
                        (neighbor.id,[p.id for p in self.parentSet]),
                    )
                    
                    tsList=[ts for ts, cell in self.schedule.iteritems() if cell['neighbor']==neighbor and cell['dir']==self.DIR_TX]
                    if tsList:
                        self._sixtop_cell_deletion_sender(neighbor,tsList)
    
    def _rpl_calcRankIncrease(self, neighbor):
        
        with self.dataLock:
            
            # estimate the ETX to that neighbor
            etx = self._estimateETX(neighbor)
            
            # return if that failed
            if not etx:
                return
            
            # per draft-ietf-6tisch-minimal, rank increase is (3*ETX-2)*RPL_MIN_HOP_RANK_INCREASE
            return int(((3*etx) - 2)*self.RPL_MIN_HOP_RANK_INCREASE)

    def _rpl_getSourceRoute(self, destAddr):
        '''
        Retrieve the source route to a given mote.

        :param destAddr: [in] The EUI64 address of the final destination.

        :returns: The source route, a list of EUI64 address, ordered from
            destination to source.
        '''

        sourceRoute = []
        with self.dataLock:
                parents = self.parents
                self._rpl_getSourceRoute_internal(destAddr, sourceRoute, parents)

        if sourceRoute:
            sourceRoute.pop()

        return sourceRoute

    def _rpl_getSourceRoute_internal(self, destAddr, sourceRoute, parents):

        if not destAddr:
            # no more parents
            return

        if not parents.get(tuple(destAddr)):
            # this node does not have a list of parents
            return

        # first time add destination address
        if destAddr not in sourceRoute:
            sourceRoute += [destAddr]

        # pick a parent
        parent = parents.get(tuple(destAddr))[0]

        # avoid loops
        if parent not in sourceRoute:
            sourceRoute += [parent]

            # add non empty parents recursively
            nextparent = self._rpl_getSourceRoute_internal(parent, sourceRoute, parents)

            if nextparent:
                sourceRoute += [nextparent]

    def _rpl_addNextHop(self, packet):
        assert self != packet['dstIp']

        if not (self.preferredParent or self.dagRoot):
            return False

        nextHop = None

        if packet['dstIp'] == self.BROADCAST_ADDRESS:
            nextHop = self._myNeigbors()
        # 6Top packet. don't send to the parent necessarily. Send it directly to your neighbor (1 hop)
        elif packet['type'] == self.IANA_6TOP_TYPE_REQUEST or packet['type'] == self.IANA_6TOP_TYPE_RESPONSE:
            nextHop = [packet['dstIp']]
        elif packet['dstIp'] == self.dagRootAddress:  # upward packet
            nextHop = [self.preferredParent]
        elif packet['sourceRoute']:                   # downward packet with source route info filled correctly
            nextHopId = packet['sourceRoute'].pop()
            for nei in self._myNeigbors():
                if [nei.id] == nextHopId:
                    nextHop = [nei]
        elif packet['dstIp'] in self._myNeigbors():   #used for 1hop packets, such as 6top messages. This has to be the last one, since some neighbours can have very low PDR
            nextHop = [packet['dstIp']]

        packet['nextHop'] = nextHop
        return True if nextHop else False
#===== otf
    
    def _otf_schedule_housekeeping(self):
        
        self.engine.scheduleIn(
            delay       = self.otfHousekeepingPeriod*(0.9+0.2*random.random()),
            cb          = self._otf_action_housekeeping,
            uniqueTag   = (self.id,'_otf_action_housekeeping'),
            priority    = 4,
        )
    
    def _otf_action_housekeeping(self):
        '''
        OTF algorithm: decides when to add/delete cells.
        '''
        
        with self.dataLock:
            
            if not self.dagRoot:
                #if node does not have parent, or has not joined or is not sync, do not perform OTF
                if self.preferredParent==None or self.isJoined==False or self.isSync==False:
                    self._otf_schedule_housekeeping()
                    return

            # calculate the "moving average" incoming traffic, in pkts since last cycle, per neighbor
                
            # collect all neighbors I have RX cells to
            rxNeighbors = [cell['neighbor'] for (ts,cell) in self.schedule.items() if cell['dir']==self.DIR_RX]
            
            # remove duplicates
            rxNeighbors = list(set(rxNeighbors))
            rxNeighbors = sorted(rxNeighbors, key=lambda x: x.id, reverse=True)
            
            # reset inTrafficMovingAve                
            neighbors = self.inTrafficMovingAve.keys()
            for neighbor in neighbors:
                if neighbor not in rxNeighbors:
                    del self.inTrafficMovingAve[neighbor]
            
            # set inTrafficMovingAve 
            for neighborOrMe in rxNeighbors+[self]:
                if neighborOrMe in self.inTrafficMovingAve:
                    newTraffic   = 0
                    newTraffic  += self.inTraffic[neighborOrMe]*self.OTF_TRAFFIC_SMOOTHING               # new
                    newTraffic  += self.inTrafficMovingAve[neighborOrMe]*(1-self.OTF_TRAFFIC_SMOOTHING)  # old
                    self.inTrafficMovingAve[neighborOrMe] = newTraffic
                elif self.inTraffic[neighborOrMe] != 0:
                    self.inTrafficMovingAve[neighborOrMe] = self.inTraffic[neighborOrMe]
            
            # reset the incoming traffic statistics, so they can build up until next housekeeping
            self._otf_resetInboundTrafficCounters()
            
            # calculate my total generated traffic, in pkt/s
            genTraffic       = 0
            # generated/relayed by me
            for neighborOrMe in self.inTrafficMovingAve:
                genTraffic  += self.inTrafficMovingAve[neighborOrMe]/self.otfHousekeepingPeriod
            # convert to pkts/cycle
            genTraffic      *= self.settings.slotframeLength*self.settings.slotDuration
            
            remainingPortion = 0.0
            parent_portion   = self.trafficPortionPerParent.items()
            # sort list so that the parent assigned larger traffic can be checked first
            sorted_parent_portion = sorted(parent_portion, key = lambda x: x[1], reverse=True)
            
            # split genTraffic across parents, trigger 6top to add/delete cells accordingly
            for (parent,portion) in sorted_parent_portion:
                
                # if some portion is remaining, this is added to this parent
                if remainingPortion!=0.0:
                    portion                               += remainingPortion
                    remainingPortion                       = 0.0
                    self.trafficPortionPerParent[parent]   = portion
                    
                # calculate required number of cells to that parent
                etx = self._estimateETX(parent)
                if etx>self.RPL_MAX_ETX: # cap ETX
                    etx  = self.RPL_MAX_ETX
                reqCells      = int(math.ceil(portion*genTraffic*etx))
                
                # calculate the OTF threshold
                threshold     = int(math.ceil(portion*self.settings.otfThreshold))
                
                # measure how many cells I have now to that parent
                nowCells      = self.numCellsToNeighbors.get(parent,0)
                
                if nowCells==0 or nowCells<reqCells:
                    # I don't have enough cells
                    
                    # calculate how many to add
                    if reqCells>0:
                        # according to traffic
                        
                        numCellsToAdd = reqCells-nowCells+(threshold+1)/2
                    else:
                        # but at least one cell
                        
                        numCellsToAdd = 1
                    
                    # log
                    self._log(
                        self.INFO,
                        "[otf] not enough cells to {0}: have {1}, need {2}, add {3}",
                        (parent.id,nowCells,reqCells,numCellsToAdd),
                    )
                    
                    # update mote stats
                    self._stats_incrementMoteStats('otfAdd')
                    
                    # have 6top add cells
                    self._sixtop_cell_reservation_request(parent,numCellsToAdd)
                    
                    # measure how many cells I have now to that parent
                    nowCells     = self.numCellsToNeighbors.get(parent,0)
                    
                    # store handled portion and remaining portion
                    if nowCells<reqCells:
                        handledPortion   = (float(nowCells)/etx)/genTraffic
                        remainingPortion = portion - handledPortion
                        self.trafficPortionPerParent[parent] = handledPortion
                    
                    # remember OTF triggered
                    otfTriggered = True
                
                elif reqCells<nowCells-threshold:
                    # I have too many cells
                    
                    # calculate how many to remove
                    numCellsToRemove = nowCells-reqCells
                    
                    if reqCells==0:        #I want always there is at least 1 cell available
                        numCellsToRemove=numCellsToRemove-self.settings.otfThreshold-1
                    
                    # have 6top remove cells
                    if numCellsToRemove > 0:
                    
                        # log
                        self._log(
                            self.INFO,
                            "[otf] too many cells to {0}:  have {1}, need {2}, remove {3}",
                            (parent.id,nowCells,reqCells,numCellsToRemove),
                        )
                    
                        self._sixtop_removeCells(parent,numCellsToRemove)
                        # update mote stats
                        self._stats_incrementMoteStats('otfRemove')

                        # remember OTF triggered
                        otfTriggered = True
                    else:
                        otfTriggered = False

                else:
                    # nothing to do
                    
                    # remember OTF did NOT trigger
                    otfTriggered = False
                
                # maintain stats
                if otfTriggered:
                    now = self.engine.getAsn()
                    if not self.asnOTFevent:
                        assert not self.timeBetweenOTFevents
                    else:
                        self.timeBetweenOTFevents += [now-self.asnOTFevent]
                    self.asnOTFevent = now
            
            # schedule next housekeeping
            self._otf_schedule_housekeeping()
    
    def _otf_resetInboundTrafficCounters(self):
        with self.dataLock:
            for neighbor in self._myNeigbors()+[self]:
                self.inTraffic[neighbor] = 0
    
    def _otf_incrementIncomingTraffic(self,neighbor):
        with self.dataLock:
            self.inTraffic[neighbor] += 1
    
    #===== 6top
    
    def _sixtop_timer_fired(self):
        found = False
        for n in self.sixtopStates.keys():
            if 'tx' in self.sixtopStates[n] and 'timer' in self.sixtopStates[n]['tx'] and self.sixtopStates[n]['tx']['timer']['asn'] == self.engine.getAsn(): # if it is this ASN, we have the correct state and we have to abort it
                self.sixtopStates[n]['tx']['state'] = self.SIX_STATE_IDLE # put back to IDLE
                self.sixtopStates[n]['tx']['blockedCells']=[] # transaction gets aborted, so also delete the blocked cells
                del self.sixtopStates[n]['tx']['timer']
                found = True
                # log
                self._log(
                    self.INFO,
                    "[6top] fired timer on mote {0} for neighbor {1}.",
                    (self.id, n),
                )

        if not found: # if we did not find it, assert
            assert False

    def _sixtop_schedule_housekeeping(self):
        
        self.engine.scheduleIn(
            delay       = self.sixtopHousekeepingPeriod*(0.9+0.2*random.random()),
            cb          = self._sixtop_action_housekeeping,
            uniqueTag   = (self.id,'_sixtop_action_housekeeping'),
            priority    = 5,
        )
    
    def _sixtop_action_housekeeping(self):
        '''
        For each neighbor I have TX cells to, relocate cells if needed.
        '''
        
        #=== tx-triggered housekeeping 
        
        # collect all neighbors I have TX cells to
        txNeighbors = [cell['neighbor'] for (ts,cell) in self.schedule.items() if cell['dir']==self.DIR_TX]
        
        # remove duplicates
        txNeighbors = list(set(txNeighbors))
        txNeighbors = sorted(txNeighbors, key=lambda x: x.id, reverse=True)

        for neighbor in txNeighbors:
            nowCells = self.numCellsToNeighbors.get(neighbor,0)
            assert nowCells == len([t for (t,c) in self.schedule.items() if c['dir']==self.DIR_TX and c['neighbor']==neighbor])
        
        # do some housekeeping for each neighbor
        for neighbor in txNeighbors:
            self._sixtop_txhousekeeping_per_neighbor(neighbor)
        
        #=== rx-triggered housekeeping 
        
        # collect neighbors from which I have RX cells that is detected as collision cell
        rxNeighbors = [cell['neighbor'] for (ts,cell) in self.schedule.items() if cell['dir']==self.DIR_RX and cell['rxDetectedCollision']]
        
        # remove duplicates
        rxNeighbors = list(set(rxNeighbors))
        rxNeighbors = sorted(rxNeighbors, key=lambda x: x.id, reverse=True)
        
        for neighbor in rxNeighbors:
            nowCells = self.numCellsFromNeighbors.get(neighbor,0)
            assert nowCells == len([t for (t,c) in self.schedule.items() if c['dir']==self.DIR_RX and c['neighbor']==neighbor])
        
        # do some housekeeping for each neighbor
        for neighbor in rxNeighbors:
            self._sixtop_rxhousekeeping_per_neighbor(neighbor)
        
        #=== schedule next housekeeping
        
        self._sixtop_schedule_housekeeping()
    
    def _sixtop_txhousekeeping_per_neighbor(self,neighbor):
        '''
        For a particular neighbor, decide to relocate cells if needed.
        '''
        
        #===== step 1. collect statistics
        
        # pdr for each cell
        cell_pdr = []
        for (ts,cell) in self.schedule.items():
            if cell['neighbor']==neighbor and cell['dir']==self.DIR_TX:
                # this is a TX cell to that neighbor
                
                # abort if not enough TX to calculate meaningful PDR
                if cell['numTx']<self.NUM_SUFFICIENT_TX:
                    continue
                
                # calculate pdr for that cell
                recentHistory = cell['history'][-self.NUM_MAX_HISTORY:]
                pdr = float(sum(recentHistory)) / float(len(recentHistory))
                
                # store result
                cell_pdr += [(ts,pdr)]
                
        # pdr for the bundle as a whole
        bundleNumTx     = sum([len(cell['history'][-self.NUM_MAX_HISTORY:]) for cell in self.schedule.values() if cell['neighbor']==neighbor and cell['dir']==self.DIR_TX])
        bundleNumTxAck  = sum([sum(cell['history'][-self.NUM_MAX_HISTORY:]) for cell in self.schedule.values() if cell['neighbor']==neighbor and cell['dir']==self.DIR_TX])
        if bundleNumTx<self.NUM_SUFFICIENT_TX:
            bundlePdr   = None
        else:
            bundlePdr   = float(bundleNumTxAck) / float(bundleNumTx)
        
        #===== step 2. relocate worst cell in bundle, if any
        # this step will identify the cell with the lowest PDR in the bundle.
        # If its PDR is self.sixtopPdrThreshold lower than the average of the bundle
        # this step will move that cell.
        
        relocation = False
        
        if cell_pdr:
            
            # identify the cell with worst pdr, and calculate the average
            
            worst_ts   = None
            worst_pdr  = None
            
            for (ts,pdr) in cell_pdr:
                if worst_pdr==None or pdr<worst_pdr:
                    worst_ts  = ts
                    worst_pdr = pdr
            
            assert worst_ts!=None
            assert worst_pdr!=None
            
            # ave pdr for other cells
            othersNumTx      = sum([len(cell['history'][-self.NUM_MAX_HISTORY:]) for (ts,cell) in self.schedule.items() if cell['neighbor']==neighbor and cell['dir']==self.DIR_TX and ts != worst_ts])
            othersNumTxAck   = sum([sum(cell['history'][-self.NUM_MAX_HISTORY:]) for (ts,cell) in self.schedule.items() if cell['neighbor']==neighbor and cell['dir']==self.DIR_TX and ts != worst_ts])           
            if othersNumTx<self.NUM_SUFFICIENT_TX:
                ave_pdr      = None
            else:
                ave_pdr      = float(othersNumTxAck) / float(othersNumTx)

            # relocate worst cell if "bad enough"
            if ave_pdr and worst_pdr<(ave_pdr/self.sixtopPdrThreshold):
                
                # log
                self._log(
                    self.INFO,
                    "[6top] relocating cell ts {0} to {1} (pdr={2:.3f} significantly worse than others {3})",
                    (worst_ts,neighbor.id,worst_pdr,cell_pdr),
                )
                
                # measure how many cells I have now to that parent
                nowCells = self.numCellsToNeighbors.get(neighbor,0)
                
                # relocate: add new first
                self._sixtop_cell_reservation_request(neighbor,1)
                
                # relocate: remove old only when successfully added 
                if nowCells < self.numCellsToNeighbors.get(neighbor,0):
                    self._sixtop_cell_deletion_sender(neighbor,[worst_ts])
                
                    # update stats
                    self._stats_incrementMoteStats('topTxRelocatedCells')
                
                    # remember I relocated a cell for that bundle
                    relocation = True
        
        #===== step 3. relocate the complete bundle
        # this step only runs if the previous hasn't, and we were able to
        # calculate a bundle PDR.
        # This step verifies that the average PDR for the complete bundle is
        # expected, given the RSSI to that neighbor. If it's lower, this step
        # will move all cells in the bundle.
        
        bundleRelocation = False
        
        if (not relocation) and bundlePdr!=None:
            
            # calculate the theoretical PDR to that neighbor, using the measured RSSI
            rssi            = self.getRSSI(neighbor)
            theoPDR         = Topology.Topology.rssiToPdr(rssi)
            
            # relocate complete bundle if measured RSSI is significantly worse than theoretical
            if bundlePdr<(theoPDR/self.sixtopPdrThreshold):
                for (ts,_) in cell_pdr:
                    
                    # log
                    self._log(
                        self.INFO,
                        "[6top] relocating cell ts {0} to {1} (bundle pdr {2} << theoretical pdr {3})",
                        (ts,neighbor.id,bundlePdr,theoPDR),
                    )
                    
                    # measure how many cells I have now to that parent
                    nowCells = self.numCellsToNeighbors.get(neighbor,0)
                    
                    # relocate: add new first
                    self._sixtop_cell_reservation_request(neighbor,1)

                    # relocate: remove old only when successfully added 
                    if nowCells < self.numCellsToNeighbors.get(neighbor,0):
                        
                        self._sixtop_cell_deletion_sender(neighbor,[ts])
                        
                        bundleRelocation = True
                
                # update stats
                if bundleRelocation:
                    self._stats_incrementMoteStats('topTxRelocatedBundles')
    
    def _sixtop_rxhousekeeping_per_neighbor(self,neighbor):
        '''
        The RX node triggers a relocation when it has heard a packet
        from a neighbor it did not expect ('rxDetectedCollision')
        '''
        
        rxCells = [(ts,cell) for (ts,cell) in self.schedule.items() if cell['dir']==self.DIR_RX and cell['rxDetectedCollision'] and cell['neighbor']==neighbor]
        
        relocation = False
        for ts,cell in rxCells:
            
            # measure how many cells I have now from that child
            nowCells = self.numCellsFromNeighbors.get(neighbor,0)
            
            # relocate: add new first
            self._sixtop_cell_reservation_request(neighbor,1,dir=self.DIR_RX)
            
            # relocate: remove old only when successfully added 
            if nowCells < self.numCellsFromNeighbors.get(neighbor,0):
                neighbor._sixtop_cell_deletion_sender(self,[ts])
                
                # remember I relocated a cell
                relocation = True
                
        if relocation:
            # update stats
            self._stats_incrementMoteStats('topRxRelocatedCells')
    
    def _sixtop_cell_reservation_request(self,neighbor,numCells,dir=DIR_TX):
        ''' tries to reserve numCells cells to a neighbor. '''
        
        with self.dataLock:
            if self.settings.sixtopMessaging:
                if neighbor.id not in self.sixtopStates or (neighbor.id in self.sixtopStates and 'tx' in self.sixtopStates[neighbor.id] and self.sixtopStates[neighbor.id]['tx']['state'] == self.SIX_STATE_IDLE):

                    # if neighbor not yet in states dict, add it
                    if neighbor.id not in self.sixtopStates:
                        self.sixtopStates[neighbor.id] = {}
                    if 'tx' not in self.sixtopStates[neighbor.id]:
                        self.sixtopStates[neighbor.id]['tx'] = {}
                        self.sixtopStates[neighbor.id]['tx']['state'] = self.SIX_STATE_IDLE
                        self.sixtopStates[neighbor.id]['tx']['blockedCells'] = []
                        self.sixtopStates[neighbor.id]['tx']['seqNum']=0

                    #get blocked cells from other 6top operations
                    blockedCells = []
                    for n in self.sixtopStates.keys():
                        if n!=neighbor.id:
                            if 'tx' in self.sixtopStates[n] and len(self.sixtopStates[n]['tx']['blockedCells'])>0:
                                blockedCells+=self.sixtopStates[n]['tx']['blockedCells']
                            if 'rx' in self.sixtopStates[n] and len(self.sixtopStates[n]['rx']['blockedCells'])>0:
                                blockedCells+=self.sixtopStates[n]['rx']['blockedCells']

                    #convert blocked cells into ts
                    tsBlocked=[]
                    if len(blockedCells)>0:
                        for c in blockedCells:
                                tsBlocked.append(c[0])

                    # randomly picking cells (SF0)
                    availableTimeslots    = list(set(range(self.settings.slotframeLength))-set(self.schedule.keys())-set(tsBlocked))
                    random.shuffle(availableTimeslots)
                    cells                 = dict([(ts,random.randint(0,self.settings.numChans-1)) for ts in availableTimeslots[:numCells]])
                    cellList              = [(ts, ch, dir) for (ts, ch) in cells.iteritems()]

                    self._sixtop_enqueue_ADD_REQUEST(neighbor, cellList, numCells,self.sixtopStates[neighbor.id]['tx']['seqNum'])
                else:
                    self._log(
                        self.DEBUG,
                        "[6top] can not send 6top ADD request to {0} because timer still did not fire on mote {1}.",
                        (neighbor.id, self.id),
                    )

            else: 
                cells       = neighbor._sixtop_cell_reservation_response(self,numCells,dir)
                cellList    = []
                for (ts,ch) in cells.iteritems():
                    # log
                    self._log(
                        self.INFO,
                        '[6top] add TX cell ts={0},ch={1} from {2} to {3}',
                        (ts,ch,self.id,neighbor.id),
                    )
                    cellList += [(ts,ch,dir)]
                self._tsch_addCells(neighbor,cellList)

                # update counters
                if dir==self.DIR_TX:
                    if neighbor not in self.numCellsToNeighbors:
                        self.numCellsToNeighbors[neighbor]     = 0
                    self.numCellsToNeighbors[neighbor]        += len(cells)
                else:
                    if neighbor not in self.numCellsFromNeighbors:
                        self.numCellsFromNeighbors[neighbor]   = 0
                    self.numCellsFromNeighbors[neighbor]      += len(cells)

                if len(cells)!=numCells:
                    # log
                    self._log(
                        self.ERROR,
                        '[6top] scheduled {0} cells out of {1} required between motes {2} and {3}',
                        (len(cells),numCells,self.id,neighbor.id),
                    )

    def _sixtop_enqueue_ADD_REQUEST(self, neighbor, cellList, numCells,seq):
        ''' enqueue a new 6P ADD request '''

        self._log(
            self.INFO,
            '[6top] enqueueing a new 6P ADD message cellList={0}, numCells={1} from {2} to {3}',
            (cellList, numCells, self.id, neighbor.id),
        )

        # create new packet
        newPacket = {
            'asn':            self.engine.getAsn(),
            'type':           self.IANA_6TOP_TYPE_REQUEST,
            'code':           self.IANA_6TOP_CMD_ADD,
            'payload':        [cellList, numCells, self.DIR_TX,seq,self.engine.getAsn()],
            'retriesLeft':    self.TSCH_MAXTXRETRIES,
            'srcIp':          self,
            'dstIp':          neighbor, # currently upstream
            'sourceRoute':    [],
        }

        # enqueue packet in TSCH queue
        isEnqueued = self._tsch_enqueue(newPacket)

        if not isEnqueued:
            # update mote stats
            self._stats_incrementMoteStats('droppedFailedEnqueue')
        else:
            # set state to sending request for this neighbor
            self.sixtopStates[neighbor.id]['tx']['state'] = self.SIX_STATE_SENDING_REQUEST
            self.sixtopStates[neighbor.id]['tx']['blockedCells'] = cellList

    def _sixtop_receive_ADD_REQUEST(self, type, smac, payload):
        with self.dataLock:

            neighbor = smac
            cellList = payload[0]
            numCells = payload[1]
            dirNeighbor = payload[2]
            seq=payload[3]

            self.tsSixTopReqRecv[neighbor]=payload[4]        #has the asn of when the req packet was enqueued in the neighbor
            self._stats_incrementMoteStats('rxAddReq')

            if smac.id in self.sixtopStates and 'rx' in self.sixtopStates[smac.id] and self.sixtopStates[smac.id]['rx']['state'] != self.SIX_STATE_IDLE:
                for pkt in self.txQueue:
                    if pkt['type'] == self.IANA_6TOP_TYPE_RESPONSE and pkt['dstIp'].id == smac.id:
                        self.txQueue.remove(pkt)
                        self._log(
                            self.INFO,
                            "[6top] removed a 6TOP_TYPE_RESPONSE packet in the queue of mote {0} to neighbor {1}, because a new TYPE_REQUEST (add) was received.",
                            (self.id, smac.id),
                        )
                returnCode = self.IANA_6TOP_RC_RESET # error, neighbor has to abort transaction
                if smac.id not in self.sixtopStates:
                    self.sixtopStates[smac.id] = {}
                if 'rx' not in self.sixtopStates[smac.id]:
                    self.sixtopStates[smac.id]['rx'] = {}
                    self.sixtopStates[smac.id]['rx']['blockedCells']=[]
                    self.sixtopStates[smac.id]['rx']['seqNum']=0
                self.sixtopStates[smac.id]['rx']['state'] = self.SIX_STATE_REQUEST_ADD_RECEIVED
                self._sixtop_enqueue_RESPONSE(neighbor, [], returnCode, dirNeighbor,seq)
                return

            # go to the correct state
            # set state to receiving request for this neighbor
            if smac.id not in self.sixtopStates:
                self.sixtopStates[smac.id] = {}
            if 'rx' not in self.sixtopStates[smac.id]:
                self.sixtopStates[smac.id]['rx'] = {}
                self.sixtopStates[smac.id]['rx']['blockedCells']=[]
                self.sixtopStates[smac.id]['rx']['seqNum']=0
            self.sixtopStates[smac.id]['rx']['state'] = self.SIX_STATE_REQUEST_ADD_RECEIVED

            # set direction of cells
            if dirNeighbor == self.DIR_TX:
                newDir = self.DIR_RX
            else: 
                newDir = self.DIR_TX

            # cells that will be in the response
            newCellList = []

            #get blocked cells from other 6top operations
            blockedCells = []
            for n in self.sixtopStates.keys():
                if n!=neighbor.id:
                    if 'rx' in self.sixtopStates[n] and len(self.sixtopStates[n]['rx']['blockedCells'])>0:
                        blockedCells+=self.sixtopStates[n]['rx']['blockedCells']
                    if 'tx' in self.sixtopStates[n] and len(self.sixtopStates[n]['tx']['blockedCells'])>0:
                        blockedCells+=self.sixtopStates[n]['tx']['blockedCells']
            #convert blocked cells into ts
            tsBlocked=[]
            if len(blockedCells)>0:
                for c in blockedCells:
                    tsBlocked.append(c[0])

            # available timeslots on this mote
            availableTimeslots = list(set(range(self.settings.slotframeLength))-set(self.schedule.keys())-set(tsBlocked))
            random.shuffle(cellList)
            for (ts, ch, dir) in cellList:
                if len(newCellList) == numCells:
                    break
                if ts in availableTimeslots:
                    newCellList += [(ts, ch, newDir)]

            #  if len(newCellList) < numCells it is considered still a success as long as len(newCellList) is bigger than 0
            if len(newCellList) <= 0:
                returnCode = self.IANA_6TOP_RC_NORES # not enough resources
            else: 
                returnCode = self.IANA_6TOP_RC_SUCCESS # enough resources

            #set blockCells for this 6top operation
            self.sixtopStates[neighbor.id]['rx']['blockedCells'] = newCellList

            #enqueue response
            self._sixtop_enqueue_RESPONSE(neighbor, newCellList, returnCode, newDir,seq)

    def _sixtop_cell_reservation_response(self,neighbor,numCells,dirNeighbor):
        ''' get a response from the neighbor. '''
        
        with self.dataLock:
            
            # set direction of cells
            if dirNeighbor == self.DIR_TX:
                dir = self.DIR_RX
            else:
                dir = self.DIR_TX
                
            availableTimeslots    = list(set(range(self.settings.slotframeLength))-set(neighbor.schedule.keys())-set(self.schedule.keys()))
            random.shuffle(availableTimeslots)
            cells                 = dict([(ts,random.randint(0,self.settings.numChans-1)) for ts in availableTimeslots[:numCells]])
            cellList              = []
            
            for ts, ch in cells.iteritems():
                # log
                self._log(
                    self.INFO,
                    '[6top] add RX cell ts={0},ch={1} from {2} to {3}',
                    (ts,ch,self.id,neighbor.id),
                )
                cellList         += [(ts,ch,dir)]
            self._tsch_addCells(neighbor,cellList)
            
            # update counters
            if dir==self.DIR_TX:
                if neighbor not in self.numCellsToNeighbors:
                    self.numCellsToNeighbors[neighbor]     = 0
                self.numCellsToNeighbors[neighbor]        += len(cells)
            else:
                if neighbor not in self.numCellsFromNeighbors:
                    self.numCellsFromNeighbors[neighbor]   = 0
                self.numCellsFromNeighbors[neighbor]      += len(cells)
            
            return cells
    
    def _sixtop_enqueue_RESPONSE(self, neighbor, cellList, returnCode, dir,seq):
        ''' enqueue a new 6P ADD or DELETE response '''

        self._log(
            self.INFO,
            '[6top] enqueueing a new 6P RESPONSE message cellList={0}, numCells={1}, returnCode={2}, from {3} to {4}',
            (cellList, len(cellList), returnCode, self.id, neighbor.id),
        )
        seq+=1
        # create new packet
        newPacket = {
            'asn':            self.engine.getAsn(),
            'type':           self.IANA_6TOP_TYPE_RESPONSE,
            'code':           returnCode,
            'payload':        [cellList,len(cellList), dir,seq],
            'retriesLeft':    self.TSCH_MAXTXRETRIES,
            'srcIp':          self,
            'dstIp':          neighbor, # currently upstream
            'sourceRoute':    [],
        }

        # enqueue packet in TSCH queue
        isEnqueued = self._tsch_enqueue(newPacket)

        if not isEnqueued:
            # update mote stats
            self._stats_incrementMoteStats('droppedFailedEnqueue')

    def _sixtop_receive_RESPONSE(self, type, code, smac, payload):
        ''' receive a 6P response messages '''

        with self.dataLock:
            if self.sixtopStates[smac.id]['tx']['state'] == self.SIX_STATE_WAIT_ADDRESPONSE:
                 # TODO: now this is still an assert, later this should be handled appropriately
                assert code == self.IANA_6TOP_RC_SUCCESS or code == self.IANA_6TOP_RC_NORES or code == self.IANA_6TOP_RC_RESET        #RC_BUSY not implemented yet

                self._stats_incrementMoteStats('rxAddResp')

                neighbor = smac
                receivedCellList = payload[0]
                numCells = payload[1]
                receivedDir = payload[2]
                seq = payload[3]

                #seqNum mismatch, transaction failed, ignore packet
                if seq!=self.sixtopStates[neighbor.id]['tx']['seqNum']:
                    # log
                    self._log(
                            self.INFO,
                            '[6top] The node {1} has received a wrong seqNum in a sixtop operation with mote {0}',
                            (neighbor.id, self.id),
                    )
                    # go back to IDLE, i.e. remove the neighbor form the states
                    self.sixtopStates[neighbor.id]['tx']['state'] = self.SIX_STATE_IDLE
                    self.sixtopStates[neighbor.id]['tx']['blockedCells']=[]
                    return False

                #transaction is considered as failed since the timeout has already scheduled for this ASN. Too late for removing the event, ignore packet
                if self.sixtopStates[n]['tx']['timer']['asn'] == self.engine.getAsn():
                    # log
                    self._log(
                            self.INFO,
                            '[6top] The node {1} has received a ADD response from mote {0} too late',
                            (neighbor.id, self.id),
                    )
                    # go back to IDLE, i.e. remove the neighbor form the states
                    self.sixtopStates[neighbor.id]['tx']['state'] = self.SIX_STATE_IDLE
                    self.sixtopStates[neighbor.id]['tx']['blockedCells']=[]
                    return False

                # delete the timer.
                uniqueTag = '_sixtop_timer_fired_dest_%s' % (neighbor.id)
                uniqueTag = (self.id, uniqueTag)
                self.engine.removeEvent(uniqueTag=uniqueTag)
                self._log(
                    self.INFO,
                    "[6top] removed timer for mote {0} to neighbor {1} on asn {2}, tag {3}",
                    (self.id, neighbor.id, self.sixtopStates[neighbor.id]['tx']['timer']['asn'], str(uniqueTag)),
                )
                del self.sixtopStates[neighbor.id]['tx']['timer']

                # if the request was successfull and there were enough resources
                if code == self.IANA_6TOP_RC_SUCCESS:
                    cellList=[]

                    # set direction of cells
                    if receivedDir == self.DIR_TX:
                        newDir = self.DIR_RX
                    else:
                        newDir = self.DIR_TX

                    for (ts, ch, cellDir) in receivedCellList:
                        # log
                        self._log(
                            self.INFO,
                            '[6top] add {4} cell ts={0},ch={1} from {2} to {3}',
                            (ts,ch,self.id,neighbor.id,newDir),
                        )
                        cellList         += [(ts,ch,newDir)]
                    self._tsch_addCells(neighbor, cellList)

                    # update counters
                    if newDir==self.DIR_TX:
                        if neighbor not in self.numCellsToNeighbors:
                            self.numCellsToNeighbors[neighbor]     = 0
                        self.numCellsToNeighbors[neighbor]        += len(receivedCellList)
                    else:
                        if neighbor not in self.numCellsFromNeighbors:
                            self.numCellsFromNeighbors[neighbor]   = 0
                        self.numCellsFromNeighbors[neighbor]      += len(receivedCellList)

                    # go back to IDLE, i.e. remove the neighbor form the states
                    self.sixtopStates[neighbor.id]['tx']['state'] = self.SIX_STATE_IDLE
                    self.sixtopStates[neighbor.id]['tx']['blockedCells']=[]
                    return True
                elif code == self.IANA_6TOP_RC_NORES:
                    # log
                    self._log(
                            self.INFO,
                            '[6top] The node {0} do not have available resources to allocate for node {0}',
                            (neighbor.id, self.id),
                    )
                    # go back to IDLE, i.e. remove the neighbor form the states
                    self.sixtopStates[neighbor.id]['tx']['state'] = self.SIX_STATE_IDLE
                    self.sixtopStates[neighbor.id]['tx']['blockedCells']=[]
                    return True
                    #TODO: increase stats of RC_NORES
                #only when devices are not powerfull enough. Not used in the simulator
                elif code == self.IANA_6TOP_RC_BUSY:
                    # log
                    self._log(
                            self.INFO,
                            '[6top] The node {0} is busy and do not have available resources for perform another 6top add operation with mote {1}',
                            (neighbor.id, self.id),
                    )
                    # go back to IDLE, i.e. remove the neighbor form the states
                    self.sixtopStates[neighbor.id]['tx']['state'] = self.SIX_STATE_IDLE
                    self.sixtopStates[neighbor.id]['tx']['blockedCells']=[]
                    return True
                    #TODO: increase stats of RC_BUSY
                elif code == self.IANA_6TOP_RC_RESET: # should not happen
                    # log
                    self._log(
                            self.INFO,
                            '[6top] The node {0} has detected an state inconsistency in a 6top add operation with mote {1}',
                            (neighbor.id, self.id),
                    )
                    # go back to IDLE, i.e. remove the neighbor form the states
                    self.sixtopStates[neighbor.id]['tx']['state'] = self.SIX_STATE_IDLE
                    self.sixtopStates[neighbor.id]['tx']['blockedCells']=[]
                    return True
                    #TODO: increase stats of RC_BUSY
                else:
                    assert False

            elif self.sixtopStates[smac.id]['tx']['state'] == self.SIX_STATE_WAIT_DELETERESPONSE:
                # TODO: now this is still an assert, later this should be handled appropriately
                assert code == self.IANA_6TOP_RC_SUCCESS or code == self.IANA_6TOP_RC_NORES or code == self.IANA_6TOP_RC_RESET

                self._stats_incrementMoteStats('rxDelResp')

                neighbor=smac
                receivedCellList = payload[0]
                numCells = payload[1]
                receivedDir = payload[2]
                seq=payload[3]

                #seqNum mismatch, transaction failed, ignore packet
                if seq!=self.sixtopStates[neighbor.id]['tx']['seqNum']:
                    # log
                    self._log(
                            self.INFO,
                            '[6top] The node {1} has received a wrong seqNum in a sixtop operation with mote {0}',
                            (neighbor.id, self.id),
                    )
                    # go back to IDLE, i.e. remove the neighbor form the states
                    self.sixtopStates[neighbor.id]['tx']['state'] = self.SIX_STATE_IDLE
                    self.sixtopStates[neighbor.id]['tx']['blockedCells']=[]
                    return False

                #transaction is considered as failed since the timeout has already scheduled for this ASN. Too late for removing the event, ignore packet
                if seq!=self.sixtopStates[neighbor.id]['tx']['seqNum']:
                    # log
                    self._log(
                            self.INFO,
                            '[6top] The node {1} has received a Delete response from mote {0} too late',
                            (neighbor.id, self.id),
                    )
                    # go back to IDLE, i.e. remove the neighbor form the states
                    self.sixtopStates[neighbor.id]['tx']['state'] = self.SIX_STATE_IDLE
                    self.sixtopStates[neighbor.id]['tx']['blockedCells']=[]
                    return False

                # delete the timer.
                uniqueTag = '_sixtop_timer_fired_dest_%s' % (neighbor.id)
                uniqueTag = (self.id, uniqueTag)
                self.engine.removeEvent(uniqueTag=uniqueTag)
                self._log(
                    self.INFO,
                    "[6top] removed timer for mote {0} to neighbor {1} on asn {2}, tag {3}",
                    (self.id, neighbor.id, self.sixtopStates[neighbor.id]['tx']['timer']['asn'], str(uniqueTag)),
                )
                del self.sixtopStates[neighbor.id]['tx']['timer']

                # if the request was successfull and there were enough resources
                if code == self.IANA_6TOP_RC_SUCCESS:
                    cellList=[]

                    # set direction of cells
                    if receivedDir == self.DIR_TX:
                        newDir = self.DIR_RX
                    else:
                        newDir = self.DIR_TX

                    for (ts, ch, cellDir) in receivedCellList:
                        # log
                        self._log(
                            self.INFO,
                            '[6top] Delete {4} cell ts={0},ch={1} from {2} to {3}',
                            (ts,ch,self.id,neighbor.id,newDir),
                        )
                        cellList         += [(ts,ch,newDir)]

                    self._tsch_removeCells(neighbor,cellList)

                    self.numCellsFromNeighbors[neighbor]     -= len(cellList)
                    assert self.numCellsFromNeighbors[neighbor]>=0

                    # go back to IDLE, i.e. remove the neighbor form the states
                    self.sixtopStates[neighbor.id]['tx']['state'] = self.SIX_STATE_IDLE
                    self.sixtopStates[neighbor.id]['tx']['blockedCells']=[]
                    return True
                elif code == self.IANA_6TOP_RC_NORES:
                    # log
                    self._log(
                            self.INFO,
                            '[6top] The resources requested for delete were not available for {1} in {0}',
                            (neighbor.id,self.id),
                    )
                    # go back to IDLE, i.e. remove the neighbor form the states
                    self.sixtopStates[neighbor.id]['tx']['state'] = self.SIX_STATE_IDLE
                    self.sixtopStates[neighbor.id]['tx']['blockedCells']=[]
                    return True
                    #TODO: increase stats of RC_NORES
                #only when devices are not powerfull enough. Not used in the simulator
                elif code == self.IANA_6TOP_RC_BUSY:
                    # log
                    self._log(
                            self.INFO,
                             '[6top] The node {0} is busy and has not available resources for perform another 6top deletion operation with mote {1}',
                            (neighbor.id,self.id),
                    )
                    # go back to IDLE, i.e. remove the neighbor form the states
                    self.sixtopStates[neighbor.id]['tx']['state'] = self.SIX_STATE_IDLE
                    self.sixtopStates[neighbor.id]['tx']['blockedCells']=[]
                    return True
                    #TODO: increase stats of RC_BUSY
                elif code == self.IANA_6TOP_RC_RESET:
                    # log
                    self._log(
                            self.INFO,
                             '[6top] The node {0} has detected an state inconsistency in a 6top deletion operation with mote {1}',
                            (neighbor.id,self.id),
                    )
                    # go back to IDLE, i.e. remove the neighbor form the states
                    self.sixtopStates[neighbor.id]['tx']['state'] = self.SIX_STATE_IDLE
                    self.sixtopStates[neighbor.id]['tx']['blockedCells']=[]
                    #TODO: increase stats of RC_RESET
                    return True
                else: # should not happen
                    assert False
            else:
                #only ADD and DELETE implemented so far
                # do not do an assert because it can be you come here if a timer expires
                # assert False
                pass

    def _sixtop_receive_RESPONSE_ACK(self, packet):
        with self.dataLock:

            if self.sixtopStates[packet['dstIp'].id]['rx']['state'] == self.SIX_STATE_WAIT_ADD_RESPONSE_SENDDONE:

                    confirmedCellList = packet['payload'][0]
                    receivedDir = packet['payload'][1]
                    neighbor=packet['dstIp']
                    code=packet['code']

                    self._stats_logSixTopLatencyStat(self.engine.asn-self.tsSixTopReqRecv[neighbor])
                    self.tsSixTopReqRecv[neighbor]=0

                    if code == self.IANA_6TOP_RC_SUCCESS:

                            for (ts, ch, cellDir) in confirmedCellList:
                                    # log
                                    self._log(
                                        self.INFO,
                                        '[6top] add {4} cell ts={0},ch={1} from {2} to {3}',
                                        (ts,ch,self.id,neighbor.id,cellDir),
                                    )
                            self._tsch_addCells(neighbor, confirmedCellList)

                            # update counters
                            if receivedDir==self.DIR_TX:
                                    if neighbor not in self.numCellsToNeighbors:
                                        self.numCellsToNeighbors[neighbor]     = 0
                                    self.numCellsToNeighbors[neighbor]        += len(confirmedCellList)
                            else:
                                    if neighbor not in self.numCellsFromNeighbors:
                                        self.numCellsFromNeighbors[neighbor]   = 0
                                    self.numCellsFromNeighbors[neighbor]      += len(confirmedCellList)

                    # go back to IDLE, i.e. remove the neighbor form the states
                    # but if the node received another, already new request, from the same node (because its timer fired), do not go to IDLE
                    self.sixtopStates[neighbor.id]['rx']['state'] = self.SIX_STATE_IDLE
                    self.sixtopStates[neighbor.id]['rx']['blockedCells']=[]

            elif self.sixtopStates[packet['dstIp'].id]['rx']['state'] == self.SIX_STATE_WAIT_DELETE_RESPONSE_SENDDONE:

                    confirmedCellList = packet['payload'][0]
                    receivedDir = packet['payload'][1]
                    neighbor=packet['dstIp']
                    code=packet['code']

                    self._stats_logSixTopLatencyStat(self.engine.asn-self.tsSixTopReqRecv[neighbor])
                    self.tsSixTopReqRecv[neighbor]=0

                    if code == self.IANA_6TOP_RC_SUCCESS:

                            for (ts, ch, cellDir) in confirmedCellList:
                                    # log
                                    self._log(
                                        self.INFO,
                                        '[6top] delete {4} cell ts={0},ch={1} from {2} to {3}',
                                        (ts,ch,self.id,neighbor.id,cellDir),
                                    )
                            self._tsch_removeCells(neighbor, confirmedCellList)

                    self.numCellsFromNeighbors[neighbor]     -= len(confirmedCellList)
                    assert self.numCellsFromNeighbors[neighbor]>=0


                    # go back to IDLE, i.e. remove the neighbor form the states
                    self.sixtopStates[neighbor.id]['rx']['state'] = self.SIX_STATE_IDLE
                    self.sixtopStates[neighbor.id]['rx']['blockedCells']=[]

            else:
                #only add and delete are implemented so far
                assert False

    def _sixtop_cell_deletion_sender(self,neighbor,tsList):
        with self.dataLock:
            if self.settings.sixtopMessaging:
                if neighbor.id not in self.sixtopStates or (neighbor.id in self.sixtopStates and 'tx' in self.sixtopStates[neighbor.id] and self.sixtopStates[neighbor.id]['tx']['state'] == self.SIX_STATE_IDLE):

                        # if neighbor not yet in states dict, add it
                        if neighbor.id not in self.sixtopStates:
                            self.sixtopStates[neighbor.id] = {}
                        if 'tx' not in self.sixtopStates:
                            self.sixtopStates[neighbor.id]['tx'] = {}
                            self.sixtopStates[neighbor.id]['tx']['state'] = self.SIX_STATE_IDLE
                            self.sixtopStates[neighbor.id]['tx']['blockedCells'] = []
                            self.sixtopStates[neighbor.id]['tx']['seqNum']=0

                        self._sixtop_enqueue_DELETE_REQUEST(neighbor, tsList, len(tsList),self.sixtopStates[neighbor.id]['tx']['seqNum'])
                else:
                    self._log(
                        self.DEBUG,
                        "[6top] can not send 6top DELETE request to {0} because timer still did not fire on mote {1}.",
                        (neighbor.id, self.id),
                    )
            else:
                # log
                self._log(
                    self.INFO,
                    "[6top] remove timeslots={0} with {1}",
                    (tsList,neighbor.id),
                )
                self._tsch_removeCells(
                    neighbor     = neighbor,
                    tsList       = tsList,
                )

                neighbor._sixtop_cell_deletion_receiver(self,tsList)
                self.numCellsToNeighbors[neighbor]       -= len(tsList)
                assert self.numCellsToNeighbors[neighbor]>=0

    def _sixtop_cell_deletion_receiver(self,neighbor,tsList):
        with self.dataLock:
            self._tsch_removeCells(
                neighbor     = neighbor,
                tsList       = tsList,
            )
            self.numCellsFromNeighbors[neighbor]     -= len(tsList)
            assert self.numCellsFromNeighbors[neighbor]>=0
    
    def _sixtop_removeCells(self,neighbor,numCellsToRemove):
        '''
        Finds cells to neighbor, and remove it.
        '''
        
        # get cells to the neighbors
        scheduleList = []
        
        # worst cell removing initialized by theoretical pdr
        for (ts,cell) in self.schedule.iteritems():
            if cell['neighbor']==neighbor and cell['dir']==self.DIR_TX:
                cellPDR           = (float(cell['numTxAck'])+(self.getPDR(neighbor)*self.NUM_SUFFICIENT_TX))/(cell['numTx']+self.NUM_SUFFICIENT_TX)
                scheduleList     += [(ts,cell['numTxAck'],cell['numTx'],cellPDR)]
        
        # introduce randomness in the cell list order
        random.shuffle(scheduleList)
        
        if not self.settings.sixtopNoRemoveWorstCell:
            # triggered only when worst cell selection is due
            # (cell list is sorted according to worst cell selection)
            scheduleListByPDR     = {}
            for tscell in scheduleList:
                if not scheduleListByPDR.has_key(tscell[3]):
                    scheduleListByPDR[tscell[3]]=[]
                scheduleListByPDR[tscell[3]]+=[tscell]
            rssi                  = self.getRSSI(neighbor)
            theoPDR               = Topology.Topology.rssiToPdr(rssi)
            scheduleList          = []
            for pdr in sorted(scheduleListByPDR.keys()):
                if pdr<theoPDR:
                    scheduleList += sorted(scheduleListByPDR[pdr], key=lambda x: x[2], reverse=True)
                else:
                    scheduleList += sorted(scheduleListByPDR[pdr], key=lambda x: x[2])        
            
        # remove a given number of cells from the list of available cells (picks the first numCellToRemove)
        tsList=[]
        for tscell in scheduleList[:numCellsToRemove]:
            
            # log
            self._log(
                self.INFO,
                "[otf] remove cell ts={0} to {1} (pdr={2:.3f})",
                (tscell[0],neighbor.id,tscell[3]),
            )
            tsList += [tscell[0]]
        
        # remove cells
        self._sixtop_cell_deletion_sender(neighbor,tsList)
    
    def _sixtop_enqueue_DELETE_REQUEST(self, neighbor, cellList, numCells,seq):
        ''' enqueue a new 6P DELETE request '''

        self._log(
            self.INFO,
            '[6top] enqueueing a new 6P DEL message cellList={0}, numCells={1} from {2} to {3}',
            (cellList, numCells, self.id, neighbor.id),
        )

        # create new packet
        newPacket = {
            'asn':            self.engine.getAsn(),
            'type':           self.IANA_6TOP_TYPE_REQUEST,
            'code':           self.IANA_6TOP_CMD_DELETE,
            'payload':        [cellList, numCells, self.DIR_TX,seq,self.engine.getAsn()],
            'retriesLeft':    self.TSCH_MAXTXRETRIES,
            'srcIp':          self,
            'dstIp':          neighbor, # currently upstream
            'sourceRoute':    [],
        }

        # enqueue packet in TSCH queue
        isEnqueued = self._tsch_enqueue(newPacket)

        if not isEnqueued:
            # update mote stats
            self._stats_incrementMoteStats('droppedFailedEnqueue')
        else:
            # set state to sending request for this neighbor
            self.sixtopStates[neighbor.id]['tx']['state'] = self.SIX_STATE_SENDING_REQUEST

    def _sixtop_receive_DELETE_REQUEST(self, type, smac, payload):
        ''' receive a 6P delete request message '''
        with self.dataLock:

            neighbor = smac
            cellList = payload[0]
            numCells = payload[1]
            dirNeighbor = payload[2]
            seq=payload[3]

            self._stats_incrementMoteStats('rxDelReq')
            self.tsSixTopReqRecv[neighbor]=payload[4]        #has the asn of when the req packet was enqueued in the neighbor. Used for calculate avg 6top latency

            self.sixtopStates[smac.id]['rx']['state'] = self.SIX_STATE_REQUEST_DELETE_RECEIVED

            if smac.id in self.sixtopStates and 'rx' in self.sixtopStates[smac.id] and self.sixtopStates[smac.id]['rx']['state'] != self.SIX_STATE_IDLE:
                for pkt in self.txQueue:
                    if pkt['type'] == self.IANA_6TOP_TYPE_RESPONSE and pkt['dstIp'].id == smac.id:
                        self.txQueue.remove(pkt)
                        self._log(
                            self.INFO,
                            "[6top] removed a 6TOP_TYPE_RESPONSE packet in the queue of mote {0} to neighbor {1}, because a new TYPE_REQUEST (delete) was received.",
                            (self.id, smac.id),
                        )
                returnCode = self.IANA_6TOP_RC_RESET # error, neighbor has to abort transaction
                if smac.id not in self.sixtopStates:
                    self.sixtopStates[smac.id] = {}
                if 'rx' not in self.sixtopStates:
                    self.sixtopStates[smac.id]['rx'] = {}
                    self.sixtopStates[smac.id]['rx']['blockedCells']=[]
                    self.sixtopStates[smac.id]['rx']['seqNum']=0
                self.sixtopStates[smac.id]['rx']['state'] = self.SIX_STATE_REQUEST_ADD_RECEIVED
                self._sixtop_enqueue_RESPONSE(neighbor, [], returnCode, dirNeighbor,seq)
                return

            # set state to receiving request for this neighbor
            if smac.id not in self.sixtopStates:
                self.sixtopStates[smac.id] = {}
            if 'rx' not in self.sixtopStates[neighbor.id]:
                self.sixtopStates[smac.id]['rx'] = {}
                self.sixtopStates[smac.id]['rx']['blockedCells']=[]
                self.sixtopStates[smac.id]['rx']['seqNum']=0
                #if neighbor is not in sixtopstates and receives a delete, something has gone wrong. Sending reset
                returnCode = self.IANA_6TOP_RC_RESET # error, neighbor has to abort transaction
                self._sixtop_enqueue_RESPONSE(neighbor, [], returnCode, dirNeighbor,seq)
                return

            # set direction of cells
            if dirNeighbor == self.DIR_TX:
                newDir = self.DIR_RX
            else:
                newDir = self.DIR_TX

            returnCode = self.IANA_6TOP_RC_SUCCESS # all is fine

            for cell in cellList:
                if cell not in self.schedule.keys():
                    returnCode = self.IANA_6TOP_RC_NORES # resources are not present

            #enqueue response
            self._sixtop_enqueue_RESPONSE(neighbor, cellList, returnCode, newDir,seq)

    #===== tsch

    def _tsch_resetBackoff(self):
        self.backoff = 0
        self.backoffExponent = self.TSCH_MIN_BACKOFF_EXPONENT - 1
    
    def _tsch_enqueue(self,packet):
        
        if not self._rpl_addNextHop(packet):
            # I don't have a route
            
            # increment mote state
            self._stats_incrementMoteStats('droppedNoRoute')
            
            return False
        
        elif not (self.getTxCells() or self.getSharedCells()):
            # I don't have any transmit cells
            
            # increment mote state
            self._stats_incrementMoteStats('droppedNoTxCells')

            return False
        
        elif len(self.txQueue)==self.TSCH_QUEUE_SIZE:
            #my TX queue is full.

            #However, I will allow to add an additional packet in some specific ocasions
            #This is because if the queues of the nodes are filled with DATA packets, new nodes won't be able to enter properly in the network. So there are exceptions.

            #if Bootstrap is enabled, all nodes will wait until all nodes have at least 1 Tx cell. So it is allowed to enqueue 1 aditional DAO, JOIN or 6P packet
            if self.settings.withBootstrap:
                if packet['type'] == self.APP_TYPE_JOIN or packet['type'] == self.RPL_TYPE_DAO or packet['type'] == self.IANA_6TOP_TYPE_REQUEST or packet['type'] == self.IANA_6TOP_TYPE_RESPONSE:
                    for p in self.txQueue:
                        if packet['type'] == p['type']:
                            #There is already a DAO, JOIN or 6P in que queue, don't add more
                            self._stats_incrementMoteStats('droppedQueueFull')
                            return False
                    self.txQueue    += [packet]
                    return True

            #if Joining is enabled, all nodes will wait until all nodes have joined. So it is allowed to enqueue 1 aditional DAO or JOIN
            if self.settings.withJoin:
                if packet['type'] == self.APP_TYPE_JOIN or packet['type'] == self.RPL_TYPE_DAO:
                    for p in self.txQueue:
                        if packet['type'] == p['type']:
                            #There is already a DAO or JOIN in que queue, don't add more
                            self._stats_incrementMoteStats('droppedQueueFull')
                            return False
                    self.txQueue    += [packet]
                    return True
            
            # update mote stats
            self._stats_incrementMoteStats('droppedQueueFull')

            return False
        
        else:
            # all is good
            
            # enqueue packet
            self.txQueue    += [packet]

            return True
    
    def _tsch_schedule_activeCell(self):
        
        asn        = self.engine.getAsn()
        tsCurrent  = asn%self.settings.slotframeLength
        
        # find closest active slot in schedule
        with self.dataLock:
            
            if not self.schedule:
                self.engine.removeEvent(uniqueTag=(self.id,'_tsch_action_activeCell'))
                return
            
            tsDiffMin             = None
            for (ts,cell) in self.schedule.items():
                if   ts==tsCurrent:
                    tsDiff        = self.settings.slotframeLength
                elif ts>tsCurrent:
                    tsDiff        = ts-tsCurrent
                elif ts<tsCurrent:
                    tsDiff        = (ts+self.settings.slotframeLength)-tsCurrent
                else:
                    raise SystemError()
                
                if (not tsDiffMin) or (tsDiffMin>tsDiff):
                    tsDiffMin     = tsDiff
        
        # schedule at that ASN
        self.engine.scheduleAtAsn(
            asn         = asn+tsDiffMin,
            cb          = self._tsch_action_activeCell,
            uniqueTag   = (self.id,'_tsch_action_activeCell'),
            priority    = 0,
        )
    
    def _tsch_action_activeCell(self):
        '''
        active slot starts. Determine what todo, either RX or TX, use the propagation model to introduce
        interference and Rx packet drops.
        '''
        
        asn = self.engine.getAsn()
        ts  = asn%self.settings.slotframeLength
        
        with self.dataLock:
            
            # make sure this is an active slot   
            assert ts in self.schedule
            # make sure we're not in the middle of a TX/RX operation
            assert not self.waitingFor
            
            cell = self.schedule[ts]
            
            if  cell['dir']==self.DIR_RX:
                
                # start listening
                self.propagation.startRx(
                    mote          = self,
                    channel       = cell['ch'],
                )
                
                # indicate that we're waiting for the RX operation to finish
                self.waitingFor   = self.DIR_RX
            
            elif cell['dir']==self.DIR_TX:
                
                # check whether packet to send
                self.pktToSend = None
                if self.txQueue:
                    for pkt in self.txQueue:
                        # do not send join traffic neither EB in dedicated slots, 6top messages can be sent either in dedicated or shared cells
                        if (pkt['nextHop'] == [cell['neighbor']] and pkt['type'] != self.APP_TYPE_JOIN and pkt['type'] != self.TSCH_TYPE_EB) or (pkt['nextHop'] == [cell['neighbor']] and pkt['type']==self.IANA_6TOP_TYPE_RESPONSE) or (pkt['nextHop'] == [cell['neighbor']] and pkt['type']==self.IANA_6TOP_TYPE_REQUEST):
                            self.pktToSend = pkt
                            break
                    
                # send packet
                if self.pktToSend:
                    
                    cell['numTx'] += 1
                    
                    if pkt['type']==self.IANA_6TOP_TYPE_REQUEST:
                        if pkt['code']==self.IANA_6TOP_CMD_ADD:
                            self._stats_incrementMoteStats('txAddReq')
                            self.sixtopStates[self.pktToSend['nextHop'][0].id]['tx']['state'] = self.SIX_STATE_WAIT_ADDREQUEST_SENDDONE
                        elif pkt['code']==self.IANA_6TOP_CMD_DELETE:
                            self._stats_incrementMoteStats('txDelReq')
                            self.sixtopStates[self.pktToSend['nextHop'][0].id]['tx']['state'] = self.SIX_STATE_WAIT_DELETEREQUEST_SENDDONE
                        else:
                            assert False

                    if pkt['type']==self.IANA_6TOP_TYPE_RESPONSE:
                        if self.sixtopStates[self.pktToSend['nextHop'][0].id]['rx']['state']==self.SIX_STATE_REQUEST_ADD_RECEIVED:
                            self._stats_incrementMoteStats('txAddResp')
                            self.sixtopStates[self.pktToSend['nextHop'][0].id]['rx']['state'] = self.SIX_STATE_WAIT_ADD_RESPONSE_SENDDONE
                        elif self.sixtopStates[self.pktToSend['nextHop'][0].id]['rx']['state']==self.SIX_STATE_REQUEST_DELETE_RECEIVED:
                            self._stats_incrementMoteStats('txDelResp')
                            self.sixtopStates[self.pktToSend['nextHop'][0].id]['rx']['state'] = self.SIX_STATE_WAIT_DELETE_RESPONSE_SENDDONE
                        elif self.sixtopStates[self.pktToSend['nextHop'][0].id]['rx']['state'] == self.SIX_STATE_WAIT_ADD_RESPONSE_SENDDONE:
                            pass
                        elif self.sixtopStates[self.pktToSend['nextHop'][0].id]['rx']['state'] == self.SIX_STATE_WAIT_DELETE_RESPONSE_SENDDONE:
                            pass
                        else:
                            assert False

                    self.propagation.startTx(
                        channel   = cell['ch'],
                        type      = self.pktToSend['type'],
                        code      = self.pktToSend['code'],
                        smac      = self,
                        dmac      = [cell['neighbor']],
                        srcIp     = self.pktToSend['srcIp'],
                        dstIp     = self.pktToSend['dstIp'],
                        srcRoute  = self.pktToSend['sourceRoute'],
                        payload   = self.pktToSend['payload'],
                    )
                    
                    # indicate that we're waiting for the TX operation to finish
                    self.waitingFor   = self.DIR_TX
             
            elif cell['dir']==self.DIR_TXRX_SHARED:
                self.pktToSend = None
                if self.txQueue and self.backoff == 0:
                    for pkt in self.txQueue:
                        # 6top messages can be sent either in dedicated or shared cells, but if there are already TX cells, use those. The same for DAOs
                        if pkt['type']!=self.APP_TYPE_DATA:
                            if ( len(self.getTxCells(pkt['nextHop'][0]))>0 and ( pkt['type']==self.IANA_6TOP_TYPE_RESPONSE or pkt['type']==self.IANA_6TOP_TYPE_REQUEST) ):
                                continue
                            elif ( len(self.getTxCells(pkt['nextHop'][0]))>0 and ( pkt['type']==self.RPL_TYPE_DAO) ):
                                continue
                            else:
                                self.pktToSend = pkt        
                                break

                # Decrement backoff
                if self.backoff > 0:
                    self.backoff -= 1
                # send packet
                if self.pktToSend:
                    
                    cell['numTx'] += 1
                    
                    if pkt['type']==self.IANA_6TOP_TYPE_REQUEST:
                        if pkt['code']==self.IANA_6TOP_CMD_ADD:
                            self._stats_incrementMoteStats('txAddReq')
                            self.sixtopStates[self.pktToSend['nextHop'][0].id]['tx']['state'] = self.SIX_STATE_WAIT_ADDREQUEST_SENDDONE
                        elif pkt['code']==self.IANA_6TOP_CMD_DELETE:
                            self._stats_incrementMoteStats('txDelReq')
                            self.sixtopStates[self.pktToSend['nextHop'][0].id]['tx']['state'] = self.SIX_STATE_WAIT_DELETEREQUEST_SENDDONE
                        else:
                            assert False

                    if pkt['type']==self.IANA_6TOP_TYPE_RESPONSE:
                        if self.sixtopStates[self.pktToSend['nextHop'][0].id]['rx']['state']==self.SIX_STATE_REQUEST_ADD_RECEIVED:
                            self._stats_incrementMoteStats('txAddResp')
                            self.sixtopStates[self.pktToSend['nextHop'][0].id]['rx']['state'] = self.SIX_STATE_WAIT_ADD_RESPONSE_SENDDONE
                        elif self.sixtopStates[self.pktToSend['nextHop'][0].id]['rx']['state']==self.SIX_STATE_REQUEST_DELETE_RECEIVED:
                            self._stats_incrementMoteStats('txDelResp')
                            self.sixtopStates[self.pktToSend['nextHop'][0].id]['rx']['state'] = self.SIX_STATE_WAIT_DELETE_RESPONSE_SENDDONE
                        elif self.sixtopStates[self.pktToSend['nextHop'][0].id]['rx']['state'] == self.SIX_STATE_WAIT_ADD_RESPONSE_SENDDONE:
                            pass
                        elif self.sixtopStates[self.pktToSend['nextHop'][0].id]['rx']['state'] == self.SIX_STATE_WAIT_DELETE_RESPONSE_SENDDONE:
                            pass
                        else:
                            assert False

                    self.propagation.startTx(
                        channel   = cell['ch'],
                        type      = self.pktToSend['type'],
                        code      = self.pktToSend['code'],
                        smac      = self,
                        dmac      = self.pktToSend['nextHop'],
                        srcIp     = self.pktToSend['srcIp'],
                        dstIp     = self.pktToSend['dstIp'],
                        srcRoute  = self.pktToSend['sourceRoute'],
                        payload   = self.pktToSend['payload'],
                    )
                    # indicate that we're waiting for the TX operation to finish
                    self.waitingFor   = self.DIR_TX
                
                else:
                    # start listening
                    self.propagation.startRx(
                         mote          = self,
                         channel       = cell['ch'],
                     )
                    # indicate that we're waiting for the RX operation to finish
                    self.waitingFor = self.DIR_RX

            # schedule next active cell
            self._tsch_schedule_activeCell()
    
    def _tsch_addCells(self,neighbor,cellList):
        ''' adds cell(s) to the schedule '''
        
        with self.dataLock:
            for cell in cellList:
                assert cell[0] not in self.schedule.keys()
                self.schedule[cell[0]] = {
                    'ch':                        cell[1],
                    'dir':                       cell[2],
                    'neighbor':                  neighbor,
                    'numTx':                     0,
                    'numTxAck':                  0,
                    'numRx':                     0,
                    'history':                   [],
                    'sharedCellSuccess':         0,                       # indicator of success for shared cells
                    'sharedCellCollision':       0,                       # indicator of a collision for shared cells
                    'rxDetectedCollision':       False,
                    'debug_canbeInterfered':     [],                      # [debug] shows schedule collision that can be interfered with minRssi or larger level
                    'debug_interference':        [],                      # [debug] shows an interference packet with minRssi or larger level
                    'debug_lockInterference':    [],                      # [debug] shows locking on the interference packet
                    'debug_cellCreatedAsn':      self.engine.getAsn(),    # [debug]
                }
                # log
                self._log(
                    self.INFO,
                    "[tsch] add cell ts={0} ch={1} dir={2} with {3}",
                    (cell[0],cell[1],cell[2],neighbor.id if not type(neighbor) == list else self.BROADCAST_ADDRESS),
                )
            self._tsch_schedule_activeCell()
    
    
    def _tsch_removeCells(self,neighbor,tsList):
        ''' removes cell(s) from the schedule '''
        
        with self.dataLock:
            for cell in tsList:
                if type(cell) == list:
                        # log
                        self._log(
                                self.INFO,
                                "[tsch] remove cell=({0}{1}) with {2}",
                                (cell[0],cell[1],neighbor.id if not type(neighbor) == list else self.BROADCAST_ADDRESS),
                        )

                        assert cell[0] in self.schedule.keys()
                        assert self.schedule[cell[0]]['neighbor']==neighbor
                        self.schedule.pop(cell[0])
                else:
                        assert type(cell)==int
                        # log
                        self._log(
                                self.INFO,
                                "[tsch] remove cell=({0}) with {1}",
                                (cell,neighbor.id if not type(neighbor) == list else self.BROADCAST_ADDRESS),
                        )

                        assert cell in self.schedule.keys()
                        assert self.schedule[cell]['neighbor']==neighbor
                        self.schedule.pop(cell)

            self._tsch_schedule_activeCell()
    def _tsch_action_synchronize(self):

        if not self.isSync:
            channel = random.randint(0,self.settings.numChans-1)
            # start listening
            self.propagation.startRx(
                mote=self,
                channel=channel,
            )
            # indicate that we're waiting for the RX operation to finish
            self.waitingFor = self.DIR_RX

            self._tsch_schedule_synchronize()

    def _tsch_schedule_synchronize(self):
        asn = self.engine.getAsn()

        self.engine.scheduleAtAsn(
            asn=asn + 1,
            cb=self._tsch_action_synchronize,
            uniqueTag=(self.id, '_tsch_action_synchronize'),
            priority=3,
        )

    
    #===== radio

    def radio_isSync(self):
        with self.dataLock:
            return self.isSync
    
    def radio_txDone(self,isACKed,isNACKed):
        '''end of tx slot'''
        
        asn   = self.engine.getAsn()
        ts    = asn%self.settings.slotframeLength
        
        with self.dataLock:
            
            assert ts in self.schedule
            assert self.schedule[ts]['dir']==self.DIR_TX or self.schedule[ts]['dir']==self.DIR_TXRX_SHARED
            assert self.waitingFor==self.DIR_TX
            
            if isACKed:
                # ACK received
                self._logChargeConsumed(self.CHARGE_TxDataRxAck_uC)
                
                # update schedule stats
                self.schedule[ts]['numTxAck'] += 1
                
                # update history
                self.schedule[ts]['history'] += [1]
                
                # update queue stats
                self._stats_logQueueDelay(asn-self.pktToSend['asn'])
                
                # time correction
                if self.schedule[ts]['neighbor'] == self.preferredParent:
                    self.timeCorrectedSlot = asn
                
                # received an ACK for the request, change state and increase the sequence number
                if self.pktToSend['type'] == self.IANA_6TOP_TYPE_REQUEST:
                    if self.pktToSend['code']==self.IANA_6TOP_CMD_ADD:
                        assert self.sixtopStates[self.pktToSend['dstIp'].id]['tx']['state']==self.SIX_STATE_WAIT_ADDREQUEST_SENDDONE
                        self.sixtopStates[self.pktToSend['dstIp'].id]['tx']['state'] = self.SIX_STATE_WAIT_ADDRESPONSE

                        # calculate the asn at which it should fire
                        fireASN = int(self.engine.getAsn()+(float(self.SIXTOP_TIMEOUT)/float(self.settings.slotDuration)))
                        uniqueTag1 = '_sixtop_timer_fired_dest_%s' % (self.pktToSend['dstIp'].id)
                        self.engine.scheduleAtAsn(
                            asn         = fireASN,
                            cb          = self._sixtop_timer_fired,
                            uniqueTag   = (self.id, uniqueTag1),
                            priority    = 5,
                        )
                        self.sixtopStates[self.pktToSend['dstIp'].id]['tx']['timer'] = {}
                        self.sixtopStates[self.pktToSend['dstIp'].id]['tx']['timer']['tag'] = (self.id, uniqueTag1)
                        self.sixtopStates[self.pktToSend['dstIp'].id]['tx']['timer']['asn'] = fireASN
                        self._log(
                            self.DEBUG,
                            "[6top] activated a timer for mote {0} to neighbor {1} on asn {2} with tag {3}",
                            (self.id,self.pktToSend['dstIp'].id, fireASN, str((self.id, uniqueTag1))),
                        )
                    elif self.pktToSend['code']==self.IANA_6TOP_CMD_DELETE:
                        assert self.sixtopStates[self.pktToSend['dstIp'].id]['tx']['state']==self.SIX_STATE_WAIT_DELETEREQUEST_SENDDONE
                        self.sixtopStates[self.pktToSend['dstIp'].id]['tx']['state'] = self.SIX_STATE_WAIT_DELETERESPONSE

                        # calculate the asn at which it should fire
                        fireASN = int(self.engine.getAsn()+(float(self.SIXTOP_TIMEOUT)/float(self.settings.slotDuration)))
                        uniqueTag2 = '_sixtop_timer_fired_dest_%s' % (self.pktToSend['dstIp'].id)
                        self.engine.scheduleAtAsn(
                            asn         = fireASN,
                            cb          = self._sixtop_timer_fired,
                            uniqueTag   = (self.id, uniqueTag2),
                            priority    = 5,
                        )
                        self.sixtopStates[self.pktToSend['dstIp'].id]['tx']['timer'] = {}
                        self.sixtopStates[self.pktToSend['dstIp'].id]['tx']['timer']['tag'] = (self.id, uniqueTag2)
                        self.sixtopStates[self.pktToSend['dstIp'].id]['tx']['timer']['asn'] = fireASN
                        self._log(
                            self.DEBUG,
                            "[6top] activated a timer for mote {0} to neighbor {1} on asn {2} with tag {3}",
                            (self.id,self.pktToSend['dstIp'].id, fireASN, str((self.id, uniqueTag2))),
                        )
                    else:
                        assert False

                    self.sixtopStates[self.pktToSend['dstIp'].id]['tx']['seqNum']+=1


                if self.pktToSend['type'] == self.IANA_6TOP_TYPE_RESPONSE: # received an ACK for the response, handle the schedule
                    self._sixtop_receive_RESPONSE_ACK(self.pktToSend)

                # remove packet from queue
                self.txQueue.remove(self.pktToSend)
                # reset backoff in case of shared slot or in case of a tx slot when the queue is empty
                if self.schedule[ts]['dir'] == self.DIR_TXRX_SHARED or (self.schedule[ts]['dir'] == self.DIR_TX and not self.txQueue):
                   self._tsch_resetBackoff()
                
            elif isNACKed:
                # NACK received
                self._logChargeConsumed(self.CHARGE_TxDataRxAck_uC)
                
                # update schedule stats as if it were successfully transmitted
                self.schedule[ts]['numTxAck'] += 1
                
                # update history
                self.schedule[ts]['history'] += [1]
                
                # time correction
                if self.schedule[ts]['neighbor'] == self.preferredParent:
                    self.timeCorrectedSlot = asn
                
                # decrement 'retriesLeft' counter associated with that packet
                i = self.txQueue.index(self.pktToSend)
                if self.txQueue[i]['retriesLeft'] > 0:
                    self.txQueue[i]['retriesLeft'] -= 1
                
                # drop packet if retried too many time
                if self.txQueue[i]['retriesLeft'] == 0:
                    
                    if  len(self.txQueue) == self.TSCH_QUEUE_SIZE:
                        
                        #only count drops of DATA packets that are part of the experiment
                        if self.pktToSend['type'] == self.APP_TYPE_DATA:
                            if self.pktToSend['payload'][1] >= self.engine.asnInitExperiment and self.engine.asn <= self.engine.asnEndExperiment:
                                self.pktDropMac+=1
                            self._stats_incrementMoteStats('dataDroppedMacRetries')

                        # update mote stats
                        self._stats_incrementMoteStats('droppedMacRetries')
                        
                        # remove packet from queue
                        self.txQueue.remove(self.pktToSend)

                        # reset state for this neighbor
                        # go back to IDLE, i.e. remove the neighbor form the states
                        # but, in the case of a response msg, if the node received another, already new request, from the same node (because its timer fired), do not go to IDLE
                        if self.pktToSend['type'] == self.IANA_6TOP_TYPE_REQUEST:
                            self.sixtopStates[self.pktToSend['dstIp'].id]['tx']['state'] = self.SIX_STATE_IDLE
                            self.sixtopStates[self.pktToSend['dstIp'].id]['tx']['blockedCells'] = []
                        elif self.pktToSend['type'] == self.IANA_6TOP_TYPE_RESPONSE:
                            self.sixtopStates[self.pktToSend['dstIp'].id]['rx']['state'] = self.SIX_STATE_IDLE
                            self.sixtopStates[self.pktToSend['dstIp'].id]['rx']['blockedCells'] = []

                # reset backoff in case of shared slot or in case of a tx slot when the queue is empty
                if self.schedule[ts]['dir'] == self.DIR_TXRX_SHARED or (self.schedule[ts]['dir'] == self.DIR_TX and not self.txQueue):
                    self._tsch_resetBackoff()
            elif self.pktToSend['dstIp'] == self.BROADCAST_ADDRESS:
                # broadcast packet is not acked, remove from queue and update stats
                self._logChargeConsumed(self.CHARGE_TxData_uC)
                self.txQueue.remove(self.pktToSend)
                self._tsch_resetBackoff()
            
            else:
                # neither ACK nor NACK received
                self._logChargeConsumed(self.CHARGE_TxDataRxAck_uC)

                # increment backoffExponent and get new backoff value
                if self.schedule[ts]['dir'] == self.DIR_TXRX_SHARED:
                    if self.backoffExponent < self.TSCH_MAX_BACKOFF_EXPONENT:
                        self.backoffExponent += 1

                    self.backoff = random.randint(0, 2 ** self.backoffExponent - 1)
                
                # update history
                self.schedule[ts]['history'] += [0]

                # decrement 'retriesLeft' counter associated with that packet
                i = self.txQueue.index(self.pktToSend)
                if self.txQueue[i]['retriesLeft'] > 0:
                    self.txQueue[i]['retriesLeft'] -= 1
                
                # drop packet if retried too many time
                if self.txQueue[i]['retriesLeft'] == 0:
                    
                    if  len(self.txQueue) == self.TSCH_QUEUE_SIZE:
                        
                        #only count drops of DATA packets that are part of the experiment
                        if self.pktToSend['type'] == self.APP_TYPE_DATA:
                            if self.pktToSend['payload'][1] >= self.engine.asnInitExperiment and self.engine.asn <= self.engine.asnEndExperiment:
                                self.pktDropMac+=1
                            self._stats_incrementMoteStats('dataDroppedMacRetries')

                        # update mote stats
                        self._stats_incrementMoteStats('droppedMacRetries')
                        
                        # remove packet from queue
                        self.txQueue.remove(self.pktToSend)
            
                        # reset state for this neighbor
                        # go back to IDLE, i.e. remove the neighbor form the states
                        if self.pktToSend['type'] == self.IANA_6TOP_TYPE_REQUEST:
                            self.sixtopStates[self.pktToSend['dstIp'].id]['tx']['state'] = self.SIX_STATE_IDLE
                            self.sixtopStates[self.pktToSend['dstIp'].id]['tx']['blockedCells'] = []
                        elif self.pktToSend['type'] == self.IANA_6TOP_TYPE_RESPONSE:
                            self.sixtopStates[self.pktToSend['dstIp'].id]['rx']['state'] = self.SIX_STATE_IDLE
                            self.sixtopStates[self.pktToSend['dstIp'].id]['rx']['blockedCells'] = []

            # end of radio activity, not waiting for anything
            self.waitingFor = None
            
            # for debug
            ch = self.schedule[ts]['ch']
            rx = self.schedule[ts]['neighbor']
            canbeInterfered = 0
            for mote in self.engine.motes:
                if mote == self:
                    continue
                if ts in mote.schedule and ch == mote.schedule[ts]['ch'] and mote.schedule[ts]['dir'] == self.DIR_TX:
                    if mote.getRSSI(rx)>rx.minRssi:
                        canbeInterfered = 1
            self.schedule[ts]['debug_canbeInterfered'] += [canbeInterfered]        
    
    def radio_rxDone(self,type=None,code=None,smac=None,dmac=None,srcIp=None,dstIp=None,srcRoute=None,payload=None):
        '''end of RX radio activity'''
        
        asn   = self.engine.getAsn()
        ts    = asn%self.settings.slotframeLength
        
        with self.dataLock:
            if self.isSync:
                assert ts in self.schedule
                assert self.schedule[ts]['dir']==self.DIR_RX or self.schedule[ts]['dir']==self.DIR_TXRX_SHARED
                assert self.waitingFor==self.DIR_RX

            if smac and self in dmac: # layer 2 addressing
                # I received a packet

                if [self] == dmac: # unicast packet
                    self._logChargeConsumed(self.CHARGE_RxDataTxAck_uC)
                else: # broadcast
                    self._logChargeConsumed(self.CHARGE_RxData_uC)

                if self.isSync:
                    # update schedule stats
                    self.schedule[ts]['numRx'] += 1

                if (dstIp == self.BROADCAST_ADDRESS):
                    if (type == self.RPL_TYPE_DIO):
                        # got a DIO
                        self._rpl_action_receiveDIO(type, smac, payload)

                        (isACKed, isNACKed) = (False, False)

                        self.waitingFor = None

                        return isACKed, isNACKed
                    elif (type == self.TSCH_TYPE_EB):
                        self._tsch_action_receiveEB(type, smac, payload)

                        (isACKed, isNACKed) = (False, False)

                        self.waitingFor = None

                        return isACKed, isNACKed
                elif (dstIp == self):
                    # receiving packet
                    if type == self.RPL_TYPE_DAO:
                        self._rpl_action_receiveDAO(type, smac, payload)
                        (isACKed, isNACKed) = (True, False)
                    elif type == self.IANA_6TOP_TYPE_REQUEST and code == self.IANA_6TOP_CMD_ADD: # received an 6P ADD request
                        self._sixtop_receive_ADD_REQUEST(type, smac, payload)
                        (isACKed, isNACKed) = (True, False)
                    elif type == self.IANA_6TOP_TYPE_REQUEST and code == self.IANA_6TOP_CMD_DELETE: # received an 6P DELETE request
                        self._sixtop_receive_DELETE_REQUEST(type, smac, payload)
                        (isACKed, isNACKed) = (True, False)
                    elif type == self.IANA_6TOP_TYPE_RESPONSE: # received an 6P response
                        if self._sixtop_receive_RESPONSE(type, code, smac, payload):
                            (isACKed, isNACKed) = (True, False)
                        else:
                            (isACKed, isNACKed) = (False, False)
                    elif type == self.APP_TYPE_DATA: # application packet
                        self._app_action_receivePacket(srcIp=srcIp, payload=payload, timestamp = asn)
                        (isACKed, isNACKed) = (True, False)
                    elif type == self.APP_TYPE_ACK:
                        self._app_action_receiveAck(srcIp=srcIp, payload=payload,timestamp=asn)
                        (isACKed, isNACKed) = (True, False)
                    elif type == self.APP_TYPE_JOIN:
                        self.join_receiveJoinPacket(srcIp=srcIp, payload=payload, timestamp=asn)
                        (isACKed, isNACKed) = (True, False)
                    else:
                        assert False
                else:
                    # relaying packet
                    # count incoming traffic for each node
                    self._otf_incrementIncomingTraffic(smac)

                    if type == self.APP_TYPE_DATA:
                        # update the number of hops
                        newPayload     = copy.deepcopy(payload)
                        newPayload[2] += 1
                    else:
                        # copy the payload and forward
                        newPayload     = copy.deepcopy(payload)
                    
                    # create packet
                    relayPacket = {
                        'asn':         asn,
                        'type':        type,
                        'code':        code,
                        'payload':     newPayload,
                        'retriesLeft': self.TSCH_MAXTXRETRIES,
                        'srcIp':       srcIp,
                        'dstIp':       dstIp,
                        'sourceRoute': srcRoute,
                    }
                    
                    # enqueue packet in TSCH queue
                    isEnqueued = self._tsch_enqueue(relayPacket)
                    
                    if isEnqueued:

                        # update mote stats
                        self._stats_incrementMoteStats('appRelayed')
                        
                        (isACKed, isNACKed) = (True, False)
                    
                    else:
                        (isACKed, isNACKed) = (False, True)
                
            else:
                # this was an idle listen
                
                # log charge usage
                if self.isSync:
                    self._logChargeConsumed(self.CHARGE_Idle_uC)
                else:
                    self._logChargeConsumed(self.CHARGE_IdleNotSync_uC)
                
                (isACKed, isNACKed) = (False, False)
            
            self.waitingFor = None
            
            return isACKed, isNACKed
    
    #===== wireless
    
    def setPDR(self,neighbor,pdr):
        ''' sets the pdr to that neighbor'''
        with self.dataLock:
            self.PDR[neighbor] = pdr
    
    def getPDR(self,neighbor):
        ''' returns the pdr to that neighbor'''
        with self.dataLock:
            return self.PDR[neighbor]
    
    def setRSSI(self,neighbor,rssi):
        ''' sets the RSSI to that neighbor'''
        with self.dataLock:
            self.RSSI[neighbor.id] = rssi
    
    def getRSSI(self,neighbor):
        ''' returns the RSSI to that neighbor'''
        with self.dataLock:
            return self.RSSI[neighbor.id]
    
    def _estimateETX(self,neighbor):
        
        with self.dataLock:
            
            # set initial values for numTx and numTxAck assuming PDR is exactly estimated
            pdr                   = self.getPDR(neighbor)
            numTx                 = self.NUM_SUFFICIENT_TX
            numTxAck              = math.floor(pdr*numTx)
            
            for (_,cell) in self.schedule.items():
                if (cell['neighbor'] == neighbor) and (cell['dir'] == self.DIR_TX):
                    numTx        += cell['numTx']
                    numTxAck     += cell['numTxAck']
            
            # abort if about to divide by 0
            if not numTxAck:
                return
            
            # calculate ETX
            etx = float(numTx)/float(numTxAck)
            
            return etx
    
    def _myNeigbors(self):
        return [n for n in self.PDR.keys() if self.PDR[n]>0]
    
    #===== clock
    
    def clock_getOffsetToDagRoot(self):
        ''' calculate time offset compared to the DAGroot '''

        if self.dagRoot:
            return 0.0
        
        asn                  = self.engine.getAsn()
        offset               = 0.0
        child                = self
        parent               = self.preferredParent
        
        while True:
            secSinceSync     = (asn-child.timeCorrectedSlot)*self.settings.slotDuration  # sec
            # FIXME: for ppm, should we not /10^6?
            relDrift         = child.drift - parent.drift                                # ppm
            offset          += relDrift * secSinceSync                                   # us
            if parent.dagRoot:
                break
            else:
                child        = parent
                parent       = child.preferredParent
        
        return offset
    
    #===== location
    
    def setLocation(self,x,y):
        with self.dataLock:
            self.x = x
            self.y = y
    
    def getLocation(self):
        with self.dataLock:
            return (self.x,self.y)
    
    #==== battery
    
    def boot(self):
        if self.settings.withJoin:
            if self.dagRoot:
                self._init_stack()  # initialize the stack and start sending beacons and DIOs
            else:
                self._tsch_schedule_synchronize()  # permanent rx until node hears an enhanced beacon to sync
        else:
            self.isSync = True  # without join we skip the always-on listening for EBs
            self._init_stack()

    def _init_stack(self):
        # start the stack layer by layer
        
        # add minimal cell
        self._tsch_addCells(self._myNeigbors(),[(0,0,self.DIR_TXRX_SHARED)])

        # RPL
        self._rpl_schedule_sendDIO(firstDIO=True)

        if self.dagRoot:
            self._tsch_schedule_sendEB(firstEB=True)
        else:
            self._rpl_schedule_sendDAO(firstDAO=True)

        # OTF
        self._otf_resetInboundTrafficCounters()
        self._otf_schedule_housekeeping()
        # 6top
        if not self.settings.sixtopNoHousekeeping:
            self._sixtop_schedule_housekeeping()

        if self.settings.withJoin and not self.dagRoot:
            self.join_scheduleJoinProcess()  # trigger the join process

        # app
        if not self.dagRoot:
            if not self.settings.withJoin:
                if self.settings.numPacketsBurst != None and self.settings.burstTimestamp != None:
                    self._app_schedule_sendPacketBurst()
                else:
                    self._app_schedule_sendSinglePacket(firstPacket=True)
                self._tsch_schedule_sendEB(firstEB=True)
        # tsch
        self._tsch_schedule_activeCell()
    
    def _logChargeConsumed(self,charge):
        with self.dataLock:
            self.chargeConsumed  += charge
    
    #======================== private =========================================
    
    #===== getters
    
    def getTxCells(self, neighbor = None):
        with self.dataLock:
            if neighbor is None:
                return [(ts,c['ch'],c['neighbor']) for (ts,c) in self.schedule.items() if c['dir']==self.DIR_TX]
            else:
                return [(ts, c['ch'], c['neighbor']) for (ts, c) in self.schedule.items() if
                        c['dir'] == self.DIR_TX and c['neighbor'] == neighbor]
    def getRxCells(self, neighbor = None):
        with self.dataLock:
            if neighbor is None:
                return [(ts,c['ch'],c['neighbor']) for (ts,c) in self.schedule.items() if c['dir']==self.DIR_RX]
            else:
                return [(ts, c['ch'], c['neighbor']) for (ts, c) in self.schedule.items() if
                        c['dir'] == self.DIR_RX and c['neighbor'] == neighbor]

    def getSharedCells(self):
        with self.dataLock:
            return [(ts, c['ch'], c['neighbor']) for (ts, c) in self.schedule.items() if c['dir'] == self.DIR_TXRX_SHARED]

    #===== stats
    
    # mote state
    
    def getMoteStats(self):
        
        # gather statistics
        with self.dataLock:
            dataPktQueues=0
            for p in self.txQueue:
                if p['type']==self.APP_TYPE_DATA:
                    if p['payload'][1] > self.engine.asnInitExperiment and self.engine.asn <= self.engine.asnEndExperiment:
                        dataPktQueues+=1

            returnVal = copy.deepcopy(self.motestats)
            returnVal['numTxCells']         = len(self.getTxCells())
            returnVal['numRxCells']         = len(self.getRxCells())
            returnVal['numSharedCells']     = len(self.getSharedCells())
            returnVal['aveQueueDelay']      = self._stats_getAveQueueDelay()
            returnVal['aveLatency']         = self._stats_getAveLatency()
            returnVal['aveHops']            = self._stats_getAveHops()
            returnVal['probableCollisions'] = self._stats_getRadioStats('probableCollisions')
            returnVal['txQueueFill']        = len(self.txQueue)
            returnVal['chargeConsumed']     = self.chargeConsumed
            returnVal['numTx']              = sum([cell['numTx'] for (_,cell) in self.schedule.items()])
            returnVal['pktReceived']        = self.pktReceived
            returnVal['pktGen']             = self.pktGen
            returnVal['pktDropQueue']       = self.pktDropQueue
            returnVal['pktDropMac']         = self.pktDropMac
            returnVal['dataQueueFill']      = dataPktQueues
            returnVal['aveSixtopLatency']   = self._stats_getAveSixTopLatency()
        
        # reset the statistics
        self._stats_resetMoteStats()
        self._stats_resetQueueStats()
        self._stats_resetLatencyStats()
        self._stats_resetHopsStats()
        self._stats_resetRadioStats()
        self._stats_resetSixTopLatencyStats()
        
        return returnVal
    
    def _stats_resetMoteStats(self):
        with self.dataLock:
            self.motestats = {
                # app
                'appGenerated':            0,   # number of packets app layer generated
                'appRelayed':              0,   # number of packets relayed
                'appReachesDagroot':       0,   # number of packets received at the DAGroot
                'droppedFailedEnqueue':    0,   # dropped packets because failed enqueue them
                'droppedDataFailedEnqueue': 0,  # dropped DATA packets because app failed enqueue them
                # queue
                'droppedQueueFull':        0,   # dropped packets because queue is full
                # rpl
                'rplTxDIO':                0,   # number of TX'ed DIOs
                'rplRxDIO':                0,   # number of RX'ed DIOs
                'rplTxDAO':                0,  # number of TX'ed DAOs
                'rplRxDAO':                0,  # number of RX'ed DAOs
                'rplChurnPrefParent':      0,   # number of time the mote changes preferred parent
                'rplChurnRank':            0,   # number of time the mote changes rank
                'rplChurnParentSet':       0,   # number of time the mote changes parent set
                'droppedNoRoute':          0,   # packets dropped because no route (no preferred parent)
                # otf
                'otfAdd':                  0,   # OTF adds some cells
                'otfRemove':               0,   # OTF removes some cells
                'droppedNoTxCells':        0,   # packets dropped because no TX cells
                # 6top
                'topTxRelocatedCells':     0,   # number of time tx-triggered 6top relocates a single cell
                'topTxRelocatedBundles':   0,   # number of time tx-triggered 6top relocates a bundle
                'topRxRelocatedCells':     0,   # number of time rx-triggered 6top relocates a single cell
                'txAddReq':                   0,        # number of 6P Add request transmitted
                'txAddResp':                  0,        # number of 6P Add responses transmitted
                'txDelReq':                   0,        # number of 6P del request transmitted
                'txDelResp':                  0,        # number of 6P del responses transmitted
                'rxAddReq':                   0,        # number of 6P Add request received
                'rxAddResp':                  0,        # number of 6P Add responses received
                'rxDelReq':                   0,        # number of 6P Del request received
                'rxDelResp':                  0,        # number of 6P Del responses received
                # tsch
                'droppedMacRetries':       0,   # packets dropped because more than TSCH_MAXTXRETRIES MAC retries
                'dataDroppedMacRetries':   0,        # packets dropped because more than TSCH_MAXTXRETRIES MAC retries in a DATA packet
                'tschTxEB':                0,   # number of TX'ed EBs
                'tschRxEB':                0,   # number of RX'ed EBs
            }
    
    def _stats_incrementMoteStats(self,name):
        with self.dataLock:
            self.motestats[name] += 1
    
    # cell stats
    
    def getCellStats(self,ts_p,ch_p):
        ''' retrieves cell stats '''
        
        returnVal = None
        with self.dataLock:
            for (ts,cell) in self.schedule.items():
                if ts==ts_p and cell['ch']==ch_p:
                    returnVal = {
                        'dir':            cell['dir'],
                        'neighbor':       [node.id for node in cell['neighbor']] if type(cell['neighbor']) is list else cell['neighbor'].id,
                        'numTx':          cell['numTx'],
                        'numTxAck':       cell['numTxAck'],
                        'numRx':          cell['numRx'],
                    }
                    break
        return returnVal
    
    def stats_sharedCellCollisionSignal(self):
        asn = self.engine.getAsn()
        ts = asn % self.settings.slotframeLength

        assert self.schedule[ts]['dir'] == self.DIR_TXRX_SHARED

        with self.dataLock:
            self.schedule[ts]['sharedCellCollision'] = 1

    def stats_sharedCellSuccessSignal(self):
        asn = self.engine.getAsn()
        ts = asn % self.settings.slotframeLength

        assert self.schedule[ts]['dir'] == self.DIR_TXRX_SHARED

        with self.dataLock:
            self.schedule[ts]['sharedCellSuccess'] = 1


    def getSharedCellStats(self):
        returnVal = {}
        # gather statistics
        with self.dataLock:
            for (ts, cell) in self.schedule.items():
                if cell['dir'] == self.DIR_TXRX_SHARED:

                    returnVal['sharedCellCollision_{0}_{1}'.format(ts, cell['ch'])] = cell['sharedCellCollision']
                    returnVal['sharedCellSuccess_{0}_{1}'.format(ts, cell['ch'])] = cell['sharedCellSuccess']

                    # reset the statistics
                    cell['sharedCellCollision'] = 0
                    cell['sharedCellSuccess']   = 0

        return returnVal

    # queue stats
    
    def _stats_logQueueDelay(self,delay):
        with self.dataLock:
            self.queuestats['delay'] += [delay]
    
    def _stats_getAveQueueDelay(self):
        d = self.queuestats['delay']
        return float(sum(d))/len(d) if len(d)>0 else 0
    
    def _stats_resetQueueStats(self):
        with self.dataLock:
            self.queuestats = {
                'delay':               [],
            }
    
    # latency stats
    
    def _stats_logLatencyStat(self,latency):
        with self.dataLock:
            self.packetLatencies += [latency]
    
    def _stats_logSixTopLatencyStat(self,latency):
        with self.dataLock:
            self.avgsixtopLatency += [latency]

    def _stats_getAveLatency(self):
        with self.dataLock:
            d = self.packetLatencies
            return float(sum(d))/float(len(d)) if len(d)>0 else 0
    
    def _stats_getAveSixTopLatency(self):
        with self.dataLock:

            d = self.avgsixtopLatency
            return float(sum(d))/float(len(d)) if len(d)>0 else 0

    def _stats_resetLatencyStats(self):
        with self.dataLock:
            self.packetLatencies = []
    
    def _stats_resetSixTopLatencyStats(self):
        with self.dataLock:
            self.avgsixtopLatency = []

    # hops stats
    
    def _stats_logHopsStat(self,hops):
        with self.dataLock:
            self.packetHops += [hops]
    
    def _stats_getAveHops(self):
        with self.dataLock:
            d = self.packetHops
            return float(sum(d))/float(len(d)) if len(d)>0 else 0
    
    def _stats_resetHopsStats(self):
        with self.dataLock:
            self.packetHops = []
    
    # radio stats
    
    def stats_incrementRadioStats(self,name):
        with self.dataLock:
            self.radiostats[name] += 1
    
    def _stats_getRadioStats(self,name):
        return self.radiostats[name]
    
    def _stats_resetRadioStats(self):
        with self.dataLock:
            self.radiostats = {
                'probableCollisions':      0,   # number of packets that can collide with another packets 
            }
    
    #===== log
    
    def _log(self,severity,template,params=()):
        
        if   severity==self.DEBUG:
            if not log.isEnabledFor(logging.DEBUG):
                return
            logfunc = log.debug
        elif severity==self.INFO:
            if not log.isEnabledFor(logging.INFO):
                return
            logfunc = log.info
        elif severity==self.WARNING:
            if not log.isEnabledFor(logging.WARNING):
                return
            logfunc = log.warning
        elif severity==self.ERROR:
            if not log.isEnabledFor(logging.ERROR):
                return
            logfunc = log.error
        else:
            raise NotImplementedError()
        
        output  = []
        output += ['[ASN={0:>6} id={1:>4}] '.format(self.engine.getAsn(),self.id)]
        output += [template.format(*params)]
        output  = ''.join(output)
        logfunc(output)
