#!/usr/bin/python
"""
\brief Model of a 6TiSCH mote.

\author Thomas Watteyne <watteyne@eecs.berkeley.edu>
\author Kazushi Muraoka <k-muraoka@eecs.berkeley.edu>
\author Nicola Accettura <nicola.accettura@eecs.berkeley.edu>
\author Xavier Vilajosana <xvilajosana@eecs.berkeley.edu>
\author Malisa Vucinic <malisa.vucinic@inria.fr>
\author Esteban Municio <esteban.municio@uantwerpen.be>
\author Glenn Daneels <glenn.daneels@uantwerpen.be>
"""

# =========================== imports =========================================

import copy
import threading

# Mote sub-modules
import app
import sixlowpan
import rpl
import sf
import sixp
import secjoin
import tsch
import radio
import batt

import MoteDefines as d

# Simulator-wide modules
import SimEngine

# =========================== defines =========================================

# =========================== body ============================================

class Mote(object):

    def __init__(self, id):

        # store params
        self.id                        = id

        # admin
        self.dataLock                  = threading.RLock()
        
        # identifiers
        self.dagRoot                   = False
        self.dagRootId                 = None

        # stats
        self.packetLatencies           = []      # in slots
        self.packetHops                = []
        self.numCellsToNeighbors       = {}      # indexed by neighbor, contains int
        self.numCellsFromNeighbors     = {}      # indexed by neighbor, contains int

        # singletons (to access quicker than recreate every time)
        self.log                       = SimEngine.SimLog.SimLog().log
        self.engine                    = SimEngine.SimEngine.SimEngine()
        self.settings                  = SimEngine.SimSettings.SimSettings()

        # stack
        self.app                       = app.App(self)
        self.sixlowpan                 = sixlowpan.Sixlowpan(self)
        self.rpl                       = rpl.Rpl(self)
        self.sixp                      = sixp.SixP(self)
        self.secjoin                   = secjoin.SecJoin(self)
        self.tsch                      = tsch.Tsch(self)
        self.sf                        = sf.SchedulingFunction.get_sf(self)
        self.radio                     = radio.Radio(self)
        self.batt                      = batt.Batt(self)
        
        # stats
        self.motestats                 = {}
        self._stats_resetMoteStats()
        self._stats_resetQueueStats()
        self._stats_resetLatencyStats()
        self._stats_resetHopsStats()
        self._stats_resetRadioStats()

    # ======================= stack ===========================================

    # ===== role

    def setDagRoot(self):
        self.dagRoot              = True

    # ==== stack

    def activate_tsch_stack(self):
        # start the stack layer by layer, we are sync'ed and joined

        # activate different layers
        self.tsch.activate()
        self.rpl.activate()
        self.sf.activate()

        # app
        if not self.dagRoot:
            if self.settings.app_burstNumPackets and self.settings.app_burstTimestamp:
                self.app.schedule_mote_sendPacketBurstToDAGroot()
            else:
                self.app.schedule_mote_sendSinglePacketToDAGroot(firstPacket=True)

    # ==== wireless

    def getCellPDR(self, cell):
        """ returns the pdr of the cell """

        assert cell['neighbor'] is not type(list)

        with self.dataLock:
            if cell['numTx'] < d.NUM_SUFFICIENT_TX:
                return self.getPDR(cell['neighbor'])
            else:
                return float(cell['numTxAck']) / float(cell['numTx'])
    
    def getPDR(self, neighbor):
        """ returns the pdr to that neighbor"""
        with self.dataLock:
            return self.engine.connectivity.get_pdr(
                source       = self.id,
                destination  = neighbor,
                channel      = 0, #FIXME
            )

    def _myNeighbors(self):
        return [n.id for n in self.engine.motes if self.engine.connectivity.get_pdr(self.id,n.id,0) > 0]

    # ==== location

    def setLocation(self, x, y):
        with self.dataLock:
            self.x = x
            self.y = y

    def getLocation(self):
        with self.dataLock:
            return self.x, self.y

    # ==== battery

    def boot(self):
        if self.dagRoot:
            # I'm the DAG root
            
            # secjoin
            self.secjoin.setIsJoined(True)
            # rpl
            self.rpl.setRank(256)
            self.parentChildfromDAOs  = {}  # from DAOs, {'c': 'p', ...}
            # tsch
            self.tsch.add_minimal_cell()
            self.tsch.setIsSync(True)
            # stats
            self.packetLatencies      = []  # in slots
            self.packetHops           = []
            
            # activate the TSCH stack
            self.activate_tsch_stack()
            
            # give DAGroot's ID to each mote FIXME: remove
            for mote in self.engine.motes:
                mote.dagRootId  = self.id
            
            # schedule the first active cell
            self.tsch.tsch_schedule_active_cell()
            
        else:
            # I'm NOT the DAG root
            
            # schedule the first listeningForE cell
            self.tsch.tsch_schedule_listeningForEB_cell()

    #======================== private =========================================

    #===== stats

    # mote state

    def getMoteStats(self):

        # gather statistics
        with self.dataLock:
            dataPktQueues = 0
            for p in self.tsch.getTxQueue():
                if p['type'] == d.APP_TYPE_DATA:
                    dataPktQueues += 1

            returnVal = copy.deepcopy(self.motestats)
            returnVal['numTxCells']         = len(self.tsch.getTxCells())
            returnVal['numRxCells']         = len(self.tsch.getRxCells())
            returnVal['numDedicatedCells']  = len([(ts, c) for (ts, c) in self.tsch.getSchedule().items() if type(self) == type(c['neighbor'])])
            returnVal['numSharedCells']     = len(self.tsch.getSharedCells())
            returnVal['aveQueueDelay']      = self._stats_getAveQueueDelay()
            returnVal['aveLatency']         = self._stats_getAveLatency()
            returnVal['aveHops']            = self._stats_getAveHops()
            returnVal['probableCollisions'] = self._stats_getRadioStats('probableCollisions')
            returnVal['txQueueFill']        = len(self.tsch.getTxQueue())
            returnVal['chargeConsumed']     = self.batt.chargeConsumed
            returnVal['numTx']              = sum([cell['numTx'] for (_, cell) in self.tsch.getSchedule().items()])
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
            self.motestats = {}
    
    # cell stats

    def getCellStats(self, ts_p, ch_p):
        """ retrieves cell stats """

        returnVal = None
        with self.dataLock:
            for (ts, cell) in self.tsch.getSchedule().items():
                if ts == ts_p and cell['ch'] == ch_p:
                    returnVal = {
                        'dir':            cell['dir'],
                        'neighbor':       [node.id for node in cell['neighbor']] if type(cell['neighbor']) is list else cell['neighbor'].id,
                        'numTx':          cell['numTx'],
                        'numTxAck':       cell['numTxAck'],
                        'numRx':          cell['numRx'],
                    }
                    break
        return returnVal

    # queue stats

    def _stats_logQueueDelay(self, delay):
        with self.dataLock:
            self.queuestats['delay'] += [delay]

        # log
        self.log(
            SimEngine.SimLog.LOG_QUEUE_DELAY,
            {
                "delay": delay
            }
        )

    def _stats_getAveQueueDelay(self):
        d = self.queuestats['delay']
        return float(sum(d))/len(d) if len(d) > 0 else 0

    def _stats_resetQueueStats(self):
        with self.dataLock:
            self.queuestats = {
                'delay':               [],
            }

    # latency stats

    def _stats_logLatencyStat(self, latency):
        with self.dataLock:
            self.packetLatencies += [latency]

    def _stats_logSixTopLatencyStat(self, latency):
        with self.dataLock:
            l = self.sixp.getavgsixtopLatency()
            l += [latency]

    def _stats_getAveLatency(self):
        with self.dataLock:
            d = self.packetLatencies
            return float(sum(d))/float(len(d)) if len(d) > 0 else 0

    def _stats_getAveSixTopLatency(self):
        with self.dataLock:

            d = self.sixp.getavgsixtopLatency()
            return float(sum(d))/float(len(d)) if len(d) > 0 else 0

    def _stats_resetLatencyStats(self):
        with self.dataLock:
            self.packetLatencies = []

    def _stats_resetSixTopLatencyStats(self):
        with self.dataLock:
            l = self.sixp.getavgsixtopLatency()
            l = []

    # hops stats

    def _stats_logHopsStat(self, hops):
        with self.dataLock:
            self.packetHops += [hops]

    def _stats_getAveHops(self):
        with self.dataLock:
            d = self.packetHops
            return float(sum(d))/float(len(d)) if len(d) > 0 else 0

    def _stats_resetHopsStats(self):
        with self.dataLock:
            self.packetHops = []

    # radio stats

    def stats_incrementRadioStats(self, name):
        with self.dataLock:
            self.radiostats[name] += 1

    def _stats_getRadioStats(self, name):
        return self.radiostats[name]

    def _stats_resetRadioStats(self):
        with self.dataLock:
            self.radiostats = {
                'probableCollisions':      0,  # number of packets that can collide with another packets
            }
