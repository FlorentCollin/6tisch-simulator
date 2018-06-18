"""
"""

# =========================== imports =========================================

import copy
import random

# Mote sub-modules
import MoteDefines as d

# Simulator-wide modules
import SimEngine

# =========================== defines =========================================

# =========================== helpers =========================================

# =========================== body ============================================

class Tsch(object):

    MINIMAL_SHARED_CELL = {
        'slotOffset'   : 0,
        'channelOffset': 0,
        'neighbor'     : None, # None means "any"
        'cellOptions'  : [d.CELLOPTION_TX,d.CELLOPTION_RX,d.CELLOPTION_SHARED]
    }

    def __init__(self, mote):

        # store params
        self.mote                           = mote

        # singletons (quicker access, instead of recreating every time)
        self.engine                         = SimEngine.SimEngine.SimEngine()
        self.settings                       = SimEngine.SimSettings.SimSettings()
        self.log                            = SimEngine.SimLog.SimLog().log

        # local variables
        self.schedule                       = {}      # indexed by slotOffset, contains cell
        self.txQueue                        = []
        self.pktToSend                      = None
        self.waitingFor                     = None
        self.channel                        = None
        self.asnLastSync                    = None
        self.isSync                         = False
        self.join_proxy                     = None
        self.iAmSendingEBs                  = False
        self.iAmSendingDIOs                 = False
        self.drift                          = random.uniform(-d.RADIO_MAXDRIFT, d.RADIO_MAXDRIFT)
        # backoff state
        self.backoff_exponent               = d.TSCH_MIN_BACKOFF_EXPONENT
        self.backoff_remaining_delay        = 0

    #======================== public ==========================================

    # getters/setters

    def getSchedule(self):
        return self.schedule

    def getTxQueue(self):
        return self.txQueue

    def getIsSync(self):
        return self.isSync

    def setIsSync(self,val):
        # set
        self.isSync      = val

        if self.isSync:
            # log
            self.log(
                SimEngine.SimLog.LOG_TSCH_SYNCED,
                {
                    "_mote_id":   self.mote.id,
                }
            )

            self.asnLastSync = self.engine.getAsn()

            # transition: listeningForEB->active
            self.engine.removeFutureEvent(      # remove previously scheduled listeningForEB cells
                uniqueTag=(self.mote.id, '_tsch_action_listeningForEB_cell')
            )
            self.tsch_schedule_next_active_cell()    # schedule next active cell
        else:
            # log
            self.log(
                SimEngine.SimLog.LOG_TSCH_DESYNCED,
                {
                    "_mote_id":   self.mote.id,
                }
            )

            self.delete_minimal_cell()
            self.mote.sf.stop()
            self.join_proxy  = None
            self.asnLastSync = None

            # transition: active->listeningForEB
            self.engine.removeFutureEvent(      # remove previously scheduled listeningForEB cells
                uniqueTag=(self.mote.id, '_tsch_action_active_cell')
            )
            self.tsch_schedule_next_listeningForEB_cell()

    def _getCells(self, neighbor, cellOptions=None):
        """
        Returns a dict containing the cells
        The dict keys are the cell slotOffset
        :param neighbor:
        :param cellOptions:
        :rtype: dict
        """
        if neighbor is not None:
            assert type(neighbor) == int

        # configure filtering condition
        if (neighbor is None) and (cellOptions is not None):    # filter by cellOptions
            condition = lambda (_, c): sorted(c['cellOptions']) == sorted(cellOptions)
        elif (neighbor is not None) and (cellOptions is None):  # filter by neighbor
            condition = lambda (_, c): c['neighbor'] == neighbor
        elif (neighbor is None) and (cellOptions is None):      # don't filter
            condition = lambda (_, c): True
        else:                                                   # filter by cellOptions and neighbor
            condition = lambda (_, c): (
                    sorted(c['cellOptions']) == sorted(cellOptions) and
                    c['neighbor'] == neighbor
            )

        # apply filter
        return dict(filter(condition, self.schedule.items()))

    def getTxCells(self, neighbor=None):
        return self._getCells(
            neighbor    = neighbor,
            cellOptions = [d.CELLOPTION_TX],
        )

    def getRxCells(self, neighbor=None):
        return self._getCells(
            neighbor    = neighbor,
            cellOptions = [d.CELLOPTION_RX],
        )

    def getTxRxSharedCells(self, neighbor=None):
        return self._getCells(
            neighbor    = neighbor,
            cellOptions = [d.CELLOPTION_TX, d.CELLOPTION_RX, d.CELLOPTION_SHARED],
        )

    def getDedicatedCells(self, neighbor):
        return self._getCells(
            neighbor    = neighbor,
        )

    # activate

    def startSendingEBs(self):
        self.iAmSendingEBs  = True

    def startSendingDIOs(self):
        self.iAmSendingDIOs = True

    # minimal

    def add_minimal_cell(self):

        self.addCell(**self.MINIMAL_SHARED_CELL)

    def delete_minimal_cell(self):

        self.deleteCell(**self.MINIMAL_SHARED_CELL)

    # schedule interface

    def addCell(self, slotOffset, channelOffset, neighbor, cellOptions):

        assert isinstance(slotOffset, int)
        assert isinstance(channelOffset, int)
        if neighbor!=None:
            assert isinstance(neighbor, int)
        assert isinstance(cellOptions, list)

        # make sure I have no activity at that slotOffset already
        assert slotOffset not in self.schedule.keys()

        # log
        self.log(
            SimEngine.SimLog.LOG_TSCH_ADD_CELL,
            {
                '_mote_id':       self.mote.id,
                'slotOffset':     slotOffset,
                'channelOffset':  channelOffset,
                'neighbor':       neighbor,
                'cellOptions':    cellOptions,
            }
        )

        # add cell
        self.schedule[slotOffset] = {
            'channelOffset':      channelOffset,
            'neighbor':           neighbor,
            'cellOptions':        cellOptions,
            # per-cell statistics
            'numTx':              0,
            'numTxAck':           0,
            'numRx':              0,
        }

        # reschedule the next active cell, in case it is now earlier
        if self.getIsSync():
            self.tsch_schedule_next_active_cell()

    def deleteCell(self, slotOffset, channelOffset, neighbor, cellOptions):
        assert isinstance(slotOffset, int)
        assert isinstance(channelOffset, int)
        assert (neighbor is None) or (isinstance(neighbor, int))
        assert isinstance(cellOptions, list)

        # make sure I'm removing a cell that I have in my schedule
        assert slotOffset in self.schedule.keys()
        assert self.schedule[slotOffset]['channelOffset']  == channelOffset
        assert self.schedule[slotOffset]['neighbor']       == neighbor
        assert self.schedule[slotOffset]['cellOptions']    == cellOptions

        # log
        self.log(
            SimEngine.SimLog.LOG_TSCH_DELETE_CELL,
            {
                '_mote_id':       self.mote.id,
                'slotOffset':     slotOffset,
                'channelOffset':  channelOffset,
                'neighbor':       neighbor,
                'cellOptions':    cellOptions,
            }
        )

        # delete cell
        del self.schedule[slotOffset]

        # reschedule the next active cell, in case it is now earlier
        if self.getIsSync():
            self.tsch_schedule_next_active_cell()

    # data interface with upper layers

    def enqueue(self, packet):

        assert packet['type'] != d.PKT_TYPE_DIO
        assert packet['type'] != d.PKT_TYPE_EB
        assert 'srcMac' in packet['mac']
        assert 'dstMac' in packet['mac']

        goOn = True

        # check there is space in txQueue
        if goOn:
            if len(self.txQueue) >= d.TSCH_QUEUE_SIZE:
                # my TX queue is full

                # drop
                self.mote.drop_packet(
                    packet  = packet,
                    reason  = SimEngine.SimLog.DROPREASON_TXQUEUE_FULL,
                )

                # couldn't enqueue
                goOn = False

        # check that I have cell to transmit on
        if goOn:
            if (not self.getTxCells()) and (not self.getTxRxSharedCells()):
                # I don't have any cell to transmit on

                # drop
                self.mote.drop_packet(
                    packet  = packet,
                    reason  = SimEngine.SimLog.DROPREASON_NO_TX_CELLS,
                )

                # couldn't enqueue
                goOn = False

        # if I get here, everyting is OK, I can enqueue
        if goOn:
            # set retriesLeft which should be renewed at every hop
            packet['mac']['retriesLeft'] = d.TSCH_MAXTXRETRIES
            # add to txQueue
            self.txQueue    += [packet]

        return goOn

    # interface with radio

    def txDone(self, isACKed):
        assert isACKed in [True,False]

        asn        = self.engine.getAsn()
        slotOffset = asn % self.settings.tsch_slotframeLength
        cell       = self.schedule[slotOffset]

        assert slotOffset in self.getSchedule()
        assert d.CELLOPTION_TX in cell['cellOptions']
        assert self.waitingFor == d.WAITING_FOR_TX

        # log
        self.log(
            SimEngine.SimLog.LOG_TSCH_TXDONE,
            {
                '_mote_id':       self.mote.id,
                'channel':        self.channel,
                'packet':         self.pktToSend,
                'isACKed':        isACKed,
            }
        )

        if self.pktToSend['mac']['dstMac'] == d.BROADCAST_ADDRESS:
            # I just sent a broadcast packet

            assert self.pktToSend['type'] in [d.PKT_TYPE_EB,d.PKT_TYPE_DIO]
            assert isACKed==False

            # DIOs and EBs were never in txQueue, no need to remove

        else:
            # I just sent a unicast packet...

            # TODO send txDone up; need a more general way
            if (
                    (isACKed is True)
                    and
                    (self.pktToSend['type'] == d.PKT_TYPE_SIXP)
                ):
                self.mote.sixp.recv_mac_ack(self.pktToSend)

            # update the backoff exponent
            self._update_backoff_state(
                isRetransmission = self._is_retransmission(self.pktToSend),
                isSharedLink     = d.CELLOPTION_SHARED in cell['cellOptions'],
                isTXSuccess      = isACKed
            )

            # indicate unicast transmission to the neighbor table
            self.mote.neighbors_indicate_tx(self.pktToSend,isACKed)

            if isACKed:
                # ... which was ACKed

                # update schedule stats
                cell['numTxAck'] += 1

                # time correction
                if cell['neighbor'] == self.mote.rpl.getPreferredParent():
                    self.asnLastSync = asn # ACK-based sync

                # remove packet from queue
                self.getTxQueue().remove(self.pktToSend)

            else:
                # ... which was NOT ACKed

                # decrement 'retriesLeft' counter associated with that packet
                assert self.pktToSend['mac']['retriesLeft'] > 0
                self.pktToSend['mac']['retriesLeft'] -= 1

                # drop packet if retried too many time
                if self.pktToSend['mac']['retriesLeft'] == 0:

                    # remove packet from queue
                    self.getTxQueue().remove(self.pktToSend)

                    # drop
                    self.mote.drop_packet(
                        packet  = self.pktToSend,
                        reason  = SimEngine.SimLog.DROPREASON_MAX_RETRIES,
                    )

        # end of radio activity, not waiting for anything
        self.waitingFor = None
        self.pktToSend  = None

    def rxDone(self, packet):

        # local variables
        asn        = self.engine.getAsn()
        slotOffset = asn % self.settings.tsch_slotframeLength

        # copy the received packet to a new packet instance since the passed
        # "packet" should be kept as it is so that Connectivity can use it
        # after this rxDone() process.
        new_packet = copy.deepcopy(packet)
        packet = new_packet

        # make sure I'm in the right state
        if self.getIsSync():
            assert slotOffset in self.getSchedule()
            assert d.CELLOPTION_RX in self.getSchedule()[slotOffset]['cellOptions']
            assert self.waitingFor == d.WAITING_FOR_RX

        # not waiting for anything anymore
        self.waitingFor = None

        # abort if received nothing (idle listen)
        if packet==None:
            return False # isACKed

        # indicate reception to the neighbor table
        self.mote.neighbors_indicate_rx(packet)

        # abort if I received a frame for someone else
        if packet['mac']['dstMac'] not in [d.BROADCAST_ADDRESS, self.mote.id]:
            return False # isACKed

        # if I get here, I received a frame at the link layer (either unicast for me, or broadcast)

        # log
        self.log(
            SimEngine.SimLog.LOG_TSCH_RXDONE,
            {
                '_mote_id':        self.mote.id,
                'packet':          packet,
            }
        )

        # time correction
        if packet['mac']['srcMac'] == self.mote.rpl.getPreferredParent():
            self.asnLastSync = asn # packet-based sync

        # update schedule stats
        if self.getIsSync():
            self.getSchedule()[slotOffset]['numRx'] += 1

        if   packet['mac']['dstMac'] == self.mote.id:
            # link-layer unicast to me

            # ACK frame
            isACKed = True

            # dispatch to the right upper layer
            if   packet['type'] == d.PKT_TYPE_SIXP:
                self.mote.sixp.recv_packet(packet)
            elif 'net' in packet:
                self.mote.sixlowpan.recvPacket(packet)
            else:
                raise SystemError()

        elif packet['mac']['dstMac']==d.BROADCAST_ADDRESS:
            # link-layer broadcast

            # do NOT ACK frame (broadcast)
            isACKed = False

            # dispatch to the right upper layer
            if   packet['type'] == d.PKT_TYPE_EB:
                self._tsch_action_receiveEB(packet)
            elif 'net' in packet:
                assert packet['type']==d.PKT_TYPE_DIO
                self.mote.sixlowpan.recvPacket(packet)
            else:
                raise SystemError()

        else:
            raise SystemError()

        return isACKed

    def computeTimeOffsetToDagRoot(self):
        """
        calculate time offset compared to the DAGroot
        """

        assert self.getIsSync()
        assert self.asnLastSync!=None

        if self.mote.dagRoot:
            return 0.0

        offset               = 0.0

        child                = self.mote
        if self.mote.rpl.getPreferredParent()!=None:
            parent_id        = self.mote.rpl.getPreferredParent()
        else:
            parent_id        = self.mote.tsch.join_proxy
        parent               = self.engine.motes[parent_id]
        parents              = []
        while True:

            if (
                    (parent.id in parents)
                    or
                    (child.tsch.asnLastSync is None)
                ):
                # loop is detected or 'child' is desync-ed; return the current
                # offset value
                return offset
            else:
                # record the current parent for loop detection
                parents.append(parent.id)

            secSinceSync     = (self.engine.getAsn()-child.tsch.asnLastSync)*self.settings.tsch_slotDuration
            # FIXME: for ppm, should we not /10^6?
            relDrift         = child.tsch.drift - parent.tsch.drift  # ppm
            offset          += relDrift * secSinceSync               # us
            if parent.dagRoot:
                break
            else:
                child        = parent
                parent       = self.engine.motes[child.rpl.getPreferredParent()]

        return offset

    def remove_frame_from_tx_queue(self, type, dstMac=None):
        i = 0
        while i<len(self.txQueue):
            if (
                    (self.txQueue[i]['type'] == type)
                    and
                    (
                        (dstMac is None)
                        or
                        (self.txQueue[i]['mac']['dstMac'] == dstMac)
                    )
                ):
                del self.txQueue[i]
            else:
                i += 1

    #======================== private ==========================================

    # listeningForEB

    def tsch_schedule_next_listeningForEB_cell(self):

        assert not self.getIsSync()

        # schedule at next ASN
        self.engine.scheduleAtAsn(
            asn              = self.engine.getAsn()+1,
            cb               = self._tsch_action_listeningForEB_cell,
            uniqueTag        = (self.mote.id, '_tsch_action_listeningForEB_cell'),
            intraSlotOrder   = d.INTRASLOTORDER_STARTSLOT,
        )

    def _tsch_action_listeningForEB_cell(self):
        """
        active slot starts, while mote is listening for EBs
        """

        assert not self.getIsSync()

        # choose random channel
        channel = random.randint(0, self.settings.phy_numChans-1)

        # start listening
        self.mote.radio.startRx(
            channel = channel,
        )

        # indicate that we're waiting for the RX operation to finish
        self.waitingFor = d.WAITING_FOR_RX

        # schedule next listeningForEB cell
        self.tsch_schedule_next_listeningForEB_cell()

    # active cell

    def tsch_schedule_next_active_cell(self):

        assert self.getIsSync()

        asn        = self.engine.getAsn()
        tsCurrent  = asn % self.settings.tsch_slotframeLength

        # find closest active slot in schedule

        if not self.schedule:
            self.engine.removeFutureEvent(uniqueTag=(self.mote.id, '_tsch_action_active_cell'))
            return

        tsDiffMin             = None

        for (slotOffset, cell) in self.schedule.items():
            if   slotOffset == tsCurrent:
                tsDiff        = self.settings.tsch_slotframeLength
            elif slotOffset > tsCurrent:
                tsDiff        = slotOffset-tsCurrent
            elif slotOffset < tsCurrent:
                tsDiff        = (slotOffset+self.settings.tsch_slotframeLength)-tsCurrent
            else:
                raise SystemError()

            if (not tsDiffMin) or (tsDiff < tsDiffMin):
                tsDiffMin     = tsDiff

        # schedule at that ASN
        self.engine.scheduleAtAsn(
            asn              = asn+tsDiffMin,
            cb               = self._tsch_action_active_cell,
            uniqueTag        = (self.mote.id, '_tsch_action_active_cell'),
            intraSlotOrder   = d.INTRASLOTORDER_STARTSLOT,
        )

    def _tsch_action_active_cell(self):

        # local shorthands
        asn        = self.engine.getAsn()
        slotOffset = asn % self.settings.tsch_slotframeLength
        cell       = self.schedule[slotOffset]

        # make sure this is an active slot
        assert slotOffset in self.schedule

        # make sure we're not in the middle of a TX/RX operation
        assert self.waitingFor == None

        # make sure we are not busy sending a packet
        assert self.pktToSend == None

        # execute cell
        if cell['neighbor'] is None:
            # on a shared cell
            if sorted(cell['cellOptions']) == sorted([d.CELLOPTION_TX,d.CELLOPTION_RX,d.CELLOPTION_SHARED]):
                # on minimal cell
                # try to find a packet to neighbor to which I don't have any dedicated cell(s)...
                if not self.pktToSend:
                    for pkt in self.txQueue:
                        if  (
                                # DIOs and EBs always on minimal cell
                                (
                                    pkt['type'] in [d.PKT_TYPE_DIO,d.PKT_TYPE_EB]
                                )
                                or
                                # other frames on the minimal cell if no dedicated cells to the nextHop
                                (
                                    len(self.getTxCells(pkt['mac']['dstMac'])) == 0
                                    and
                                    len(self.getTxRxSharedCells(pkt['mac']['dstMac'])) == 0
                                )
                            ):
                            self.pktToSend = pkt
                            break

                # retransmission backoff algorithm
                if (
                        self.pktToSend
                        and
                        self._is_retransmission(self.pktToSend)
                    ):
                        if self.backoff_remaining_delay > 0:
                            # need to wait for retransmission
                            self.pktToSend = None
                            # decrement the remaining delay
                            self.backoff_remaining_delay -= 1
                        else:
                            # ready for retransmission
                            pass

                # ... if no such packet, probabilistically generate an EB or a DIO
                if not self.pktToSend:
                    if self.mote.clear_to_send_EBs_DIOs_DATA():
                        prob = self.settings.tsch_probBcast_ebDioProb/(1+self.mote.numNeighbors())
                        if random.random()<prob:
                            if random.random()<0.50:
                                if self.iAmSendingEBs:
                                    self.pktToSend = self._create_EB()
                            else:
                                if self.iAmSendingDIOs:
                                    self.pktToSend = self.mote.rpl._create_DIO()

                # send packet, or receive
                if self.pktToSend:
                    self._tsch_action_TX(self.pktToSend)
                else:
                    self._tsch_action_RX()
            else:
                # We don't support shared cells which are not [TX=1, RX=1,
                # SHARED=1]
                raise NotImplementedError()
        else:
            # on a dedicated cell

            # find a possible pktToSend first
            _pktToSend = None
            for pkt in self.txQueue:
                if pkt['mac']['dstMac'] == cell['neighbor']:
                    _pktToSend = pkt
                    break

            # retransmission backoff algorithm
            if (
                    (_pktToSend is not None)
                    and
                    (d.CELLOPTION_SHARED in cell['cellOptions'])
                    and
                    self._is_retransmission(_pktToSend)
                ):
                    if self.backoff_remaining_delay > 0:
                        # need to wait for retransmission
                        _pktToSend = None
                        # decrement the remaining delay
                        self.backoff_remaining_delay -= 1
                    else:
                        # ready for retransmission
                        pass

            if (
                    (_pktToSend is not None)
                    and
                    (d.CELLOPTION_TX in cell['cellOptions'])
                ):
                # we're going to transmit the packet
                self.pktToSend = _pktToSend
                self._tsch_action_TX(self.pktToSend)

            elif d.CELLOPTION_RX in cell['cellOptions']:
                # receive
                self._tsch_action_RX()
            else:
                # do nothing
                pass

            # notify SF
            if d.CELLOPTION_TX in cell['cellOptions']:
                self.mote.sf.indication_dedicated_tx_cell_elapsed(
                    cell    = cell,
                    used    = (self.pktToSend is not None),
                )

        # schedule next active cell
        self.tsch_schedule_next_active_cell()

    def _tsch_action_TX(self,pktToSend):

        # local shorthands
        asn        = self.engine.getAsn()
        slotOffset = asn % self.settings.tsch_slotframeLength
        cell       = self.schedule[slotOffset]

        # update cell stats
        cell['numTx'] += 1

        # Seciton 4.3 of draft-chang-6tisch-msf-01: "When NumTx reaches 256,
        # both NumTx and NumTxAck MUST be divided by 2.  That is, for example,
        # from NumTx=256 and NumTxAck=128, they become NumTx=128 and
        # NumTxAck=64."
        if cell['numTx'] == 256:
            cell['numTx']    /= 2
            cell['numTxAck'] /= 2

        # send packet to the radio
        self.mote.radio.startTx(
            channel          = cell['channelOffset'],
            packet           = pktToSend,
        )

        # indicate that we're waiting for the TX operation to finish
        self.waitingFor      = d.WAITING_FOR_TX
        self.channel         = cell['channelOffset']

    def _tsch_action_RX(self):

        # local shorthands
        asn        = self.engine.getAsn()
        slotOffset = asn % self.settings.tsch_slotframeLength
        cell       = self.schedule[slotOffset]

        # start listening
        self.mote.radio.startRx(
            channel          = cell['channelOffset'],
        )

        # indicate that we're waiting for the RX operation to finish
        self.waitingFor      = d.WAITING_FOR_RX
        self.channel         = cell['channelOffset']

    # EBs

    def _create_EB(self):

        # create
        newEB = {
            'type':               d.PKT_TYPE_EB,
            'app': {
                'join_priority':  self.mote.rpl.getDagRank(),
            },
            'mac': {
                'srcMac':         self.mote.id,            # from mote
                'dstMac':         d.BROADCAST_ADDRESS,     # broadcast
            },
        }

        # log
        self.log(
            SimEngine.SimLog.LOG_TSCH_EB_TX,
            {
                "_mote_id":  self.mote.id,
                "packet":    newEB,
            }
        )

        return newEB

    def _tsch_action_receiveEB(self, packet):

        assert packet['type'] == d.PKT_TYPE_EB

        # log
        self.log(
            SimEngine.SimLog.LOG_TSCH_EB_RX,
            {
                "_mote_id":  self.mote.id,
                "packet":    packet,
            }
        )

        # abort if I'm the root
        if self.mote.dagRoot:
            return

        if not self.getIsSync():
            # receiving EB while not sync'ed

            # I'm now sync'ed!
            self.setIsSync(True) # mote

            # the mote that sent the EB is now by join proxy
            self.join_proxy = packet['mac']['srcMac']

            # add the minimal cell to the schedule (read from EB)
            self.add_minimal_cell() # mote

            # trigger join process
            self.mote.secjoin.startJoinProcess()

    # Retransmission backoff algorithm
    def _is_retransmission(self, packet):
        assert packet is not None
        return packet['mac']['retriesLeft'] < d.TSCH_MAXTXRETRIES

    def _decide_backoff_delay(self):
        # Section 6.2.5.3 of IEEE 802.15.4-2015: "The MAC sublayer shall delay
        # for a random number in the range 0 to (2**BE - 1) shared links (on
        # any slotframe) before attempting a retransmission on a shared link."
        self.backoff_remaining_delay = random.randint(
            0,
            pow(2, self.backoff_exponent) - 1
        )

    def _reset_backoff_state(self):
        old_be = self.backoff_exponent
        self.backoff_exponent = d.TSCH_MIN_BACKOFF_EXPONENT
        self.log(
            SimEngine.SimLog.LOG_TSCH_BACKOFF_EXPONENT_UPDATED,
            {
                '_mote_id': self.mote.id,
                'old_be'  : old_be,
                'new_be'  : self.backoff_exponent
            }
        )
        self._decide_backoff_delay()

    def _increase_backoff_state(self):
        old_be = self.backoff_exponent
        # In Figure 6-6 of IEEE 802.15.4, BE (backoff exponent) is updated as
        # "BE - min(BE 0 1, macMinBe)". However, it must be incorrect. The
        # right formula should be "BE = min(BE + 1, macMaxBe)", that we apply
        # here.
        self.backoff_exponent = min(
            self.backoff_exponent + 1,
            d.TSCH_MAX_BACKOFF_EXPONENT
        )
        self.log(
            SimEngine.SimLog.LOG_TSCH_BACKOFF_EXPONENT_UPDATED,
            {
                '_mote_id': self.mote.id,
                'old_be'  : old_be,
                'new_be'  : self.backoff_exponent
            }
        )
        self._decide_backoff_delay()

    def _update_backoff_state(
            self,
            isRetransmission,
            isSharedLink,
            isTXSuccess
        ):
        if isSharedLink:
            if isTXSuccess:
                # Section 6.2.5.3 of IEEE 802.15.4-2015: "A successful
                # transmission in a shared link resets the backoff window to
                # the minimum value."
                self._reset_backoff_state()
            else:
                if isRetransmission:
                    # Section 6.2.5.3 of IEEE 802.15.4-2015: "The backoff window
                    # increases for each consecutive failed transmission in a
                    # shared link."
                    self._increase_backoff_state()
                else:
                    # First attempt to transmit the packet
                    #
                    # Section 6.2.5.3 of IEEE 802.15.4-2015: "A device upon
                    # encountering a transmission failure in a shared link
                    # shall initialize the BE to macMinBe."
                    self._reset_backoff_state()

        else:
            # dedicated link (which is different from a dedicated *cell*)
            if isTXSuccess:
                # successful transmission
                if len(self.getTxQueue()) == 0:
                    # Section 6.2.5.3 of IEEE 802.15.4-2015: "The backoff
                    # window is reset to the minimum value if the transmission
                    # in a dedicated link is successful and the transmit queue
                    # is then empty."
                    self._reset_backoff_state()
                else:
                    # Section 6.2.5.3 of IEEE 802.15.4-2015: "The backoff
                    # window does not change when a transmission is successful
                    # in a dedicated link and the transmission queue is still
                    # not empty afterwards."
                    pass
            else:
                # Section 6.2.5.3 of IEEE 802.15.4-2015: "The backoff window
                # does not change when a transmission is a failure in a
                # dedicated link."
                pass
