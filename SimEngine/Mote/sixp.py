"""6top Protocol (6P) module

See SchedulingFunctionTwoStep class and SchedulingFunctionThreeStep class that
are implemented in test/test_sixp.py to get an insight of how to implement a
scheduling function with the SixP APIs defined here. SchedulingFunctionMSF
implemented in sf.py is another example to see.
"""

# =========================== imports =========================================

import copy
import random

# Mote sub-modules
import MoteDefines as d

# Simulator-wide modules
import SimEngine

# =========================== defines =========================================

class TransactionAdditionError(Exception):
    pass

# =========================== helpers =========================================

# =========================== body ============================================

class SixP(object):

    def __init__(self, mote):

        # store params
        self.mote                  = mote

        # singletons (quicker access, instead of recreating every time)
        self.engine                = SimEngine.SimEngine.SimEngine()
        self.settings              = SimEngine.SimSettings.SimSettings()
        self.log                   = SimEngine.SimLog.SimLog().log

        # local variables
        self.seqnum_table          = {} # indexed by neighbor_id
        self.transaction_table     = {} # indexed by [initiator, responder]

    # ======================= public ==========================================

    def recv_packet(self, packet):

        # log
        self.log(
            SimEngine.SimLog.LOG_SIXP_RX,
            {
                '_mote_id': self.mote.id,
                'packet':   packet
            }
        )

        if   packet['app']['msgType'] == d.SIXP_MSG_TYPE_REQUEST:
            self._recv_request(packet)
        elif packet['app']['msgType'] == d.SIXP_MSG_TYPE_RESPONSE:
            self._recv_response(packet)
        elif packet['app']['msgType'] == d.SIXP_MSG_TYPE_CONFIRMATION:
            self._recv_confirmation(packet)
        else:
            raise Exception()

    def recv_mac_ack(self, packet):
        # identify a transaction instance to proceed
        transaction = self._find_transaction(packet)

        if transaction is None:
            # ignore this ACK
            pass
        elif (
                (
                    (packet['app']['msgType'] == d.SIXP_MSG_TYPE_RESPONSE)
                    and
                    (transaction.type == d.SIXP_TRANSACTION_TYPE_2_STEP)
                )
                or
                (
                    (packet['app']['msgType'] == d.SIXP_MSG_TYPE_CONFIRMATION)
                    and
                    (transaction.type == d.SIXP_TRANSACTION_TYPE_3_STEP)
                )
            ):
            # invoke callback
            transaction.invoke_callback(
                event       = d.SIXP_CALLBACK_EVENT_MAC_ACK_RECEPTION,
                packet      = packet
            )

            # complete the transaction
            transaction.complete()
        else:
            # do nothing
            pass

    def send_request(
            self,
            dstMac,
            command,
            metadata           = None,
            cellOptions        = None,
            numCells           = None,
            cellList           = None,
            relocationCellList = None,
            candidateCellList  = None,
            offset             = None,
            maxNumCells        = None,
            payload            = None,
            callback           = None,
            timeout_value      = None
        ):

        # create a packet
        packet = self._create_packet(
            dstMac             = dstMac,
            msgType            = d.SIXP_MSG_TYPE_REQUEST,
            code               = command,
            metadata           = metadata,
            cellOptions        = cellOptions,
            numCells           = numCells,
            cellList           = cellList,
            relocationCellList = relocationCellList,
            candidateCellList  = candidateCellList,
            offset             = offset,
            maxNumCells        = maxNumCells,
            payload            = payload
        )

        # create & start a transaction
        try:
            transaction = SixPTransaction(self.mote, packet)
        except TransactionAdditionError:
            # there are another transaction in process; cannot send this request
            callback(packet, d.SIXP_CALLBACK_EVENT_FAILURE)
        else:
            # ready to send the packet
            transaction.start(callback, timeout_value)

            # enqueue
            self._tsch_enqueue(packet)

    def send_response(
            self,
            dstMac,
            return_code,
            seqNum        = None,
            numCells      = None,
            cellList      = None,
            payload       = None,
            callback      = None,
            timeout_value = None
        ):

        packet = self._create_packet(
            dstMac   = dstMac,
            msgType  = d.SIXP_MSG_TYPE_RESPONSE,
            code     = return_code,
            seqNum   = seqNum,
            numCells = numCells,
            cellList = cellList,
            payload  = payload,
        )

        # if seqNum is specified, we assume we don't have a valid transaction
        # for the response.
        if seqNum is not None:
            # do nothing
            pass
        else:
            # update the transaction
            transaction = self._find_transaction(packet)
            assert transaction is not None

            # A corresponding transaction instance is supposed to be created
            # when it receives the request.
            transaction.start(callback, timeout_value)

        # enqueue
        self._tsch_enqueue(packet)

    def send_confirmation(
            self,
            dstMac,
            return_code,
            numCells = None,
            cellList = None,
            payload  = None,
            callback = None
        ):

        packet = self._create_packet(
            dstMac   = dstMac,
            msgType  = d.SIXP_MSG_TYPE_CONFIRMATION,
            code     = return_code,
            numCells = numCells,
            cellList = cellList,
            payload  = payload
        )

        # update the transaction
        transaction = self._find_transaction(packet)
        transaction.set_callback(callback)

        # enqueue
        self._tsch_enqueue(packet)

    def add_transaction(self, transaction):
        if transaction.key in self.transaction_table:
            raise TransactionAdditionError()
        else:
            self.transaction_table[transaction.key] = transaction

    def delete_transaction(self, transaction):
        if transaction.key in self.transaction_table:
            assert transaction == self.transaction_table[transaction.key]
            del self.transaction_table[transaction.key]
        else:
            # do nothing if the transaction is not found in the table
            pass

    def increment_seqnum(self, peerMac):
        assert peerMac in self.seqnum_table.keys()
        self.seqnum_table[peerMac] += 1
        if self.seqnum_table[peerMac] == 0x100:
            # SeqNum is two-octet long and the value of 0 is treated specially
            # as the special (initial) value. Then, the next value of 0xFF
            # (255) is 0x01 (1).
            self.seqnum_table[peerMac] = 1

    # ======================= private ==========================================

    def _tsch_enqueue(self, packet):
        self.log(
            SimEngine.SimLog.LOG_SIXP_TX,
            {
                '_mote_id': self.mote.id,
                'packet':   packet
            }
        )
        self.mote.tsch.enqueue(packet)

    def _recv_request(self, request):
        # identify a transaction instance to proceed
        transaction = self._find_transaction(request)

        if transaction is None:
            # create a new transaction instance for the incoming request
            try:
                transaction = SixPTransaction(self.mote, request)
            except TransactionAdditionError:
                # We cannot have more than one transaction for the same pair of
                # initiator and responder. This is the case when a CLAER
                # transaction expires on the initiator and the transaction is
                # alive on the responder. The initiator would issue another
                # request which has SeqNum 1, but the responder still has the
                # transaction of CLEAR with SeqNum 0. In such a case, respond
                # with RC_ERR_BUSY using SeqNum of the incoming request.
                self.send_response(
                    dstMac      = request['mac']['srcMac'],
                    return_code = d.SIXP_RC_ERR_BUSY,
                    seqNum      = request['app']['seqNum']
                )
            else:
                peerMac = transaction.get_peerMac()
                if self._is_schedule_inconsistency_detected(transaction):
                    # schedule inconsistency is detected; respond with RC_ERR_SEQNUM
                    self.send_response(
                        dstMac      = peerMac,
                        return_code = d.SIXP_RC_ERR_SEQNUM
                    )
                    self.mote.sf.detect_schedule_inconsistency(peerMac)
                else:
                    # reset SeqNum when it's a CLEAR request
                    if request['app']['code'] == d.SIXP_CMD_CLEAR:
                        self._reset_seqnum(request['mac']['srcMac'])
                    # pass the incoming packet to the scheduling function
                    self.mote.sf.recv_request(request)
        else:
            # The incoming request should be duplicate one or should be a new
            # one even though the previous transaction is not completed; we
            # treat this packet as the latter case, if the incoming request is
            # not identical to the one we received before. Otherwise, a
            # response packet sent to the initiator will be handled in
            # different ways between the peers. The initiator thinks it's for
            # the second request, the responder thinks it's for the first
            # request.
            if request == transaction.request:
                # treat the incoming packet as duplicate one; ignore it
                pass
            else:
                # it seems the initiator has already terminated the transaction
                # by timeout and sent a request for a new transaction. Respond
                # with RC_ERR_BUSY.
                self.send_response(
                    dstMac      = request['mac']['srcMac'],
                    return_code = d.SIXP_RC_ERR_BUSY,
                )

    def _recv_response(self, response):
        self.transaction_table
        transaction = self._find_transaction(response)
        if transaction is None:
            # Cannot find an corresponding transaction; ignore this packet
            pass
        else:
            # invoke callback
            transaction.invoke_callback(
                event       = d.SIXP_CALLBACK_EVENT_PACKET_RECEPTION,
                packet      = response
            )

            # complete the transaction if necessary
            if transaction.type == d.SIXP_TRANSACTION_TYPE_2_STEP:
                transaction.complete()
            elif transaction.type == d.SIXP_TRANSACTION_TYPE_3_STEP:
                # the transaction is not finished yet
                pass
            else:
                # never happens
                raise Exception()

    def _recv_confirmation(self, confirmation):
        transaction = self._find_transaction(confirmation)
        if transaction is None:
            # Cannot find an corresponding transaction; ignore this packet
            pass
        else:
            # pass this to the scheduling function
            transaction.invoke_callback(
                event       = d.SIXP_CALLBACK_EVENT_PACKET_RECEPTION,
                packet      = confirmation
            )

            if transaction.type == d.SIXP_TRANSACTION_TYPE_2_STEP:
                # This shouldn't happen; ignore this packet
                pass
            elif transaction.type == d.SIXP_TRANSACTION_TYPE_3_STEP:
                # complete the transaction
                transaction.complete()
            else:
                # never happens
                raise Exception()

    def _create_packet(
            self,
            dstMac,
            msgType,
            code,
            seqNum             = None,
            metadata           = None,
            cellOptions        = None,
            numCells           = None,
            cellList           = None,
            relocationCellList = None,
            candidateCellList  = None,
            offset             = None,
            maxNumCells        = None,
            payload            = None
        ):
        packet = {
            'type'       : d.PKT_TYPE_SIXP,
            'mac': {
                'srcMac' : self.mote.id,
                'dstMac' : dstMac
            },
            'app': {
                'msgType': msgType,
                'code'   : code,
                'seqNum' : None
            }
        }

        if   msgType == d.SIXP_MSG_TYPE_REQUEST:
            # put the next SeqNum
            packet['app']['seqNum'] = self._get_seqnum(dstMac)

            # command specific
            if (
                    (code == d.SIXP_CMD_ADD)
                    or
                    (code == d.SIXP_CMD_DELETE)
                ):
                packet['app']['metadata']           = metadata
                packet['app']['cellOptions']        = cellOptions
                packet['app']['numCells']           = numCells
                packet['app']['cellList']           = cellList
            elif code == d.SIXP_CMD_RELOCATE:
                packet['app']['metadata']           = metadata
                packet['app']['cellOptions']        = cellOptions
                packet['app']['numCells']           = numCells
                packet['app']['relocationCellList'] = relocationCellList
                packet['app']['candidateCellList']  = candidateCellList
            elif code == d.SIXP_CMD_COUNT:
                packet['app']['metadata']           = metadata
                packet['app']['cellOptions']        = cellOptions
            elif code == d.SIXP_CMD_LIST:
                packet['app']['metadata']           = metadata
                packet['app']['cellOptions']        = cellOptions
                packet['app']['offset']             = offset
                packet['app']['maxNumCells']        = maxNumCells
            elif code == d.SIXP_CMD_CLEAR:
                packet['app']['metadata']           = metadata
            elif code == d.SIXP_CMD_SIGNAL:
                packet['app']['metadata']           = metadata
                packet['app']['payload']            = payload
            else:
                raise NotImplementedError()

        elif msgType in [
                d.SIXP_MSG_TYPE_RESPONSE,
                d.SIXP_MSG_TYPE_CONFIRMATION
            ]:
            transaction = self._find_transaction(packet)
            assert transaction is not None

            # put SeqNum of request unless it's requested to use a specific
            # value.
            if seqNum is None:
                packet['app']['seqNum'] = transaction.request['app']['seqNum']
            else:
                assert isinstance(seqNum, int)
                assert seqNum >= 0
                assert seqNum < 256
                packet['app']['seqNum'] = seqNum

            command = transaction.request['app']['code']
            if (
                    (command == d.SIXP_CMD_ADD)
                    or
                    (command == d.SIXP_CMD_DELETE)
                    or
                    (command == d.SIXP_CMD_RELOCATE)
                    or
                    (command == d.SIXP_CMD_LIST)
                ):
                packet['app']['cellList'] = cellList
            elif command == d.SIXP_CMD_COUNT:
                packet['app']['numCells'] = numCells
            elif command == d.SIXP_CMD_CLEAR:
                # no additional field
                pass
            elif command == d.SIXP_CMD_SIGNAL:
                packet['app']['payload']  = payload

        else:
            # shouldn't come here
            raise Exception()

        return packet

    def _get_seqnum(self, peerMac):
        if peerMac not in self.seqnum_table.keys():
            # the initial value of SeqNum is 0
            self._reset_seqnum(peerMac)
            return 0
        else:
            return self.seqnum_table[peerMac]

    def _reset_seqnum(self, peerMac):
        self.seqnum_table[peerMac] = 0

    def _find_transaction(self, packet):
        transaction_key = SixPTransaction.get_transaction_key(packet)

        if transaction_key in self.transaction_table:
            transaction = self.transaction_table[transaction_key]
            request = transaction.request
            if (
                    (packet['app']['seqNum'] is None)
                    or
                    (packet['app']['seqNum'] == request['app']['seqNum'])
                ):
                # The input packet has the same seqNum as the request has. This
                # is a valid packet for this transaction
                pass
            else:
                # The input packet is an invalid packet for this transaction
                transaction = None
        else:
            transaction = None

        return transaction

    def _is_schedule_inconsistency_detected(self, transaction):
        request = transaction.request
        peerMac = request['mac']['srcMac']

        if   request['app']['code'] == d.SIXP_CMD_CLEAR:
            returnVal = False
        elif request['app']['seqNum'] == self._get_seqnum(peerMac):
            returnVal = False
        else:
            returnVal = True

        return returnVal


class SixPTransaction(object):

    def __init__(self, mote, request):

        # sanity check
        assert request['type']           == d.PKT_TYPE_SIXP
        assert request['app']['msgType'] == d.SIXP_MSG_TYPE_REQUEST

        # keep external instances
        self.mote             = mote
        self.engine           = SimEngine.SimEngine.SimEngine()
        self.settings         = SimEngine.SimSettings.SimSettings()
        self.log              = SimEngine.SimLog.SimLog().log

        self.request          = copy.deepcopy(request)
        self.callbakc         = None
        self.type             = self._determine_transaction_type()
        self.key              = self.get_transaction_key(request)

        # for quick access
        self.seqNum           = request['app']['seqNum']
        self.initiator        = request['mac']['srcMac']
        self.responder        = request['mac']['dstMac']
        self.isInitiator      = (request['mac']['srcMac'] == self.mote.id)
        if self.isInitiator:
            self.peerMac      = self.responder
        else:
            self.peerMac      = self.initiator
        self.event_unique_tag = '{0}-{1}-{2}-{3}'.format(
            self.mote.id,
            self.initiator,
            self.responder,
            '6P-transaction-timeout'
        )

        # register itself to sixp
        self.mote.sixp.add_transaction(self)

    # ======================= public ==========================================

    @staticmethod
    def get_transaction_key(packet):
        if (
                (packet['app']['msgType'] == d.SIXP_MSG_TYPE_REQUEST)
                or
                (packet['app']['msgType'] == d.SIXP_MSG_TYPE_CONFIRMATION)
            ):
            initiator       = packet['mac']['srcMac']
            responder       = packet['mac']['dstMac']
        elif packet['app']['msgType'] == d.SIXP_MSG_TYPE_RESPONSE:
            initiator       = packet['mac']['dstMac']
            responder       = packet['mac']['srcMac']
        else:
            # shouldn't come here
            raise Exception()

        return '{0}-{1}'.format(initiator, responder)

    def get_peerMac(self):
        return self.peerMac

    def set_callback(self, callback):
        self.callback = callback

    def start(self, callback, timeout_value):
        self.set_callback(callback)

        if timeout_value is None:
            # use the default timeout value
            timeout_value = self._get_default_timeout_value()

        self.engine.scheduleAtAsn(
            asn              = self.engine.getAsn() + timeout_value,
            cb               = self._timeout_handler,
            uniqueTag        = self.event_unique_tag,
            intraSlotOrder   = d.INTRASLOTORDER_STACKTASKS,
        )

    def complete(self):
        self.log(
            SimEngine.SimLog.LOG_SIXP_TRANSACTION_COMPLETED,
            {
                '_mote_id': self.mote.id,
                'peerMac' : self.peerMac,
                'seqNum'  : self.seqNum,
                'cmd'     : self.request['app']['code']
            }
        )

        # update SeqNum managed by SixP
        self.mote.sixp.increment_seqnum(self.peerMac)

        # invalidate itself
        self._invalidate()

    def invoke_callback(self, event, packet):
        if   event in [
            d.SIXP_CALLBACK_EVENT_PACKET_RECEPTION,
            d.SIXP_CALLBACK_EVENT_MAC_ACK_RECEPTION
            ]:
            assert packet is not None
        elif event == d.SIXP_CALLBACK_EVENT_TIMEOUT:
            assert packet is None

        if self.callback is not None:
            self.callback(event, packet)

    # ======================= private ==========================================

    def _invalidate(self):
        # remove its timeout event if it exists
        self.engine.removeFutureEvent(self.event_unique_tag)

        # delete the transaction from the 6P transaction table
        self.mote.sixp.delete_transaction(self)

    def _determine_transaction_type(self):
        if (
                (
                    (self.request['app']['code'] == d.SIXP_CMD_ADD)
                    and
                    (len(self.request['app']['cellList']) == 0)
                )
                or
                (
                    (self.request['app']['code'] == d.SIXP_CMD_DELETE)
                    and
                    (len(self.request['app']['cellList']) == 0)
                )
                or
                (
                    (self.request['app']['code'] == d.SIXP_CMD_RELOCATE)
                    and
                    (len(self.request['app']['candidateCellList']) == 0)
                )
            ):
            transaction_type = d.SIXP_TRANSACTION_TYPE_3_STEP
        else:
            transaction_type = d.SIXP_TRANSACTION_TYPE_2_STEP

        return transaction_type

    def _get_default_timeout_value(self):

        # draft-ietf-6tisch-6top-protocol-11 doesn't define the default timeout
        # value.

        # When the mote has the minimal shared cell alone to
        # communicate with its peer, one-way message delay could be the largest
        # value. The first transmission could happen 101 slots after the frame
        # is enqueued. After that, retransmissions could happen. We don't the
        # current TSCH TX queue length to calculate the possible maximum delay
        # at this moment. It may be better to do so.
        be = d.TSCH_MIN_BACKOFF_EXPONENT
        be_list = []
        for i in range(d.TSCH_MAXTXRETRIES):
            be_list.append(be)
            be += 1
            if d.TSCH_MAX_BACKOFF_EXPONENT < be:
                be = d.TSCH_MAX_BACKOFF_EXPONENT
        one_way_delay = (
            self.settings.tsch_slotframeLength *
            d.TSCH_MAXTXRETRIES *
            sum(be_list)
        )

        if   (
                (self.type == d.SIXP_TRANSACTION_TYPE_2_STEP)
                and
                (self.isInitiator is False)
            ):
            # only round trip need to complete the transaction
            num_round_trips = 1
        elif (
                (
                    (self.type == d.SIXP_TRANSACTION_TYPE_2_STEP)
                    and
                    (self.isInitiator is True)
                )
                or
                (
                    (self.type == d.SIXP_TRANSACTION_TYPE_3_STEP)
                    and
                    (self.isInitiator is False)
                )
            ):
            # two round trips need to complete the transaction
            num_round_trips = 2
        elif (
                (self.type == d.SIXP_TRANSACTION_TYPE_3_STEP)
                and
                (self.isInitiator is True)
            ):
            # three round trips need to complete the transaction
            num_round_trips = 3
        else:
            raise Exception()

        return one_way_delay * num_round_trips

    def _timeout_handler(self):
        self.log(
            SimEngine.SimLog.LOG_SIXP_TRANSACTION_TIMEOUT,
            {
                '_mote_id': self.mote.id,
                'peerMac' : self.peerMac,
                'seqNum'  : self.seqNum,
                'cmd'     : self.request['app']['code']
            }
        )

        self._invalidate()

        # need to invoke the callback after the invalidation; otherwise, a new
        # transaction to the same peer would fail due to duplicate (concurrent)
        # transaction.
        self.invoke_callback(
            event       = d.SIXP_CALLBACK_EVENT_TIMEOUT,
            packet      = None
        )
