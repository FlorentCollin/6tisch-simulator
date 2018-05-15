"""
Secure joining layer of a mote.
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

class SecJoin(object):

    # parameters from draft-ietf-6tisch-minimal-security
    TIMEOUT_BASE          = 10
    TIMEOUT_RANDOM_FACTOR = 1.5
    MAX_RETRANSMIT        = 4

    def __init__(self, mote):

        # store params
        self.mote                           = mote

        # singletons (quicker access, instead of recreating every time)
        self.engine                         = SimEngine.SimEngine.SimEngine()
        self.settings                       = SimEngine.SimSettings.SimSettings()
        self.log                            = SimEngine.SimLog.SimLog().log

        # local variables
        self._isJoined                      = False
        self._request_timeout               = None
        self._retransmission_count          = None
        self._retransmission_tag            = (self.mote.id, '_retransmit_join_request')

    #======================== public ==========================================

    # getters/setters

    def setIsJoined(self, newState):
        assert newState in [True, False]

        # log
        self.log(
            SimEngine.SimLog.LOG_JOINED,
            {
                '_mote_id': self.mote.id,
            }
        )

        # record
        self._isJoined = newState
    def getIsJoined(self):
        return self._isJoined

    # admin

    def startJoinProcess(self):

        assert self.mote.dagRoot==False
        assert self.mote.tsch.getIsSync()==True
        assert self.mote.tsch.join_proxy!=None
        assert self.getIsJoined()==False

        if self.settings.secjoin_enabled:
            self._retransmission_count = 0

            # initialize request timeout; pick a number randomly between
            # TIMEOUT_BASE and (TIMEOUT_BASE * TIMEOUT_RANDOM_FACTOR)
            self._request_timeout  = self.TIMEOUT_BASE * random.uniform(1, self.TIMEOUT_RANDOM_FACTOR)

            self._send_join_request()
        else:
            # consider I'm already joined
            self.setIsJoined(True) # forced (secjoin_enabled==False)

    # from lower stack

    def receive(self, packet):

        if   packet['type']== d.PKT_TYPE_JOIN_REQUEST:

            if self.mote.dagRoot==False:
                # I'm the join proxy

                assert self.mote.dodagId!=None

                # proxy join request to dagRoot
                proxiedJoinRequest = {
                    'type':                 d.PKT_TYPE_JOIN_REQUEST,
                    'app': {
                        'stateless_proxy': {
                            'pledge_id':    packet['mac']['srcMac']
                        }
                    },
                    'net': {
                        'srcIp':            self.mote.id,                      # join proxy (this mote)
                        'dstIp':            self.mote.dodagId,                 # from dagRoot
                        'packet_length':    packet['net']['packet_length'],
                    },
                }

                # send proxied join response
                self.mote.sixlowpan.sendPacket(proxiedJoinRequest)

            else:
                # I'm the dagRoot

                # echo back 'stateless_proxy' element in the join response, if present in the join request
                app = {}
                if 'stateless_proxy' in packet['app']:
                    app['stateless_proxy'] = copy.deepcopy(packet['app']['stateless_proxy'])

                # format join response
                joinResponse = {
                    'type':                 d.PKT_TYPE_JOIN_RESPONSE,
                    'app':                  app,
                    'net': {
                        'srcIp':            self.mote.id,                      # from dagRoot (this mote)
                        'dstIp':            packet['net']['srcIp'],            # to join proxy
                        'packet_length':    d.PKT_LEN_JOIN_RESPONSE,
                    },
                }

                # send join response
                self.mote.sixlowpan.sendPacket(joinResponse)

        elif packet['type']== d.PKT_TYPE_JOIN_RESPONSE:
            assert self.mote.dagRoot==False

            if self.getIsJoined()==True:
                # I'm the join proxy

                if 'stateless_proxy' not in packet['app']:
                    # this must be a duplicate response; ignore it
                    pass
                else:
                    # remove the 'stateless_proxy' element from the app payload
                    app       = copy.deepcopy(packet['app'])
                    pledge_id = app['stateless_proxy']['pledge_id']
                    del app['stateless_proxy']

                    # proxy join response to pledge
                    proxiedJoinResponse = {
                        'type':                 d.PKT_TYPE_JOIN_RESPONSE,
                        'app':                  app,
                        'net': {
                            'srcIp':            self.mote.id,                      # join proxy (this mote)
                            'dstIp':            pledge_id,                         # to pledge
                            'packet_length':    packet['net']['packet_length'],
                        },
                    }

                    # send proxied join response
                    self.mote.sixlowpan.sendPacket(proxiedJoinResponse)

            else:
                # I'm the pledge

                if self._retransmission_count is None:
                    # now it's not in the middle of a secjoin process:
                    # this response corresponds to a request which this mote
                    # had sent before it left the network; ignore this response
                    return

                # cancel the event for retransmission
                self.engine.removeFutureEvent(self._retransmission_tag)

                # I'm now joined!
                self.setIsJoined(True) # mote
        else:
            raise SystemError()

    #======================== private ==========================================

    def _retransmit_join_request(self):
        if  self._retransmission_count == self.MAX_RETRANSMIT:

            # Back to listening phase, although
            # draft-ietf-6tisch-minimal-security says, "If the retransmission
            # counter reaches MAX_RETRANSMIT on a timeout, the pledge SHOULD
            # attempt to join the next advertised 6TiSCH network."
            self._request_timeout      = None
            self._retransmission_count = None
            self.mote.tsch.setIsSync(False)
            return
        elif self._retransmission_count < self.MAX_RETRANSMIT:
            # double the timeout value
            self._request_timeout *= 2
        else:
            # shouldn't happen
            assert False

        self._send_join_request()
        self._retransmission_count += 1

    def _send_join_request(self):
        # log
        self.log(
            SimEngine.SimLog.LOG_SECJOIN_TX,
            {
                '_mote_id': self.mote.id,
            }
        )

        # create join request
        joinRequest = {
            'type':                     d.PKT_TYPE_JOIN_REQUEST,
            'app': {
            },
            'net': {
                'srcIp':                self.mote.id,                      # from pledge (this mote)
                'dstIp':                self.mote.tsch.join_proxy,         # to join proxy
                'packet_length':        d.PKT_LEN_JOIN_REQUEST,
            },
        }

        # send join request
        self.mote.sixlowpan.sendPacket(joinRequest)

        # convert seconds to slots
        target_asn = (
            self.engine.getAsn() +
            int(self._request_timeout / self.settings.tsch_slotDuration)
        )
        self.engine.scheduleAtAsn(
            asn              = target_asn,
            cb               = self._retransmit_join_request,
            uniqueTag        = self._retransmission_tag,
            intraSlotOrder   = d.INTRASLOTORDER_STACKTASKS
        )
