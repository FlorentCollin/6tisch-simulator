"""
6LoWPAN layer including reassembly/fragmentation
"""

# =========================== imports =========================================

from abc import abstractmethod
import copy
import math
import random

import netaddr

# Simulator-wide modules
import SimEngine
import MoteDefines as d

# =========================== defines =========================================

# =========================== helpers =========================================

# =========================== body ============================================

class Sixlowpan(object):

    def __init__(self, mote):

        # store params
        self.mote                 = mote

        # singletons (quicker access, instead of recreating every time)
        self.settings             = SimEngine.SimSettings.SimSettings()
        self.engine               = SimEngine.SimEngine.SimEngine()
        self.log                  = SimEngine.SimLog.SimLog().log

        # local variables
        self.fragmentation        = globals()[self.settings.fragmentation](self)

        self.on_link_neighbor_list = []

    #======================== public ==========================================

    def sendPacket(self, packet):
        assert sorted(packet.keys()) == sorted(['type','app','net'])
        assert packet['type'] in [
            d.PKT_TYPE_JOIN_REQUEST,
            d.PKT_TYPE_JOIN_RESPONSE,
            d.PKT_TYPE_DIS,
            d.PKT_TYPE_DIO,
            d.PKT_TYPE_DAO,
            d.PKT_TYPE_DATA,
        ]
        assert 'srcIp' in packet['net']
        assert 'dstIp' in packet['net']

        goOn = True

        # put hop_limit field to the net header
        packet['net']['hop_limit'] = d.IPV6_DEFAULT_HOP_LIMIT

        # mark a downward packet like 'O' option in RPL Option defined by RFC
        # 6553
        if self.mote.dagRoot:
            packet['net']['downward'] = True
        else:
            packet['net']['downward'] = False

        # log
        self.log(
            SimEngine.SimLog.LOG_SIXLOWPAN_PKT_TX,
            {
                '_mote_id':       self.mote.id,
                'packet':         packet,
            }
        )

        # add source route, if needed
        if goOn:
            if (
                    (self.mote.dagRoot)
                    and
                    ((netaddr.IPAddress(packet['net']['srcIp']).words[0] & 0xFE80) != 0xFE80)
                ):
                sourceRoute = self.mote.rpl.computeSourceRoute(packet['net']['dstIp'])
                if sourceRoute==None:

                    # we cannot find a next-hop; drop this packet
                    self.mote.drop_packet(
                        packet  = packet,
                        reason  = SimEngine.SimLog.DROPREASON_NO_ROUTE,
                    )

                    # stop handling this packet
                    goOn = False
                else:
                    assert 1 <= len(sourceRoute)
                    packet['net']['dstIp'] = sourceRoute.pop(0)
                    if len(sourceRoute) > 0:
                        packet['net']['sourceRoute'] = sourceRoute

        # find link-layer destination
        if goOn:
            dstMac = self._find_nexthop_mac_addr(packet)
            if dstMac == None:
                # we cannot find a next-hop; drop this packet
                self.mote.drop_packet(
                    packet  = packet,
                    reason  = SimEngine.SimLog.DROPREASON_NO_ROUTE,
                )
                # stop handling this packet
                goOn = False

        # add MAC header
        if goOn:
            packet['mac'] = {
                'srcMac': self.mote.get_mac_addr(),
                'dstMac': dstMac
            }

        # cut packet into fragments
        if goOn:
            frags = self.fragmentation.fragmentPacket(packet)

        # enqueue each fragment
        if goOn:
            for frag in frags:
                self.mote.tsch.enqueue(frag)

    def recvPacket(self, packet):

        assert packet['type'] in [
            d.PKT_TYPE_DATA,
            d.PKT_TYPE_DIS,
            d.PKT_TYPE_DIO,
            d.PKT_TYPE_DAO,
            d.PKT_TYPE_FRAG,
            d.PKT_TYPE_JOIN_REQUEST,
            d.PKT_TYPE_JOIN_RESPONSE,
        ]

        goOn = True

        # log
        self.log(
            SimEngine.SimLog.LOG_SIXLOWPAN_PKT_RX,
            {
                '_mote_id':        self.mote.id,
                'packet':          packet,
            }
        )

        # add the source mode to the neighbor_cache if it's on-link
        # FIXME: IPv6 prefix should be examined
        if self._is_on_link_neighbor(packet['mac']['srcMac']) is False:
            self._add_on_link_neighbor(packet['mac']['srcMac'])

        # hand fragment to fragmentation sublayer. Returns a packet to process further, or else stop.
        if goOn:
            if packet['type'] == d.PKT_TYPE_FRAG:
                packet = self.fragmentation.fragRecv(packet)
                if not packet:
                    goOn = False

            # source routing header
            elif 'sourceRoute' in packet['net']:
                packet['net']['dstIp'] = packet['net']['sourceRoute'].pop(0)
                if len(packet['net']['sourceRoute']) == 0:
                    del packet['net']['sourceRoute']

        # handle packet
        if goOn:
            if  (
                    packet['type']!=d.PKT_TYPE_FRAG # in case of fragment forwarding
                    and
                    (
                        (self.mote.is_my_ipv6_addr(packet['net']['dstIp']))
                        or
                        (packet['mac']['dstMac'] == d.BROADCAST_ADDRESS)
                    )
                ):
                # packet for me

                # dispatch to upper component
                if   packet['type'] in [d.PKT_TYPE_JOIN_REQUEST,d.PKT_TYPE_JOIN_RESPONSE]:
                    self.mote.secjoin.receive(packet)
                elif packet['type'] == d.PKT_TYPE_DAO:
                    self.mote.rpl.action_receiveDAO(packet)
                elif packet['type'] == d.PKT_TYPE_DIO:
                    self.mote.rpl.action_receiveDIO(packet)
                elif packet['type'] == d.PKT_TYPE_DIS:
                    self.mote.rpl.action_receiveDIS(packet)
                elif packet['type'] == d.PKT_TYPE_DATA:
                    self.mote.app.recvPacket(packet)

            else:
                # packet not for me

                # check if there is a possible routing loop by seeing the
                # packet source address
                preferred_parent_mac_addr = self.mote.rpl.getPreferredParent()
                if (
                        (
                            ('downward' not in packet['net'])
                            or
                            (packet['net']['downward'] is False)
                        )
                        and
                        (preferred_parent_mac_addr is not None)
                        and
                        (packet['mac']['srcMac'] == preferred_parent_mac_addr)
                    ):
                    # we received an upward packet from our parent
                    if (
                            ('rank_error' in packet['net'])
                            and
                            (packet['net']['rank_error'] is True)
                        ):
                        # this packet should be discarded
                        # https://tools.ietf.org/html/rfc6550#section-11.2.2
                        self.mote.drop_packet(
                            packet = packet,
                            reason = SimEngine.SimLog.DROPREASON_RANK_ERROR
                        )
                        # reset Trickle timer
                        self.mote.rpl.trickle_timer.reset()
                        return
                    else:
                        # set Rank-Error and forward this packet
                        packet['net']['rank_error'] = True

                # forward
                self.forward(packet)

    def forward(self, rxPacket):
        # packet can be:
        # - an IPv6 packet (which may need fragmentation)
        # - a fragment (fragment forwarding)

        assert 'type' in rxPacket
        assert 'net' in rxPacket

        goOn = True

        if (
                ('hop_limit' in rxPacket['net'])
                and
                (rxPacket['net']['hop_limit'] < 2)
            ):
            # we shouldn't receive any frame having hop_limit of 0
            assert rxPacket['net']['hop_limit'] == 1
            self.mote.drop_packet(
                packet = rxPacket,
                reason = SimEngine.SimLog.DROPREASON_TIME_EXCEEDED
            )
            goOn = False

        # === create forwarded packet
        if goOn:
            fwdPacket             = {}
            # type
            fwdPacket['type']     = copy.deepcopy(rxPacket['type'])
            # app
            if 'app' in rxPacket:
                fwdPacket['app']  = copy.deepcopy(rxPacket['app'])
            # net
            fwdPacket['net']      = copy.deepcopy(rxPacket['net'])
            if 'hop_limit' in fwdPacket['net']:
                assert fwdPacket['net']['hop_limit'] > 1
                fwdPacket['net']['hop_limit'] -= 1

            # mac
            if fwdPacket['type'] == d.PKT_TYPE_FRAG:
                # fragment already has mac header (FIXME: why?)
                fwdPacket['mac']  = copy.deepcopy(rxPacket['mac'])
            else:
                # find next hop
                dstMac = self._find_nexthop_mac_addr(fwdPacket)
                if dstMac==None:
                    # we cannot find a next-hop; drop this packet
                    self.mote.drop_packet(
                        packet  = rxPacket,
                        reason  = SimEngine.SimLog.DROPREASON_NO_ROUTE,
                    )
                    # stop handling this packet
                    goOn = False
                else:
                    # add MAC header
                    fwdPacket['mac'] = {
                        'srcMac': self.mote.get_mac_addr(),
                        'dstMac': dstMac
                    }

        # log
        if goOn:
            self.log(
                SimEngine.SimLog.LOG_SIXLOWPAN_PKT_FWD,
                {
                    '_mote_id':       self.mote.id,
                    'packet':         fwdPacket,
                }
            )

        # cut the forwarded packet into fragments
        if goOn:
            if fwdPacket['type']==d.PKT_TYPE_FRAG:
                fwdFrags = [fwdPacket] # don't re-frag a frag
            else:
                fwdFrags = self.fragmentation.fragmentPacket(fwdPacket)

        # enqueue all frags
        if goOn:
            for fwdFrag in fwdFrags:
                self.mote.tsch.enqueue(fwdFrag)

    #======================== private ==========================================

    def _add_on_link_neighbor(self, mac_addr):
        # FIXME: we may need _add_on_link_neighbor() as well
        self.on_link_neighbor_list.append(mac_addr)
        self.mote.sf.indication_neighbor_added(mac_addr)

    def _is_on_link_neighbor(self, mac_addr):
        return mac_addr in self.on_link_neighbor_list

    def _find_nexthop_mac_addr(self, packet):
        mac_addr = None
        src_ip_addr = netaddr.IPAddress(packet['net']['srcIp'])
        dst_ip_addr = netaddr.IPAddress(packet['net']['dstIp'])
        # use lower 64 bits and invert U/L bit
        derived_dst_mac = str(
            netaddr.EUI(
                (int(dst_ip_addr) & 0xFFFFFFFFFFFFFFFF) ^ 0x0200000000000000
            )
        )

        if (dst_ip_addr.words[0] & 0xFF00) == 0xFF00:
            # this is an IPv6 multicast address
            mac_addr = d.BROADCAST_ADDRESS

        elif self.mote.dagRoot:
            if derived_dst_mac in self.on_link_neighbor_list:
                # on-link
                mac_addr = derived_dst_mac
            else:
                # off-link
                mac_addr = None
        else:
            if self.mote.rpl.dodagId is None:
                # upward during secure join process
                mac_addr = str(self.mote.tsch.join_proxy)
            elif (
                    (
                        ((src_ip_addr.words[0] & 0xFE80) == 0xFE80)
                    )
                    or
                    (
                        ('downward' in packet['net'])
                        and
                        (packet['net']['downward'] is True)
                    )
                ):
                if derived_dst_mac in self.on_link_neighbor_list:
                    # on-link
                    mac_addr = derived_dst_mac
                else:
                    mac_addr = None
            else:
                # use the default router (preferred parent)
                mac_addr = self.mote.rpl.getPreferredParent()

        return mac_addr


class Fragmentation(object):
    """The base class for forwarding implementations of fragments
    """

    def __init__(self, sixlowpan):

        # store params
        self.sixlowpan            = sixlowpan

        # singletons (quicker access, instead of recreating every time)
        self.settings             = SimEngine.SimSettings.SimSettings()
        self.engine               = SimEngine.SimEngine.SimEngine()
        self.log                  = SimEngine.SimLog.SimLog().log

        # local variables
        self.mote                 = sixlowpan.mote
        self.next_datagram_tag    = random.randint(0, 2**16-1)
        # "reassembly_buffers" has mote instances as keys. Each value is a list.
        # A list is indexed by incoming datagram_tags.
        #
        # An element of the list a dictionary consisting of three key-values:
        # "net", "expiration" and "fragments".
        #
        # - "net" has srcIp and dstIp of the packet
        # - "fragments" holds received fragments, although only their
        # datagram_offset and lengths are stored in the "fragments" list.
        self.reassembly_buffers   = {}

    #======================== public ==========================================

    def fragmentPacket(self, packet):
        """Fragments a packet into fragments

        Returns a list of fragments, possibly with one element.

        First fragment (no app field):
            {
                'net': {
                    'srcIp':                src_ip_address,
                    'dstIp':                dst_ip_address,
                    'hop_limit':            hop_limit,
                    'packet_length':        packet_length,
                    'datagram_size':        original_packet_length,
                    'datagram_tag':         tag_for_the_packet,
                    'datagram_offset':      offset_for_this_fragment,
                   ['sourceRoute':          [...]]
                }
            }

        Subsequent fragments (no app, no srcIp/dstIp):
            {
                'net': {
                    'packet_length':        packet_length,
                    'datagram_size':        original_packet_length,
                    'datagram_tag':         tag_for_the_packet,
                    'datagram_offset':      offset_for_this_fragment,
                }
            }

        Last fragment (app, no srcIp/dstIp):
            {
                'app':                      (if applicable)
                'net': {
                    'packet_length':        packet_length,
                    'datagram_size':        original_packet_length,
                    'datagram_tag':         tag_for_the_packet,
                    'datagram_offset':      offset_for_this_fragment,
                    'original_packet_type': original_packet_type,
                }
            }
        """
        assert packet['type'] in [
            d.PKT_TYPE_DATA,
            d.PKT_TYPE_DIS,
            d.PKT_TYPE_DIO,
            d.PKT_TYPE_DAO,
            d.PKT_TYPE_JOIN_REQUEST,
            d.PKT_TYPE_JOIN_RESPONSE,
        ]
        assert 'type' in packet
        assert 'net'  in packet

        returnVal = []

        if  self.settings.tsch_max_payload_len < packet['net']['packet_length']:
            # the packet needs fragmentation

            # choose tag (same for all fragments)
            outgoing_datagram_tag = self._get_next_datagram_tag()
            number_of_fragments   = int(math.ceil(float(packet['net']['packet_length']) / self.settings.tsch_max_payload_len))
            datagram_offset       = 0

            for i in range(0, number_of_fragments):

                # common part of fragment packet
                fragment = {
                    'type':                d.PKT_TYPE_FRAG,
                    'net': {
                        'datagram_size':   packet['net']['packet_length'],
                        'datagram_tag':    outgoing_datagram_tag,
                        'datagram_offset': datagram_offset
                    }
                }

                # put additional fields to the first and the last fragment
                if   i == 0:
                    # first fragment

                    # copy 'net' header
                    for key, value in packet['net'].items():
                        fragment['net'][key] = value
                    if 'sourceRoute' in packet['net']:
                        fragment['net']['sourceRoute']      = copy.deepcopy(packet['net']['sourceRoute'])
                elif i == (number_of_fragments - 1):
                    # the last fragment

                    # add original_packet_type and 'app' field
                    fragment['app']                         = copy.deepcopy(packet['app'])
                    fragment['net']['original_packet_type'] = packet['type']

                # populate packet_length
                if  (
                        (i == 0) and
                        ((packet['net']['packet_length'] % self.settings.tsch_max_payload_len) > 0)
                    ):
                    # slop is in the first fragment
                    fragment['net']['packet_length'] = packet['net']['packet_length'] % self.settings.tsch_max_payload_len
                else:
                    fragment['net']['packet_length'] = self.settings.tsch_max_payload_len

                # update datagram_offset which will be used for the next fragment
                datagram_offset += fragment['net']['packet_length']

                # copy the MAC header
                fragment['mac'] = copy.deepcopy(packet['mac'])

                # add the fragment to a returning list
                returnVal += [fragment]

                # log
                self.log(
                    SimEngine.SimLog.LOG_SIXLOWPAN_FRAG_GEN,
                    {
                        '_mote_id': self.mote.id,
                        'packet':   fragment
                    }
                )

        else:
            # the input packet doesn't need fragmentation
            returnVal += [packet]

        return returnVal

    @abstractmethod
    def fragRecv(self, fragment):
        """This method is supposed to return a packet to be processed further

        This could return None.
        """
        raise NotImplementedError() # abstractmethod

    def reassemblePacket(self, fragment):
        srcMac                    = fragment['mac']['srcMac']
        datagram_size             = fragment['net']['datagram_size']
        datagram_offset           = fragment['net']['datagram_offset']
        incoming_datagram_tag     = fragment['net']['datagram_tag']
        buffer_lifetime           = d.SIXLOWPAN_REASSEMBLY_BUFFER_LIFETIME / self.settings.tsch_slotDuration

        self._delete_expired_reassembly_buffer()

        # make sure we can allocate a reassembly buffer if necessary
        if (srcMac not in self.reassembly_buffers) or (incoming_datagram_tag not in self.reassembly_buffers[srcMac]):
            # dagRoot has no memory limitation for reassembly buffer
            if not self.mote.dagRoot:
                total_reassembly_buffers_num = 0
                for i in self.reassembly_buffers:
                    total_reassembly_buffers_num += len(self.reassembly_buffers[i])
                if total_reassembly_buffers_num == self.settings.sixlowpan_reassembly_buffers_num:
                    # no room for a new entry
                    self.mote.drop_packet(
                        packet = fragment,
                        reason = SimEngine.SimLog.DROPREASON_REASSEMBLY_BUFFER_FULL,
                    )
                    return

            # create a new reassembly buffer
            if srcMac not in self.reassembly_buffers:
                self.reassembly_buffers[srcMac] = {}
            if incoming_datagram_tag not in self.reassembly_buffers[srcMac]:
                self.reassembly_buffers[srcMac][incoming_datagram_tag] = {
                    'expiration': self.engine.getAsn() + buffer_lifetime,
                    'fragments': []
                }

        if datagram_offset not in map(lambda x: x['datagram_offset'], self.reassembly_buffers[srcMac][incoming_datagram_tag]['fragments']):

            if fragment['net']['datagram_offset'] == 0:
                # store srcIp and dstIp which only the first fragment has
                self.reassembly_buffers[srcMac][incoming_datagram_tag]['net'] = copy.deepcopy(fragment['net'])
                del self.reassembly_buffers[srcMac][incoming_datagram_tag]['net']['datagram_size']
                del self.reassembly_buffers[srcMac][incoming_datagram_tag]['net']['datagram_offset']
                del self.reassembly_buffers[srcMac][incoming_datagram_tag]['net']['datagram_tag']

            self.reassembly_buffers[srcMac][incoming_datagram_tag]['fragments'].append({
                'datagram_offset': datagram_offset,
                'fragment_length': fragment['net']['packet_length']
            })
        else:
            # it's a duplicate fragment
            return

        # check whether we have a full packet in the reassembly buffer
        total_fragment_length = sum([f['fragment_length'] for f in self.reassembly_buffers[srcMac][incoming_datagram_tag]['fragments']])
        assert total_fragment_length <= datagram_size
        if total_fragment_length < datagram_size:
            # reassembly is not completed
            return

        # construct an original packet
        packet = copy.copy(fragment)
        packet['type'] = fragment['net']['original_packet_type']
        packet['net'] = copy.deepcopy(self.reassembly_buffers[srcMac][incoming_datagram_tag]['net'])
        packet['net']['packet_length'] = datagram_size

        # reassembly is done, delete buffer
        del self.reassembly_buffers[srcMac][incoming_datagram_tag]
        if len(self.reassembly_buffers[srcMac]) == 0:
            del self.reassembly_buffers[srcMac]

        return packet

    # ======================= private =========================================    

    def _get_next_datagram_tag(self):
        ret = self.next_datagram_tag
        self.next_datagram_tag = (ret + 1) % 65536
        return ret

    def _delete_expired_reassembly_buffer(self):
        if len(self.reassembly_buffers) == 0:
            return

        for srcMac in self.reassembly_buffers.keys():
            for incoming_datagram_tag in self.reassembly_buffers[srcMac].keys():
                # delete expired reassembly buffer
                if self.reassembly_buffers[srcMac][incoming_datagram_tag]['expiration'] < self.engine.getAsn():
                    del self.reassembly_buffers[srcMac][incoming_datagram_tag]

            # delete an reassembly buffer entry if it's empty
            if len(self.reassembly_buffers[srcMac]) == 0:
                del self.reassembly_buffers[srcMac]

class PerHopReassembly(Fragmentation):
    """
    RFC4944-like per-hop fragmentation and reassembly.
    """
    #======================== public ==========================================

    def fragRecv(self, fragment):
        """Reassemble an original packet
        """
        return self.reassemblePacket(fragment)


class FragmentForwarding(Fragmentation):
    """
    Fragment forwarding, per https://tools.ietf.org/html/draft-watteyne-6lo-minimal-fragment
    """

    def __init__(self, sixlowpan):
        super(FragmentForwarding, self).__init__(sixlowpan)
        self.vrb_table       = {}

    #======================== public ==========================================

    def fragRecv(self, fragment):

        srcMac                = fragment['mac']['srcMac']
        datagram_size         = fragment['net']['datagram_size']
        datagram_offset       = fragment['net']['datagram_offset']
        incoming_datagram_tag = fragment['net']['datagram_tag']
        packet_length         = fragment['net']['packet_length']
        entry_lifetime        = d.SIXLOWPAN_VRB_TABLE_ENTRY_LIFETIME / self.settings.tsch_slotDuration

        self._delete_expired_vrb_table_entry()

        # handle first fragments
        if datagram_offset == 0:

            if self.mote.is_my_ipv6_addr(fragment['net']['dstIp']) is False:

                dstMac = self.sixlowpan._find_nexthop_mac_addr(fragment)
                if dstMac == None:
                    # no route to the destination
                    return

            # check if we have enough memory for a new entry if necessary
            if self.mote.dagRoot:
                # dagRoot has no memory limitation for VRB Table
                pass
            else:
                total_vrb_table_entry_num = sum([len(e) for _, e in self.vrb_table.items()])
                assert total_vrb_table_entry_num <= self.settings.fragmentation_ff_vrb_table_size
                if total_vrb_table_entry_num == self.settings.fragmentation_ff_vrb_table_size:
                    # no room for a new entry
                    self.mote.drop_packet(
                        packet = fragment,
                        reason = SimEngine.SimLog.DROPREASON_VRB_TABLE_FULL,
                    )
                    return


            if srcMac not in self.vrb_table:
                self.vrb_table[srcMac] = {}

            # By specification, a VRB Table entry is supposed to have:
            # - incoming srcMac
            # - incoming datagram_tag
            # - outgoing dstMac (nexthop)
            # - outgoing datagram_tag

            if incoming_datagram_tag in self.vrb_table[srcMac]:
                # duplicate first fragment is silently discarded
                return
            else:
                self.vrb_table[srcMac][incoming_datagram_tag] = {}

            if self.mote.is_my_ipv6_addr(fragment['net']['dstIp']):
                # this is a special entry for fragments destined to the mote
                self.vrb_table[srcMac][incoming_datagram_tag]['outgoing_datagram_tag'] = None
            else:
                self.vrb_table[srcMac][incoming_datagram_tag]['dstMac']                = dstMac
                self.vrb_table[srcMac][incoming_datagram_tag]['outgoing_datagram_tag'] = self._get_next_datagram_tag()

            self.vrb_table[srcMac][incoming_datagram_tag]['expiration'] = self.engine.getAsn() + entry_lifetime

            if 'missing_fragment' in self.settings.fragmentation_ff_discard_vrb_entry_policy:
                self.vrb_table[srcMac][incoming_datagram_tag]['next_offset'] = 0

        # when missing_fragment is in discard_vrb_entry_policy
        # - if the incoming fragment is the expected one, update the next_offset
        # - otherwise, delete the corresponding VRB table entry
        if (
                ('missing_fragment' in self.settings.fragmentation_ff_discard_vrb_entry_policy) and
                (srcMac in self.vrb_table) and
                (incoming_datagram_tag in self.vrb_table[srcMac])
           ):
            if datagram_offset == self.vrb_table[srcMac][incoming_datagram_tag]['next_offset']:
                self.vrb_table[srcMac][incoming_datagram_tag]['next_offset'] += packet_length
            else:
                del self.vrb_table[srcMac][incoming_datagram_tag]
                if len(self.vrb_table[srcMac]) == 0:
                    del self.vrb_table[srcMac]

        # find entry in VRB table and forward fragment
        if (srcMac in self.vrb_table) and (incoming_datagram_tag in self.vrb_table[srcMac]):
            # VRB entry found!

            if self.vrb_table[srcMac][incoming_datagram_tag]['outgoing_datagram_tag'] is None:
                # fragment for me: do not forward but reassemble. ret will have
                # either a original packet or None
                ret = self.reassemblePacket(fragment)

            else:
                # need to create a new packet in order to distinguish between the
                # received packet and a forwarding packet.
                fwdFragment = {
                    'type':       copy.deepcopy(fragment['type']),
                    'net':        copy.deepcopy(fragment['net']),
                    'mac': {
                        'srcMac': self.mote.get_mac_addr(),
                        'dstMac': self.vrb_table[srcMac][incoming_datagram_tag]['dstMac']
                    }
                }

                # forwarding fragment should have the outgoing datagram_tag
                fwdFragment['net']['datagram_tag'] = self.vrb_table[srcMac][incoming_datagram_tag]['outgoing_datagram_tag']

                # copy app field if necessary
                if 'app' in fragment:
                    fwdFragment['app'] = copy.deepcopy(fragment['app'])

                ret = fwdFragment

        else:
            # no VRB table entry is found
            ret = None

        # when last_fragment is in discard_vrb_entry_policy
        # - if the incoming fragment is the last fragment of a packet, delete the corresponding entry
        # - otherwise, do nothing
        if (
                ('last_fragment' in self.settings.fragmentation_ff_discard_vrb_entry_policy)
                and
                (srcMac in self.vrb_table)
                and
                (incoming_datagram_tag in self.vrb_table[srcMac])
                and
                ((datagram_offset + packet_length) == datagram_size)
           ):
            del self.vrb_table[srcMac][incoming_datagram_tag]
            if len(self.vrb_table[srcMac]) == 0:
                del self.vrb_table[srcMac]

        return ret

    #======================== private ==========================================

    def _delete_expired_vrb_table_entry(self):
        if len(self.vrb_table) == 0:
            return

        for srcMac in self.vrb_table.keys():
            for incoming_datagram_tag in self.vrb_table[srcMac].keys():
                # too old
                if self.vrb_table[srcMac][incoming_datagram_tag]['expiration'] < self.engine.getAsn():
                    del self.vrb_table[srcMac][incoming_datagram_tag]
            # empty
            if len(self.vrb_table[srcMac]) == 0:
                del self.vrb_table[srcMac]
