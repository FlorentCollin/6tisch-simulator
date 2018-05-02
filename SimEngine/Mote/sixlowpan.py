"""
6LoWPAN layer including reassembly/fragmentation
"""

# =========================== imports =========================================

from abc import abstractmethod
import copy
import math
import random

# Simulator-wide modules
import SimEngine
import MoteDefines as d

# =========================== defines =========================================

# =========================== helpers =========================================

# =========================== body ============================================

class Sixlowpan(object):

    def __init__(self, mote):
        self.mote                 = mote
        self.settings             = SimEngine.SimSettings.SimSettings()
        self.engine               = SimEngine.SimEngine.SimEngine()
        self.log                  = SimEngine.SimLog.SimLog().log
        self.fragmentation        = globals()[self.settings.fragmentation](self)
        self.next_datagram_tag    = random.randint(0, 2**16-1)
        # "reassembly_buffers" has mote instances as keys. Each value is a list.
        # A list is indexed by incoming datagram_tags.
        # An element of the list a dictionary consisting of two key-values:
        # "expiration" and "fragments".
        # "fragments" holds received fragments, although only their
        # datagram_offset and lengths are stored in the "fragments" list.
        self.reassembly_buffers    = {}

    #======================== public ==========================================
    
    def send(self, packet):
        
        assert sorted(packet.keys()) == sorted(['type','net','app'])
        
        # log
        self.log(
            SimEngine.SimLog.LOG_SIXLOWPAN_PKT_TX,
            {
                '_mote_id':       self.mote.id,
                'packet':         packet,
            }
        )
        
        for p in self.fragment(packet):
            # fragment() returns a list of sending fragments. It could return a
            # list having one packet when the packet doesn't need fragmentation
            self.mote.tsch.enqueue(p)
    
    def fragment(self, packet):
        """Fragment the input packet into fragments

        This method returns fragments in a list. If the input packet doesn't
        need fragmentation, this methods returns the packet in a list.

        Formats of fragment packets are shown below:

            The format of the first fragment (no app field):
            {
                'net': {
                    'srcIp':                src_ip_address,
                    'dstIp':                dst_ip_address,
                    'packet_length':        packet_length,
                    'datagram_size':        original_packet_length,
                    'datagram_tag':         tag_for_the_packet,
                    'datagram_offset':      offset_for_this_fragment,
                    'original_packet_type': original_packet_type
                }
            }

            The format of the last fragment (no srcIp/dstIp):
            {
                'net': {
                    'packet_length':        packet_length,
                    'datagram_size':        original_packet_length,
                    'datagram_tag':         tag_for_the_packet,
                    'datagram_offset':      offset_for_this_fragment,
                }
                'app':                      (if applicable)
            }

            The format of other fragments (neither app nor srcIp/dstIp):
            {
                'net': {
                    'packet_length':        packet_length,
                    'datagram_size':        original_packet_length,
                    'datagram_tag':         tag_for_the_packet,
                    'datagram_offset':      offset_for_this_fragment,
                }
            }
        """
        # choose tag (same for all fragments)
        outgoing_datagram_tag = self._get_next_datagram_tag()

        if packet['payload']['length'] > self.settings.tsch_max_payload_len:
            # the packet needs fragmentation
            ret = []
            number_of_fragments = int(math.ceil(float(packet['payload']['length']) / self.settings.tsch_max_payload_len))
            datagram_offset = 0
            for i in range(0, number_of_fragments):

                # copy (fake) contents of the packets in fragment
                fragment = copy.copy(packet)
                # change fields so looks like fragment
                fragment['type']                        = d.APP_TYPE_FRAG
                fragment['payload']                     = copy.deepcopy(packet['payload'])
                fragment['payload']['original_type']    = packet['type']
                fragment['payload']['datagram_size']    = packet['payload']['length']
                fragment['payload']['datagram_tag']     = outgoing_datagram_tag
                fragment['payload']['datagram_offset']  = datagram_offset

                if (
                        (i == 0) and
                        ((packet['payload']['length'] % self.settings.tsch_max_payload_len) > 0)
                   ):
                    # Make the first fragment have some room to handle size changes
                    # of the compressed header, although header compaction and
                    # inflation have been not implemented yet.
                    fragment['payload']['length'] = packet['payload']['length'] % self.settings.tsch_max_payload_len
                else:
                    fragment['payload']['length'] = self.settings.tsch_max_payload_len

                datagram_offset        += fragment['payload']['length']
                fragment['sourceRoute'] = copy.deepcopy(packet['sourceRoute'])

                # add the fragment to a returning list
                ret.append(fragment)

                # log
                self.log(
                    SimEngine.SimLog.LOG_SIXLOWPAN_FRAG_GEN,
                    {
                        '_mote_id':              self.mote.id,
                        'srcIp':                 fragment['srcIp'].id,
                        'dstIp':                 fragment['dstIp'].id,
                        'datagram_size':         fragment['payload']['datagram_size'],
                        'datagram_offset':       fragment['payload']['datagram_offset'],
                        'datagram_tag':          fragment['payload']['datagram_tag'],
                        'length':                fragment['payload']['length'],
                        'original_packet_type':  fragment['payload']['original_type']
                    }
                )

        else:
            # the input packet doesn't need fragmentation
            ret = [packet]

        return ret

    def forward(self, packet):
        
        # forwarded packet still has the old mac header
        assert sorted(packet.keys()) == sorted(['type','app','net','mac'])
        
        # keep only headers that are needed
        fwdPacket = {
            'type':     copy.deepcopy(packet['type']),
            'app':      copy.deepcopy(packet['app']),
            'net':      copy.deepcopy(packet['net']),
        }
        
        # log
        self.log(
            SimEngine.SimLog.LOG_SIXLOWPAN_PKT_FWD,
            {
                '_mote_id':       self.mote.id,
                'packet':         fwdPacket,
            }
        )
        
        # fragment, if needed
        if self.settings.tsch_max_payload_len < fwdPacket['app']['app_payload_length']:
            # packet doesn't fit into a single frame; needs fragmentation
            self.fragment(fwdPacket) # FIXME: pass fragmentation to new packet format
        else:
            self.mote.tsch.enqueue(fwdPacket)
    
    def reassemble(self, smac, fragment):
        datagram_size         = fragment['payload']['datagram_size']
        datagram_offset       = fragment['payload']['datagram_offset']
        incoming_datagram_tag = fragment['payload']['datagram_tag']
        buffer_lifetime  = d.SIXLOWPAN_REASSEMBLY_BUFFER_LIFETIME / self.settings.tsch_slotDuration

        self._remove_expired_reassembly_buffer()

        # make sure we can allocate a reassembly buffer if necessary
        if (smac not in self.reassembly_buffers) or (incoming_datagram_tag not in self.reassembly_buffers[smac]):
            # dagRoot has no memory limitation for reassembly buffer
            if not self.mote.dagRoot:
                total_reassembly_buffers_num = 0
                for i in self.reassembly_buffers:
                    total_reassembly_buffers_num += len(self.reassembly_buffers[i])
                if total_reassembly_buffers_num == self.settings.sixlowpan_reassembly_buffers_num:
                    # no room for a new entry
                    self.mote.radio.drop_packet(fragment, 'frag_reassembly_buffer_full')
                    return

            # create a new reassembly buffer
            if smac not in self.reassembly_buffers:
                self.reassembly_buffers[smac] = {}
            if incoming_datagram_tag not in self.reassembly_buffers[smac]:
                self.reassembly_buffers[smac][incoming_datagram_tag] = {
                    'expiration': self.engine.getAsn() + buffer_lifetime,
                    'fragments': []
                }

        if datagram_offset not in map(lambda x: x['datagram_offset'], self.reassembly_buffers[smac][incoming_datagram_tag]['fragments']):
            self.reassembly_buffers[smac][incoming_datagram_tag]['fragments'].append({
                'datagram_offset': datagram_offset,
                'fragment_length': fragment['payload']['length']
            })
        else:
            # it's a duplicate fragment
            return

        # check whether we have a full packet in the reassembly buffer
        total_fragment_length = sum([f['fragment_length'] for f in self.reassembly_buffers[smac][incoming_datagram_tag]['fragments']])
        assert total_fragment_length <= datagram_size
        if total_fragment_length < datagram_size:
            # reassembly is not completed
            return

        # reassembly is done; we don't need the reassembly buffer for the
        # packet any more. remove the buffer.
        del self.reassembly_buffers[smac][incoming_datagram_tag]
        if len(self.reassembly_buffers[smac]) == 0:
            del self.reassembly_buffers[smac]

        # construct an original packet
        packet = copy.copy(fragment)
        packet['type'] = fragment['payload']['original_type']
        packet['payload'] = copy.deepcopy(fragment['payload'])
        packet['payload']['length'] = datagram_size
        del packet['payload']['datagram_tag']
        del packet['payload']['datagram_size']
        del packet['payload']['datagram_offset']
        del packet['payload']['original_type']

        return packet
    
    def recv(self, packet):

        self.log(
            SimEngine.SimLog.LOG_SIXLOWPAN_PKT_RX,
            {
                '_mote_id':        self.mote.id,
                'packet':          packet,
            }
        )

        if packet['type'] == d.APP_TYPE_FRAG:
            # self.fragmentaiton.recv() returns a packet to be processed
            # here. `packet` could be None when no packet should be processed
            # further.
            packet = self.fragmentation.recv(packet)

        # process switch:
        # - packet is None      : do nothing
        # - packet is for us    : hand to app
        # - packet is for other : forward
        if not packet:
            # packet is None; do nothing
            pass
        elif (
                (packet['type'] == d.APP_TYPE_DATA)
                and
                (packet['net']['dstIp'] == self.mote.id)
             ):
            # packet is for us; hand to app
            self.mote.app._action_receivePacket(
                packet       = packet,
            )
        elif (
                (
                    (packet['type']         == d.APP_TYPE_DATA)
                    and
                    (packet['net']['dstIp'] != self.mote.id)
                )
                or
                (
                    (packet['type']         == d.APP_TYPE_FRAG)
                )
             ):
            # packet is for other; forward it
                # log
                self.log(
                    SimEngine.SimLog.LOG_SIXLOWPAN_PKT_FWD,
                    {
                        '_mote_id':       self.mote.id,
                        'packet':         fwdPacket,
                    }
                )
                self.forward(packet)


    #======================== private ==========================================

    def _get_next_datagram_tag(self):
        ret = self.next_datagram_tag
        self.next_datagram_tag = (ret + 1) % 65536
        return ret

    def _remove_expired_reassembly_buffer(self):
        if len(self.reassembly_buffers) == 0:
            return

        for smac in self.reassembly_buffers.keys():
            for incoming_datagram_tag in self.reassembly_buffers[smac].keys():
                # remove a reassembly buffer which expires
                if self.reassembly_buffers[smac][incoming_datagram_tag]['expiration'] < self.engine.getAsn():
                    del self.reassembly_buffers[smac][incoming_datagram_tag]

            # remove an reassembly buffer entry if it's empty
            if len(self.reassembly_buffers[smac]) == 0:
                del self.reassembly_buffers[smac]


class Fragmentation(object):
    """The base class for forwarding implementations of fragments
    """

    def __init__(self, sixlowpan):
        self.sixlowpan = sixlowpan
        self.mote      = sixlowpan.mote
        self.settings  = SimEngine.SimSettings.SimSettings()
        self.engine    = SimEngine.SimEngine.SimEngine()
        self.log       = SimEngine.SimLog.SimLog().log

    #======================== public ==========================================

    @abstractmethod
    def recv(self, fragment):
        """This method is supposed to return a packet to be processed further

        This could return None.
        """
        raise NotImplementedError()


class PerHopReassembly(Fragmentation):
    """The typical 6LoWPAN reassembly and fragmentation implementation.

    Each intermediate node between the source node and the destination
    node of the fragments of a packet reasembles the original packet and
    fragment it again before forwarding the fragments to its next-hop.
    """
    #======================== public ==========================================

    def recv(self, smac, fragment):
        """Reassemble an original packet
        """
        return self.sixlowpan.reassemble(smac, fragment)


class FragmentForwarding(Fragmentation):
    """Fragment Forwarding implementation.

    A fragment is forwarded without reassembling the original packet
    using VRB Table. For further information, see this I-d:
    https://tools.ietf.org/html/draft-watteyne-6lo-minimal-fragment
    """

    def __init__(self, sixlowpan):
        super(FragmentForwarding, self).__init__(sixlowpan)
        self.vrb_table   = {}

    #======================== public ==========================================

    def recv(self, smac, fragment):

        dstIp                 = fragment['dstIp']
        datagram_size         = fragment['payload']['datagram_size']
        datagram_offset       = fragment['payload']['datagram_offset']
        incoming_datagram_tag = fragment['payload']['datagram_tag']
        entry_lifetime        = d.SIXLOWPAN_VRB_TABLE_ENTRY_LIFETIME / self.settings.tsch_slotDuration

        self._remove_expired_vrb_table_entry()

        # handle first fragments
        if datagram_offset == 0:
            # check if we have enough memory for a new entry if necessary
            if self.mote.dagRoot:
                # dagRoot has no memory limitation for VRB Table
                pass
            else:
                total_vrb_table_entry_num = sum([len(e) for _, e in self.vrb_table.items()])
                assert total_vrb_table_entry_num <= self.settings.fragmentation_ff_vrb_table_size
                if total_vrb_table_entry_num == self.settings.fragmentation_ff_vrb_table_size:
                    # no room for a new entry
                    self.mote.radio.drop_packet(fragment, 'frag_vrb_table_full')
                    return

            if smac not in self.vrb_table:
                self.vrb_table[smac] = {}

            # By specification, a VRB Table entry is supposed to have:
            # - incoming smac
            # - incoming datagram_tag
            # - outgoing dmac (nexthop)
            # - outgoing datagram_tag
            # However, a VRB Table entry in this implementation doesn't have
            # nexthop mac address since nexthop is determined at TSCH layer in
            # this simulation.
            if incoming_datagram_tag in self.vrb_table[smac]:
                # duplicate first fragment is silently discarded
                return
            else:
                self.vrb_table[smac][incoming_datagram_tag] = {}

            if dstIp == self.mote:
                # this is a special entry for fragments destined to the mote
                self.vrb_table[smac][incoming_datagram_tag]['outgoing_datagram_tag'] = None
            else:
                self.vrb_table[smac][incoming_datagram_tag]['outgoing_datagram_tag'] = self.sixlowpan._get_next_datagram_tag()
            self.vrb_table[smac][incoming_datagram_tag]['expiration'] = self.engine.getAsn() + entry_lifetime

            if 'missing_fragment' in self.settings.fragmentation_ff_discard_vrb_entry_policy:
                self.vrb_table[smac][incoming_datagram_tag]['next_offset'] = 0

        # when missing_fragment is in discard_vrb_entry_policy
        # - if the incoming fragment is the expected one, update the next_offset
        # - otherwise, remove the corresponding VRB table entry
        if (
                ('missing_fragment' in self.settings.fragmentation_ff_discard_vrb_entry_policy) and
                (smac in self.vrb_table) and
                (incoming_datagram_tag in self.vrb_table[smac])
           ):
            if datagram_offset == self.vrb_table[smac][incoming_datagram_tag]['next_offset']:
                self.vrb_table[smac][incoming_datagram_tag]['next_offset'] += fragment['payload']['length']
            else:
                del self.vrb_table[smac][incoming_datagram_tag]
                if len(self.vrb_table[smac]) == 0:
                    del self.vrb_table[smac]

        # find entry in VRB table and forward fragment
        if (smac in self.vrb_table) and (incoming_datagram_tag in self.vrb_table[smac]):
            # VRB entry found!

            if self.vrb_table[smac][incoming_datagram_tag]['outgoing_datagram_tag'] is None:
                # fragment for me: do not forward but reassemble. ret will have
                # either a original packet or None
                ret = self.sixlowpan.reassemble(smac, fragment)

            else:
                fragment['asn'] = self.engine.getAsn()
                # update the number of hops
                # FIXME: this shouldn't be done in application payload.
                if fragment['payload']['original_type'] == d.APP_TYPE_DATA:
                    fragment['payload']['hops'] += 1 # update the number of hops
                fragment['payload']['datagram_tag'] = self.vrb_table[smac][incoming_datagram_tag]['outgoing_datagram_tag']

                ret = fragment

        else:
            # no VRB table entry is found
            ret = None

        # when last_fragment is in discard_vrb_entry_policy
        # - if the incoming fragment is the last fragment of a packet, remove the corresponding entry
        # - otherwise, do nothing
        if (
                ('last_fragment' in self.settings.fragmentation_ff_discard_vrb_entry_policy) and
                (smac in self.vrb_table) and
                (incoming_datagram_tag in self.vrb_table[smac]) and
                ((fragment['payload']['datagram_offset'] + fragment['payload']['length']) == fragment['payload']['datagram_size'])
           ):
            del self.vrb_table[smac][incoming_datagram_tag]
            if len(self.vrb_table[smac]) == 0:
                del self.vrb_table[smac]

        return ret

    #======================== private ==========================================

    def _remove_expired_vrb_table_entry(self):
        if len(self.vrb_table) == 0:
            return

        for smac in self.vrb_table.keys():
            for incoming_datagram_tag in self.vrb_table[smac].keys():
                # too old
                if self.vrb_table[smac][incoming_datagram_tag]['expiration'] < self.engine.getAsn():
                    del self.vrb_table[smac][incoming_datagram_tag]
            # empty
            if len(self.vrb_table[smac]) == 0:
                del self.vrb_table[smac]
