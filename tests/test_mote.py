"""
\brief Tests for Mote

\author Yasuyuki Tanaka <yasuyuki.tanaka@inria.fr>
"""

import types

import pytest

import SimEngine.Mote.MoteDefines as d
from   SimEngine import SimSettings
from   SimEngine import SimLog

pytestmark = pytest.mark.skip('all tests needs to be updated')

def test_app_schedule_transmit(sim):
    sim = sim(
        **{
            'exec_numMotes':           2,
            'app_pkPeriod':            0,
            'rpl_daoPeriod':           0,
            'sf_type':                 'SSFCascading'
        }
    )

    node = sim.motes[1]

    # active TX cell event for node, active RX cell event for root, and
    # propagation event
    assert len(sim.events) == 7
    node.app.pkPeriod = 100
    node.app.schedule_mote_sendSinglePacketToDAGroot(firstPacket=True)
    assert len(sim.events) == 8
    print sim.events[7][2]
    assert sim.events[7][2] == node.app._action_mote_sendSinglePacketToDAGroot


def test_drop_forwarding_frag_vrb_table_full(sim):
    sim = sim(
        **{
            'exec_numMotes':                   3,
            'app_pkPeriod':                    0,
            'sf_type':                         'SSFCascading',
            'app_pkLength':                    180,
            'fragmentation':                   'FragmentForwarding',
            'fragmentation_ff_options':        [],
            'fragmentation_ff_vrb_table_size': 50
        }
    )
    root = sim.motes[0]
    node = sim.motes[1]
    leaf = sim.motes[2]

    frag = {
        'smac': leaf,
        'dstIp': root,
        'type': d.APP_TYPE_FRAG,
        'payload': {
            'asn_at_source':      0,
            'hops':               1,
            'datagram_tag':       1,
            'datagram_size':      2,
            'datagram_offset':    0,
        }
    }

    node.sixlowpan.fragmentation.vrb_table[leaf] = {}
    for i in range(0, SimSettings.SimSettings().fragmentation_ff_vrb_table_size):
        # fill VRB Table
        node.sixlowpan.fragmentation.vrb_table[leaf][i] = {'outgoing_datagram_tag': 0,
                                                           'expiration': 6000}

    node.original_radio_drop_packet = node.radio.drop_packet
    test_is_called = {'result': False}

    def test(self, pkt, reason):
        test_is_called['result'] = True
        assert len(pkt) > 0
        assert reason == 'frag_vrb_table_full'
        self.original_radio_drop_packet(pkt, reason)
        assert len(pkt) == 0

    node.radio.drop_packet = types.MethodType(test, node)
    node.sixlowpan.recv(leaf, frag)
    assert test_is_called['result'] is True

def test_drop_forwarding_frag_no_vrb_entry(sim):
    sim = sim(
        **{
            'exec_numMotes':                   3,
            'app_pkPeriod':                    0,
            'sf_type':                         'SSFCascading',
            'app_pkLength':                    180,
            'fragmentation':                   'FragmentForwarding',
            'fragmentation_ff_options':        [],
            'fragmentation_ff_vrb_table_size': 50
        }
    )

    root = sim.motes[0]
    node = sim.motes[1]
    leaf = sim.motes[2]

    frag = {
        'smac': leaf,
        'dstIp': root,
        'type': d.APP_TYPE_FRAG,
        'payload': {
            'asn_at_source':      0,
            'hops':               1,
            'datagram_tag':       1,
            'datagram_size':      2,
            'datagram_offset':    1,
            'length':             90,
        }
    }

    node.original_radio_drop_packet = node.radio.drop_packet
    test_is_called = {'result': False}

    def test(self, pkt, reason):
        test_is_called['result'] = True
        assert len(pkt) > 0
        assert reason == 'frag_no_vrb_entry'
        self.original_radio_drop_packet(pkt, reason)
        assert len(pkt) == 0

    node.radio.drop_packet = types.MethodType(test, node)
    node.sixlowpan.recv(leaf, frag)
    assert test_is_called['result'] is True


def test_drop_forwarding_data_tx_queue_full(sim):
    sim = sim(
        **{
            'exec_numMotes':                   3,
            'app_pkPeriod':                    0,
            'sf_type':                         'SSFCascading',
            'app_pkLength':                    180,
            'fragmentation':                   'FragmentForwarding',
            'fragmentation_ff_options':        [],
            'fragmentation_ff_vrb_table_size': 50
        }
    )

    root = sim.motes[0]
    node = sim.motes[1]
    leaf = sim.motes[2]

    packet = {'dstIp': root, 'type': d.APP_TYPE_DATA}

    for i in range(0, 10):
        # fill txQueue, whose size is 10
        assert len(node.tsch.getTxQueue()) == i
        assert node.tsch.enqueue(packet) is True
        assert len(node.tsch.getTxQueue()) == i + 1

    node.original_radio_drop_packet = node.radio.drop_packet
    test_is_called = {'result': False}

    def test(self, pkt, reason):
        test_is_called['result'] = True
        assert len(pkt) > 0
        assert reason == SimLog.LOG_TSCH_DROP_RELAY_FAIL_ENQUEUE['type']
        self.original_radio_drop_packet(pkt, reason)
        assert len(pkt) == 0

    node.radio.drop_packet = types.MethodType(test, node)
    node.tsch.waitingFor = d.DIR_RX
    node.radio.rxDone(
        type       = d.APP_TYPE_DATA,
        smac       = leaf,
        dmac       = [node],
        srcIp      = leaf,
        dstIp      = root,
        payload    = {
            'asn_at_source':   0,
            'hops':            1,
            'length':          90,
        },
    )
    assert test_is_called['result'] is True


def test_drop_frag_reassembly_queue_full(sim):
    sim = sim(
        **{
            'fragmentation'                   : 'PerHopReassembly',
            'app_pkLength'                    : 180,
            'exec_numMotes'                   : 4,
            'sf_type'                         : 'SSFSymmetric',
            'sixlowpan_reassembly_buffers_num': 1,
        }
    )

    root  = sim.motes[0]
    node  = sim.motes[1]
    leaf1 = sim.motes[2]
    leaf2 = sim.motes[3]

    # fragment can be enqueued even if datagram_offset is not 0
    packet = {
        'asn':                0,
        'type':               d.APP_TYPE_FRAG,
        'code':               None,
        'dstIp':              root,
        'payload': {
            'asn_at_source':  0,
            'hops':           1,
            'datagram_tag':       12345,
            'datagram_size':      sim.settings.app_pkLength,
            'datagram_offset':    0,
            'length':         90,
            'original_type':  d.APP_TYPE_DATA
        },
        'sourceRoute':        [],
    }

    node.original_radio_drop_packet = node.radio.drop_packet
    test_is_called = {'result': False}

    def test(self, pkt, reason):
        test_is_called['result'] = True
        assert len(pkt) > 0
        assert reason == 'frag_reassembly_buffer_full'
        self.original_radio_drop_packet(pkt, reason)
        assert len(pkt) == 0

    node.radio.drop_packet = types.MethodType(test, node)

    assert len(node.sixlowpan.reassembly_buffers) == 0
    node.sixlowpan.recv(leaf1, packet)
    assert len(node.sixlowpan.reassembly_buffers) == 1
    assert leaf1 in node.sixlowpan.reassembly_buffers
    assert 12345 in node.sixlowpan.reassembly_buffers[leaf1]

    node.sixlowpan.recv(leaf2, packet)
    assert test_is_called['result'] is True
    assert len(node.sixlowpan.reassembly_buffers) == 1
