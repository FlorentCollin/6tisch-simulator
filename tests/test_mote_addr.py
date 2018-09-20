import netaddr
import pytest

import SimEngine.Mote.MoteDefines as d
from SimEngine.Mote.Mote import Mote


@pytest.fixture(params=[0, 1])
def fixture_mote_id(request):
    return request.param


@pytest.fixture(params=[None, '01-23-45-67-89-ab-cd-ef'])
def fixture_eui64(request):
    return request.param


def test_init_mac_addr(sim_engine, fixture_mote_id, fixture_eui64):
    # instantiate SimEngine that is necessary to create a Mote by ourselves
    sim_engine = sim_engine(diff_config={'exec_numMotes': 1})

    mote = Mote(fixture_mote_id, fixture_eui64)

    if fixture_eui64 is None:
        if fixture_mote_id == 0:
            expected_eui64 = netaddr.EUI('02-00-00-00-00-01-00-00')
        elif fixture_mote_id == 1:
            expected_eui64 = netaddr.EUI('02-00-00-00-00-00-00-01')
        else:
            raise NotImplementedError()
    else:
        expected_eui64 = netaddr.EUI(fixture_eui64)

    assert mote.eui64 == expected_eui64
    assert mote.get_mac_addr() == expected_eui64


def test_is_my_mac(sim_engine):
    sim_engine = sim_engine(diff_config={'exec_numMotes': 1})

    root = sim_engine.motes[0]

    assert root.is_my_mac_addr(netaddr.EUI('02-00-00-00-00-01-00-00')) is True
    assert root.is_my_mac_addr(netaddr.EUI('02-00-00-00-00-00-00-01')) is False

    assert root.is_my_mac_addr('02-00-00-00-00-01-00-00') is True
    assert root.is_my_mac_addr('02-00-00-00-00-00-00-01') is False


def test_get_ipv6_global_addr(sim_engine):
    sim_engine = sim_engine(diff_config={'exec_numMotes': 2})

    root = sim_engine.motes[0]
    non_root = sim_engine.motes[1]

    # root should have an IPv6 global(-scope) address whose prefix is
    # d.IPV6_DEFAULT_PREFIX
    root_global_addr = netaddr.IPAddress(root.get_ipv6_global_addr())
    assert root_global_addr == netaddr.IPAddress('fd00::1:0')

    # non_root shouldn't have one by default
    assert non_root.get_ipv6_global_addr() is None

    non_root.add_ipv6_prefix(d.IPV6_DEFAULT_PREFIX)
    non_root_global_addr = netaddr.IPAddress(non_root.get_ipv6_global_addr())
    assert non_root_global_addr == netaddr.IPAddress('fd00::1')

    non_root.delete_ipv6_prefix()
    assert non_root.get_ipv6_global_addr() is None


def test_get_ipv6_link_local_addr(sim_engine):
    sim_engine = sim_engine(diff_config={'exec_numMotes': 2})

    root = sim_engine.motes[0]
    non_root = sim_engine.motes[1]

    root_link_local_addr = netaddr.IPAddress(root.get_ipv6_link_local_addr())
    assert root_link_local_addr == netaddr.IPAddress('fe80::1:0')

    non_root_link_local_addr = netaddr.IPAddress(non_root.get_ipv6_link_local_addr())
    assert non_root_link_local_addr == netaddr.IPAddress('fe80::1')


def test_is_my_ipv6_addr(sim_engine):
    sim_engine = sim_engine(diff_config={'exec_numMotes': 1})

    root = sim_engine.motes[0]

    assert root.is_my_ipv6_addr(netaddr.IPAddress('fe80::1:0')) is True
    assert root.is_my_ipv6_addr(netaddr.IPAddress('fd00::1:0')) is True

    assert root.is_my_ipv6_addr(netaddr.IPAddress('fe80::1')) is False
    assert root.is_my_ipv6_addr(netaddr.IPAddress('fd00::1')) is False

    assert root.is_my_ipv6_addr('fe80::1:0') is True
    assert root.is_my_ipv6_addr('fd00::1:0') is True

    assert root.is_my_ipv6_addr('fe80::1') is False
    assert root.is_my_ipv6_addr('fd00::1') is False
