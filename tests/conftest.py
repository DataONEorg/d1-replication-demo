import socket

import pytest

from d1clone.context import Deadlines, build_context
from d1clone.state import StateDB
from fakes import FakeClock, FakeNetwork

SOURCE = "urn:node:mnTestSOURCE"
TARGET = "urn:node:mnTestTARGET"


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    """The tests must never reach a real DataONE service."""
    def refuse(*args, **kwargs):
        raise RuntimeError("network access is disabled in the tests")
    monkeypatch.setattr(socket.socket, "connect", refuse)
    monkeypatch.setattr(socket, "create_connection", refuse)


@pytest.fixture
def clock():
    return FakeClock()


@pytest.fixture
def net(clock):
    n = FakeNetwork(clock)
    n.add_mn(SOURCE)
    n.add_mn(TARGET)
    return n


@pytest.fixture
def make_ctx(net, clock):
    def _make(mode="automatic", **settings):
        cn_args = {}
        if mode == "cn":
            cn_args = dict(cn_as_cn=net.cn("cn"), target_as_cn=net.mn(TARGET, "cn"), act_as_cn=True)
        return build_context(net.cn(), net.mn(SOURCE), net.mn(TARGET), clock=clock,
                             deadlines=settings.pop("deadlines", Deadlines()), **cn_args, **settings)
    return _make


@pytest.fixture
def db(tmp_path, clock):
    return StateDB(str(tmp_path / "state.sqlite"), TARGET, clock)


@pytest.fixture
def package(net):
    """A package with two versions: data d1 (two versions), d2; metadata eml.1 -> eml.2."""
    net.publish(SOURCE, "d1.v1", b"a,b\n1,2\n")
    net.publish(SOURCE, "d2", b"x\n9\n")
    net.publish(SOURCE, "eml.1", b"<eml>one</eml>", "eml://ecoinformatics.org/eml-2.1.1")
    net.publish_package(SOURCE, "rm.1", "eml.1", ["d1.v1", "d2"])
    net.publish(SOURCE, "d1.v2", b"a,b\n1,3\n", obsoletes="d1.v1")
    net.publish(SOURCE, "eml.2", b"<eml>two</eml>", "eml://ecoinformatics.org/eml-2.1.1", obsoletes="eml.1")
    net.publish_package(SOURCE, "rm.2", "eml.2", ["d1.v2", "d2"], obsoletes="rm.1")
    return {"rm": "rm.2", "pids": {"d1.v1", "d1.v2", "d2", "eml.1", "eml.2", "rm.1", "rm.2"}}
