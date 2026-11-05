import pytest

from conftest import SOURCE, TARGET
from d1clone.context import SetupError, build_context, make_context


def test_automatic_mode_by_default(make_ctx):
    ctx = make_ctx()
    assert ctx.mode == "automatic"
    assert ctx.source_node_id == SOURCE and ctx.target_node_id == TARGET
    assert ctx.sync_schedule.expression == "10 0/3 * * * ? *"
    assert ctx.cn_as_cn is None and ctx.target_as_cn is None


def test_cn_mode_needs_act_as_cn(net, clock):
    ctx = build_context(net.cn(), net.mn(SOURCE), net.mn(TARGET), cn_as_cn=net.cn("cn"),
                        target_as_cn=net.mn(TARGET, "cn"), clock=clock)
    assert ctx.mode == "automatic"            # a certificate alone doesn't switch on CN mode
    assert ctx.cn_as_cn is None


def test_cn_mode(make_ctx):
    ctx = make_ctx("cn")
    assert ctx.mode == "cn" and ctx.cn_as_cn is not None and ctx.target_as_cn is not None


def test_act_as_cn_without_credentials(net, clock):
    with pytest.raises(SetupError):
        build_context(net.cn(), net.mn(SOURCE), net.mn(TARGET), act_as_cn=True, clock=clock)


def test_automatic_mode_needs_synchronization(net, make_ctx):
    net.registry[TARGET]["synchronize"] = False
    with pytest.raises(SetupError):
        make_ctx()
    net.registry[TARGET]["synchronize"] = True


def test_unknown_target_node(net, clock):
    net.add_mn("urn:node:NOTREGISTERED")
    del net.registry["urn:node:NOTREGISTERED"]
    with pytest.raises(SetupError):
        build_context(net.cn(), net.mn(SOURCE), net.mn("urn:node:NOTREGISTERED"), clock=clock)


def test_unparseable_schedule_falls_back(net, make_ctx):
    net.registry[TARGET]["schedule"] = "0 0 0 L * ? *"
    ctx = make_ctx()
    assert ctx.mode == "automatic" and ctx.sync_schedule is None


def test_make_context_builds_clients_with_the_right_credentials(net, clock):
    made = []

    def factory(kind, url, **opts):
        made.append((kind, url, sorted(opts)))
        if kind == "cn":
            return net.cn("cn" if "cert_pem_path" in opts else "user")
        node = SOURCE if "source" in url else TARGET
        return net.mn(node, "cn" if "cert_pem_path" in opts else "user")

    ctx = make_context("https://cn.example/cn", "https://source.example/mn", "https://target.example/mn",
                       "TOKEN", cn_cert="c.pem", cn_key="k.pem", act_as_cn=True,
                       client_factory=factory, clock=clock)
    assert ctx.mode == "cn"
    assert [m for m in made if m[2] == ["headers"]] == [
        ("cn", "https://cn.example/cn", ["headers"]),
        ("mn", "https://source.example/mn", ["headers"]),
        ("mn", "https://target.example/mn", ["headers"])]
    assert ("cn", "https://cn.example/cn", ["cert_key_path", "cert_pem_path"]) in made
