import pytest

from conftest import SOURCE, TARGET
from d1clone.discovery import NotAResourceMap, collect_package_pids
from d1clone.plan import build_plan, check_version_graph


def plan_for(net, rm, **kw):
    d = collect_package_pids(net.cn(), net.mn(SOURCE), rm)
    p = build_plan(net.cn(), net.mn(TARGET), d, source_node_id=SOURCE, target_node_id=TARGET, **kw)
    return d, p


# -- Step 1 -------------------------------------------------------------------------------

def test_discovers_every_version_of_every_member(net, package):
    d = collect_package_pids(net.cn(), net.mn(SOURCE), "rm.1")    # any version of the resource map
    assert d.pids == package["pids"]
    assert d.resource_maps == {"rm.1", "rm.2"}
    assert d.format_types["d1.v1"] == "DATA" and d.format_types["eml.2"] == "METADATA"
    assert not d.unreachable and not d.warnings


def test_needs_a_resource_map(net, package):
    with pytest.raises(NotAResourceMap):
        collect_package_pids(net.cn(), net.mn(SOURCE), "eml.2")


def test_unreadable_version_is_recorded_and_the_walk_continues(net, package):
    net.publish(SOURCE, "d1.v3", b"a,b\n1,4\n", obsoletes="d1.v2")
    net.unreadable.add("d1.v2")
    d = collect_package_pids(net.cn(), net.mn(SOURCE), "rm.2")
    assert d.unreachable == {"d1.v2": "unreadable"}
    assert "d1.v1" in d.pids          # still reached, through rm.1
    assert "d1.v3" not in d.pids      # only reachable through the unreadable version


def test_member_missing_from_the_cn(net, package):
    net.publish(SOURCE, "eml.3", b"<eml>3</eml>", "eml://ecoinformatics.org/eml-2.1.1")
    net.publish_package(SOURCE, "rm.3", "eml.3", ["d2", "not-on-cn"], obsoletes="rm.2")
    d = collect_package_pids(net.cn(), net.mn(SOURCE), "rm.3")
    assert d.unresolved_members == {"not-on-cn": "missing"}
    assert any("not-on-cn" in w for w in d.warnings)


def test_nested_packages(net, package):
    net.publish(SOURCE, "child.eml", b"<eml>c</eml>", "eml://ecoinformatics.org/eml-2.1.1")
    net.publish_package(SOURCE, "child.rm", "child.eml", [])
    net.publish(SOURCE, "parent.eml", b"<eml>p</eml>", "eml://ecoinformatics.org/eml-2.1.1")
    net.publish_package(SOURCE, "parent.rm", "parent.eml", ["child.rm"])
    d = collect_package_pids(net.cn(), net.mn(SOURCE), "parent.rm")
    assert {"child.rm", "child.eml", "parent.rm", "parent.eml"} <= d.pids
    d2 = collect_package_pids(net.cn(), net.mn(SOURCE), "parent.rm", follow_nested=False)
    assert d2.not_followed == {"child.rm"} and "child.eml" not in d2.pids


# -- Step 2 -------------------------------------------------------------------------------

def test_plan_creates_everything_in_type_then_age_order(net, package):
    d, p = plan_for(net, "rm.2")
    assert set(p.order) == package["pids"]
    types = [p.items[x].format_type for x in p.order]
    assert types == sorted(types, key=["DATA", "METADATA", "RESOURCE"].index)
    assert p.order.index("d1.v1") < p.order.index("d1.v2")
    assert check_version_graph(d, p) == []


def test_plan_skips_other_authoritative_nodes_and_policy(net, package):
    net.add_mn("urn:node:OTHER")
    net.publish("urn:node:OTHER", "foreign", b"f")
    net.publish(SOURCE, "blocked", b"b", blocked=[TARGET])
    net.publish(SOURCE, "noreplication", b"n", replication_allowed=False)
    net.publish(SOURCE, "eml.3", b"<eml>3</eml>", "eml://ecoinformatics.org/eml-2.1.1")
    net.publish_package(SOURCE, "rm.3", "eml.3", ["foreign", "blocked", "noreplication"], obsoletes="rm.2")
    d, p = plan_for(net, "rm.3")
    assert p.items["foreign"].reason == "authoritative node is urn:node:OTHER"
    assert p.items["blocked"].reason == "replication policy: the target is a blocked node"
    assert p.items["noreplication"].reason == "replication policy: replication isn't allowed"
    d, p = plan_for(net, "rm.3", allow_policy_override=True)
    assert p.items["blocked"].action == "create"


def test_plan_skips_doi_shoulders(net):
    net.publish(SOURCE, "doi:10.5072/FK2abc", b"x")
    net.publish(SOURCE, "eml", b"<eml/>", "eml://ecoinformatics.org/eml-2.1.1")
    net.publish_package(SOURCE, "rm", "eml", ["doi:10.5072/FK2abc"])
    _, p = plan_for(net, "rm", doi_shoulders=("doi:10.5072/FK2",))
    assert p.items["doi:10.5072/FK2abc"].action == "skip"


def test_plan_detects_objects_already_on_the_target(net, package):
    tgt = net.mns[TARGET]
    tgt.data["d2"] = net.mns[SOURCE].data["d2"]
    tgt.sysmeta["d2"] = net.cn().getSystemMetadata("d2")
    tgt.data["d1.v1"] = b"different"
    tgt.sysmeta["d1.v1"] = net.cn().getSystemMetadata("d1.v2")   # wrong checksum for d1.v1
    _, p = plan_for(net, "rm.2")
    assert p.items["d2"].action == "present"
    assert p.items["d1.v1"].action == "conflict"


def test_plan_warns_about_existing_replica_entries(net, package):
    net.add_replica("d2", TARGET, "requested", 1)
    _, p = plan_for(net, "rm.2")
    assert any("d2" in w and "requested" in w for w in p.warnings)


def test_sid_heads(net):
    net.publish(SOURCE, "s.1", b"1", sid="S")
    net.publish(SOURCE, "s.2", b"2", sid="S", obsoletes="s.1")
    net.publish(SOURCE, "s.3", b"3", sid="S", obsoletes="s.2")
    net.publish(SOURCE, "eml", b"<eml/>", "eml://ecoinformatics.org/eml-2.1.1")
    net.publish_package(SOURCE, "rm", "eml", ["s.3"])
    _, p = plan_for(net, "rm")
    assert p.sid_heads == {"S": "s.3"}


# -- gaps ---------------------------------------------------------------------------------

def test_gap_kinds(net, package):
    net.publish(SOURCE, "d1.v3", b"a,b\n1,4\n", obsoletes="d1.v2")
    net.unreadable.add("d1.v2")
    d, p = plan_for(net, "rm.2")
    kinds = {(g.pid, g.other, g.kind) for g in check_version_graph(d, p)}
    assert ("d1.v1", "d1.v2", "unreadable") in kinds


def test_broken_link_and_branch(net, package):
    cn_store = net.cn_store
    cn_store["d1.v1"].obsoletedBy = "d2"                 # d2 doesn't point back
    net.publish(SOURCE, "d1.v2b", b"branch", obsoletes="d1.v1")
    cn_store["d1.v1"].obsoletedBy = "d1.v2"
    net.publish(SOURCE, "eml.3", b"<eml>3</eml>", "eml://ecoinformatics.org/eml-2.1.1")
    net.publish_package(SOURCE, "rm.3", "eml.3", ["d1.v2", "d1.v2b", "d2"], obsoletes="rm.2")
    d, p = plan_for(net, "rm.3")
    kinds = {g.kind for g in check_version_graph(d, p)}
    assert "branch" in kinds and "broken link" in kinds


def test_excluded_version_is_a_gap(net, package):
    net.cn_store["d1.v1"].authoritativeMemberNode = "urn:node:OTHER"
    d, p = plan_for(net, "rm.2")
    gaps = check_version_graph(d, p)
    assert any(g.pid == "d1.v2" and g.kind.startswith("excluded") for g in gaps)
