"""End-to-end runs of clone() against the fakes, in both modes."""

import datetime

import pytest

from conftest import SOURCE, TARGET
from d1clone import state as S
from d1clone.context import Deadlines
from d1clone.orchestrator import VersionGraphHasGaps, clone
from d1clone.sysmeta import text


def target_matches_cn(net, pid):
    import copy
    import d1_common.system_metadata as d1_sm
    return d1_sm.are_equivalent_pyxb(copy.deepcopy(net.cn_store[pid]), copy.deepcopy(net.mns[TARGET].sysmeta[pid]))


def test_dry_run_changes_nothing(net, package, make_ctx, db):
    report = clone("rm.2", make_ctx(), db, dry_run=True)
    assert set(report.plan.order) == package["pids"] and report.gaps == []
    assert net.mns[TARGET].sysmeta == {} and db.states("rm.2") == {}


@pytest.mark.parametrize("mode", ["automatic", "cn"])
def test_clone_a_package_with_versions(net, package, make_ctx, db, mode):
    report = clone("rm.2", make_ctx(mode), db, dry_run=False)
    assert report.states == {S.VERIFIED: len(package["pids"])}
    assert report.failures == {} and report.package_problems == []
    for pid in package["pids"]:
        assert target_matches_cn(net, pid), pid
        assert text(net.mns[TARGET].sysmeta[pid].originMemberNode) == SOURCE
    assert text(net.mns[TARGET].sysmeta["d1.v1"].obsoletedBy) == "d1.v2"      # chains restored
    assert text(net.mns[TARGET].sysmeta["rm.2"].obsoletes) == "rm.1"
    calls = {c[0] for c in net.calls}
    if mode == "automatic":
        assert calls == set()                       # only the CN's own harvests and notifications
    else:
        assert "updateReplicationMetadata" in calls


def test_creation_order_data_metadata_resource_maps(net, package, make_ctx, db):
    clone("rm.2", make_ctx("cn"), db, dry_run=False)
    created = net.mns[TARGET].created
    rank = {"d1.v1": 0, "d1.v2": 0, "d2": 0, "eml.1": 1, "eml.2": 1, "rm.1": 2, "rm.2": 2}
    assert [rank[p] for p in created] == sorted(rank[p] for p in created)


def test_automatic_mode_reports_a_missed_notification(net, package, make_ctx, db):
    net.mns[TARGET].unreachable = True
    ctx = make_ctx(deadlines=Deadlines(refresh=datetime.timedelta(minutes=30)))
    report = clone("rm.2", ctx, db, dry_run=False)
    assert set(report.failures) == {"notification missed"}
    assert sorted(report.failures["notification missed"]) == sorted(package["pids"])


def test_cn_mode_repairs_a_missed_notification(net, package, make_ctx, db):
    net.mns[TARGET].unreachable = True                 # the CN's notifications don't arrive
    report = clone("rm.2", make_ctx("cn"), db, dry_run=False)
    assert report.states == {S.VERIFIED: len(package["pids"])}
    assert any(c[0] == "systemMetadataChanged" for c in net.calls)


def test_rerun_in_cn_mode_after_automatic_mode(net, package, make_ctx, db):
    net.mns[TARGET].unreachable = True
    clone("rm.2", make_ctx(deadlines=Deadlines(refresh=datetime.timedelta(minutes=30))), db, dry_run=False)
    report = clone("rm.2", make_ctx("cn"), db, dry_run=False)
    assert report.states == {S.VERIFIED: len(package["pids"])}


@pytest.mark.parametrize("mode", ["automatic", "cn"])
def test_sid_chain(net, make_ctx, db, mode):
    net.publish(SOURCE, "s.1", b"1", sid="S")
    net.publish(SOURCE, "s.2", b"2", sid="S", obsoletes="s.1")
    net.publish(SOURCE, "s.3", b"3", sid="S", obsoletes="s.2")
    net.publish(SOURCE, "eml", b"<eml/>", "eml://ecoinformatics.org/eml-2.1.1")
    net.publish_package(SOURCE, "rm", "eml", ["s.3"])
    report = clone("rm", make_ctx(mode, deadlines=Deadlines(refresh=datetime.timedelta(minutes=30))),
                   db, dry_run=False)
    expected = "probably sid" if mode == "automatic" else "sid"
    assert report.failures == {expected: ["s.1", "s.2"]}
    assert db.row("s.3").state == S.VERIFIED
    assert report.package_problems == []                # S resolves to s.3 on the target


def test_gaps_stop_the_clone_unless_allowed(net, package, make_ctx, db):
    net.publish(SOURCE, "d1.v3", b"a,b\n1,4\n", obsoletes="d1.v2")
    net.unreadable.add("d1.v2")
    with pytest.raises(VersionGraphHasGaps):
        clone("rm.2", make_ctx(), db, dry_run=False)
    report = clone("rm.2", make_ctx(), db, dry_run=False, allow_gaps=True)
    assert "d1.v2" not in report.states and report.failures == {}


def test_large_clone_needs_several_harvests(net, make_ctx, db):
    """With a per-run limit, the harvester takes several runs; the tool keeps waiting."""
    net.harvest_limit = 3
    members = []
    for i in range(10):
        net.publish(SOURCE, f"o{i}", f"{i}".encode())
        members.append(f"o{i}")
    net.publish(SOURCE, "eml", b"<eml/>", "eml://ecoinformatics.org/eml-2.1.1")
    net.publish_package(SOURCE, "rm", "eml", members)
    report = clone("rm", make_ctx(), db, dry_run=False)
    assert report.states == {S.VERIFIED: 12} and report.failures == {}


def test_stalled_harvest_is_reported(net, package, make_ctx, db):
    real_harvest = net.harvest
    net.harvest = lambda node_id, at=None: None if node_id == TARGET else real_harvest(node_id, at)
    report = clone("rm.2", make_ctx(), db, dry_run=False)
    assert set(report.failures) == {"harvest stalled"}


def test_daily_schedule(net, package, make_ctx, db, clock):
    net.registry[TARGET]["schedule"] = "0 9 9 * * ? *"
    start = clock.now()
    report = clone("rm.2", make_ctx(), db, dry_run=False)
    assert report.states == {S.VERIFIED: len(package["pids"])}
    assert clock.now() - start < datetime.timedelta(days=1, hours=1)


def test_resume_after_an_interruption(net, package, make_ctx, db):
    store = net.mns[TARGET]
    real_create = store.create
    calls = []

    def create_then_stop(*args, **kwargs):
        calls.append(args[0])
        if len(calls) == 3:
            raise KeyboardInterrupt            # the run stops part-way through the creates
        return real_create(*args, **kwargs)

    store.create = create_then_stop
    with pytest.raises(KeyboardInterrupt):
        clone("rm.2", make_ctx("cn"), db, dry_run=False)
    assert db.count("rm.2", S.CREATING) == 1 and not db.all_finished("rm.2")
    store.create = real_create
    report = clone("rm.2", make_ctx("cn"), db, dry_run=False)   # the run lock was released
    assert report.states == {S.VERIFIED: len(package["pids"])}
    assert len(net.mns[TARGET].created) == len(package["pids"])      # nothing created twice


def test_parallel_creates(net, package, make_ctx, db):
    report = clone("rm.2", make_ctx("cn", workers=4), db, dry_run=False)
    assert report.states == {S.VERIFIED: len(package["pids"])}


def test_checksum_problem_on_the_source_is_reported(net, package, make_ctx, db):
    net.mns[SOURCE].data["d2"] = b"corrupted"
    report = clone("rm.2", make_ctx("cn"), db, dry_run=False)
    assert report.failures == {"checksum": ["d2"]}
