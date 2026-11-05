"""Steps 3-6, one action at a time."""

import datetime

import pytest

from conftest import SOURCE, TARGET
from d1clone.context import Deadlines
from d1clone.create import create_on_target, prepare_sysmeta
from d1clone.quartz import QuartzSchedule
from d1clone.refresh import refresh_one
from d1clone.registration import (HarvestProgress, RateLimiter, check_harvested, check_registered,
                                  make_replica, register_one, replica_status)
from d1clone.results import Failed, Ok, Retry, Stale, Wait
from d1clone.sysmeta import serial, text
from d1clone.verify import field_differences, stale_cause, verify_one, verify_package

UTC = datetime.timezone.utc


def create(net, pid, sid_heads=None):
    sm = prepare_sysmeta(net.cn().getSystemMetadata(pid), sid_heads or {})
    return create_on_target(net.mn(SOURCE), net.mn(TARGET), pid, sm)


# -- Step 3 -------------------------------------------------------------------------------

def test_prepare_sysmeta(net, package):
    net.publish(SOURCE, "s.1", b"1", sid="S")
    net.publish(SOURCE, "s.2", b"2", sid="S", obsoletes="s.1")
    cn_sm = net.cn().getSystemMetadata("s.1")
    sm = prepare_sysmeta(cn_sm, {"S": "s.2"})
    assert sm.obsoletes is None and sm.obsoletedBy is None and len(sm.replica) == 0
    assert sm.seriesId is None                                    # not the head
    assert text(sm.authoritativeMemberNode) == SOURCE             # kept
    assert text(prepare_sysmeta(net.cn().getSystemMetadata("s.2"), {"S": "s.2"}).seriesId) == "S"
    assert text(cn_sm.obsoletedBy) == "s.2"                       # the original isn't changed


def test_create_keeps_the_source_as_authoritative(net, package):
    result = create(net, "d1.v2")
    assert isinstance(result, Ok) and result.value == net.clock.now()
    t_sm = net.mns[TARGET].sysmeta["d1.v2"]
    assert text(t_sm.authoritativeMemberNode) == SOURCE
    assert text(t_sm.originMemberNode) == TARGET and serial(t_sm) == 0   # what create() overwrites


def test_create_detects_a_bad_download(net, package):
    net.mns[SOURCE].data["d2"] = b"corrupted"
    assert create(net, "d2") == Failed("checksum", pytest.approx(create(net, "d2").detail))
    assert "d2" not in net.mns[TARGET].sysmeta


def test_create_again_after_an_interrupted_run(net, package):
    assert isinstance(create(net, "d2"), Ok)
    assert isinstance(create(net, "d2"), Ok)                      # IdentifierNotUnique, same bytes
    net.mns[TARGET].data["d2"] = b"other"
    assert create(net, "d2").reason == "conflict"


def test_create_with_a_sid_on_an_older_version_is_rejected(net):
    net.publish(SOURCE, "s.1", b"1", sid="S")
    net.publish(SOURCE, "s.2", b"2", sid="S", obsoletes="s.1")
    assert isinstance(create(net, "s.2", {"S": "s.2"}), Ok)
    sm = net.cn().getSystemMetadata("s.1")
    sm.obsoletedBy = None                                        # keep the SID, as a naive copy would
    assert create_on_target(net.mn(SOURCE), net.mn(TARGET), "s.1", sm).reason == "create"


# -- Step 4, automatic mode ---------------------------------------------------------------

def test_harvest_gate_and_stall(net, package, clock):
    schedule = QuartzSchedule("10 0/3 * * * ? *")
    deadlines = Deadlines(stalled_fires=3)
    progress = HarvestProgress()
    progress.observe(net.cn(), TARGET, clock.now())
    created = clock.now()
    r = check_harvested(progress, created, schedule, deadlines, clock.now())
    assert isinstance(r, Wait) and r.until == schedule.next_after(clock.now()) + deadlines.after_harvest
    clock.advance(60 * 7)                                         # two harvests, nothing new on the target
    assert isinstance(check_harvested(progress, created, schedule, deadlines, clock.now()), Wait)
    clock.advance(60 * 6)                                         # more than three without progress
    assert check_harvested(progress, created, schedule, deadlines, clock.now()).reason == "harvest stalled"


def test_harvest_progress_keeps_waiting(net, package, clock):
    schedule = QuartzSchedule("10 0/3 * * * ? *")
    progress = HarvestProgress()
    progress.observe(net.cn(), TARGET, clock.now())
    for _ in range(10):                                            # the harvester keeps advancing
        clock.advance(180)
        net.registry[TARGET]["last_harvested"] = clock.now() - datetime.timedelta(days=1)
        progress.observe(net.cn(), TARGET, clock.now())
        assert isinstance(check_harvested(progress, clock.now(), schedule, Deadlines(), clock.now()), Wait)


def test_harvest_limit_takes_several_runs(net, clock):
    """Objects beyond max_harvest_size are picked up by later harvests, oldest first."""
    net.registry[TARGET]["synchronize"] = False                   # harvests are run by hand here
    net.harvest_limit = 2
    for i in range(5):
        net.publish(SOURCE, f"o{i}", f"{i}".encode())
        assert isinstance(create(net, f"o{i}"), Ok)
        clock.advance(1)
    net.harvest(TARGET)
    registered = [p for p in (f"o{i}" for i in range(5))
                  if replica_status(net.cn_store[p], TARGET) == "completed"]
    assert registered == ["o0", "o1"]
    net.harvest(TARGET)
    net.harvest(TARGET)
    assert all(replica_status(net.cn_store[f"o{i}"], TARGET) == "completed" for i in range(5))


def test_runs_ahead(net, clock):
    for i in range(5):
        net.publish(SOURCE, f"o{i}", f"{i}".encode())
        create(net, f"o{i}")
        clock.advance(1)
    progress = HarvestProgress(last_harvested=datetime.datetime(1900, 1, 1, tzinfo=UTC))
    assert progress.runs_ahead(net.mn(TARGET), clock.now(), harvest_limit=2) == 3


def test_check_registered(net, package, clock):
    net.registry[TARGET]["synchronize"] = False                   # harvests are run by hand here
    create(net, "d2")
    harvested_at = clock.now()
    deadlines = Deadlines(registration=datetime.timedelta(hours=1))
    assert isinstance(check_registered(net.cn(), "d2", TARGET, harvested_at, deadlines, clock.now()), Wait)
    clock.advance(7200)
    assert check_registered(net.cn(), "d2", TARGET, harvested_at, deadlines, clock.now()).reason == "not registered"
    net.sync_object(TARGET, "d2")
    assert isinstance(check_registered(net.cn(), "d2", TARGET, harvested_at, deadlines, clock.now()), Ok)


def test_harvest_registers_and_notifies_the_target(net, package):
    create(net, "d2")
    net.sync_object(TARGET, "d2")
    t_sm = net.mns[TARGET].sysmeta["d2"]
    assert text(t_sm.originMemberNode) == SOURCE                  # the CN's copy replaced the target's
    assert serial(t_sm) == serial(net.cn_store["d2"])


# -- Step 4, CN mode ------------------------------------------------------------------------

def test_register_one(net, package, clock):
    create(net, "d1.v2")
    r = register_one(net.cn(), net.cn("cn"), net.mn(TARGET), "d1.v2", TARGET, clock.now())
    assert r == Ok()
    assert replica_status(net.cn_store["d1.v2"], TARGET) == "completed"
    assert text(net.mns[TARGET].sysmeta["d1.v2"].obsoletes) == "d1.v1"    # notified and refreshed
    again = register_one(net.cn(), net.cn("cn"), net.mn(TARGET), "d1.v2", TARGET, clock.now())
    assert again == Ok("already registered")


def test_register_one_checks_the_bytes_first(net, package, clock):
    create(net, "d2")
    net.mns[TARGET].data["d2"] = b"tampered"
    r = register_one(net.cn(), net.cn("cn"), net.mn(TARGET), "d2", TARGET, clock.now())
    assert r.reason == "checksum" and replica_status(net.cn_store["d2"], TARGET) is None


def test_register_one_version_mismatch_is_retried(net, package, clock, monkeypatch):
    create(net, "d2")
    cn_as_cn = net.cn("cn")
    real = cn_as_cn.updateReplicationMetadata
    monkeypatch.setattr(cn_as_cn, "updateReplicationMetadata",
                        lambda pid, rep, sv: real(pid, rep, sv - 1))
    r = register_one(net.cn(), cn_as_cn, net.mn(TARGET), "d2", TARGET, clock.now())
    assert isinstance(r, Retry)


def test_make_replica_serializes():
    r = make_replica(TARGET, datetime.datetime(2026, 1, 1, tzinfo=UTC))
    assert b"<replicationStatus>completed</replicationStatus>" in r.toxml("utf-8")


def test_rate_limiter(clock):
    limiter = RateLimiter(per_minute=6, clock=clock)
    for _ in range(3):
        limiter.wait()
    assert clock.slept == pytest.approx(20)


# -- Steps 5 and 6 ----------------------------------------------------------------------------

def test_missed_notification_is_stale_then_refreshed(net, package, clock):
    create(net, "d1.v2")
    net.mns[TARGET].unreachable = True
    net.sync_object(TARGET, "d1.v2")
    net.mns[TARGET].unreachable = False
    assert verify_one(net.cn(), net.mn(TARGET), "d1.v2", TARGET) == Stale("notification missed")
    assert refresh_one(net.cn(), net.mn(TARGET, "cn"), "d1.v2") == Ok()
    assert verify_one(net.cn(), net.mn(TARGET), "d1.v2", TARGET) == Ok()


def test_refresh_needs_the_cn_identity(net, package):
    create(net, "d2")
    net.sync_object(TARGET, "d2")
    assert refresh_one(net.cn(), net.mn(TARGET), "d2").reason == "refresh"


def test_sid_chain_older_versions_cant_be_refreshed(net, clock):
    net.publish(SOURCE, "s.1", b"1", sid="S")
    net.publish(SOURCE, "s.2", b"2", sid="S", obsoletes="s.1")
    heads = {"S": "s.2"}
    for pid in ("s.2", "s.1"):                                     # the head first
        assert isinstance(create(net, pid, heads), Ok)
        net.sync_object(TARGET, pid)
    assert verify_one(net.cn(), net.mn(TARGET), "s.2", TARGET) == Ok()
    assert verify_one(net.cn(), net.mn(TARGET), "s.1", TARGET) == Stale("probably sid")
    assert (TARGET, "s.1", "1334") in net.notification_errors
    assert refresh_one(net.cn(), net.mn(TARGET, "cn"), "s.1").reason == "sid"
    assert verify_package(net.mn(TARGET), heads) == []


def test_older_version_refreshed_before_its_head_blocks_the_head(net):
    """Why the plan creates each series head first."""
    net.publish(SOURCE, "s.1", b"1", sid="S")
    net.publish(SOURCE, "s.2", b"2", sid="S", obsoletes="s.1")
    heads = {"S": "s.2"}
    assert isinstance(create(net, "s.1", heads), Ok)             # created without the SID ...
    net.sync_object(TARGET, "s.1")                                # ... then refreshed: it gets the SID
    assert text(net.mns[TARGET].sysmeta["s.1"].seriesId) == "S"
    assert create(net, "s.2", heads).reason == "create"           # the head can't be created any more


def test_verify_reports_differences(net, package):
    create(net, "d2")
    net.sync_object(TARGET, "d2")
    net.mns[TARGET].sysmeta["d2"].fileName = "renamed.csv"
    r = verify_one(net.cn(), net.mn(TARGET), "d2", TARGET)
    assert r.reason == "verify" and r.detail[0]["fields"] == ["fileName"]


def test_field_differences_ignores_ordering(net, package):
    a = net.cn().getSystemMetadata("d2")
    b = net.cn().getSystemMetadata("d2")
    assert field_differences(a, b) == []
    b.originMemberNode = TARGET
    assert field_differences(a, b) == ["originMemberNode"]


def test_stale_cause_changed_since_refresh(net, package):
    cn_sm = net.cn().getSystemMetadata("d2")
    t_sm = net.cn().getSystemMetadata("d2")
    t_sm.serialVersion = 1
    cn_sm.serialVersion = 2
    assert stale_cause(cn_sm, t_sm) == "changed since refresh"
