import datetime

from conftest import SOURCE, TARGET
from d1clone import state as S
from d1clone.discovery import collect_package_pids
from d1clone.plan import build_plan
from d1clone.results import Failed, Ok, Retry, Stale, Wait


def loaded(net, db, rm="rm.2"):
    d = collect_package_pids(net.cn(), net.mn(SOURCE), rm)
    plan = build_plan(net.cn(), net.mn(TARGET), d, source_node_id=SOURCE, target_node_id=TARGET)
    db.load_plan(rm, plan)
    return plan


def test_load_plan_and_states(net, package, db):
    loaded(net, db)
    assert db.states("rm.2") == {S.PLANNED: len(package["pids"])}
    assert not db.all_finished("rm.2")


def test_load_plan_never_moves_existing_rows_backwards(net, package, db):
    loaded(net, db)
    db.move("d2", S.VERIFIED)
    loaded(net, db)
    assert db.row("d2").state == S.VERIFIED


def test_apply_results(net, package, db, clock):
    loaded(net, db)
    assert db.apply("d2", Ok(), next_state=S.CREATED) == 1
    until = clock.now() + datetime.timedelta(minutes=3)
    assert db.apply("d2", Wait(until), next_state=S.HARVESTED) == 0
    assert db.row("d2").next_check == until
    assert db.pids("rm.2", S.CREATED, due=True) == []
    clock.advance(200)
    assert db.pids("rm.2", S.CREATED, due=True) == ["d2"]
    db.apply("d2", Stale("notification missed"), next_state=S.VERIFIED, stale_state=S.STALE)
    assert db.row("d2").state == S.STALE and db.row("d2").reason == "notification missed"
    db.apply("d2", Failed("probably sid"), next_state=S.VERIFIED)
    assert db.failures("rm.2") == {"probably sid": ["d2"]}


def test_retries_then_fails(net, package, db):
    loaded(net, db)
    db.move("d2", S.CREATING)
    for _ in range(4):
        db.apply("d2", Retry("create", "boom"), next_state=S.CREATED, retry_state=S.PLANNED, max_attempts=5)
        assert db.row("d2").state == S.PLANNED
    assert db.row("d2").attempts == 4
    db.move("d2", S.CREATING)
    # moving to CREATING resets the attempts, as a new state; retries count within a state
    db.apply("d2", Retry("create", "boom"), next_state=S.CREATED, max_attempts=1)
    assert db.row("d2").state == S.FAILED


def test_interrupted_and_failed_pids_are_picked_up_again(net, package, db):
    loaded(net, db)
    db.move("d2", S.CREATING)
    assert db.reset_interrupted("rm.2") == 1 and db.row("d2").state == S.PLANNED
    db.move("eml.2", S.STALE)
    db.move("eml.2", S.FAILED, reason="notification missed")
    db.move("d1.v1", S.FAILED, reason="checksum")
    assert db.recheck_failed("rm.2") == 1
    assert db.row("eml.2").state == S.STALE
    assert db.row("d1.v1").state == S.FAILED          # needs a person first


def test_run_lock(db):
    import pytest
    with db.lock_run():
        with pytest.raises(RuntimeError):
            with db.lock_run():
                pass
    with db.lock_run():
        pass


def test_waiting_for_sid_head(net, db):
    net.publish(SOURCE, "s.1", b"1", sid="S")
    net.publish(SOURCE, "s.2", b"2", sid="S", obsoletes="s.1")
    net.publish(SOURCE, "eml", b"<eml/>", "eml://ecoinformatics.org/eml-2.1.1")
    net.publish_package(SOURCE, "rm", "eml", ["s.2"])
    loaded(net, db, "rm")
    assert db.waiting_for_sid_head("s.1") and not db.waiting_for_sid_head("s.2")
    db.move("s.2", S.CREATED)
    assert not db.waiting_for_sid_head("s.1")
