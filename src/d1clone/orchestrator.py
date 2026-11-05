"""The orchestrator: Steps 1-2 once per run, then a reconcile loop over each PID's state.

``clone()`` is safe to run again with the same package and database: the plan is
rebuilt, PIDs already in the database keep their state, PIDs reported as failed (other
than those needing a person to act) are looked at again, and every action checks the
target and the CN before acting.
"""

from __future__ import annotations

import datetime
import logging
from concurrent.futures import ThreadPoolExecutor

from d1clone import state as S
from d1clone.context import Context
from d1clone.create import create_on_target, existing_copy_matches, prepare_sysmeta
from d1clone.discovery import Discovery, collect_package_pids
from d1clone.plan import Plan, build_plan, check_version_graph
from d1clone.refresh import refresh_one
from d1clone.registration import (HarvestProgress, RateLimiter, check_harvested, check_registered,
                                  register_one)
from d1clone.results import CloneReport, Failed, Ok, Retry, Stale
from d1clone.sysmeta import utc
from d1clone.verify import verify_one, verify_package

import d1_common.types.exceptions as d1_exc

log = logging.getLogger(__name__)

FORMAT_ORDER = ("DATA", "UNKNOWN", "METADATA", "RESOURCE")


class VersionGraphHasGaps(RuntimeError):
    def __init__(self, gaps):
        super().__init__(f"{len(gaps)} gap(s) in the version graph; pass allow_gaps=True to clone anyway")
        self.gaps = gaps


# -- Step 3 ------------------------------------------------------------------------------------

def _create_work(ctx: Context, pid: str, sid_heads: dict[str, str]):
    """Runs in a worker thread: everything except the database update."""
    try:
        cn_sm = ctx.cn.getSystemMetadata(pid)
        try:
            ctx.target.getSystemMetadata(pid)
            if existing_copy_matches(ctx.target, pid, cn_sm):    # an earlier run created it
                return Ok(value=utc(ctx.target.getSystemMetadata(pid).dateSysMetadataModified))
            return Failed("conflict", "the PID exists on the target with a different checksum")
        except d1_exc.NotFound:
            pass
        return create_on_target(ctx.source, ctx.target, pid, prepare_sysmeta(cn_sm, sid_heads),
                                workdir=ctx.workdir)
    except Exception as e:                      # keep the loop going; the PID is retried
        log.exception("create of %s failed", pid)
        return Retry("create", f"{e.__class__.__name__}: {e}")


def _create_stage(db: S.StateDB, ctx: Context, package_pid: str, pool) -> int:
    moved = 0
    sid_heads = db.sid_heads()
    for ftype in FORMAT_ORDER:
        ready = [p for p in db.pids(package_pid, S.PLANNED, format_type=ftype, due=True)
                 if not db.waiting_for_sid_head(p)]
        heads_first = [p for p in ready if p in set(sid_heads.values())]
        if heads_first:
            ready = heads_first             # create the series heads before anything else of this type
        for pid in ready:
            db.move(pid, S.CREATING)
        results = pool.map(lambda p: (p, _create_work(ctx, p, sid_heads)), ready)
        for pid, result in results:
            columns = {"target_modified": result.value} if isinstance(result, Ok) else {}
            moved += db.apply(pid, result, next_state=S.CREATED, retry_state=S.PLANNED,
                              max_attempts=ctx.deadlines.max_attempts, **columns)
        if db.count(package_pid, [S.PLANNED, S.CREATING], format_type=ftype) > 0:
            break                                  # don't start the next type until this one is created
    return moved


# -- Steps 4-6 -----------------------------------------------------------------------------------

def _automatic_registration(db: S.StateDB, ctx: Context, package_pid: str) -> int:
    moved = 0
    now = ctx.now()
    waiting = db.pids(package_pid, S.CREATED, due=True)
    queued = db.pids(package_pid, S.HARVESTED, due=True)
    if waiting:
        progress = HarvestProgress.load(db)
        progress.observe(ctx.cn, ctx.target_node_id, now)
        progress.save(db)
        for pid in waiting:
            result = check_harvested(progress, db.row(pid).target_modified, ctx.sync_schedule,
                                     ctx.deadlines, now)
            moved += db.apply(pid, result, next_state=S.HARVESTED)
    for pid in queued:
        result = check_registered(ctx.cn, pid, ctx.target_node_id, db.row(pid).state_since,
                                  ctx.deadlines, now)
        moved += db.apply(pid, result, next_state=S.REGISTERED, max_attempts=ctx.deadlines.max_attempts)
    return moved


def _cn_registration(db: S.StateDB, ctx: Context, package_pid: str, limiter: RateLimiter) -> int:
    moved = 0
    for pid in db.pids(package_pid, S.CREATED, due=True):
        limiter.wait()
        result = register_one(ctx.cn, ctx.cn_as_cn, ctx.target, pid, ctx.target_node_id, ctx.now())
        moved += db.apply(pid, result, next_state=S.REGISTERED, max_attempts=ctx.deadlines.max_attempts)
    return moved


def _verify_stage(db: S.StateDB, ctx: Context, package_pid: str) -> int:
    moved = 0
    now = ctx.now()
    for pid in db.pids(package_pid, [S.REGISTERED, S.STALE], due=True):
        row = db.row(pid)
        result = verify_one(ctx.cn, ctx.target, pid, ctx.target_node_id)
        if isinstance(result, Stale) and ctx.mode == "automatic" and row.state == S.STALE \
                and now - row.state_since > ctx.deadlines.refresh:
            result = Failed(result.cause)         # report it: notification missed, probably sid, ...
        if isinstance(result, Stale) and ctx.mode == "cn":
            # CN mode refreshes right away; no need to wait for a poll interval.
            moved += db.apply(pid, result, next_state=S.VERIFIED, stale_state=S.STALE,
                              stale_poll=datetime.timedelta(0))
        else:
            moved += db.apply(pid, result, next_state=S.VERIFIED, stale_state=S.STALE,
                              stale_poll=ctx.deadlines.poll)
    return moved


def _refresh_stage(db: S.StateDB, ctx: Context, package_pid: str) -> int:
    moved = 0
    for pid in db.pids(package_pid, S.STALE, due=True):
        if db.row(pid).refreshes >= ctx.deadlines.max_refreshes:
            db.move(pid, S.FAILED, reason="keeps going stale")
            moved += 1
            continue
        result = refresh_one(ctx.cn, ctx.target_as_cn, pid)
        if isinstance(result, Ok):
            db.count_refresh(pid)
        moved += db.apply(pid, result, next_state=S.REGISTERED, max_attempts=ctx.deadlines.max_attempts)
    return moved


def reconcile_once(db: S.StateDB, ctx: Context, package_pid: str, pool, limiter: RateLimiter) -> int:
    """One pass: move every PID that's ready by one state. Returns how many moved."""
    moved = _create_stage(db, ctx, package_pid, pool)
    if ctx.mode == "cn":
        moved += _cn_registration(db, ctx, package_pid, limiter)
    else:
        moved += _automatic_registration(db, ctx, package_pid)
    moved += _verify_stage(db, ctx, package_pid)
    if ctx.mode == "cn":
        moved += _refresh_stage(db, ctx, package_pid)
    return moved


# -- clone() ----------------------------------------------------------------------------------

def _sleep_interval(db: S.StateDB, ctx: Context, package_pid: str, moved: int) -> float:
    if moved:
        return 1.0
    cap = datetime.timedelta(minutes=1) if ctx.mode == "cn" else ctx.deadlines.poll
    now = ctx.now()
    nxt = db.earliest_next_check(package_pid)
    until = min(nxt, now + cap) if nxt else now + cap
    return max(1.0, (until - now).total_seconds())


def plan_package(ctx: Context, package_pid: str, **plan_options) -> tuple[Discovery, Plan, list]:
    """Steps 1 and 2, with no changes anywhere."""
    discovery = collect_package_pids(ctx.cn, ctx.source, package_pid,
                                     follow_nested=plan_options.pop("follow_nested", True))
    plan = build_plan(ctx.cn, ctx.target, discovery, source_node_id=ctx.source_node_id,
                      target_node_id=ctx.target_node_id, **plan_options)
    gaps = check_version_graph(discovery, plan)
    return discovery, plan, gaps


def clone(package_pid: str, ctx: Context, db: S.StateDB, *, dry_run: bool = True,
          allow_gaps: bool = False, max_passes: int | None = None, **plan_options) -> CloneReport:
    """Clone a package, every version of every member, from the source to the target.

    ``dry_run=True`` (the default) only discovers, plans and reports.
    """
    discovery, plan, gaps = plan_package(ctx, package_pid, **plan_options)
    report = CloneReport(package_pid=package_pid, mode=ctx.mode, dry_run=dry_run, plan=plan,
                         gaps=gaps, warnings=list(plan.warnings))
    for pid in sorted(discovery.not_followed):
        report.warnings.append(f"nested package {pid} wasn't followed")
    if dry_run:
        report.skipped = {i.reason: [] for i in plan.items.values() if i.action == "skip"}
        for i in plan.items.values():
            if i.action == "skip":
                report.skipped[i.reason].append(i.pid)
        return report
    if gaps and not allow_gaps:
        raise VersionGraphHasGaps(gaps)

    limiter = RateLimiter(ctx.registrations_per_minute, ctx.clock)
    passes = 0
    with db.lock_run(), ThreadPoolExecutor(max_workers=max(1, ctx.workers)) as pool:
        db.load_plan(package_pid, plan)
        db.reset_interrupted(package_pid)
        db.recheck_failed(package_pid)
        while not db.all_finished(package_pid):
            moved = reconcile_once(db, ctx, package_pid, pool, limiter)
            passes += 1
            if max_passes is not None and passes >= max_passes:
                break
            if not db.all_finished(package_pid):
                ctx.clock.sleep(_sleep_interval(db, ctx, package_pid, moved))

    report.states = db.states(package_pid)
    report.failures = db.failures(package_pid)
    report.skipped = db.failures(package_pid, state=S.SKIPPED)
    if db.all_finished(package_pid):
        verified = set(db.pids(package_pid, S.VERIFIED))
        heads = {sid: head for sid, head in db.sid_heads().items() if head in verified}
        report.package_problems = verify_package(ctx.target, heads)
    return report
