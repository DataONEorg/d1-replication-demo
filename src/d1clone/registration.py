"""Step 4: registration of each created object as a replica on the CN.

Automatic mode: the CN's synchronization service registers the replica when it harvests
the target. The node registry gives the target's harvest schedule (a Quartz cron
expression) and ``lastHarvested`` (the dateSysMetadataModified of the newest object the
last harvest queued). Each harvest takes at most ``Synchronization.max_harvest_size``
objects (2,000 in production), oldest first, so a large clone can take many harvests:
the tool waits as long as ``lastHarvested`` keeps advancing, and reports "harvest
stalled" only when it stops moving over several scheduled harvests.

CN mode: the tool calls ``CN.updateReplicationMetadata()`` itself with the CN's
certificate, after checking the target's checksum and size (the check synchronization
would otherwise have made).
"""

from __future__ import annotations

import datetime
import logging
import math

import d1_common.types.dataoneTypes_v1 as v1
import d1_common.types.exceptions as d1_exc

from d1clone.context import find_node
from d1clone.results import Failed, Ok, Retry, Wait
from d1clone.sysmeta import checksum_of, serial, text, utc

log = logging.getLogger(__name__)


def replica_status(sm, node_id: str) -> str | None:
    for r in sm.replica or []:
        if text(r.replicaMemberNode) == node_id:
            return str(r.replicationStatus)
    return None


def make_replica(node_id: str, verified: datetime.datetime):
    """A Replica bound to its XML element, as updateReplicationMetadata() serializes it."""
    r = v1.replica()
    r.replicaMemberNode = node_id
    r.replicationStatus = "completed"
    r.replicaVerified = verified
    return r


# -- automatic mode --------------------------------------------------------------------------

class HarvestProgress:
    """Whether the CN's harvests of the target are getting through its new objects."""

    def __init__(self, last_harvested=None, changed_at=None):
        self.last_harvested = last_harvested
        self.changed_at = changed_at

    @classmethod
    def load(cls, db) -> "HarvestProgress":
        return cls(*db.load_harvest_progress())

    def save(self, db) -> None:
        db.save_harvest_progress(self.last_harvested, self.changed_at)

    def observe(self, cn, target_node_id: str, now: datetime.datetime) -> None:
        """Read lastHarvested from the node registry (one call per pass, for every PID)."""
        node = find_node(cn.listNodes(), target_node_id)
        last = utc(node.synchronization.lastHarvested) if node is not None and node.synchronization else None
        if self.changed_at is None or last != self.last_harvested:
            self.last_harvested, self.changed_at = last, now

    def stalled(self, schedule, deadlines, now: datetime.datetime) -> bool:
        if self.changed_at is None:
            return False
        if schedule is None:
            return now - self.changed_at > deadlines.harvest_fallback
        fires = schedule.fires_between(self.changed_at, now - deadlines.after_harvest)
        return fires >= deadlines.stalled_fires

    def runs_ahead(self, target, target_modified, harvest_limit: int) -> int:
        """At least how many harvests are needed before one reaches ``target_modified``."""
        ahead = target.listObjects(fromDate=self.last_harvested, toDate=target_modified, count=0).total
        return math.ceil(int(ahead) / harvest_limit) if ahead else 0


def check_harvested(progress: HarvestProgress, target_modified, schedule, deadlines,
                    now: datetime.datetime):
    """Has a harvest queued this object for synchronization on the CN?"""
    if progress.last_harvested is not None and target_modified is not None \
            and progress.last_harvested >= target_modified:
        return Ok()
    if progress.stalled(schedule, deadlines, now):
        return Failed("harvest stalled",
                      f"lastHarvested has been {progress.last_harvested} since {progress.changed_at}")
    if schedule is None:
        return Wait(until=now + deadlines.poll)
    return Wait(until=schedule.next_after(now) + deadlines.after_harvest)


def check_registered(cn, pid: str, target_node_id: str, harvested_at: datetime.datetime,
                     deadlines, now: datetime.datetime):
    """After the harvest: has the CN's synchronization recorded the replica?"""
    try:
        sm = cn.getSystemMetadata(pid)
    except d1_exc.DataONEException as e:
        return Retry("cn", str(e))
    if replica_status(sm, target_node_id) == "completed":
        return Ok()
    if now > harvested_at + deadlines.registration:
        return Failed("not registered", f"harvested at {harvested_at}, still no completed replica entry")
    return Wait(until=now + deadlines.registered_poll)


# -- CN mode ---------------------------------------------------------------------------------

TRANSIENT = (d1_exc.ServiceFailure,)


def _is_transport_error(e: Exception) -> bool:
    return e.__class__.__module__.startswith(("requests", "urllib3")) or isinstance(e, (TimeoutError, ConnectionError))


def register_one(cn, cn_as_cn, target, pid: str, target_node_id: str, now: datetime.datetime):
    """Record the target as a completed replica with CN.updateReplicationMetadata()."""
    sm = cn.getSystemMetadata(pid)
    if replica_status(sm, target_node_id) == "completed":
        return Ok("already registered")            # for example, by a harvest of the target
    alg, value = checksum_of(sm)
    t_sm = target.getSystemMetadata(pid)
    t_sum = target.getChecksum(pid, alg).value().lower()
    if t_sum != value or int(t_sm.size) != int(sm.size):
        return Failed("checksum", {"target": [t_sum, int(t_sm.size)], "cn": [value, int(sm.size)]})
    try:
        cn_as_cn.updateReplicationMetadata(pid, make_replica(target_node_id, now), serial(sm))
        return Ok()                                # the CN has also notified the target
    except d1_exc.VersionMismatch as e:
        return Retry("register", f"version mismatch: {e}")   # read again; maybe a harvest got there first
    except TRANSIENT as e:
        return Retry("register", str(e))           # the call may have succeeded: the retry looks first
    except Exception as e:
        if _is_transport_error(e):
            return Retry("register", str(e))
        raise


class RateLimiter:
    """At most ``per_minute`` calls per minute (CN mode registrations)."""

    def __init__(self, per_minute: int, clock):
        self.interval = 60.0 / per_minute if per_minute else 0.0
        self.clock = clock
        self._last = None

    def wait(self) -> None:
        if self.interval and self._last is not None:
            elapsed = (self.clock.now() - self._last).total_seconds()
            if elapsed < self.interval:
                self.clock.sleep(self.interval - elapsed)
        self._last = self.clock.now()
