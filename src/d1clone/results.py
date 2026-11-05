"""Results returned by the per-PID actions of each stage.

Every action in stages 3-6 returns one of these, and the orchestrator decides
what to do with the PID's state from it (see ``state.StateDB.apply``).
"""

from __future__ import annotations

import datetime
from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class Ok:
    """The action succeeded; the PID can move to its next state."""

    detail: str | None = None
    value: Any = None


@dataclass(frozen=True)
class Wait:
    """Nothing to do yet; look at the PID again at ``until``."""

    until: datetime.datetime


@dataclass(frozen=True)
class Retry:
    """A transient problem; try the same action again later."""

    reason: str
    detail: str | None = None


@dataclass(frozen=True)
class Failed:
    """The action can't succeed without someone looking at it; report it."""

    reason: str
    detail: Any = None


@dataclass(frozen=True)
class Stale:
    """The target's system metadata isn't the CN's latest copy (Step 6)."""

    cause: str


@dataclass
class Skip:
    """A PID the plan leaves out (Step 2)."""

    reason: str


Result = Ok | Wait | Retry | Failed | Stale


# Reasons whose failures need a person to act before a later run can succeed.
# Other FAILED PIDs are looked at again when clone() is run again.
NEEDS_ATTENTION = frozenset({"checksum", "conflict"})


# What each failure reason means, and what to do about it (the notebook's failure report).
GUIDANCE: dict[str, str] = {
    "harvest stalled": (
        "The CN's harvests of the target stopped making progress before reaching the object. "
        "Check that synchronization is enabled and running for the target, and whether the CN's "
        "synchronization queue is full; run again later, or in CN mode."
    ),
    "not registered": (
        "Harvested, but the CN didn't record the replica. Look for synchronizationFailed in the "
        "target's Metacat log; usually a checksum problem."
    ),
    "checksum": (
        "The bytes don't match the CN's checksum. Delete the object on the target and clone it again."
    ),
    "conflict": (
        "The PID already exists on the target with different content. Investigate before cloning."
    ),
    "notification missed": (
        "Registered, but the target was never refreshed. Run again in CN mode, or make a change "
        "to the object's system metadata on the source."
    ),
    "changed since refresh": (
        "The CN changed the object after the refresh, and the target missed that notification. "
        "Run again in CN mode, or make a change to the object on the source."
    ),
    "probably sid": (
        "An older version in a SID chain; the target can't accept the CN's system metadata. "
        "See 'Series IDs (SIDs): a known limitation' in the notebook."
    ),
    "sid": "An older version in a SID chain (the target returned 1334). See the notebook's SID section.",
    "verify": "The copies have the same serialVersion but differ; the detail lists the fields.",
    "keeps going stale": "The CN kept changing the object while it was being refreshed.",
}


@dataclass
class Gap:
    """A gap in a version chain (Step 2)."""

    pid: str
    field: str
    other: str
    kind: str


@dataclass
class CloneReport:
    """What clone() returns."""

    package_pid: str
    mode: str
    dry_run: bool
    plan: Any = None
    gaps: list[Gap] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    states: dict[str, int] = field(default_factory=dict)
    failures: dict[str, list[str]] = field(default_factory=dict)
    skipped: dict[str, list[str]] = field(default_factory=dict)
    package_problems: list[str] = field(default_factory=list)

    @property
    def finished_ok(self) -> bool:
        return not self.failures and not self.package_problems and not self.dry_run
