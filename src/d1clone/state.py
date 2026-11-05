"""The per-PID state machine, kept in SQLite (see 'Orchestration' in the notebook).

States:

    PLANNED -> CREATING -> CREATED -> HARVESTED (automatic mode) -> REGISTERED -> VERIFIED
                                   \\-> REGISTERED (CN mode)       \\-> STALE -> ...
    SKIPPED, FAILED

The database is the record that lets a run be stopped and started again. Every action
still checks the target and the CN before acting, so the database never has to be
trusted blindly.
"""

from __future__ import annotations

import datetime
import json
import sqlite3
import threading
from dataclasses import dataclass

from d1clone.results import NEEDS_ATTENTION, Failed, Ok, Retry, Stale, Wait

UTC = datetime.timezone.utc

PLANNED, CREATING, CREATED, HARVESTED = "PLANNED", "CREATING", "CREATED", "HARVESTED"
REGISTERED, STALE, VERIFIED = "REGISTERED", "STALE", "VERIFIED"
SKIPPED, FAILED = "SKIPPED", "FAILED"
FINISHED = (VERIFIED, SKIPPED, FAILED)

_SCHEMA = """
CREATE TABLE IF NOT EXISTS clone_pid (
    target_node     TEXT NOT NULL,
    pid             TEXT NOT NULL,
    package_pid     TEXT NOT NULL,   -- the package that first brought this PID into a clone
    format_type     TEXT NOT NULL,   -- DATA, METADATA, RESOURCE (or UNKNOWN)
    sid             TEXT,            -- the CN's seriesId, if any
    date_uploaded   TEXT NOT NULL,   -- for the creation order
    target_modified TEXT,            -- the target's dateSysMetadataModified after create()
    state           TEXT NOT NULL,
    reason          TEXT,            -- for SKIPPED and FAILED, and the cause for STALE
    detail          TEXT,
    failed_state    TEXT,            -- the state a FAILED PID was in, to resume from
    attempts        INTEGER NOT NULL DEFAULT 0,
    refreshes       INTEGER NOT NULL DEFAULT 0,
    state_since     TEXT NOT NULL,
    next_check      TEXT,
    PRIMARY KEY (target_node, pid)
);
CREATE TABLE IF NOT EXISTS package_member (
    package_pid TEXT NOT NULL,
    target_node TEXT NOT NULL,
    pid         TEXT NOT NULL,
    PRIMARY KEY (package_pid, target_node, pid)
);
CREATE TABLE IF NOT EXISTS sid_head (
    target_node TEXT NOT NULL,
    sid         TEXT NOT NULL,
    head_pid    TEXT NOT NULL,
    PRIMARY KEY (target_node, sid)
);
CREATE TABLE IF NOT EXISTS harvest_progress (
    target_node    TEXT PRIMARY KEY,
    last_harvested TEXT,
    changed_at     TEXT NOT NULL
);
"""


def _iso(dt: datetime.datetime | None) -> str | None:
    return dt.astimezone(UTC).isoformat() if dt is not None else None


def _dt(s: str | None) -> datetime.datetime | None:
    return datetime.datetime.fromisoformat(s) if s else None


@dataclass
class Row:
    pid: str
    state: str
    format_type: str
    reason: str | None
    detail: str | None
    attempts: int
    refreshes: int
    state_since: datetime.datetime
    next_check: datetime.datetime | None
    target_modified: datetime.datetime | None
    date_uploaded: datetime.datetime | None


class StateDB:
    def __init__(self, path: str, target_node: str, clock):
        self.target_node = target_node
        self.clock = clock
        self._lock = threading.RLock()
        self.conn = sqlite3.connect(path, check_same_thread=False, isolation_level=None)
        self.conn.row_factory = sqlite3.Row
        self.conn.executescript(_SCHEMA)

    # -- locking: one run per target node -------------------------------------------------

    def lock_run(self) -> "_RunLock":
        return _RunLock(self)

    # -- loading the plan -----------------------------------------------------------------

    def load_plan(self, package_pid: str, plan) -> None:
        """Add the plan's PIDs. Existing rows keep their state (never moved backwards)."""
        now = _iso(self.clock.now())
        with self._lock:
            for item in plan.items.values():
                self.conn.execute(
                    "INSERT OR IGNORE INTO package_member VALUES (?, ?, ?)",
                    (package_pid, self.target_node, item.pid))
                if self._row(item.pid) is not None:
                    continue
                if item.action == "skip":
                    state, reason = SKIPPED, item.reason
                elif item.action == "conflict":
                    state, reason = FAILED, "conflict"
                elif item.action == "present":
                    state, reason = CREATED, None
                else:
                    state, reason = PLANNED, None
                sid = item.sysmeta.seriesId.value() if item.sysmeta is not None and item.sysmeta.seriesId is not None else None
                self.conn.execute(
                    "INSERT INTO clone_pid (target_node, pid, package_pid, format_type, sid, date_uploaded,"
                    " target_modified, state, reason, failed_state, state_since)"
                    " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (self.target_node, item.pid, package_pid, item.format_type, sid,
                     _iso(item.date_uploaded) or now, _iso(item.target_modified), state, reason,
                     PLANNED if state == FAILED else None, now))
            for sid, head in plan.sid_heads.items():
                self.conn.execute("INSERT OR REPLACE INTO sid_head VALUES (?, ?, ?)",
                                  (self.target_node, sid, head))

    def sid_heads(self) -> dict[str, str]:
        rows = self.conn.execute("SELECT sid, head_pid FROM sid_head WHERE target_node = ?",
                                 (self.target_node,))
        return {r["sid"]: r["head_pid"] for r in rows}

    # -- queries ---------------------------------------------------------------------------

    def _row(self, pid: str):
        return self.conn.execute("SELECT * FROM clone_pid WHERE target_node = ? AND pid = ?",
                                 (self.target_node, pid)).fetchone()

    def row(self, pid: str) -> Row:
        r = self._row(pid)
        return Row(pid=r["pid"], state=r["state"], format_type=r["format_type"], reason=r["reason"],
                   detail=r["detail"], attempts=r["attempts"], refreshes=r["refreshes"],
                   state_since=_dt(r["state_since"]), next_check=_dt(r["next_check"]),
                   target_modified=_dt(r["target_modified"]), date_uploaded=_dt(r["date_uploaded"]))

    def pids(self, package_pid: str, state, *, format_type: str | None = None,
             due: bool = False) -> list[str]:
        states = [state] if isinstance(state, str) else list(state)
        sql = ("SELECT c.pid, c.next_check FROM clone_pid c JOIN package_member m"
               " ON m.pid = c.pid AND m.target_node = c.target_node"
               f" WHERE m.package_pid = ? AND c.target_node = ? AND c.state IN ({','.join('?' * len(states))})")
        args: list = [package_pid, self.target_node, *states]
        if format_type is not None:
            sql += " AND c.format_type = ?"
            args.append(format_type)
        sql += " ORDER BY c.date_uploaded, c.pid"
        rows = self.conn.execute(sql, args).fetchall()
        if not due:
            return [r["pid"] for r in rows]
        now = self.clock.now()
        return [r["pid"] for r in rows if r["next_check"] is None or _dt(r["next_check"]) <= now]

    def waiting_for_sid_head(self, pid: str) -> bool:
        """An older version of a series waits until the series head has been created."""
        r = self.conn.execute(
            "SELECT h.head_pid FROM clone_pid c JOIN sid_head h"
            " ON h.sid = c.sid AND h.target_node = c.target_node"
            " WHERE c.target_node = ? AND c.pid = ?", (self.target_node, pid)).fetchone()
        if r is None or r["head_pid"] == pid:
            return False
        head = self._row(r["head_pid"])
        return head is not None and head["state"] in (PLANNED, CREATING)

    def count(self, package_pid: str, state, *, format_type: str | None = None) -> int:
        return len(self.pids(package_pid, state, format_type=format_type))

    def states(self, package_pid: str) -> dict[str, int]:
        rows = self.conn.execute(
            "SELECT c.state, COUNT(*) AS n FROM clone_pid c JOIN package_member m"
            " ON m.pid = c.pid AND m.target_node = c.target_node"
            " WHERE m.package_pid = ? AND c.target_node = ? GROUP BY c.state",
            (package_pid, self.target_node))
        return {r["state"]: r["n"] for r in rows}

    def all_finished(self, package_pid: str) -> bool:
        return all(s in FINISHED for s in self.states(package_pid))

    def earliest_next_check(self, package_pid: str) -> datetime.datetime | None:
        r = self.conn.execute(
            "SELECT MIN(c.next_check) AS t FROM clone_pid c JOIN package_member m"
            " ON m.pid = c.pid AND m.target_node = c.target_node"
            f" WHERE m.package_pid = ? AND c.target_node = ? AND c.state NOT IN ({','.join('?' * len(FINISHED))})",
            (package_pid, self.target_node, *FINISHED)).fetchone()
        return _dt(r["t"])

    def failures(self, package_pid: str, state: str = FAILED) -> dict[str, list[str]]:
        rows = self.conn.execute(
            "SELECT c.pid, c.reason FROM clone_pid c JOIN package_member m"
            " ON m.pid = c.pid AND m.target_node = c.target_node"
            " WHERE m.package_pid = ? AND c.target_node = ? AND c.state = ? ORDER BY c.pid",
            (package_pid, self.target_node, state))
        out: dict[str, list[str]] = {}
        for r in rows:
            out.setdefault(r["reason"] or "unknown", []).append(r["pid"])
        return out

    # -- changes ---------------------------------------------------------------------------

    def move(self, pid: str, state: str, *, reason: str | None = None, detail=None, **columns) -> None:
        now = self.clock.now()
        with self._lock:
            current = self._row(pid)
            same = current is not None and current["state"] == state
            sets = {
                "state": state, "reason": reason,
                "detail": None if detail is None else (detail if isinstance(detail, str) else json.dumps(detail, default=str)),
                "next_check": None,
            }
            if not same:
                sets["state_since"] = _iso(now)
                sets["attempts"] = 0
            if state == FAILED and current is not None and current["state"] != FAILED:
                sets["failed_state"] = current["state"] if current["state"] != CREATING else PLANNED
            for k, v in columns.items():
                sets[k] = _iso(v) if isinstance(v, datetime.datetime) else v
            assignments = ", ".join(f"{k} = ?" for k in sets)
            self.conn.execute(f"UPDATE clone_pid SET {assignments} WHERE target_node = ? AND pid = ?",
                              (*sets.values(), self.target_node, pid))

    def check_later(self, pid: str, when: datetime.datetime) -> None:
        self.conn.execute("UPDATE clone_pid SET next_check = ? WHERE target_node = ? AND pid = ?",
                          (_iso(when), self.target_node, pid))

    def apply(self, pid: str, result, *, next_state: str, stale_state: str | None = None,
              retry_state: str | None = None,
              max_attempts: int = 5, retry_backoff: datetime.timedelta = datetime.timedelta(minutes=1),
              stale_poll: datetime.timedelta = datetime.timedelta(minutes=5), **ok_columns) -> int:
        """Update the PID's state from an action's result. Returns 1 if the state changed."""
        before = self.row(pid).state
        now = self.clock.now()
        if isinstance(result, Ok):
            self.move(pid, next_state, **ok_columns)
        elif isinstance(result, Wait):
            self.check_later(pid, result.until)
        elif isinstance(result, Retry):
            attempts = self.row(pid).attempts + 1
            if attempts >= max_attempts:
                self.move(pid, FAILED, reason=result.reason, detail=result.detail)
            else:
                self.conn.execute(
                    "UPDATE clone_pid SET attempts = ?, next_check = ?, detail = ?, state = ?"
                    " WHERE target_node = ? AND pid = ?",
                    (attempts, _iso(now + retry_backoff * (2 ** (attempts - 1))), result.detail,
                     retry_state or before, self.target_node, pid))
        elif isinstance(result, Failed):
            self.move(pid, FAILED, reason=result.reason, detail=result.detail)
        elif isinstance(result, Stale):
            if stale_state is None:
                raise ValueError("a Stale result needs a stale_state")
            if before == stale_state:
                self.conn.execute("UPDATE clone_pid SET reason = ?, next_check = ? WHERE target_node = ? AND pid = ?",
                                  (result.cause, _iso(now + stale_poll), self.target_node, pid))
            else:
                self.move(pid, stale_state, reason=result.cause, next_check=now + stale_poll)
        else:
            raise TypeError(f"unexpected result {result!r}")
        after = self.row(pid).state
        return int(after != before and not (isinstance(result, Retry) and after == retry_state))

    def count_refresh(self, pid: str) -> None:
        self.conn.execute("UPDATE clone_pid SET refreshes = refreshes + 1 WHERE target_node = ? AND pid = ?",
                          (self.target_node, pid))

    def reset_interrupted(self, package_pid: str) -> int:
        """PIDs left in CREATING by a run that stopped go back to PLANNED."""
        pids = self.pids(package_pid, CREATING)
        for pid in pids:
            self.move(pid, PLANNED)
        return len(pids)

    def recheck_failed(self, package_pid: str) -> int:
        """Put FAILED PIDs back where they failed, except those that need a person to act."""
        rows = self.conn.execute(
            "SELECT c.pid, c.reason, c.failed_state FROM clone_pid c JOIN package_member m"
            " ON m.pid = c.pid AND m.target_node = c.target_node"
            " WHERE m.package_pid = ? AND c.target_node = ? AND c.state = ?",
            (package_pid, self.target_node, FAILED)).fetchall()
        n = 0
        for r in rows:
            if r["reason"] in NEEDS_ATTENTION or not r["failed_state"]:
                continue
            self.move(r["pid"], r["failed_state"])
            n += 1
        return n

    # -- harvest progress (automatic mode) -------------------------------------------------

    def load_harvest_progress(self):
        r = self.conn.execute("SELECT * FROM harvest_progress WHERE target_node = ?",
                              (self.target_node,)).fetchone()
        if r is None:
            return None, None
        return _dt(r["last_harvested"]), _dt(r["changed_at"])

    def save_harvest_progress(self, last_harvested, changed_at) -> None:
        self.conn.execute("INSERT OR REPLACE INTO harvest_progress VALUES (?, ?, ?)",
                          (self.target_node, _iso(last_harvested), _iso(changed_at)))


class _RunLock:
    """Holds an exclusive SQLite transaction marker so that two runs on the same database
    and target node don't interleave. (Two different databases aren't protected.)"""

    def __init__(self, db: StateDB):
        self.db = db

    def __enter__(self):
        self.db.conn.execute("CREATE TABLE IF NOT EXISTS run_lock (target_node TEXT PRIMARY KEY, since TEXT)")
        try:
            self.db.conn.execute("INSERT INTO run_lock VALUES (?, ?)",
                                 (self.db.target_node, _iso(self.db.clock.now())))
        except sqlite3.IntegrityError:
            raise RuntimeError(f"another run holds the lock for {self.db.target_node} "
                               "(delete the run_lock row if that run is no longer running)") from None
        return self

    def __exit__(self, *exc):
        self.db.conn.execute("DELETE FROM run_lock WHERE target_node = ?", (self.db.target_node,))
        return False
