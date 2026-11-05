"""A small evaluator for the Quartz cron expressions in the CN's node registry.

Each node's ``synchronization/schedule`` element has attributes ``sec min hour mday
mon wday year``, the fields of a Quartz Scheduler cron expression, in that order.
Quartz differs from Unix cron:

- the first field is seconds, and an optional last field is the year;
- one of the two day fields must be ``?`` ("no specific value");
- days of the week run from 1 (Sunday) to 7 (Saturday);
- ``a/b`` means "every b, starting at a";
- ``L``, ``W`` and ``#`` have special meanings in the day fields.

This module supports ``*``, ``?``, numbers, names (JAN-DEC, SUN-SAT), lists, ranges and
``/`` steps, which covers the schedules in use. Anything else raises
``UnsupportedSchedule``; the caller then falls back to fixed deadlines.

Times are evaluated in UTC (assumed to be the CN scheduler's time zone; to be confirmed).
"""

from __future__ import annotations

import datetime

UTC = datetime.timezone.utc

FIELDS = ("sec", "min", "hour", "mday", "mon", "wday", "year")

_MONTHS = {m: i for i, m in enumerate(
    ["JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"], start=1)}
_DAYS = {d: i for i, d in enumerate(["SUN", "MON", "TUE", "WED", "THU", "FRI", "SAT"], start=1)}

# (minimum, maximum, names) for each field
_RANGES = {
    "sec": (0, 59, None),
    "min": (0, 59, None),
    "hour": (0, 23, None),
    "mday": (1, 31, None),
    "mon": (1, 12, _MONTHS),
    "wday": (1, 7, _DAYS),
    "year": (1970, 2199, None),
}

# How far ahead to look for the next firing before giving up.
_HORIZON_DAYS = 366 * 8


class UnsupportedSchedule(ValueError):
    """The expression uses Quartz features this module doesn't evaluate."""


def _value(token: str, field: str) -> int:
    lo, hi, names = _RANGES[field]
    token = token.upper()
    if names and token in names:
        return names[token]
    if not token.isdigit():
        raise UnsupportedSchedule(f"unsupported value {token!r} in the {field} field")
    v = int(token)
    if not lo <= v <= hi:
        raise UnsupportedSchedule(f"{v} is out of range for the {field} field")
    return v


def _parse_field(expr: str, field: str) -> set[int] | None:
    """The set of allowed values, or None for '?' (no specific value)."""
    lo, hi, _ = _RANGES[field]
    expr = expr.strip()
    if expr == "?":
        if field not in ("mday", "wday"):
            raise UnsupportedSchedule(f"'?' is only allowed in the day fields, not {field}")
        return None
    if any(c in expr.upper() for c in ("L", "W", "#")) and not any(
            n in expr.upper() for n in list(_MONTHS) + list(_DAYS)):
        raise UnsupportedSchedule(f"'L', 'W' and '#' aren't supported ({field}={expr!r})")
    values: set[int] = set()
    for part in expr.split(","):
        step = 1
        if "/" in part:
            part, step_s = part.split("/", 1)
            if not step_s.isdigit() or int(step_s) == 0:
                raise UnsupportedSchedule(f"bad step {step_s!r} in the {field} field")
            step = int(step_s)
        if part in ("*", ""):
            start, end = lo, hi
        elif "-" in part:
            a, b = part.split("-", 1)
            start, end = _value(a, field), _value(b, field)
        else:
            start = _value(part, field)
            end = hi if step > 1 else start   # "a/b": from a to the end of the range
        if start > end:
            raise UnsupportedSchedule(f"wrapping ranges aren't supported ({field}={expr!r})")
        values.update(range(start, end + 1, step))
    return values


class QuartzSchedule:
    """A parsed Quartz cron expression."""

    def __init__(self, expression: str):
        parts = expression.split()
        if len(parts) == 6:
            parts.append("*")                 # the year field is optional
        if len(parts) != 7:
            raise UnsupportedSchedule(f"expected 6 or 7 fields, got {len(parts)}: {expression!r}")
        self.expression = " ".join(parts)
        parsed = {f: _parse_field(p, f) for f, p in zip(FIELDS, parts, strict=True)}
        mday, wday = parsed["mday"], parsed["wday"]
        full_mday = set(range(1, 32))
        full_wday = set(range(1, 8))
        if mday is None and wday is None:
            raise UnsupportedSchedule("only one of the day fields can be '?'")
        if mday is not None and wday is not None:
            # Quartz requires one '?'; accept '*' in one of them, as it adds no restriction.
            if mday == full_mday:
                mday = None
            elif wday == full_wday:
                wday = None
            else:
                raise UnsupportedSchedule("restricting both day-of-month and day-of-week isn't supported")
        self._sec = sorted(parsed["sec"])
        self._min = sorted(parsed["min"])
        self._hour = sorted(parsed["hour"])
        self._mday = mday
        self._wday = wday
        self._mon = parsed["mon"]
        self._year = parsed["year"]

    @classmethod
    def from_node(cls, node) -> "QuartzSchedule":
        """Build from a node-registry Node (``node.synchronization.schedule``)."""
        s = node.synchronization.schedule
        return cls(" ".join(str(getattr(s, f)) for f in FIELDS))

    def __repr__(self) -> str:
        return f"QuartzSchedule({self.expression!r})"

    def _day_matches(self, d: datetime.date) -> bool:
        if d.year not in self._year or d.month not in self._mon:
            return False
        if self._mday is not None:
            return d.day in self._mday
        quartz_wday = (d.isoweekday() % 7) + 1       # Monday=1..Sunday=7  ->  Sunday=1..Saturday=7
        return quartz_wday in self._wday

    def next_after(self, t: datetime.datetime) -> datetime.datetime:
        """The first time the schedule fires strictly after ``t``."""
        t = _utc(t).replace(microsecond=0) + datetime.timedelta(seconds=1)
        day = t.date()
        for _ in range(_HORIZON_DAYS):
            if self._day_matches(day):
                same_day = day == t.date()
                for h in self._hour:
                    if same_day and h < t.hour:
                        continue
                    for m in self._min:
                        if same_day and h == t.hour and m < t.minute:
                            continue
                        for s in self._sec:
                            if same_day and h == t.hour and m == t.minute and s < t.second:
                                continue
                            return datetime.datetime(day.year, day.month, day.day, h, m, s, tzinfo=UTC)
            day += datetime.timedelta(days=1)
        raise UnsupportedSchedule(f"{self.expression!r} doesn't fire within {_HORIZON_DAYS} days")

    def fires_after(self, t: datetime.datetime, n: int) -> list[datetime.datetime]:
        """The next ``n`` times the schedule fires after ``t``."""
        out = []
        for _ in range(n):
            t = self.next_after(t)
            out.append(t)
        return out

    def fires_between(self, t0: datetime.datetime, t1: datetime.datetime, cap: int = 100_000) -> int:
        """How many times the schedule fired in the interval (t0, t1]."""
        count, t = 0, _utc(t0)
        t1 = _utc(t1)
        while count < cap:
            t = self.next_after(t)
            if t > t1:
                break
            count += 1
        return count


def _utc(t: datetime.datetime) -> datetime.datetime:
    if t.tzinfo is None:
        return t.replace(tzinfo=UTC)
    return t.astimezone(UTC)
