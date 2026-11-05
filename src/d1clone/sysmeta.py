"""Small helpers for pyxb SystemMetadata objects."""

from __future__ import annotations

import copy
import datetime

import d1_common.date_time

UTC = datetime.timezone.utc


def text(value) -> str | None:
    """The string value of an optional pyxb element (Identifier, NodeReference, ...)."""
    if value is None:
        return None
    if hasattr(value, "value"):
        return value.value()
    return str(value)


def copy_sysmeta(sm):
    return copy.deepcopy(sm)


def utc(dt) -> datetime.datetime | None:
    """A pyxb dateTime or datetime as an aware UTC datetime, truncated to milliseconds.

    DataONE nodes store milliseconds, so truncating makes timestamps from different
    sources comparable.
    """
    if dt is None:
        return None
    dt = d1_common.date_time.normalize_datetime_to_utc(dt)
    dt = datetime.datetime(dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second,
                           dt.microsecond // 1000 * 1000, tzinfo=UTC)
    return dt


def serial(sm) -> int:
    return int(sm.serialVersion) if sm.serialVersion is not None else 0


def checksum_of(sm) -> tuple[str, str]:
    """(algorithm, lower-case value)."""
    return sm.checksum.algorithm, sm.checksum.value().lower()
