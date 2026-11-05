"""Step 1: discover every PID in every version of a package.

Starting from the package's resource map, collect every version of the resource map,
every member of each of those versions (read from the ORE document itself, not the
search index), every version of each member, and, optionally, nested packages.

System metadata always comes from the CN. A version that can't be read is recorded as
*unreachable* with the reason (``unreadable`` for NotAuthorized, ``missing`` for
NotFound), and the walk carries on with the other branches: Step 2 reports these as
gaps in the version graph.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

import d1_common.resource_map
import d1_common.types.exceptions as d1_exc

from d1clone.sysmeta import text

log = logging.getLogger(__name__)

RESOURCE = "RESOURCE"


class NotAResourceMap(ValueError):
    pass


class FormatTypes:
    """Caches the formatType (DATA, METADATA, RESOURCE) of each formatId, from the CN."""

    def __init__(self, cn):
        self.cn = cn
        self._cache: dict[str, str] = {}

    def of(self, sm) -> str:
        fid = text(sm.formatId)
        if fid not in self._cache:
            try:
                self._cache[fid] = str(self.cn.getFormat(fid).formatType)
            except d1_exc.DataONEException as e:
                log.warning("can't get the format type of %s: %s", fid, e)
                self._cache[fid] = "UNKNOWN"
        return self._cache[fid]


@dataclass
class Discovery:
    package_pid: str
    pids: set[str] = field(default_factory=set)
    sysmeta: dict = field(default_factory=dict)
    format_types: dict[str, str] = field(default_factory=dict)
    unreachable: dict[str, str] = field(default_factory=dict)       # pid -> "unreadable" | "missing" | ...
    resource_maps: set[str] = field(default_factory=set)
    members: dict[str, set[str]] = field(default_factory=dict)      # resource map -> member PIDs
    not_followed: set[str] = field(default_factory=set)             # nested packages left out
    unreadable_maps: dict[str, str] = field(default_factory=dict)   # resource map -> why its ORE couldn't be read
    unresolved_members: dict[str, str] = field(default_factory=dict)  # member -> why the CN can't provide it
    warnings: list[str] = field(default_factory=list)


def unreachable_reason(e: Exception) -> str:
    if isinstance(e, d1_exc.NotAuthorized):
        return "unreadable"
    if isinstance(e, d1_exc.NotFound):
        return "missing"
    return f"error: {e.__class__.__name__}"


def fetch_sysmeta(cn, pid, discovery: Discovery):
    """The CN's system metadata for ``pid``, or None (recorded as unreachable)."""
    if pid in discovery.sysmeta:
        return discovery.sysmeta[pid]
    if pid in discovery.unreachable:
        return None
    try:
        sm = cn.getSystemMetadata(pid)
    except d1_exc.DataONEException as e:
        discovery.unreachable[pid] = unreachable_reason(e)
        return None
    discovery.sysmeta[pid] = sm
    return sm


def version_neighbors(sm) -> list[str]:
    return [p for p in (text(sm.obsoletes), text(sm.obsoletedBy)) if p]


def walk_version_chain(cn, pid: str, discovery: Discovery) -> set[str]:
    """Every readable version linked to ``pid`` through obsoletes/obsoletedBy."""
    chain: set[str] = set()
    todo = [pid]
    while todo:
        p = todo.pop()
        if p in chain:
            continue
        sm = fetch_sysmeta(cn, p, discovery)
        if sm is None:
            continue
        chain.add(p)
        todo += version_neighbors(sm)
    return chain


def members_of(source, rm_pid: str) -> set[str]:
    """The PIDs aggregated by a resource map, read from the ORE document on the source."""
    response = source.get(rm_pid)
    doc = response.content if hasattr(response, "content") else response.read()
    ore = d1_common.resource_map.ResourceMap()
    ore.parseDoc(doc.decode("utf-8") if isinstance(doc, bytes) else doc)
    return {str(p) for p in ore.getAggregatedPids()}


def collect_package_pids(cn, source, package_pid: str, *, follow_nested: bool = True,
                         formats: FormatTypes | None = None) -> Discovery:
    """Step 1. ``package_pid`` must be a resource map PID (any version of it)."""
    formats = formats or FormatTypes(cn)
    d = Discovery(package_pid=package_pid)

    root_sm = fetch_sysmeta(cn, package_pid, d)
    if root_sm is None:
        raise NotAResourceMap(f"{package_pid} can't be read from the CN ({d.unreachable[package_pid]})")
    if formats.of(root_sm) != RESOURCE:
        raise NotAResourceMap(f"{package_pid} isn't a resource map; pass the package's resource map PID")

    root_chain = walk_version_chain(cn, package_pid, d)
    packages_todo = list(root_chain)
    packages_seen: set[str] = set()
    objects_todo: list[str] = []

    while packages_todo or objects_todo:
        while packages_todo:
            rm = packages_todo.pop()
            if rm in packages_seen:
                continue
            packages_seen.add(rm)
            for v in walk_version_chain(cn, rm, d):
                if v not in packages_seen:
                    packages_todo.append(v)
                d.pids.add(v)
                d.resource_maps.add(v)
                d.format_types[v] = RESOURCE
            try:
                members = members_of(source, rm)
            except Exception as e:          # unreadable or unparseable ORE document
                d.warnings.append(f"can't read the members of resource map {rm}: {e}")
                d.unreadable_maps[rm] = unreachable_reason(e)
                members = set()
            d.members[rm] = members
            objects_todo += sorted(members)

        while objects_todo:
            pid = objects_todo.pop()
            if pid in d.pids or pid in d.unresolved_members or pid in d.not_followed:
                continue                    # listed by more than one resource-map version
            sm = fetch_sysmeta(cn, pid, d)
            if sm is None:
                d.unresolved_members[pid] = d.unreachable[pid]
                d.warnings.append(f"package member {pid} can't be read from the CN ({d.unreachable[pid]})")
                continue
            if formats.of(sm) == RESOURCE:
                if follow_nested:
                    packages_todo.append(pid)
                else:
                    d.not_followed.add(pid)
                continue
            for v in walk_version_chain(cn, pid, d):
                if v not in d.pids:
                    d.pids.add(v)
                    d.format_types[v] = formats.of(d.sysmeta[v])
    return d
