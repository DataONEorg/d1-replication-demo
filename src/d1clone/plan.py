"""Step 2: build the plan and check the version graph for gaps.

For each discovered PID, decide whether to create it on the target, treat it as
already there, or skip it (with the reason); decide which version keeps each series ID;
choose the order of creation; and report gaps in the version chains.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from dataclasses import dataclass, field

import d1_common.types.exceptions as d1_exc

from d1clone.discovery import Discovery
from d1clone.results import Gap
from d1clone.sysmeta import checksum_of, text, utc

log = logging.getLogger(__name__)

TYPE_RANK = {"DATA": 0, "METADATA": 1, "RESOURCE": 2, "UNKNOWN": 1}


@dataclass
class PlanItem:
    pid: str
    action: str                       # "create" | "present" | "conflict" | "skip"
    format_type: str
    sysmeta: object = None            # the CN's system metadata
    reason: str | None = None         # for "skip" and "conflict"
    date_uploaded: object = None
    target_modified: object = None    # for "present": the target's dateSysMetadataModified
    notes: list[str] = field(default_factory=list)


@dataclass
class Plan:
    items: dict[str, PlanItem]
    sid_heads: dict[str, str]
    order: list[str]                  # PIDs to create, in creation order
    warnings: list[str] = field(default_factory=list)

    def by_action(self, action: str) -> list[str]:
        return sorted(p for p, i in self.items.items() if i.action == action)


def policy_problem(sm, target_node_id: str) -> str | None:
    """Why the object's replication policy doesn't allow a copy on the target, if it doesn't."""
    rp = sm.replicationPolicy
    if rp is None:
        return None
    if rp.replicationAllowed is not None and not bool(rp.replicationAllowed):
        return "replication policy: replication isn't allowed"
    if target_node_id in [text(n) for n in (rp.blockedMemberNode or [])]:
        return "replication policy: the target is a blocked node"
    return None


def replica_entry(sm, node_id: str) -> str | None:
    for r in sm.replica or []:
        if text(r.replicaMemberNode) == node_id:
            return str(r.replicationStatus)
    return None


def target_checksum_matches(target, pid: str, cn_sm, target_sm) -> bool:
    alg, value = checksum_of(cn_sm)
    t_alg, t_value = checksum_of(target_sm)
    if t_alg.upper() == alg.upper():
        return t_value == value
    return target.getChecksum(pid, alg).value().lower() == value


def compute_sid_heads(discovery: Discovery, pids) -> dict[str, str]:
    """For each SID, the version that keeps it on the target: the head of the series."""
    by_sid: dict[str, list[str]] = defaultdict(list)
    for pid in pids:
        sid = text(discovery.sysmeta[pid].seriesId)
        if sid:
            by_sid[sid].append(pid)
    heads = {}
    for sid, members in by_sid.items():
        member_set = set(members)
        not_obsoleted = [p for p in members if text(discovery.sysmeta[p].obsoletedBy) not in member_set]
        candidates = not_obsoleted or members
        heads[sid] = max(candidates, key=lambda p: (utc(discovery.sysmeta[p].dateUploaded), p))
    return heads


def build_plan(cn, target, discovery: Discovery, *, source_node_id: str, target_node_id: str,
               allow_policy_override: bool = False, doi_shoulders: tuple[str, ...] = (),
               allow_dois: bool = False) -> Plan:
    """Step 2: classify each discovered PID.

    ``doi_shoulders`` are the DOI shoulders the target registers DOIs for; PIDs that start
    with one are skipped unless ``allow_dois`` is set, because the target's create() would
    register the DOI with its own settings.
    """
    items: dict[str, PlanItem] = {}
    warnings: list[str] = list(discovery.warnings)
    for pid in sorted(discovery.pids):
        sm = discovery.sysmeta[pid]
        ftype = discovery.format_types.get(pid, "UNKNOWN")
        item = PlanItem(pid=pid, action="create", format_type=ftype, sysmeta=sm,
                        date_uploaded=utc(sm.dateUploaded))
        items[pid] = item

        amn = text(sm.authoritativeMemberNode)
        if amn != source_node_id:
            item.action, item.reason = "skip", f"authoritative node is {amn}"
            continue
        problem = policy_problem(sm, target_node_id)
        if problem and not allow_policy_override:
            item.action, item.reason = "skip", problem
            continue
        if any(pid.startswith(s) for s in doi_shoulders) and not allow_dois:
            item.action, item.reason = "skip", "DOI on a shoulder the target registers"
            continue

        status = replica_entry(sm, target_node_id)
        if status is not None and status != "completed":
            warnings.append(f"{pid}: the CN already has a '{status}' replica entry for the target "
                            "(check for a replication task before cloning)")
        try:
            target_sm = target.getSystemMetadata(pid)
        except d1_exc.NotFound:
            target_sm = None
        if target_sm is not None:
            if target_checksum_matches(target, pid, sm, target_sm):
                item.action = "present"
                item.target_modified = utc(target_sm.dateSysMetadataModified)
            else:
                item.action, item.reason = "conflict", "the PID exists on the target with a different checksum"

    creatable = [p for p, i in items.items() if i.action in ("create", "present")]
    sid_heads = compute_sid_heads(discovery, creatable)
    # The head of each series is created before any older version of it: if an older version
    # were refreshed first (by a harvest or a CN-mode registration), the target would give it
    # the SID, and the head's create() would then be rejected because the SID is in use.
    heads = set(sid_heads.values())

    def order_key(p):
        sid = text(items[p].sysmeta.seriesId)
        return (TYPE_RANK.get(items[p].format_type, 1), 0 if (sid and p in heads) else 1,
                items[p].date_uploaded, p)

    order = sorted((p for p, i in items.items() if i.action == "create"), key=order_key)
    return Plan(items=items, sid_heads=sid_heads, order=order, warnings=warnings)


def check_version_graph(discovery: Discovery, plan: Plan) -> list[Gap]:
    """Gaps in the version chains: unreadable, missing, broken link, excluded, branch."""
    gaps: list[Gap] = []
    sms = discovery.sysmeta
    for pid in sorted(discovery.pids):
        sm = sms[pid]
        for fld, back in (("obsoletes", "obsoletedBy"), ("obsoletedBy", "obsoletes")):
            other = text(getattr(sm, fld))
            if not other:
                continue
            if other in discovery.unreachable:
                gaps.append(Gap(pid, fld, other, discovery.unreachable[other]))
            elif other in sms:
                if text(getattr(sms[other], back)) != pid:
                    gaps.append(Gap(pid, fld, other, "broken link"))
                elif plan.items.get(other) is not None and plan.items[other].action == "skip":
                    gaps.append(Gap(pid, fld, other, "excluded: " + plan.items[other].reason))
    claims: dict[str, list[str]] = defaultdict(list)
    for pid in discovery.pids:
        old = text(sms[pid].obsoletes)
        if old:
            claims[old].append(pid)
    for old, newer in sorted(claims.items()):
        if len(newer) > 1:
            gaps.append(Gap(old, "obsoletedBy", ",".join(sorted(newer)), "branch"))
    return gaps
