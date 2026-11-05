"""Step 6: verify each clone against the CN, and the package as a whole.

Once refreshed, the target holds the CN's system metadata whole, so the entire record
is compared (after normalization). ``serialVersion`` decides how to read a difference:
equal serial versions must be identical; a lower serial version on the target means its
copy is stale, and ``stale_cause`` says why.
"""

from __future__ import annotations

import copy
import xml.etree.ElementTree as ET

import d1_common.system_metadata as d1_sm
import d1_common.types.exceptions as d1_exc
import d1_common.xml

from d1clone.results import Failed, Ok, Stale
from d1clone.sysmeta import checksum_of, serial, text


def stale_cause(cn_sm, t_sm) -> str:
    if text(cn_sm.seriesId) and not text(t_sm.seriesId):
        return "probably sid"
    if serial(t_sm) == 0:
        return "notification missed"            # create() set 0; the target was never refreshed
    return "changed since refresh"


def _local(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def field_differences(a, b) -> list[str]:
    """Names of the top-level fields that differ between two (normalized) system metadata."""
    a, b = copy.deepcopy(a), copy.deepcopy(b)
    d1_sm.normalize_in_place(a)
    d1_sm.normalize_in_place(b)

    def fields(sm):
        root = ET.fromstring(d1_common.xml.serialize_to_xml_str(sm))
        out: dict[str, list[str]] = {}
        for child in root:
            out.setdefault(_local(child.tag), []).append(ET.tostring(child, encoding="unicode").strip())
        return out

    fa, fb = fields(a), fields(b)
    return sorted(name for name in set(fa) | set(fb) if fa.get(name) != fb.get(name))


def verify_one(cn, target, pid: str, target_node_id: str):
    try:
        cn_sm = cn.getSystemMetadata(pid)
        t_sm = target.getSystemMetadata(pid)
    except d1_exc.DataONEException as e:
        return Failed("verify", f"can't read system metadata: {e}")
    if serial(t_sm) < serial(cn_sm):
        return Stale(stale_cause(cn_sm, t_sm))
    problems = []
    if not d1_sm.are_equivalent_pyxb(copy.deepcopy(cn_sm), copy.deepcopy(t_sm)):
        problems.append({"fields": field_differences(cn_sm, t_sm)})
    if target_node_id not in [text(r.replicaMemberNode) for r in cn_sm.replica or []]:
        problems.append("the CN doesn't list the target as a replica")
    alg, value = checksum_of(cn_sm)
    if target.getChecksum(pid, alg).value().lower() != value:
        problems.append("stored bytes")
    return Failed("verify", problems) if problems else Ok()


def verify_package(target, sid_heads: dict[str, str]) -> list[str]:
    """Package-level checks, once every PID has finished.

    Each SID must resolve to its head version on the target. (A check that the target's
    search index shows the package and its members is not implemented yet.)
    """
    problems = []
    for sid, head in sorted(sid_heads.items()):
        try:
            got = text(target.getSystemMetadata(sid).identifier)
        except d1_exc.DataONEException as e:
            problems.append(f"SID {sid} doesn't resolve on the target: {e}")
            continue
        if got != head:
            problems.append(f"SID {sid} resolves to {got} on the target, expected {head}")
    return problems
