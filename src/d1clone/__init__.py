"""Clone DataONE data packages, with every version of every object, from the authoritative
Member Node (the *source*) to another Member Node (the *target*), so that the CN
recognizes the copies as replicas.

See the "Manual replication" section of 00_replication_policy.ipynb for the design.

Typical use::

    from d1clone import make_context, StateDB, clone

    ctx = make_context(cn_url, source_url, target_url, token)          # automatic mode
    db = StateDB("clone-state.sqlite", ctx.target_node_id, ctx.clock)
    report = clone("resource_map_urn:uuid:...", ctx, db, dry_run=True)  # look first
    report = clone("resource_map_urn:uuid:...", ctx, db, dry_run=False)
"""

from d1clone.context import Context, Deadlines, SetupError, build_context, make_context
from d1clone.orchestrator import VersionGraphHasGaps, clone, plan_package
from d1clone.results import GUIDANCE, CloneReport
from d1clone.state import StateDB

__all__ = [
    "Context", "Deadlines", "SetupError", "build_context", "make_context",
    "clone", "plan_package", "VersionGraphHasGaps",
    "CloneReport", "GUIDANCE", "StateDB",
]
