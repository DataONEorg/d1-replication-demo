"""Step 5: refresh of the target's system metadata.

The CN notifies the target when it registers the replica (either mode). If that
notification was missed, CN mode calls ``MN.systemMetadataChanged()`` on the target
itself, with the CN's certificate; the target then fetches the CN's copy and stores it,
because its own copy names the source as authoritative. Automatic mode can't do this
and only reports the PID (see verify.stale_cause).
"""

from __future__ import annotations

import d1_common.types.exceptions as d1_exc

from d1clone.results import Failed, Ok, Retry
from d1clone.sysmeta import serial, text


def refresh_one(cn, target_as_cn, pid: str):
    """CN mode only."""
    sm = cn.getSystemMetadata(pid)
    try:
        target_as_cn.systemMetadataChanged(pid, serial(sm), sm.dateSysMetadataModified)
        return Ok()
    except d1_exc.InvalidRequest as e:
        if str(e.detailCode) == "1334" and text(sm.seriesId):
            return Failed("sid", str(e))           # the known SID limitation
        return Failed("refresh", str(e))
    except d1_exc.ServiceFailure as e:
        return Retry("refresh", str(e))
    except d1_exc.DataONEException as e:
        return Failed("refresh", str(e))
