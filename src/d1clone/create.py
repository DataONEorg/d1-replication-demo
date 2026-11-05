"""Step 3: prepare the system metadata and create each object on the target.

The target keeps ``authoritativeMemberNode`` = the source node, which is what later makes
it accept the CN's system metadata. ``create()`` rejects the version-chain fields and a
SID already used on the node, so those are cleared here; the refresh (Step 5) restores
them from the CN.
"""

from __future__ import annotations

import logging
import os
import tempfile

import d1_common.checksum
import d1_common.types.exceptions as d1_exc

from d1clone.results import Failed, Ok, Retry
from d1clone.sysmeta import checksum_of, copy_sysmeta, text, utc

log = logging.getLogger(__name__)

CHUNK = 1024 * 1024


def prepare_sysmeta(cn_sm, sid_heads: dict[str, str]):
    """A copy of the CN's system metadata that the target's create() will accept."""
    sm = copy_sysmeta(cn_sm)
    sm.replica = []                     # the CN maintains the replica list
    sm.obsoletes = None                 # create() rejects version-chain fields
    sm.obsoletedBy = None
    pid = text(sm.identifier)
    sid = text(sm.seriesId)
    if sid and sid_heads.get(sid) != pid:
        sm.seriesId = None              # only one PID per node can hold a SID
    # authoritativeMemberNode, checksum, size, formatId, rightsHolder, accessPolicy and
    # replicationPolicy are kept unchanged.
    return sm


def download_to_tempfile(source, pid: str, directory: str | None = None) -> str:
    """Stream the object from the source to a temporary file; returns its path."""
    response = source.get(pid, stream=True)
    fd, path = tempfile.mkstemp(prefix="d1clone-", dir=directory)
    try:
        with os.fdopen(fd, "wb") as f:
            if hasattr(response, "iter_content"):
                for chunk in response.iter_content(CHUNK):
                    if chunk:
                        f.write(chunk)
            else:
                f.write(response.read())
    except Exception:
        os.unlink(path)
        raise
    return path


def file_checksum(path: str, algorithm: str) -> str:
    with open(path, "rb") as f:
        return d1_common.checksum.calculate_checksum_on_stream(f, algorithm).lower()


def existing_copy_matches(target, pid: str, sm) -> bool:
    alg, value = checksum_of(sm)
    return target.getChecksum(pid, alg).value().lower() == value


def create_on_target(source, target, pid: str, sm, *, workdir: str | None = None):
    """Download, check, and create one object. Ok.value is the target's dateSysMetadataModified."""
    alg, expected = checksum_of(sm)
    try:
        path = download_to_tempfile(source, pid, workdir)
    except d1_exc.DataONEException as e:
        return Retry("download", str(e))
    try:
        actual = file_checksum(path, alg)
        if actual != expected:
            return Failed("checksum", f"downloaded {alg} {actual}, system metadata says {expected}")
        if os.path.getsize(path) != int(sm.size):
            return Failed("checksum", f"downloaded {os.path.getsize(path)} bytes, system metadata says {sm.size}")
        try:
            with open(path, "rb") as f:
                target.create(pid, f, sm)
        except d1_exc.IdentifierNotUnique:
            # An earlier run may have created it before stopping.
            if not existing_copy_matches(target, pid, sm):
                return Failed("conflict", "the PID exists on the target with a different checksum")
            log.info("%s was already on the target", pid)
        except d1_exc.InvalidSystemMetadata as e:
            return Failed("create", str(e))
        except d1_exc.DataONEException as e:
            return Retry("create", str(e))
        return Ok(value=utc(target.getSystemMetadata(pid).dateSysMetadataModified))
    finally:
        os.unlink(path)
