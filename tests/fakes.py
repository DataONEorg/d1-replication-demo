"""In-memory stand-ins for a CN, Member Nodes and CN synchronization.

They implement the behavior the design depends on, as read from the source code
(Metacat develop/2.19.1 and d1_synchronization 2.3.7); see the notebook's
"Manual replication" section. Nothing here talks to a network.

- FakeMN.create():                 Metacat's MNodeService.create() rules
- FakeMN.systemMetadataChanged():  Metacat's systemMetadataChanged() (SID check, authoritative test)
- FakeCN.updateReplicationMetadata(): Metacat 2.19 CNodeService (notifies every replica MN)
- FakeNetwork.harvest():           ObjectListHarvestTask + V2TransferObjectTask replica handling
"""

from __future__ import annotations

import copy
import datetime
import hashlib
import io
from types import SimpleNamespace

import d1_common.resource_map
import d1_common.system_metadata as d1_sm
import d1_common.types.dataoneTypes_v1 as v1
import d1_common.types.exceptions as d1_exc

from d1clone.quartz import QuartzSchedule
from d1clone.sysmeta import serial, text, utc

UTC = datetime.timezone.utc

ORE_FORMAT = "http://www.openarchives.org/ore/terms"
EML_FORMAT = "eml://ecoinformatics.org/eml-2.1.1"
CSV_FORMAT = "text/csv"
FORMAT_TYPES = {CSV_FORMAT: "DATA", EML_FORMAT: "METADATA", ORE_FORMAT: "RESOURCE"}
_ALGS = {"SHA-1": hashlib.sha1, "MD5": hashlib.md5, "SHA-256": hashlib.sha256}


class Val:
    """Mimics a pyxb simple value: ``.value()``."""

    def __init__(self, v):
        self._v = v

    def value(self):
        return self._v


class FakeClock:
    def __init__(self, start=datetime.datetime(2026, 1, 5, 12, 0, 0, tzinfo=UTC)):
        self._now = start
        self.listeners = []
        self.slept = 0.0

    def now(self):
        return self._now

    def advance(self, seconds: float):
        old = self._now
        self._now = old + datetime.timedelta(seconds=seconds)
        for listener in self.listeners:
            listener(old, self._now)

    def sleep(self, seconds: float):
        self.slept += seconds
        self.advance(seconds)


class FakeResponse:
    def __init__(self, data: bytes):
        self.content = data

    def iter_content(self, n):
        for i in range(0, len(self.content), n):
            yield self.content[i:i + n]


class MNStore:
    """One Member Node's objects and system metadata."""

    def __init__(self, network, node_id):
        self.network = network
        self.node_id = node_id
        self.data: dict[str, bytes] = {}
        self.sysmeta: dict = {}
        self.unreachable = False          # misses notifications from the CN
        self.created = []                 # PIDs created through create()

    def identifier_exists(self, ident: str) -> bool:
        return ident in self.sysmeta or any(text(sm.seriesId) == ident for sm in self.sysmeta.values())

    def resolve(self, ident: str) -> str:
        if ident in self.sysmeta:
            return ident
        members = [p for p, sm in self.sysmeta.items() if text(sm.seriesId) == ident]
        if not members:
            raise d1_exc.NotFound("1060", f"{ident} not found on {self.node_id}")
        heads = [p for p in members if text(self.sysmeta[p].obsoletedBy) not in members]
        return max(heads or members, key=lambda p: utc(self.sysmeta[p].dateUploaded))

    # Metacat MNodeService.create()
    def create(self, pid, data: bytes, sm, submitter: str):
        if self.identifier_exists(pid):
            raise d1_exc.IdentifierNotUnique("1120", f"{pid} is already used")
        sm = copy.deepcopy(sm)
        now = self.network.clock.now()
        sm.submitter = submitter
        sm.originMemberNode = self.node_id
        if not text(sm.authoritativeMemberNode):
            sm.authoritativeMemberNode = self.node_id
        sm.archived = False
        sm.dateSysMetadataModified = now
        sm.dateUploaded = now
        sm.serialVersion = 0
        if text(sm.obsoletes) or text(sm.obsoletedBy):
            raise d1_exc.InvalidSystemMetadata("1180", "obsoletes/obsoletedBy can't have a value on create")
        sid = text(sm.seriesId)
        if sid and (self.identifier_exists(sid) or sid == pid):
            raise d1_exc.InvalidSystemMetadata("1180", f"the series identifier {sid} is already used")
        digest = _ALGS[sm.checksum.algorithm](data).hexdigest()
        if digest != sm.checksum.value().lower():
            raise d1_exc.InvalidSystemMetadata("1180", "the checksum doesn't match the object")
        self.data[pid] = data
        self.sysmeta[pid] = sm
        self.created.append(pid)

    # Metacat MNodeService.systemMetadataChanged(needCheckAuthoriativeNode=True)
    def system_metadata_changed(self, pid, serial_version: int):
        if pid not in self.sysmeta:
            raise d1_exc.InvalidRequest("1334", f"{pid} isn't on {self.node_id}")
        local = self.sysmeta[pid]
        if serial(local) > serial_version:
            return
        new = copy.deepcopy(self.network.cn_store[pid])
        new_sid, cur_sid = text(new.seriesId), text(local.seriesId)
        if new_sid:
            if cur_sid and cur_sid != new_sid and self.identifier_exists(new_sid):
                raise d1_exc.InvalidRequest("1334", f"the series identifier {new_sid} is used by another object")
            if not cur_sid and self.identifier_exists(new_sid):
                raise d1_exc.InvalidRequest("1334", f"the series identifier {new_sid} is used by another object")
        if text(local.authoritativeMemberNode) == self.node_id:
            local.serialVersion = new.serialVersion          # keep local copy; take serial and replicas
            local.replica = list(new.replica)
        else:
            self.sysmeta[pid] = new


class FakeMN:
    """A Member Node client (d1_python MemberNodeClient_2_0 interface)."""

    def __init__(self, network, node_id, identity="user"):
        self.network = network
        self.store = network.mns[node_id]
        self.identity = identity

    def getCapabilities(self):
        return SimpleNamespace(identifier=Val(self.store.node_id))

    def get(self, pid, stream=False):
        pid = self.store.resolve(pid)
        if pid in self.network.unreadable and self.identity != "cn":
            raise d1_exc.NotAuthorized("1000", f"can't read {pid}")
        return FakeResponse(self.store.data[pid])

    def getSystemMetadata(self, pid):
        pid = self.store.resolve(pid)
        return copy.deepcopy(self.store.sysmeta[pid])

    def getChecksum(self, pid, checksumAlgorithm=None):
        pid = self.store.resolve(pid)
        alg = checksumAlgorithm or "MD5"
        return SimpleNamespace(value=lambda: _ALGS[alg](self.store.data[pid]).hexdigest(), algorithm=alg)

    def create(self, pid, obj, sysmeta_pyxb):
        data = obj.read() if hasattr(obj, "read") else obj
        self.store.create(pid, data, sysmeta_pyxb, submitter=self.network.caller_subject)

    def systemMetadataChanged(self, pid, serialVersion, dateSysMetaLastModified):
        if self.identity != "cn":
            raise d1_exc.NotAuthorized("1331", "only a CN may call systemMetadataChanged")
        self.network.calls.append(("systemMetadataChanged", self.store.node_id, pid))
        self.store.system_metadata_changed(pid, int(serialVersion))
        return True

    def listObjects(self, fromDate=None, toDate=None, count=100, **kw):
        n = sum(1 for sm in self.store.sysmeta.values()
                if (fromDate is None or utc(sm.dateSysMetadataModified) > fromDate)
                and (toDate is None or utc(sm.dateSysMetadataModified) <= toDate))
        return SimpleNamespace(total=n)


class FakeCN:
    """A CN client (d1_python CoordinatingNodeClient_2_0 interface)."""

    def __init__(self, network, identity="user"):
        self.network = network
        self.identity = identity

    def listNodes(self):
        nodes = []
        for node_id, info in self.network.registry.items():
            fields = dict(zip(("sec", "min", "hour", "mday", "mon", "wday", "year"), info["schedule"].split(), strict=True))
            nodes.append(SimpleNamespace(
                identifier=Val(node_id), type=info.get("type", "mn"), synchronize=info["synchronize"],
                synchronization=SimpleNamespace(schedule=SimpleNamespace(**fields),
                                                lastHarvested=info["last_harvested"])))
        return SimpleNamespace(node=nodes)

    def getSystemMetadata(self, pid):
        if pid in self.network.unreadable and self.identity != "cn":
            raise d1_exc.NotAuthorized("1040", f"can't read {pid}")
        if pid not in self.network.cn_store:
            raise d1_exc.NotFound("1060", f"{pid} not found on the CN")
        return copy.deepcopy(self.network.cn_store[pid])

    def getFormat(self, formatId):
        if formatId not in FORMAT_TYPES:
            raise d1_exc.NotFound("4848", formatId)
        return SimpleNamespace(formatType=FORMAT_TYPES[formatId])

    # Metacat 2.19 CNodeService.updateReplicationMetadata()
    def updateReplicationMetadata(self, pid, replicaMetadata, serialVersion):
        if self.identity != "cn":
            raise d1_exc.NotAuthorized("4851", "not a CN")
        replicaMetadata.toxml("utf-8")                  # must be serializable, as d1_python sends it
        self.network.calls.append(("updateReplicationMetadata", pid))
        self.network.add_replica(pid, text(replicaMetadata.replicaMemberNode),
                                 str(replicaMetadata.replicationStatus), int(serialVersion))
        return True


class FakeNetwork:
    """The CN's store, the Member Nodes, the node registry and synchronization."""

    def __init__(self, clock: FakeClock):
        self.clock = clock
        self.cn_store: dict = {}
        self.mns: dict[str, MNStore] = {}
        self.registry: dict[str, dict] = {}
        self.unreadable: set[str] = set()
        self.calls: list = []
        self.sync_failures: list = []
        self.notification_errors: list = []
        self.harvest_limit = 2000
        self.caller_subject = "http://orcid.org/0000-0000-0000-0001"
        clock.listeners.append(self._on_time)

    # -- setup ---------------------------------------------------------------------------

    def add_mn(self, node_id, schedule="10 0/3 * * * ? *", synchronize=True):
        self.mns[node_id] = MNStore(self, node_id)
        self.registry[node_id] = {"schedule": schedule, "synchronize": synchronize,
                                  "last_harvested": datetime.datetime(1900, 1, 1, tzinfo=UTC)}
        return node_id

    def mn(self, node_id, identity="user") -> FakeMN:
        return FakeMN(self, node_id, identity)

    def cn(self, identity="user") -> FakeCN:
        return FakeCN(self, identity)

    def publish(self, node_id, pid, data: bytes, format_id=CSV_FORMAT, *, sid=None, obsoletes=None,
                replication_allowed=True, blocked=None):
        """Create an object on its authoritative node, already synchronized to the CN."""
        self.clock.advance(1)
        now = self.clock.now()
        sm = d1_sm.generate_system_metadata_pyxb(
            pid, format_id, io.BytesIO(data), "http://orcid.org/0000-0000-0000-0002",
            "http://orcid.org/0000-0000-0000-0002", node_id, sid=sid, obsoletes_pid=obsoletes,
            serial_version=1, uploaded_datetime=now, modified_datetime=now, origin_mn_urn=node_id,
            is_replication_allowed=replication_allowed, blocked_mn_list=blocked)
        store = self.mns[node_id]
        store.data[pid] = data
        store.sysmeta[pid] = sm
        cn_sm = copy.deepcopy(sm)
        cn_sm.replica.append(_replica(node_id, now))
        self.cn_store[pid] = cn_sm
        if obsoletes:
            for s in (store.sysmeta[obsoletes], self.cn_store[obsoletes]):
                s.obsoletedBy = pid
            self.cn_store[obsoletes].serialVersion = serial(self.cn_store[obsoletes]) + 1
        return sm

    def publish_package(self, node_id, rm_pid, scimeta_pid, data_pids, *, obsoletes=None, sid=None):
        ore = d1_common.resource_map.ResourceMap(ore_pid=rm_pid, scimeta_pid=scimeta_pid,
                                                 scidata_pid_list=list(data_pids))
        return self.publish(node_id, rm_pid, ore.serialize_to_transport(), ORE_FORMAT,
                            obsoletes=obsoletes, sid=sid)

    # -- CN behavior -----------------------------------------------------------------------

    def add_replica(self, pid, node_id, status, serial_version):
        sm = self.cn_store[pid]
        if serial(sm) != serial_version:
            raise d1_exc.VersionMismatch("4855", f"serialVersion {serial_version} isn't {serial(sm)}")
        for i, r in enumerate(list(sm.replica)):
            if text(r.replicaMemberNode) == node_id:
                if str(r.replicationStatus) == "completed" and status not in ("completed", "invalidated"):
                    raise d1_exc.InvalidRequest("4853", "status change from completed is prohibited")
                del sm.replica[i]
                break
        sm.replica.append(_replica(node_id, self.clock.now(), status))
        sm.serialVersion = serial(sm) + 1
        if status == "completed":
            self.notify_replicas(pid)

    def notify_replicas(self, pid, exclude=None):
        """Metacat CN notifyReplicaNodes(): every MN in the replica list; errors only logged."""
        for r in self.cn_store[pid].replica:
            node_id = text(r.replicaMemberNode)
            if node_id == exclude or node_id not in self.mns:
                continue
            store = self.mns[node_id]
            if store.unreachable:
                self.notification_errors.append((node_id, pid, "unreachable"))
                continue
            try:
                store.system_metadata_changed(pid, serial(self.cn_store[pid]))
            except d1_exc.DataONEException as e:
                self.notification_errors.append((node_id, pid, str(e.detailCode)))

    # -- synchronization ---------------------------------------------------------------------

    def harvest(self, node_id, at=None):
        """ObjectListHarvestTask: oldest first, at most harvest_limit, whole timestamps only."""
        info, store = self.registry[node_id], self.mns[node_id]
        last = info["last_harvested"]
        now = at or self.clock.now()
        candidates = sorted((utc(sm.dateSysMetadataModified), pid) for pid, sm in store.sysmeta.items()
                            if last < utc(sm.dateSysMetadataModified) <= now)
        taken: list = []
        for ts, pid in candidates:
            if len(taken) >= self.harvest_limit and taken[-1][0] != ts:
                break
            taken.append((ts, pid))
        for _, pid in taken:
            self.sync_object(node_id, pid)
        if taken:
            info["last_harvested"] = taken[-1][0]

    def sync_object(self, node_id, pid):
        """V2TransferObjectTask.processTask() for one harvested PID."""
        mn_sm = self.mns[node_id].sysmeta[pid]
        cn_sm = self.cn_store.get(pid)
        if cn_sm is None:                                   # processNewObject: the hazard
            new = copy.deepcopy(mn_sm)
            new.replica = [_replica(node_id, self.clock.now())]
            new.serialVersion = 1
            self.cn_store[pid] = new
            return
        if text(cn_sm.authoritativeMemberNode) == node_id:  # authoritative update: not used here
            return
        if mn_sm.checksum.value().lower() != cn_sm.checksum.value().lower():
            self.sync_failures.append((node_id, pid, "checksum"))
            return
        if any(text(r.replicaMemberNode) == node_id for r in cn_sm.replica):
            return                                          # existing replica: no action
        self.add_replica(pid, node_id, "completed", serial(cn_sm))   # notifies every replica MN
        self.notify_replicas(pid, exclude=node_id)                   # synchronization's second round

    def _on_time(self, old, new):
        for node_id, info in self.registry.items():
            if not info["synchronize"]:
                continue
            schedule = QuartzSchedule(info["schedule"])
            t = old
            while True:
                t = schedule.next_after(t)
                if t > new:
                    break
                self.harvest(node_id, at=t)


def _replica(node_id, when, status="completed"):
    r = v1.replica()
    r.replicaMemberNode = node_id
    r.replicationStatus = status
    r.replicaVerified = when
    return r
