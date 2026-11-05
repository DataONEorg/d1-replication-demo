"""Step 0: clients, credentials and mode.

``build_context()`` works with any client objects that have the d1_python client
interface (the tests use in-memory fakes). ``make_context()`` builds real d1_python
clients from URLs and credentials, then calls ``build_context()``.

Automatic mode needs only the caller's own credentials. CN mode is used only when the
caller provides the CN's node certificate *and* sets ``act_as_cn=True``.
"""

from __future__ import annotations

import datetime
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Callable

from d1clone.quartz import QuartzSchedule, UnsupportedSchedule

log = logging.getLogger(__name__)

UTC = datetime.timezone.utc


class SetupError(RuntimeError):
    """The clone can't start: missing credentials, or an unsuitable target node."""


class SystemClock:
    """Wall-clock time; the tests substitute a fake clock."""

    def now(self) -> datetime.datetime:
        return datetime.datetime.now(UTC)

    def sleep(self, seconds: float) -> None:
        time.sleep(seconds)


@dataclass
class Deadlines:
    """How long to wait before reporting a PID, and how often to look again."""

    stalled_fires: int = 3                    # scheduled harvests without progress -> "harvest stalled"
    after_harvest: datetime.timedelta = datetime.timedelta(minutes=1)    # look this long after a harvest
    registration: datetime.timedelta = datetime.timedelta(hours=6)       # CN queue, after the harvest
    refresh: datetime.timedelta = datetime.timedelta(hours=1)            # automatic mode: wait for a refresh
    harvest_fallback: datetime.timedelta = datetime.timedelta(hours=24)  # if the schedule can't be parsed
    poll: datetime.timedelta = datetime.timedelta(minutes=5)             # default interval between checks
    registered_poll: datetime.timedelta = datetime.timedelta(minutes=2)  # while queued on the CN
    max_attempts: int = 5                     # retries of one action before FAILED
    max_refreshes: int = 3                    # CN mode: STALE -> REGISTERED cycles


@dataclass
class Context:
    """Everything the stages need: clients, node identifiers, mode and settings."""

    cn: Any
    source: Any
    target: Any
    source_node_id: str
    target_node_id: str
    mode: str                                 # "automatic" or "cn"
    cn_as_cn: Any = None                      # CN mode only: CN client with the CN's certificate
    target_as_cn: Any = None                  # CN mode only: target client with the CN's certificate
    sync_schedule: QuartzSchedule | None = None
    deadlines: Deadlines = field(default_factory=Deadlines)
    clock: Any = field(default_factory=SystemClock)
    workers: int = 1                          # parallel creates (Step 3)
    registrations_per_minute: int = 60        # CN mode: limit on updateReplicationMetadata() calls
    harvest_limit: int = 2000                 # Synchronization.max_harvest_size; for estimates only
    workdir: str | None = None                # where downloads are staged

    def now(self) -> datetime.datetime:
        return self.clock.now()


def find_node(node_list, node_id: str):
    """The Node with ``node_id`` in a CN node list, or None."""
    for node in node_list.node:
        if node.identifier.value() == node_id:
            return node
    return None


def select_mode(*, act_as_cn: bool, have_cn_credentials: bool) -> str:
    if act_as_cn:
        if not have_cn_credentials:
            raise SetupError("act_as_cn=True needs the CN's certificate and key")
        return "cn"
    if have_cn_credentials:
        log.warning("CN credentials were given without act_as_cn=True; they won't be used")
    return "automatic"


def build_context(cn, source, target, *, cn_as_cn=None, target_as_cn=None, act_as_cn: bool = False,
                  deadlines: Deadlines | None = None, clock=None, **settings) -> Context:
    """Check the nodes, choose the mode, and read the target's harvest schedule."""
    source_node_id = source.getCapabilities().identifier.value()
    target_node_id = target.getCapabilities().identifier.value()
    if source_node_id == target_node_id:
        raise SetupError("the source and target are the same node")

    node = find_node(cn.listNodes(), target_node_id)
    if node is None or str(node.type) != "mn":
        raise SetupError(f"{target_node_id} isn't a Member Node in the CN's node registry")

    mode = select_mode(act_as_cn=act_as_cn,
                       have_cn_credentials=cn_as_cn is not None and target_as_cn is not None)
    schedule = None
    if mode == "automatic":
        if not bool(node.synchronize):
            raise SetupError("automatic mode needs synchronization enabled for the target node")
        try:
            schedule = QuartzSchedule.from_node(node)
        except (UnsupportedSchedule, AttributeError) as e:
            log.warning("can't evaluate the target's harvest schedule (%s); using fixed deadlines", e)

    return Context(
        cn=cn, source=source, target=target,
        source_node_id=source_node_id, target_node_id=target_node_id, mode=mode,
        cn_as_cn=cn_as_cn if mode == "cn" else None,
        target_as_cn=target_as_cn if mode == "cn" else None,
        sync_schedule=schedule,
        deadlines=deadlines or Deadlines(),
        clock=clock or SystemClock(),
        **settings,
    )


def make_context(cn_url: str, source_url: str, target_url: str, token: str, *,
                 cn_cert: str | None = None, cn_key: str | None = None, act_as_cn: bool = False,
                 client_factory: Callable | None = None, **kwargs) -> Context:
    """Build d1_python clients and a Context.

    ``token`` is the caller's DataONE token: it must be able to create objects on the
    target and read the objects on the source. ``cn_cert``/``cn_key`` are the CN's node
    certificate, used only in CN mode (``act_as_cn=True``).
    """
    if client_factory is None:
        import d1_client.cnclient_2_0
        import d1_client.mnclient_2_0

        def client_factory(kind, url, **opts):
            cls = (d1_client.cnclient_2_0.CoordinatingNodeClient_2_0 if kind == "cn"
                   else d1_client.mnclient_2_0.MemberNodeClient_2_0)
            return cls(url, **opts)

    headers = {"headers": {"Authorization": "Bearer " + token}}
    cn = client_factory("cn", cn_url, **headers)
    source = client_factory("mn", source_url, **headers)
    target = client_factory("mn", target_url, **headers)
    cn_as_cn = target_as_cn = None
    if act_as_cn:
        if not (cn_cert and cn_key):
            raise SetupError("act_as_cn=True needs the CN's certificate and key")
        cert = {"cert_pem_path": cn_cert, "cert_key_path": cn_key}
        cn_as_cn = client_factory("cn", cn_url, **cert)
        target_as_cn = client_factory("mn", target_url, **cert)
    elif cn_cert:
        log.warning("a CN certificate was given without act_as_cn=True; it won't be used")
    return build_context(cn, source, target, cn_as_cn=cn_as_cn, target_as_cn=target_as_cn,
                         act_as_cn=act_as_cn, **kwargs)
