"""定义服务分流中枢在模块边界使用的数据对象。"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class Zone:
    """表示夜市中一个可承接群众的服务区（义诊、推拿、文化讲解等）。"""

    zone_id: str
    site_id: str
    name: str
    service_type: str
    capacity: int
    serving_limit: int
    status: str
    version: int


@dataclass(frozen=True)
class Expert:
    """表示当班专家及其可承接的服务类型与禁忌资格。"""

    expert_id: str
    site_id: str
    display_name: str
    qualifications: frozenset[str]
    on_duty: bool
    zone_id: str | None
    version: int


@dataclass(frozen=True)
class Participant:
    """表示一位入场群众的最新行程视图。"""

    participant_id: str
    site_id: str
    status: str
    high_risk: bool
    contraindications: frozenset[str]
    risk_statements: frozenset[str]
    preferences: tuple[str, ...]
    accepted_services: tuple[dict[str, Any], ...]
    current_assignment: dict[str, Any] | None
    state_version: int
    updated_at: str


@dataclass(frozen=True)
class TicketView:
    """表示一张叫号票的当前状态。"""

    ticket_id: str
    zone_id: str
    participant_id: str
    queue_position: int
    state: str
    issued_at: str | None
    expires_at: str | None
    called_at: str | None
    released_reason: str | None
    miss_count: int


@dataclass(frozen=True)
class ZonePressure:
    """表示协调员看到的一个区域当前压力。"""

    zone_id: str
    name: str
    service_type: str
    status: str
    capacity: int
    serving_limit: int
    waiting: int
    called: int
    serving: int
    occupied: int
    available_capacity: int
    pressure_ratio: float
    on_duty_experts: int
    queue: list[dict[str, Any]]


@dataclass(frozen=True)
class HandshakeView:
    """表示一次服务中转区交接的当前状态。"""

    handshake_id: str
    participant_id: str
    from_zone_id: str
    to_zone_id: str
    status: str
    requested_by: str
    requested_at: str
    confirmed_by: str | None
    confirmed_at: str | None
    reason: str
    state_version: int
