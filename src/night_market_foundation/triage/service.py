"""实现服务分流中枢的核心规则。

设计要点：
- 所有写操作走 BEGIN IMMEDIATE 短事务，配合单调状态版本号，使过号释放、
  暂停/恢复、改派、交接在并发下按确定顺序生效；
- 高风险陈述一律转人工，系统只做结构化规则路由，不生成诊断；
- 服务中的参与者只能通过"申请-确认-完成"的明确交接转区；
- 每次行程变化写入 append-only 行程事件，并记录当时的状态版本；
- 写请求复用基础层 request_receipts 做幂等，重放返回原决定全文。
"""

from __future__ import annotations

import json
import re
import uuid
from datetime import datetime, timedelta
from typing import Any, Callable

from ..audit import append_event, canonical_json, digest
from ..clock import Clock, SystemClock
from ..errors import ConflictError, NotFoundError, PermissionDenied, ValidationError
from .models import HandshakeView, Participant, ZonePressure
from .rules import (
    CONTRAINDICATION_MAP,
    DEFAULT_CALL_TIMEOUT_SECONDS,
    HIGH_RISK_STATEMENTS,
    MAX_MISS_COUNT,
    SERVICE_TYPES,
    SUSPEND_GRACE_SECONDS,
    incompatible_services,
    is_high_risk,
)
from .storage import ensure_triage_schema

IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,63}$")
ACTIVE_TICKET_STATES = ("waiting", "called", "serving")
RECALLABLE_REASONS = ("missed", "suspended_timeout")
DEFAULT_PREFERENCE_ORDER = ("consultation", "massage", "culture_talk")


class TriageResult:
    """描述一次幂等分流写入的稳定结果。"""

    def __init__(self, request_id: str, action: str, resource_type: str,
                 resource_id: str, replayed: bool, decision: dict[str, Any]) -> None:
        self.request_id = request_id
        self.action = action
        self.resource_type = resource_type
        self.resource_id = resource_id
        self.replayed = replayed
        self.decision = decision

    def to_dict(self) -> dict[str, Any]:
        return {
            "request_id": self.request_id,
            "action": self.action,
            "resource_type": self.resource_type,
            "resource_id": self.resource_id,
            "replayed": self.replayed,
            "decision": self.decision,
        }


class TriageService:
    """保存区域容量、专家资格与参与者行程，并给出可追溯的分流决定。"""

    def __init__(self, database: Database, clock: Clock | None = None,
                 call_timeout_seconds: int = DEFAULT_CALL_TIMEOUT_SECONDS) -> None:
        self.database = database
        self.clock = clock or SystemClock()
        self.call_timeout_seconds = int(call_timeout_seconds)
        with self.database.transaction(immediate=True) as connection:
            ensure_triage_schema(connection)

    # ------------------------------------------------------------------
    # 基础工具
    # ------------------------------------------------------------------

    def _now(self) -> str:
        return self.clock.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ")

    def _after(self, seconds: int) -> str:
        return (self.clock.now() + timedelta(seconds=seconds)).strftime("%Y-%m-%dT%H:%M:%S.%fZ")

    def _identifier(self, value: str, field: str) -> str:
        value = str(value).strip()
        if not IDENTIFIER.fullmatch(value):
            raise ValidationError(f"{field} 格式无效")
        return value

    def _text(self, value: str, field: str, limit: int = 100) -> str:
        value = str(value).strip()
        if not value or len(value) > limit:
            raise ValidationError(f"{field} 不能为空且不能超过 {limit} 个字符")
        return value

    def _actor(self, connection, actor_id: str, *, writers_only: bool = False):
        row = connection.execute("SELECT * FROM actors WHERE actor_id=?", (actor_id,)).fetchone()
        if row is None:
            raise NotFoundError("操作者不存在")
        if not row["active"]:
            raise PermissionDenied("操作者已停用")
        if writers_only and row["role"] not in ("admin", "operator"):
            raise PermissionDenied("当前角色不能执行该动作")
        return row

    def _site_scope(self, connection, actor, site_id: str):
        site = connection.execute("SELECT * FROM sites WHERE site_id=?", (site_id,)).fetchone()
        if site is None:
            raise NotFoundError("场所不存在")
        if actor["organization_id"] != site["organization_id"] and actor["role"] != "admin":
            raise PermissionDenied("不能操作其他组织的场所")
        return site

    def _bump_version(self, connection) -> int:
        connection.execute("UPDATE triage_state_versions SET version = version + 1 WHERE singleton = 1")
        return self._version(connection)

    def _version(self, connection) -> int:
        return connection.execute(
            "SELECT version FROM triage_state_versions WHERE singleton = 1"
        ).fetchone()["version"]

    def _idempotent(self, connection, *, request_id: str, action: str, payload: dict[str, Any],
                    create: Callable[[], tuple[str, str, dict[str, Any]]]) -> TriageResult:
        request_id = self._identifier(request_id, "request_id")
        payload_hash = digest(payload)
        row = connection.execute(
            "SELECT * FROM request_receipts WHERE request_id=?", (request_id,)
        ).fetchone()
        if row:
            if row["action"] != action or row["payload_hash"] != payload_hash:
                raise ConflictError("request_id 已被不同内容使用")
            return TriageResult(request_id, action, row["resource_type"], row["resource_id"],
                                True, json.loads(row["response_json"]))
        resource_type, resource_id, decision = create()
        connection.execute(
            "INSERT INTO request_receipts(request_id,action,payload_hash,resource_type,resource_id,"
            "response_json,created_at) VALUES(?,?,?,?,?,?,?)",
            (request_id, action, payload_hash, resource_type, resource_id,
             canonical_json(decision), self._now()),
        )
        return TriageResult(request_id, action, resource_type, resource_id, False, decision)

    def _journey(self, connection, *, participant_id: str, event_type: str, actor_id: str,
                 zone_id: str | None = None, expert_id: str | None = None,
                 ticket_id: str | None = None, detail: dict[str, Any] | None = None) -> None:
        version = self._version(connection)
        row = connection.execute(
            "SELECT COALESCE(MAX(seq), 0) AS seq FROM triage_journey_events WHERE participant_id=?",
            (participant_id,),
        ).fetchone()
        connection.execute(
            "INSERT INTO triage_journey_events(event_id,participant_id,seq,event_type,zone_id,expert_id,"
            "ticket_id,state_version,detail_json,actor_id,occurred_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
            (uuid.uuid4().hex, participant_id, row["seq"] + 1, event_type, zone_id, expert_id,
             ticket_id, version, canonical_json(detail or {}), actor_id, self._now()),
        )
        connection.execute(
            "UPDATE triage_participants SET state_version=?, updated_at=? WHERE participant_id=?",
            (version, self._now(), participant_id),
        )

    def _audit(self, connection, *, actor_id: str, action: str, resource_type: str,
               resource_id: str, detail: dict[str, Any]) -> None:
        append_event(connection, actor_id=actor_id, action=f"triage.{action}",
                     resource_type=resource_type, resource_id=resource_id,
                     detail=detail, occurred_at=self._now())

    # ------------------------------------------------------------------
    # 配置登记：区域与专家（变化即推进状态版本）
    # ------------------------------------------------------------------

    def register_zone(self, *, request_id: str, actor_id: str, site_id: str, zone_id: str,
                      name: str, service_type: str, capacity: int, serving_limit: int) -> TriageResult:
        payload = {"actor_id": actor_id, "site_id": site_id, "zone_id": zone_id, "name": name,
                   "service_type": service_type, "capacity": capacity, "serving_limit": serving_limit}

        with self.database.transaction(immediate=True) as connection:
            actor = self._actor(connection, actor_id, writers_only=True)
            self._site_scope(connection, actor, site_id)
            zone_id = self._identifier(zone_id, "zone_id")
            name = self._text(name, "name")
            if service_type not in SERVICE_TYPES:
                raise ValidationError("service_type 不在允许范围内")
            capacity = int(capacity)
            serving_limit = int(serving_limit)
            if capacity < 1 or serving_limit < 1 or serving_limit > capacity:
                raise ValidationError("容量必须为正整数且服务位数不超过总容量")

            def create() -> tuple[str, str, dict[str, Any]]:
                if connection.execute("SELECT 1 FROM triage_zones WHERE zone_id=?", (zone_id,)).fetchone():
                    raise ConflictError("区域编号已经存在")
                now = self._now()
                connection.execute(
                    "INSERT INTO triage_zones(zone_id,site_id,name,service_type,capacity,serving_limit,"
                    "status,suspended_at,version,created_at,updated_at) "
                    "VALUES(?,?,?,?,?,?,'active',NULL,1,?,?)",
                    (zone_id, site_id, name, service_type, capacity, serving_limit, now, now),
                )
                version = self._bump_version(connection)
                connection.execute("UPDATE triage_zones SET version=?, updated_at=? WHERE zone_id=?",
                                   (version, self._now(), zone_id))
                self._audit(connection, actor_id=actor_id, action="zone.registered",
                            resource_type="zone", resource_id=zone_id,
                            detail={"site_id": site_id, "service_type": service_type,
                                    "capacity": capacity, "serving_limit": serving_limit,
                                    "state_version": version})
                decision = {"zone_id": zone_id, "state_version": version, "status": "active"}
                return "zone", zone_id, decision

            return self._idempotent(connection, request_id=request_id, action="triage.register_zone",
                                    payload=payload, create=create)

    def set_zone_capacity(self, *, request_id: str, actor_id: str, zone_id: str,
                          capacity: int, serving_limit: int) -> TriageResult:
        payload = {"actor_id": actor_id, "zone_id": zone_id, "capacity": capacity,
                   "serving_limit": serving_limit}

        with self.database.transaction(immediate=True) as connection:
            actor = self._actor(connection, actor_id, writers_only=True)
            zone = self._zone_row(connection, actor, zone_id)
            capacity = int(capacity)
            serving_limit = int(serving_limit)
            if capacity < 1 or serving_limit < 1 or serving_limit > capacity:
                raise ValidationError("容量必须为正整数且服务位数不超过总容量")

            def create() -> tuple[str, str, dict[str, Any]]:
                connection.execute(
                    "UPDATE triage_zones SET capacity=?, serving_limit=?, updated_at=? WHERE zone_id=?",
                    (capacity, serving_limit, self._now(), zone_id),
                )
                version = self._bump_version(connection)
                connection.execute("UPDATE triage_zones SET version=?, updated_at=? WHERE zone_id=?",
                                   (version, self._now(), zone_id))
                self._audit(connection, actor_id=actor_id, action="zone.capacity_changed",
                            resource_type="zone", resource_id=zone_id,
                            detail={"capacity": capacity, "serving_limit": serving_limit,
                                    "previous_capacity": zone["capacity"],
                                    "previous_serving_limit": zone["serving_limit"],
                                    "state_version": version})
                return "zone", zone_id, {"zone_id": zone_id, "state_version": version,
                                         "capacity": capacity, "serving_limit": serving_limit}

            return self._idempotent(connection, request_id=request_id, action="triage.set_zone_capacity",
                                    payload=payload, create=create)

    def _set_zone_status(self, connection, actor, zone_id: str, status: str) -> dict[str, Any]:
        zone = self._zone_row(connection, actor, zone_id)
        now = self._now()
        suspended_at = now if status == "suspended" else None
        connection.execute(
            "UPDATE triage_zones SET status=?, suspended_at=?, updated_at=? WHERE zone_id=?",
            (status, suspended_at, now, zone_id),
        )
        version = self._bump_version(connection)
        connection.execute("UPDATE triage_zones SET version=?, updated_at=? WHERE zone_id=?",
                           (version, now, zone_id))
        self._audit(connection, actor_id=actor["actor_id"],
                    action="zone.suspended" if status == "suspended" else "zone.resumed",
                    resource_type="zone", resource_id=zone_id,
                    detail={"previous_status": zone["status"], "state_version": version})
        return {"zone_id": zone_id, "status": status, "state_version": version}

    def suspend_zone(self, *, request_id: str, actor_id: str, zone_id: str) -> TriageResult:
        payload = {"actor_id": actor_id, "zone_id": zone_id}
        with self.database.transaction(immediate=True) as connection:
            actor = self._actor(connection, actor_id, writers_only=True)

            def create() -> tuple[str, str, dict[str, Any]]:
                zone = self._zone_row(connection, actor, zone_id)
                if zone["status"] == "suspended":
                    raise ConflictError("区域已经暂停")
                decision = self._set_zone_status(connection, actor, zone_id, "suspended")
                return "zone", zone_id, decision

            return self._idempotent(connection, request_id=request_id, action="triage.suspend_zone",
                                    payload=payload, create=create)

    def resume_zone(self, *, request_id: str, actor_id: str, zone_id: str) -> TriageResult:
        payload = {"actor_id": actor_id, "zone_id": zone_id}
        with self.database.transaction(immediate=True) as connection:
            actor = self._actor(connection, actor_id, writers_only=True)

            def create() -> tuple[str, str, dict[str, Any]]:
                zone = self._zone_row(connection, actor, zone_id)
                if zone["status"] == "active":
                    raise ConflictError("区域正在运行")
                decision = self._set_zone_status(connection, actor, zone_id, "active")
                return "zone", zone_id, decision

            return self._idempotent(connection, request_id=request_id, action="triage.resume_zone",
                                    payload=payload, create=create)

    def register_expert(self, *, request_id: str, actor_id: str, site_id: str, expert_id: str,
                        display_name: str, qualifications: list[str], zone_id: str | None = None,
                        on_duty: bool = True) -> TriageResult:
        qualifications = sorted(set(qualifications or []))
        payload = {"actor_id": actor_id, "site_id": site_id, "expert_id": expert_id,
                   "display_name": display_name, "qualifications": qualifications,
                   "zone_id": zone_id, "on_duty": bool(on_duty)}

        with self.database.transaction(immediate=True) as connection:
            actor = self._actor(connection, actor_id, writers_only=True)
            self._site_scope(connection, actor, site_id)
            expert_id = self._identifier(expert_id, "expert_id")
            display_name = self._text(display_name, "display_name")
            if not qualifications or any(q not in SERVICE_TYPES for q in qualifications):
                raise ValidationError("qualifications 至少包含一个受支持的服务类型")
            if zone_id is not None:
                zone = self._zone_row(connection, actor, zone_id)
                if zone["site_id"] != site_id:
                    raise ValidationError("专家岗位与场所不一致")

            def create() -> tuple[str, str, dict[str, Any]]:
                if connection.execute("SELECT 1 FROM triage_experts WHERE expert_id=?", (expert_id,)).fetchone():
                    raise ConflictError("专家编号已经存在")
                now = self._now()
                connection.execute(
                    "INSERT INTO triage_experts(expert_id,site_id,display_name,qualifications_json,on_duty,"
                    "zone_id,version,created_at,updated_at) VALUES(?,?,?,?,?,?,1,?,?)",
                    (expert_id, site_id, display_name, canonical_json(qualifications),
                     1 if on_duty else 0, zone_id, now, now),
                )
                version = self._bump_version(connection)
                connection.execute("UPDATE triage_experts SET version=? WHERE expert_id=?",
                                   (version, expert_id))
                self._audit(connection, actor_id=actor_id, action="expert.registered",
                            resource_type="expert", resource_id=expert_id,
                            detail={"site_id": site_id, "qualifications": qualifications,
                                    "zone_id": zone_id, "on_duty": bool(on_duty),
                                    "state_version": version})
                return "expert", expert_id, {"expert_id": expert_id, "state_version": version,
                                             "zone_id": zone_id, "on_duty": bool(on_duty)}

            return self._idempotent(connection, request_id=request_id, action="triage.register_expert",
                                    payload=payload, create=create)

    def update_expert(self, *, request_id: str, actor_id: str, expert_id: str,
                      zone_id: str | None = None, on_duty: bool | None = None,
                      qualifications: list[str] | None = None) -> TriageResult:
        payload = {"actor_id": actor_id, "expert_id": expert_id, "zone_id": zone_id,
                   "on_duty": None if on_duty is None else bool(on_duty),
                   "qualifications": None if qualifications is None else sorted(set(qualifications))}

        with self.database.transaction(immediate=True) as connection:
            actor = self._actor(connection, actor_id, writers_only=True)
            expert = self._expert_row(connection, actor, expert_id)
            new_zone_id = expert["zone_id"] if zone_id is None else zone_id
            new_on_duty = expert["on_duty"] if on_duty is None else (1 if on_duty else 0)
            old_qualifications = json.loads(expert["qualifications_json"])
            if qualifications is None:
                new_qualifications = old_qualifications
            else:
                new_qualifications = sorted(set(qualifications))
                if not new_qualifications or any(q not in SERVICE_TYPES for q in new_qualifications):
                    raise ValidationError("qualifications 至少包含一个受支持的服务类型")
            if new_zone_id is not None:
                target_zone = self._zone_row(connection, actor, new_zone_id)
                if target_zone["site_id"] != expert["site_id"]:
                    raise ValidationError("专家岗位与场所不一致")
            if (new_zone_id == expert["zone_id"] and new_on_duty == expert["on_duty"]
                    and new_qualifications == old_qualifications):
                raise ValidationError("专家排班没有任何变化")

            def create() -> tuple[str, str, dict[str, Any]]:
                now = self._now()
                connection.execute(
                    "UPDATE triage_experts SET zone_id=?, on_duty=?, qualifications_json=?, "
                    "updated_at=? WHERE expert_id=?",
                    (new_zone_id, new_on_duty, canonical_json(new_qualifications), now, expert_id),
                )
                version = self._bump_version(connection)
                connection.execute("UPDATE triage_experts SET version=? WHERE expert_id=?",
                                   (version, expert_id))
                self._audit(connection, actor_id=actor_id, action="expert.updated",
                            resource_type="expert", resource_id=expert_id,
                            detail={"previous_zone_id": expert["zone_id"], "zone_id": new_zone_id,
                                    "previous_on_duty": bool(expert["on_duty"]),
                                    "on_duty": bool(new_on_duty),
                                    "previous_qualifications": old_qualifications,
                                    "qualifications": new_qualifications, "state_version": version})
                return "expert", expert_id, {"expert_id": expert_id, "state_version": version,
                                             "zone_id": new_zone_id, "on_duty": bool(new_on_duty),
                                             "qualifications": new_qualifications}

            return self._idempotent(connection, request_id=request_id, action="triage.update_expert",
                                    payload=payload, create=create)

    # ------------------------------------------------------------------
    # 入场登记与风险筛查
    # ------------------------------------------------------------------

    def intake_participant(self, *, request_id: str, actor_id: str, site_id: str,
                           participant_id: str, risk_statements: list[str] | None = None,
                           contraindications: list[str] | None = None,
                           preferences: list[str] | None = None) -> TriageResult:
        risk_statements = sorted(set(risk_statements or []))
        contraindications = sorted(set(contraindications or []))
        preferences = list(preferences) if preferences is not None else list(DEFAULT_PREFERENCE_ORDER)
        payload = {"actor_id": actor_id, "site_id": site_id, "participant_id": participant_id,
                   "risk_statements": risk_statements, "contraindications": contraindications,
                   "preferences": preferences}

        with self.database.transaction(immediate=True) as connection:
            actor = self._actor(connection, actor_id, writers_only=True)
            self._site_scope(connection, actor, site_id)
            participant_id = self._identifier(participant_id, "participant_id")
            if any(code not in HIGH_RISK_STATEMENTS for code in risk_statements):
                raise ValidationError("存在未登记的风险陈述编码")
            if any(code not in CONTRAINDICATION_MAP for code in contraindications):
                raise ValidationError("存在未登记的禁忌编码")
            if not preferences or any(p not in SERVICE_TYPES for p in preferences):
                raise ValidationError("preferences 必须是受支持服务类型的非空序列")
            if len(set(preferences)) != len(preferences):
                raise ValidationError("preferences 不能重复")
            high_risk = is_high_risk(risk_statements)
            status = "manual_review" if high_risk else "screened"

            def create() -> tuple[str, str, dict[str, Any]]:
                if connection.execute(
                    "SELECT 1 FROM triage_participants WHERE participant_id=?", (participant_id,)
                ).fetchone():
                    raise ConflictError("参与者编号已经存在")
                now = self._now()
                connection.execute(
                    "INSERT INTO triage_participants(participant_id,site_id,status,high_risk,"
                    "contraindications_json,risk_statements_json,preferences_json,accepted_services_json,"
                    "current_zone_id,current_ticket_id,state_version,created_at,updated_at) "
                    "VALUES(?,?,?,?,?,?,?,?,?,?,0,?,?)",
                    (participant_id, site_id, status, 1 if high_risk else 0,
                     canonical_json(contraindications), canonical_json(risk_statements),
                     canonical_json(preferences), canonical_json([]), None, None, now, now),
                )
                self._journey(connection, participant_id=participant_id, event_type="intake",
                              actor_id=actor_id,
                              detail={"site_id": site_id, "risk_statements": risk_statements,
                                      "contraindications": contraindications,
                                      "preferences": preferences, "high_risk": high_risk,
                                      "routing": "manual_review" if high_risk else "screened"})
                self._audit(connection, actor_id=actor_id, action="participant.intake",
                            resource_type="participant", resource_id=participant_id,
                            detail={"site_id": site_id, "high_risk": high_risk,
                                    "contraindications": contraindications,
                                    "risk_statements": risk_statements})
                decision = {"participant_id": participant_id, "status": status,
                            "high_risk": high_risk,
                            "routing": "manual_review" if high_risk else "ready_to_route",
                            "blocked_services": sorted(incompatible_services(frozenset(contraindications)))}
                return "participant", participant_id, decision

            return self._idempotent(connection, request_id=request_id, action="triage.intake_participant",
                                    payload=payload, create=create)

    def review_participant(self, *, request_id: str, actor_id: str, participant_id: str,
                           note: str) -> TriageResult:
        """由现场协调员人工处理高风险陈述后登记结论，系统不生成任何诊断。"""

        payload = {"actor_id": actor_id, "participant_id": participant_id, "note": note}
        with self.database.transaction(immediate=True) as connection:
            actor = self._actor(connection, actor_id, writers_only=True)
            participant = self._participant_row(connection, actor, participant_id)
            note = self._text(note, "note", 500)
            if participant["status"] != "manual_review":
                raise ConflictError("该参与者不在待人工处理状态")

            def create() -> tuple[str, str, dict[str, Any]]:
                now = self._now()
                connection.execute(
                    "UPDATE triage_participants SET status='screened', updated_at=? WHERE participant_id=?",
                    (now, participant_id),
                )
                self._journey(connection, participant_id=participant_id, event_type="manual_review_resolved",
                              actor_id=actor_id, detail={"note": note, "resolved_by": actor_id})
                self._audit(connection, actor_id=actor_id, action="participant.reviewed",
                            resource_type="participant", resource_id=participant_id,
                            detail={"note": note})
                return "participant", participant_id, {"participant_id": participant_id,
                                                       "status": "screened"}

            return self._idempotent(connection, request_id=request_id, action="triage.review_participant",
                                    payload=payload, create=create)

    # ------------------------------------------------------------------
    # 分流计算
    # ------------------------------------------------------------------

    def _zone_counts(self, connection, zone_id: str) -> dict[str, int]:
        counts = {"waiting": 0, "called": 0, "serving": 0}
        rows = connection.execute(
            f"SELECT state, COUNT(*) AS count FROM triage_tickets WHERE zone_id=? AND state IN "
            f"({','.join('?' * len(ACTIVE_TICKET_STATES))}) GROUP BY state",
            (zone_id, *ACTIVE_TICKET_STATES),
        ).fetchall()
        for row in rows:
            counts[row["state"]] = row["count"]
        return counts

    def _zone_has_coverage(self, connection, zone_id: str, service_type: str) -> bool:
        for row in connection.execute(
            "SELECT qualifications_json FROM triage_experts WHERE zone_id=? AND on_duty=1",
            (zone_id,),
        ):
            if service_type in json.loads(row["qualifications_json"]):
                return True
        return False

    def _zone_viable_for(self, connection, zone, participant) -> bool:
        if zone["status"] != "active":
            return False
        contraindications = frozenset(json.loads(participant["contraindications_json"]))
        if zone["service_type"] in incompatible_services(contraindications):
            return False
        if not self._zone_has_coverage(connection, zone["zone_id"], zone["service_type"]):
            return False
        counts = self._zone_counts(connection, zone["zone_id"])
        return sum(counts.values()) < zone["capacity"]

    def _best_zone(self, connection, participant, *, excluded_zone_id: str | None = None):
        contraindications = frozenset(json.loads(participant["contraindications_json"]))
        blocked = incompatible_services(contraindications)
        preferences = json.loads(participant["preferences_json"])
        accepted = {item["service_type"] for item in json.loads(participant["accepted_services_json"])}
        candidates = []
        zones = connection.execute(
            "SELECT * FROM triage_zones WHERE site_id=? ORDER BY zone_id",
            (participant["site_id"],),
        ).fetchall()
        for zone in zones:
            if zone["zone_id"] == excluded_zone_id:
                continue
            if zone["status"] != "active" or zone["service_type"] in blocked:
                continue
            if zone["service_type"] in accepted:
                continue
            counts = self._zone_counts(connection, zone["zone_id"])
            occupied = sum(counts.values())
            if occupied >= zone["capacity"]:
                continue
            if not self._zone_has_coverage(connection, zone["zone_id"], zone["service_type"]):
                continue
            try:
                preference_rank = preferences.index(zone["service_type"])
            except ValueError:
                preference_rank = len(preferences)
            pressure = occupied / zone["capacity"]
            candidates.append((preference_rank, pressure, zone["zone_id"], zone))
        if not candidates:
            return None
        candidates.sort(key=lambda item: (item[0], item[1], item[2]))
        return candidates[0][3]

    def _new_ticket_seq(self, connection, zone_id: str) -> int:
        row = connection.execute(
            "SELECT COALESCE(MAX(seq_in_zone), 0) AS seq FROM triage_tickets WHERE zone_id=?",
            (zone_id,),
        ).fetchone()
        return row["seq"] + 1

    def route_participant(self, *, request_id: str, actor_id: str, participant_id: str) -> TriageResult:
        payload = {"actor_id": actor_id, "participant_id": participant_id}
        with self.database.transaction(immediate=True) as connection:
            actor = self._actor(connection, actor_id, writers_only=True)
            participant = self._participant_row(connection, actor, participant_id)

            def create() -> tuple[str, str, dict[str, Any]]:
                self._apply_pending(connection)
                participant = connection.execute(
                    "SELECT * FROM triage_participants WHERE participant_id=?", (participant_id,)
                ).fetchone()
                if participant["status"] == "manual_review":
                    raise ConflictError("高风险陈述必须先转人工处理")
                if participant["status"] == "completed":
                    raise ConflictError("行程已结束")
                ticket = None
                if participant["current_ticket_id"]:
                    ticket = connection.execute(
                        "SELECT * FROM triage_tickets WHERE ticket_id=?",
                        (participant["current_ticket_id"],),
                    ).fetchone()
                if ticket is not None and ticket["state"] in ACTIVE_TICKET_STATES:
                    raise ConflictError("参与者仍有进行中的叫号票")
                basis_version = self._version(connection)
                zone = self._best_zone(connection, participant)
                if zone is None:
                    decision = {"participant_id": participant_id, "routing": "no_capacity",
                                "state_version": basis_version}
                    self._journey(connection, participant_id=participant_id, event_type="route_deferred",
                                  actor_id=actor_id, detail=decision)
                    return "participant", participant_id, decision
                # 重新分流时作废旧票上的过号标记，避免同一人被重复重呼。
                connection.execute(
                    "UPDATE triage_tickets SET miss_seq=NULL WHERE participant_id=? "
                    "AND state='released' AND miss_seq IS NOT NULL",
                    (participant_id,),
                )
                ticket_id = uuid.uuid4().hex
                now = self._now()
                seq = self._new_ticket_seq(connection, zone["zone_id"])
                connection.execute(
                    "INSERT INTO triage_tickets(ticket_id,zone_id,participant_id,state,seq_in_zone,"
                    "miss_count,issued_at) VALUES(?,?,?,'waiting',?,0,?)",
                    (ticket_id, zone["zone_id"], participant_id, seq, now),
                )
                connection.execute(
                    "UPDATE triage_participants SET status='routed', current_zone_id=?, "
                    "current_ticket_id=?, updated_at=? WHERE participant_id=?",
                    (zone["zone_id"], ticket_id, now, participant_id),
                )
                decision = {"participant_id": participant_id, "routing": "queued",
                            "zone_id": zone["zone_id"], "service_type": zone["service_type"],
                            "ticket_id": ticket_id, "queue_sequence": seq,
                            "state_version": basis_version}
                self._journey(connection, participant_id=participant_id, event_type="routed",
                              actor_id=actor_id, zone_id=zone["zone_id"], ticket_id=ticket_id,
                              detail=decision)
                self._audit(connection, actor_id=actor_id, action="participant.routed",
                            resource_type="participant", resource_id=participant_id,
                            detail={"zone_id": zone["zone_id"], "ticket_id": ticket_id,
                                    "state_version": basis_version})
                return "ticket", ticket_id, decision

            return self._idempotent(connection, request_id=request_id, action="triage.route_participant",
                                    payload=payload, create=create)

    # ------------------------------------------------------------------
    # 叫号、过号与超时释放
    # ------------------------------------------------------------------

    def _apply_pending(self, connection) -> list[dict[str, Any]]:
        """按确定顺序释放所有应当过期的叫号占用。

        扫描全部 called 票并以 (触发时间, ticket_id) 排序：普通超时以
        expires_at 为准；区域暂停时以暂停宽限截止时间与 expires_at 的较早者
        为准。停机期间到期的占用会在重启后的首次调用中按同一顺序补齐。
        """

        now = self._now()
        due: list[tuple[str, str, str, object]] = []
        tickets = connection.execute(
            "SELECT t.*, z.status AS zone_status, z.suspended_at AS zone_suspended_at "
            "FROM triage_tickets t JOIN triage_zones z ON z.zone_id=t.zone_id "
            "WHERE t.state='called'"
        ).fetchall()
        for ticket in tickets:
            reason = None
            deadline = None
            if ticket["expires_at"] and ticket["expires_at"] <= now:
                reason, deadline = "missed", ticket["expires_at"]
            if ticket["zone_status"] == "suspended" and ticket["zone_suspended_at"]:
                suspended_deadline = (
                    datetime.strptime(ticket["zone_suspended_at"], "%Y-%m-%dT%H:%M:%S.%fZ")
                    + timedelta(seconds=SUSPEND_GRACE_SECONDS)
                ).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
                if suspended_deadline <= now and (deadline is None or suspended_deadline < deadline):
                    reason, deadline = "suspended_timeout", suspended_deadline
            if reason is not None:
                due.append((deadline, ticket["ticket_id"], reason, ticket))
        due.sort(key=lambda item: (item[0], item[1]))
        return [self._release_called_ticket(connection, item[3], item[2], now) for item in due]

    def _next_miss_seq(self, connection, zone_id: str) -> int:
        row = connection.execute(
            "SELECT COALESCE(MAX(miss_seq), 0) AS seq FROM triage_tickets WHERE zone_id=?",
            (zone_id,),
        ).fetchone()
        return row["seq"] + 1

    def _release_called_ticket(self, connection, ticket, reason: str, now: str) -> dict[str, Any]:
        miss_count = ticket["miss_count"] + 1
        terminal = miss_count > MAX_MISS_COUNT
        miss_seq = None if terminal else self._next_miss_seq(connection, ticket["zone_id"])
        connection.execute(
            "UPDATE triage_tickets SET state='released', released_at=?, released_reason=?, "
            "miss_count=?, miss_seq=? WHERE ticket_id=?",
            (now, reason, miss_count, miss_seq, ticket["ticket_id"]),
        )
        participant_id = ticket["participant_id"]
        if terminal:
            connection.execute(
                "UPDATE triage_participants SET current_zone_id=NULL, current_ticket_id=NULL, "
                "status='screened', updated_at=? WHERE participant_id=?",
                (now, participant_id),
            )
        self._journey(connection, participant_id=participant_id, event_type="ticket_released",
                      actor_id="system", zone_id=ticket["zone_id"], ticket_id=ticket["ticket_id"],
                      detail={"reason": reason, "miss_count": miss_count,
                              "recallable": not terminal, "terminal": terminal})
        return {"ticket_id": ticket["ticket_id"], "zone_id": ticket["zone_id"],
                "participant_id": participant_id, "reason": reason, "miss_count": miss_count,
                "recallable": not terminal}

    def expire_tickets(self, *, request_id: str, actor_id: str) -> TriageResult:
        """显式触发超时扫描；同样的扫描在每次叫号/分流前自动执行。"""

        payload = {"actor_id": actor_id}
        with self.database.transaction(immediate=True) as connection:
            actor = self._actor(connection, actor_id, writers_only=True)

            def create() -> tuple[str, str, dict[str, Any]]:
                released = self._apply_pending(connection)
                for item in released:
                    self._audit(connection, actor_id=actor_id, action="ticket.expired",
                                resource_type="ticket", resource_id=item["ticket_id"],
                                detail={"reason": item["reason"], "miss_count": item["miss_count"]})
                return "expiry_scan", actor_id, {"released": released,
                                                 "state_version": self._version(connection)}

            return self._idempotent(connection, request_id=request_id, action="triage.expire_tickets",
                                    payload=payload, create=create)

    def call_next(self, *, request_id: str, actor_id: str, zone_id: str | None = None,
                  max_calls: int = 1) -> TriageResult:
        payload = {"actor_id": actor_id, "zone_id": zone_id, "max_calls": int(max_calls)}
        with self.database.transaction(immediate=True) as connection:
            actor = self._actor(connection, actor_id, writers_only=True)
            max_calls = int(max_calls)
            if max_calls < 1 or max_calls > 100:
                raise ValidationError("max_calls 必须在 1 到 100 之间")
            if zone_id is not None:
                self._zone_row(connection, actor, zone_id)

            def create() -> tuple[str, str, dict[str, Any]]:
                self._apply_pending(connection)
                calls: list[dict[str, Any]] = []
                if zone_id is None:
                    zone_rows = connection.execute(
                        "SELECT * FROM triage_zones ORDER BY zone_id"
                    ).fetchall()
                else:
                    zone_rows = [connection.execute(
                        "SELECT * FROM triage_zones WHERE zone_id=?", (zone_id,)
                    ).fetchone()]
                for zone in zone_rows:
                    if zone is None or zone["status"] != "active":
                        continue
                    while len(calls) < max_calls:
                        counts = self._zone_counts(connection, zone["zone_id"])
                        if counts["called"] + counts["serving"] >= zone["serving_limit"]:
                            break
                        ticket = self._pick_next_ticket(connection, zone["zone_id"])
                        if ticket is None:
                            break
                        calls.append(self._call_ticket(connection, zone, ticket, actor["actor_id"]))
                decision = {"calls": calls, "state_version": self._version(connection)}
                return "call_batch", actor_id, decision

            return self._idempotent(connection, request_id=request_id, action="triage.call_next",
                                    payload=payload, create=create)

    def _pick_next_ticket(self, connection, zone_id: str):
        # 过号票按 miss_seq 优先重呼；若该参与者另有进行中的票则跳过，避免重复占队。
        other_active = ("AND NOT EXISTS (SELECT 1 FROM triage_tickets t2 "
                        "WHERE t2.participant_id=t.participant_id AND t2.ticket_id<>t.ticket_id "
                        f"AND t2.state IN ({','.join('?' * len(ACTIVE_TICKET_STATES))}))")
        recalled = connection.execute(
            f"SELECT t.* FROM triage_tickets t WHERE t.zone_id=? AND t.state='released' "
            f"AND t.released_reason IN ({','.join('?' * len(RECALLABLE_REASONS))}) "
            f"AND t.miss_seq IS NOT NULL {other_active} ORDER BY t.miss_seq, t.ticket_id LIMIT 1",
            (zone_id, *RECALLABLE_REASONS, *ACTIVE_TICKET_STATES),
        ).fetchone()
        if recalled is not None:
            return recalled
        return connection.execute(
            f"SELECT t.* FROM triage_tickets t WHERE t.zone_id=? AND t.state='waiting' {other_active} "
            "ORDER BY t.issued_at, t.seq_in_zone, t.ticket_id LIMIT 1",
            (zone_id, *ACTIVE_TICKET_STATES),
        ).fetchone()

    def _call_ticket(self, connection, zone, ticket, actor_id: str) -> dict[str, Any]:
        now = self._now()
        expires = self._after(self.call_timeout_seconds)
        recalled = ticket["state"] == "released"
        connection.execute(
            "UPDATE triage_tickets SET state='called', called_at=?, expires_at=?, "
            "released_at=NULL, released_reason=NULL, miss_seq=NULL WHERE ticket_id=?",
            (now, expires, ticket["ticket_id"]),
        )
        connection.execute(
            "UPDATE triage_participants SET status='routed', current_zone_id=?, current_ticket_id=?, "
            "updated_at=? WHERE participant_id=?",
            (zone["zone_id"], ticket["ticket_id"], now, ticket["participant_id"]),
        )
        detail = {"ticket_id": ticket["ticket_id"], "zone_id": zone["zone_id"],
                  "participant_id": ticket["participant_id"], "expires_at": expires,
                  "recalled": recalled, "state_version": self._version(connection)}
        self._journey(connection, participant_id=ticket["participant_id"], event_type="ticket_called",
                      actor_id=actor_id, zone_id=zone["zone_id"], ticket_id=ticket["ticket_id"],
                      detail=detail)
        self._audit(connection, actor_id=actor_id, action="ticket.called",
                    resource_type="ticket", resource_id=ticket["ticket_id"],
                    detail={"zone_id": zone["zone_id"], "recalled": recalled})
        return detail

    def check_in(self, *, request_id: str, actor_id: str, ticket_id: str) -> TriageResult:
        payload = {"actor_id": actor_id, "ticket_id": ticket_id}
        with self.database.transaction(immediate=True) as connection:
            actor = self._actor(connection, actor_id, writers_only=True)

            def create() -> tuple[str, str, dict[str, Any]]:
                ticket = self._ticket_row(connection, ticket_id, actor)
                self._apply_pending(connection)
                ticket = connection.execute("SELECT * FROM triage_tickets WHERE ticket_id=?",
                                            (ticket_id,)).fetchone()
                if ticket["state"] != "called":
                    raise ConflictError("叫号票不在可签到状态")
                now = self._now()
                connection.execute(
                    "UPDATE triage_tickets SET state='serving', check_in_at=? WHERE ticket_id=?",
                    (now, ticket_id),
                )
                connection.execute(
                    "UPDATE triage_participants SET status='routed', updated_at=? WHERE participant_id=?",
                    (now, ticket["participant_id"]),
                )
                detail = {"ticket_id": ticket_id, "zone_id": ticket["zone_id"],
                          "participant_id": ticket["participant_id"],
                          "state_version": self._version(connection)}
                self._journey(connection, participant_id=ticket["participant_id"],
                              event_type="service_started", actor_id=actor_id,
                              zone_id=ticket["zone_id"], ticket_id=ticket_id, detail=detail)
                self._audit(connection, actor_id=actor_id, action="service.started",
                            resource_type="ticket", resource_id=ticket_id,
                            detail={"zone_id": ticket["zone_id"]})
                return "ticket", ticket_id, detail

            return self._idempotent(connection, request_id=request_id, action="triage.check_in",
                                    payload=payload, create=create)

    def _serving_expert(self, connection, zone_id: str, service_type: str) -> str | None:
        for row in connection.execute(
            "SELECT expert_id, qualifications_json FROM triage_experts WHERE zone_id=? AND on_duty=1 "
            "ORDER BY expert_id",
            (zone_id,),
        ):
            if service_type in json.loads(row["qualifications_json"]):
                return row["expert_id"]
        return None

    def finish_service(self, *, request_id: str, actor_id: str, ticket_id: str) -> TriageResult:
        payload = {"actor_id": actor_id, "ticket_id": ticket_id}
        with self.database.transaction(immediate=True) as connection:
            actor = self._actor(connection, actor_id, writers_only=True)

            def create() -> tuple[str, str, dict[str, Any]]:
                ticket = self._ticket_row(connection, ticket_id, actor)
                if ticket["state"] != "serving":
                    raise ConflictError("叫号票不在服务中状态")
                zone = connection.execute("SELECT * FROM triage_zones WHERE zone_id=?",
                                          (ticket["zone_id"],)).fetchone()
                expert_id = self._serving_expert(connection, ticket["zone_id"], zone["service_type"])
                now = self._now()
                connection.execute(
                    "UPDATE triage_tickets SET state='finished', finished_at=? WHERE ticket_id=?",
                    (now, ticket_id),
                )
                participant = connection.execute(
                    "SELECT * FROM triage_participants WHERE participant_id=?",
                    (ticket["participant_id"],),
                ).fetchone()
                accepted = json.loads(participant["accepted_services_json"])
                record = {"zone_id": ticket["zone_id"], "service_type": zone["service_type"],
                          "expert_id": expert_id, "finished_at": now}
                accepted.append(record)
                connection.execute(
                    "UPDATE triage_participants SET accepted_services_json=?, current_zone_id=NULL, "
                    "current_ticket_id=NULL, status='screened', updated_at=? WHERE participant_id=?",
                    (canonical_json(accepted), now, ticket["participant_id"]),
                )
                detail = {"ticket_id": ticket_id, **record,
                          "state_version": self._version(connection)}
                self._journey(connection, participant_id=ticket["participant_id"],
                              event_type="service_finished", actor_id=actor_id,
                              zone_id=ticket["zone_id"], expert_id=expert_id,
                              ticket_id=ticket_id, detail=detail)
                self._audit(connection, actor_id=actor_id, action="service.finished",
                            resource_type="ticket", resource_id=ticket_id,
                            detail={"zone_id": ticket["zone_id"], "expert_id": expert_id})
                return "ticket", ticket_id, detail

            return self._idempotent(connection, request_id=request_id, action="triage.finish_service",
                                    payload=payload, create=create)

    # ------------------------------------------------------------------
    # 容量/人员变化后的重新分派（等待中的票可改派，服务中必须走交接）
    # ------------------------------------------------------------------

    def reassign(self, *, request_id: str, actor_id: str, site_id: str) -> TriageResult:
        payload = {"actor_id": actor_id, "site_id": site_id}
        with self.database.transaction(immediate=True) as connection:
            actor = self._actor(connection, actor_id, writers_only=True)
            self._site_scope(connection, actor, site_id)

            def create() -> tuple[str, str, dict[str, Any]]:
                self._apply_pending(connection)
                basis_version = self._version(connection)
                moves: list[dict[str, Any]] = []
                moved_participants: set[str] = set()
                waiting = connection.execute(
                    "SELECT t.* FROM triage_tickets t JOIN triage_zones z ON z.zone_id=t.zone_id "
                    "WHERE t.state='waiting' AND z.site_id=? ORDER BY z.zone_id, t.seq_in_zone",
                    (site_id,),
                ).fetchall()
                for ticket in waiting:
                    if ticket["participant_id"] in moved_participants:
                        continue
                    participant = connection.execute(
                        "SELECT * FROM triage_participants WHERE participant_id=?",
                        (ticket["participant_id"],),
                    ).fetchone()
                    current_zone = connection.execute(
                        "SELECT * FROM triage_zones WHERE zone_id=?", (ticket["zone_id"],)
                    ).fetchone()
                    viable = self._zone_viable_for(connection, current_zone, participant)
                    target = None if viable else self._best_zone(
                        connection, participant, excluded_zone_id=ticket["zone_id"])
                    if viable or target is None:
                        continue
                    now = self._now()
                    new_seq = self._new_ticket_seq(connection, target["zone_id"])
                    new_ticket_id = uuid.uuid4().hex
                    # 保留原始入队时间：专家换岗导致的改派不应让已筛查群众重新排队，
                    # 目标队列按到达时间排序时仍承认其原有位置。
                    connection.execute(
                        "INSERT INTO triage_tickets(ticket_id,zone_id,participant_id,state,seq_in_zone,"
                        "miss_count,issued_at) VALUES(?,?,?,'waiting',?,0,?)",
                        (new_ticket_id, target["zone_id"], ticket["participant_id"], new_seq,
                         ticket["issued_at"]),
                    )
                    connection.execute(
                        "UPDATE triage_tickets SET state='released', released_at=?, "
                        "released_reason='reassigned', miss_seq=NULL WHERE ticket_id=?",
                        (now, ticket["ticket_id"]),
                    )
                    connection.execute(
                        "UPDATE triage_participants SET current_zone_id=?, current_ticket_id=?, "
                        "updated_at=? WHERE participant_id=?",
                        (target["zone_id"], new_ticket_id, now, ticket["participant_id"]),
                    )
                    move = {"participant_id": ticket["participant_id"],
                            "ticket_id": ticket["ticket_id"], "new_ticket_id": new_ticket_id,
                            "from_zone_id": ticket["zone_id"], "to_zone_id": target["zone_id"],
                            "service_type": target["service_type"], "queue_sequence": new_seq,
                            "basis_version": basis_version}
                    self._journey(connection, participant_id=ticket["participant_id"],
                                  event_type="reassigned", actor_id=actor_id,
                                  zone_id=target["zone_id"], ticket_id=new_ticket_id, detail=move)
                    self._audit(connection, actor_id=actor_id, action="participant.reassigned",
                                resource_type="participant",
                                resource_id=ticket["participant_id"], detail=move)
                    moves.append(move)
                    moved_participants.add(ticket["participant_id"])
                new_version = basis_version
                if moves:
                    new_version = self._bump_version(connection)
                    for move in moves:
                        move["state_version"] = new_version
                decision = {"moves": moves, "basis_version": basis_version,
                            "state_version": new_version}
                return "reassign_batch", site_id, decision

            return self._idempotent(connection, request_id=request_id, action="triage.reassign",
                                    payload=payload, create=create)

    # ------------------------------------------------------------------
    # 服务中转区的明确交接
    # ------------------------------------------------------------------

    def request_handshake(self, *, request_id: str, actor_id: str, participant_id: str,
                          to_zone_id: str, reason: str) -> TriageResult:
        payload = {"actor_id": actor_id, "participant_id": participant_id,
                   "to_zone_id": to_zone_id, "reason": reason}
        with self.database.transaction(immediate=True) as connection:
            actor = self._actor(connection, actor_id, writers_only=True)
            participant = self._participant_row(connection, actor, participant_id)
            target = self._zone_row(connection, actor, to_zone_id)
            reason = self._text(reason, "reason", 200)

            def create() -> tuple[str, str, dict[str, Any]]:
                ticket = None
                if participant["current_ticket_id"]:
                    ticket = connection.execute(
                        "SELECT * FROM triage_tickets WHERE ticket_id=?",
                        (participant["current_ticket_id"],),
                    ).fetchone()
                if ticket is None or ticket["state"] != "serving":
                    raise ConflictError("只有服务中的参与者需要交接转区")
                if ticket["zone_id"] == to_zone_id:
                    raise ValidationError("目标区域与当前区域相同")
                if target["status"] != "active":
                    raise ConflictError("目标区域未处于运行状态")
                contraindications = frozenset(json.loads(participant["contraindications_json"]))
                if target["service_type"] in incompatible_services(contraindications):
                    raise ConflictError("目标服务与参与者禁忌不兼容")
                if not self._zone_has_coverage(connection, to_zone_id, target["service_type"]):
                    raise ConflictError("目标区域没有当班且具备资格的专家")
                counts = self._zone_counts(connection, to_zone_id)
                if counts["called"] + counts["serving"] >= target["serving_limit"]:
                    raise ConflictError("目标区域服务位已满")
                existing = connection.execute(
                    "SELECT 1 FROM triage_handshakes WHERE participant_id=? AND status IN ('pending','confirmed')",
                    (participant_id,),
                ).fetchone()
                if existing:
                    raise ConflictError("该参与者已有未完成的交接")
                handshake_id = uuid.uuid4().hex
                now = self._now()
                version = self._version(connection)
                connection.execute(
                    "INSERT INTO triage_handshakes(handshake_id,participant_id,ticket_id,from_zone_id,"
                    "to_zone_id,status,reason,requested_by,requested_at,state_version) "
                    "VALUES(?,?,?,?,?, 'pending',?,?,?,?)",
                    (handshake_id, participant_id, ticket["ticket_id"], ticket["zone_id"],
                     to_zone_id, reason, actor_id, now, version),
                )
                detail = {"handshake_id": handshake_id, "participant_id": participant_id,
                          "from_zone_id": ticket["zone_id"], "to_zone_id": to_zone_id,
                          "reason": reason, "state_version": version}
                self._journey(connection, participant_id=participant_id, event_type="handshake_requested",
                              actor_id=actor_id, zone_id=to_zone_id, detail=detail)
                self._audit(connection, actor_id=actor_id, action="handshake.requested",
                            resource_type="handshake", resource_id=handshake_id, detail=detail)
                return "handshake", handshake_id, detail

            return self._idempotent(connection, request_id=request_id,
                                    action="triage.request_handshake",
                                    payload=payload, create=create)

    def confirm_handshake(self, *, request_id: str, actor_id: str, handshake_id: str) -> TriageResult:
        payload = {"actor_id": actor_id, "handshake_id": handshake_id}
        with self.database.transaction(immediate=True) as connection:
            actor = self._actor(connection, actor_id, writers_only=True)

            def create() -> tuple[str, str, dict[str, Any]]:
                handshake = self._handshake_row(connection, handshake_id, actor)
                if handshake["status"] != "pending":
                    raise ConflictError("交接不在待确认状态")
                target = connection.execute("SELECT * FROM triage_zones WHERE zone_id=?",
                                            (handshake["to_zone_id"],)).fetchone()
                # 确认时重新校验：等待期间区域可能暂停、专家可能换岗。
                if target["status"] != "active":
                    raise ConflictError("目标区域当前未运行，不能确认交接")
                participant = connection.execute(
                    "SELECT * FROM triage_participants WHERE participant_id=?",
                    (handshake["participant_id"],),
                ).fetchone()
                contraindications = frozenset(json.loads(participant["contraindications_json"]))
                if target["service_type"] in incompatible_services(contraindications):
                    raise ConflictError("目标服务与参与者禁忌不兼容")
                if not self._zone_has_coverage(connection, target["zone_id"], target["service_type"]):
                    raise ConflictError("目标区域已无当班且具备资格的专家")
                counts = self._zone_counts(connection, target["zone_id"])
                if counts["called"] + counts["serving"] >= target["serving_limit"]:
                    raise ConflictError("目标区域服务位已满")
                now = self._now()
                expires = self._after(self.call_timeout_seconds)
                old_ticket = connection.execute(
                    "SELECT * FROM triage_tickets WHERE ticket_id=?",
                    (handshake["ticket_id"],),
                ).fetchone()
                old_zone = connection.execute(
                    "SELECT * FROM triage_zones WHERE zone_id=?", (old_ticket["zone_id"],)
                ).fetchone()
                old_expert = self._serving_expert(connection, old_ticket["zone_id"],
                                                  old_zone["service_type"])
                # 原区域服务已实际开始，按"经交接转出"补登为已接受项目，
                # 使后续分流不会重复推荐，且行程中保留专家与区域记录。
                accepted = json.loads(participant["accepted_services_json"])
                accepted.append({"zone_id": old_ticket["zone_id"],
                                 "service_type": old_zone["service_type"],
                                 "expert_id": old_expert, "finished_at": now,
                                 "note": "transferred_via_handshake",
                                 "handshake_id": handshake_id})
                connection.execute(
                    "UPDATE triage_tickets SET state='released', released_at=?, "
                    "released_reason='transferred', miss_seq=NULL WHERE ticket_id=?",
                    (now, handshake["ticket_id"]),
                )
                new_ticket_id = uuid.uuid4().hex
                seq = self._new_ticket_seq(connection, target["zone_id"])
                connection.execute(
                    "INSERT INTO triage_tickets(ticket_id,zone_id,participant_id,state,seq_in_zone,"
                    "miss_count,issued_at,called_at,expires_at) VALUES(?,?,?, 'called',?,0,?,?,?)",
                    (new_ticket_id, target["zone_id"], handshake["participant_id"], seq,
                     now, now, expires),
                )
                connection.execute(
                    "UPDATE triage_participants SET current_zone_id=?, current_ticket_id=?, "
                    "accepted_services_json=?, updated_at=? WHERE participant_id=?",
                    (target["zone_id"], new_ticket_id, canonical_json(accepted), now,
                     handshake["participant_id"]),
                )
                connection.execute(
                    "UPDATE triage_handshakes SET status='confirmed', confirmed_by=?, confirmed_at=?, "
                    "new_ticket_id=? WHERE handshake_id=?",
                    (actor_id, now, new_ticket_id, handshake_id),
                )
                detail = {"handshake_id": handshake_id, "from_ticket_id": handshake["ticket_id"],
                          "new_ticket_id": new_ticket_id, "from_zone_id": handshake["from_zone_id"],
                          "to_zone_id": handshake["to_zone_id"], "expires_at": expires,
                          "state_version": self._version(connection)}
                self._journey(connection, participant_id=handshake["participant_id"],
                              event_type="handshake_confirmed", actor_id=actor_id,
                              zone_id=target["zone_id"], ticket_id=new_ticket_id, detail=detail)
                self._audit(connection, actor_id=actor_id, action="handshake.confirmed",
                            resource_type="handshake", resource_id=handshake_id, detail=detail)
                return "handshake", handshake_id, detail

            return self._idempotent(connection, request_id=request_id,
                                    action="triage.confirm_handshake",
                                    payload=payload, create=create)

    def complete_handshake(self, *, request_id: str, actor_id: str, handshake_id: str) -> TriageResult:
        payload = {"actor_id": actor_id, "handshake_id": handshake_id}
        with self.database.transaction(immediate=True) as connection:
            actor = self._actor(connection, actor_id, writers_only=True)

            def create() -> tuple[str, str, dict[str, Any]]:
                handshake = self._handshake_row(connection, handshake_id, actor)
                if handshake["status"] != "confirmed":
                    raise ConflictError("交接不在已确认待完成状态")
                new_ticket = connection.execute(
                    "SELECT * FROM triage_tickets WHERE ticket_id=?",
                    (handshake["new_ticket_id"],),
                ).fetchone()
                if new_ticket is None or new_ticket["state"] != "serving":
                    raise ConflictError("接收区域尚未开始服务，不能完成交接")
                now = self._now()
                connection.execute(
                    "UPDATE triage_handshakes SET status='completed', completed_at=? WHERE handshake_id=?",
                    (now, handshake_id),
                )
                detail = {"handshake_id": handshake_id, "state_version": self._version(connection)}
                self._journey(connection, participant_id=handshake["participant_id"],
                              event_type="handshake_completed", actor_id=actor_id,
                              zone_id=handshake["to_zone_id"], ticket_id=new_ticket["ticket_id"],
                              detail=detail)
                self._audit(connection, actor_id=actor_id, action="handshake.completed",
                            resource_type="handshake", resource_id=handshake_id, detail=detail)
                return "handshake", handshake_id, detail

            return self._idempotent(connection, request_id=request_id,
                                    action="triage.complete_handshake",
                                    payload=payload, create=create)

    def cancel_handshake(self, *, request_id: str, actor_id: str, handshake_id: str) -> TriageResult:
        payload = {"actor_id": actor_id, "handshake_id": handshake_id}
        with self.database.transaction(immediate=True) as connection:
            actor = self._actor(connection, actor_id, writers_only=True)

            def create() -> tuple[str, str, dict[str, Any]]:
                handshake = self._handshake_row(connection, handshake_id, actor)
                if handshake["status"] not in ("pending", "confirmed"):
                    raise ConflictError("交接已结束，不能撤销")
                now = self._now()
                aborted_ticket = None
                if handshake["status"] == "confirmed" and handshake["new_ticket_id"]:
                    new_ticket = connection.execute(
                        "SELECT * FROM triage_tickets WHERE ticket_id=?",
                        (handshake["new_ticket_id"],),
                    ).fetchone()
                    if new_ticket is not None and new_ticket["state"] == "serving":
                        raise ConflictError("接收区域已开始服务，应完成交接而不是撤销")
                    if new_ticket is not None and new_ticket["state"] in ACTIVE_TICKET_STATES:
                        # 接收票仍在候诊（called），撤销即明确释放该占用。
                        connection.execute(
                            "UPDATE triage_tickets SET state='released', released_at=?, "
                            "released_reason='handshake_cancelled', miss_seq=NULL WHERE ticket_id=?",
                            (now, new_ticket["ticket_id"]),
                        )
                        aborted_ticket = new_ticket["ticket_id"]
                    connection.execute(
                        "UPDATE triage_participants SET current_zone_id=NULL, current_ticket_id=NULL, "
                        "status='screened', updated_at=? WHERE participant_id=?",
                        (now, handshake["participant_id"]),
                    )
                connection.execute(
                    "UPDATE triage_handshakes SET status='cancelled' WHERE handshake_id=?",
                    (handshake_id,),
                )
                detail = {"handshake_id": handshake_id,
                          "previous_status": handshake["status"],
                          "aborted_ticket_id": aborted_ticket,
                          "state_version": self._version(connection)}
                self._journey(connection, participant_id=handshake["participant_id"],
                              event_type="handshake_cancelled", actor_id=actor_id,
                              zone_id=handshake["from_zone_id"], ticket_id=aborted_ticket,
                              detail=detail)
                self._audit(connection, actor_id=actor_id, action="handshake.cancelled",
                            resource_type="handshake", resource_id=handshake_id, detail=detail)
                return "handshake", handshake_id, detail

            return self._idempotent(connection, request_id=request_id,
                                    action="triage.cancel_handshake",
                                    payload=payload, create=create)

    def complete_journey(self, *, request_id: str, actor_id: str, participant_id: str) -> TriageResult:
        payload = {"actor_id": actor_id, "participant_id": participant_id}
        with self.database.transaction(immediate=True) as connection:
            actor = self._actor(connection, actor_id, writers_only=True)

            def create() -> tuple[str, str, dict[str, Any]]:
                participant = self._participant_row(connection, actor, participant_id)
                if participant["current_ticket_id"]:
                    ticket = connection.execute(
                        "SELECT state FROM triage_tickets WHERE ticket_id=?",
                        (participant["current_ticket_id"],),
                    ).fetchone()
                    if ticket and ticket["state"] in ACTIVE_TICKET_STATES:
                        raise ConflictError("参与者仍有进行中的流程，不能结束行程")
                now = self._now()
                connection.execute(
                    "UPDATE triage_participants SET status='completed', current_zone_id=NULL, "
                    "current_ticket_id=NULL, updated_at=? WHERE participant_id=?",
                    (now, participant_id),
                )
                self._journey(connection, participant_id=participant_id, event_type="journey_completed",
                              actor_id=actor_id, detail={"state_version": self._version(connection)})
                self._audit(connection, actor_id=actor_id, action="participant.completed",
                            resource_type="participant", resource_id=participant_id, detail={})
                return "participant", participant_id, {"participant_id": participant_id,
                                                       "status": "completed"}

            return self._idempotent(connection, request_id=request_id, action="triage.complete_journey",
                                    payload=payload, create=create)

    # ------------------------------------------------------------------
    # 协调员查询
    # ------------------------------------------------------------------

    def current_state_version(self, actor_id: str) -> int:
        with self.database.transaction() as connection:
            self._actor(connection, actor_id)
            return self._version(connection)

    def zone_pressures(self, actor_id: str, site_id: str) -> list[ZonePressure]:
        with self.database.transaction() as connection:
            actor = self._actor(connection, actor_id)
            self._site_scope(connection, actor, site_id)
            result = []
            zones = connection.execute(
                "SELECT * FROM triage_zones WHERE site_id=? ORDER BY zone_id", (site_id,)
            ).fetchall()
            for zone in zones:
                counts = self._zone_counts(connection, zone["zone_id"])
                occupied = counts["called"] + counts["serving"]
                experts = connection.execute(
                    "SELECT COUNT(*) AS count FROM triage_experts WHERE zone_id=? AND on_duty=1",
                    (zone["zone_id"],),
                ).fetchone()["count"]
                queue = []
                for ticket in connection.execute(
                    "SELECT * FROM triage_tickets WHERE zone_id=? AND state IN ('waiting','called') "
                    "ORDER BY (miss_seq IS NULL), miss_seq, issued_at, seq_in_zone",
                    (zone["zone_id"],),
                ):
                    queue.append({"ticket_id": ticket["ticket_id"],
                                  "participant_id": ticket["participant_id"],
                                  "state": ticket["state"],
                                  "sequence": ticket["seq_in_zone"]})
                total_in_zone = sum(counts.values())
                result.append(ZonePressure(
                    zone_id=zone["zone_id"], name=zone["name"], service_type=zone["service_type"],
                    status=zone["status"], capacity=zone["capacity"],
                    serving_limit=zone["serving_limit"], waiting=counts["waiting"],
                    called=counts["called"], serving=counts["serving"], occupied=occupied,
                    available_capacity=zone["capacity"] - total_in_zone,
                    pressure_ratio=round(total_in_zone / zone["capacity"], 4),
                    on_duty_experts=experts, queue=queue,
                ))
            return result

    def pending_handshakes(self, actor_id: str, site_id: str) -> list[HandshakeView]:
        with self.database.transaction() as connection:
            actor = self._actor(connection, actor_id)
            self._site_scope(connection, actor, site_id)
            rows = connection.execute(
                "SELECT h.* FROM triage_handshakes h JOIN triage_participants p "
                "ON p.participant_id=h.participant_id WHERE p.site_id=? AND h.status IN ('pending','confirmed') "
                "ORDER BY h.requested_at, h.handshake_id",
                (site_id,),
            ).fetchall()
            return [HandshakeView(
                handshake_id=row["handshake_id"], participant_id=row["participant_id"],
                from_zone_id=row["from_zone_id"], to_zone_id=row["to_zone_id"], status=row["status"],
                requested_by=row["requested_by"], requested_at=row["requested_at"],
                confirmed_by=row["confirmed_by"], confirmed_at=row["confirmed_at"],
                reason=row["reason"], state_version=row["state_version"]) for row in rows]

    def get_participant(self, actor_id: str, participant_id: str) -> Participant:
        with self.database.transaction() as connection:
            actor = self._actor(connection, actor_id)
            row = self._participant_row(connection, actor, participant_id)
            current = None
            if row["current_ticket_id"]:
                ticket = connection.execute(
                    "SELECT * FROM triage_tickets WHERE ticket_id=?", (row["current_ticket_id"],)
                ).fetchone()
                if ticket is not None:
                    current = {"zone_id": ticket["zone_id"], "ticket_id": ticket["ticket_id"],
                               "state": ticket["state"], "expires_at": ticket["expires_at"]}
            return Participant(
                participant_id=row["participant_id"], site_id=row["site_id"], status=row["status"],
                high_risk=bool(row["high_risk"]),
                contraindications=frozenset(json.loads(row["contraindications_json"])),
                risk_statements=frozenset(json.loads(row["risk_statements_json"])),
                preferences=tuple(json.loads(row["preferences_json"])),
                accepted_services=tuple(json.loads(row["accepted_services_json"])),
                current_assignment=current, state_version=row["state_version"],
                updated_at=row["updated_at"])

    def journey_events(self, actor_id: str, participant_id: str) -> list[dict[str, Any]]:
        with self.database.transaction() as connection:
            actor = self._actor(connection, actor_id)
            self._participant_row(connection, actor, participant_id)
            rows = connection.execute(
                "SELECT * FROM triage_journey_events WHERE participant_id=? ORDER BY seq",
                (participant_id,),
            ).fetchall()
            return [{"seq": row["seq"], "event_type": row["event_type"], "zone_id": row["zone_id"],
                     "expert_id": row["expert_id"], "ticket_id": row["ticket_id"],
                     "state_version": row["state_version"], "detail": json.loads(row["detail_json"]),
                     "actor_id": row["actor_id"], "occurred_at": row["occurred_at"]} for row in rows]

    def recover(self, actor_id: str = "system") -> dict[str, Any]:
        """重启后继续推进：补齐停机期间到期的叫号占用，返回未完成交接与在途票数。

        进程启动恢复时可使用内置 "system" 操作者；HTTP 触发时应传入真实操作者。
        """

        with self.database.transaction(immediate=True) as connection:
            if actor_id != "system":
                self._actor(connection, actor_id, writers_only=True)
            released = self._apply_pending(connection)
            for item in released:
                self._audit(connection, actor_id=actor_id, action="ticket.recovered_expiry",
                            resource_type="ticket", resource_id=item["ticket_id"],
                            detail={"reason": item["reason"], "miss_count": item["miss_count"]})
            open_handshakes = connection.execute(
                "SELECT handshake_id, participant_id, from_zone_id, to_zone_id, status, state_version "
                "FROM triage_handshakes WHERE status IN ('pending','confirmed') ORDER BY requested_at"
            ).fetchall()
            active_tickets = connection.execute(
                f"SELECT COUNT(*) AS count FROM triage_tickets WHERE state IN "
                f"({','.join('?' * len(ACTIVE_TICKET_STATES))})",
                ACTIVE_TICKET_STATES,
            ).fetchone()["count"]
            return {"released": released, "active_tickets": active_tickets,
                    "open_handshakes": [dict(row) for row in open_handshakes],
                    "state_version": self._version(connection)}

    # ------------------------------------------------------------------
    # 行读取助手（含跨组织越权校验）
    # ------------------------------------------------------------------

    def _zone_row(self, connection, actor, zone_id: str):
        zone = connection.execute("SELECT * FROM triage_zones WHERE zone_id=?", (zone_id,)).fetchone()
        if zone is None:
            raise NotFoundError("区域不存在")
        site = connection.execute("SELECT organization_id FROM sites WHERE site_id=?",
                                  (zone["site_id"],)).fetchone()
        if actor["organization_id"] != site["organization_id"] and actor["role"] != "admin":
            raise PermissionDenied("不能操作其他组织的区域")
        return zone

    def _expert_row(self, connection, actor, expert_id: str):
        expert = connection.execute("SELECT * FROM triage_experts WHERE expert_id=?",
                                    (expert_id,)).fetchone()
        if expert is None:
            raise NotFoundError("专家不存在")
        site = connection.execute("SELECT organization_id FROM sites WHERE site_id=?",
                                  (expert["site_id"],)).fetchone()
        if actor["organization_id"] != site["organization_id"] and actor["role"] != "admin":
            raise PermissionDenied("不能操作其他组织的专家")
        return expert

    def _participant_row(self, connection, actor, participant_id: str):
        participant = connection.execute(
            "SELECT * FROM triage_participants WHERE participant_id=?", (participant_id,)
        ).fetchone()
        if participant is None:
            raise NotFoundError("参与者不存在")
        site = connection.execute("SELECT organization_id FROM sites WHERE site_id=?",
                                  (participant["site_id"],)).fetchone()
        if actor["role"] != "admin" and actor["organization_id"] != site["organization_id"]:
            raise PermissionDenied("不能操作其他组织的参与者")
        return participant

    def _ticket_row(self, connection, ticket_id: str, actor):
        ticket = connection.execute("SELECT * FROM triage_tickets WHERE ticket_id=?",
                                    (ticket_id,)).fetchone()
        if ticket is None:
            raise NotFoundError("叫号票不存在")
        zone = connection.execute("SELECT site_id FROM triage_zones WHERE zone_id=?",
                                  (ticket["zone_id"],)).fetchone()
        site = connection.execute("SELECT organization_id FROM sites WHERE site_id=?",
                                  (zone["site_id"],)).fetchone()
        if actor["organization_id"] != site["organization_id"] and actor["role"] != "admin":
            raise PermissionDenied("不能操作其他组织的叫号票")
        return ticket

    def _handshake_row(self, connection, handshake_id: str, actor):
        row = connection.execute("SELECT * FROM triage_handshakes WHERE handshake_id=?",
                                 (handshake_id,)).fetchone()
        if row is None:
            raise NotFoundError("交接不存在")
        participant = connection.execute(
            "SELECT site_id FROM triage_participants WHERE participant_id=?",
            (row["participant_id"],),
        ).fetchone()
        site = connection.execute("SELECT organization_id FROM sites WHERE site_id=?",
                                  (participant["site_id"],)).fetchone()
        if actor["organization_id"] != site["organization_id"] and actor["role"] != "admin":
            raise PermissionDenied("不能操作其他组织的交接")
        return row
