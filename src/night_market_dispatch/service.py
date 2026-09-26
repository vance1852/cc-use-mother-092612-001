"""中医文化夜市现场服务分流中枢。

在基础层（角色权限、请求幂等、SQLite 事务、哈希串联审计）之上提供：

- 参与者的入场诉求、禁忌提示、已接受项目以行程事件形式持久化，可全程追溯；
- 区域容量与当班专家资格的变化会推进场所状态版本，去向可据此重新计算；
- 服务中的参与者不被重算移动，只能经由“发起—完成/取消”的明确交接转区；
- 高风险陈述一律转人工处理，系统只给出分流去向，不生成任何诊断内容；
- 叫号占用权超时释放，过号、暂停、恢复、转区由状态机按确定顺序生效；
- 写操作幂等：同一请求重放返回原决定，编号相同而内容不同则报告冲突；
- 全部状态持久化在 SQLite 中，应用重启后未结束的流程从记录继续推进。

参与者状态机（每次迁移都写入 dispatch_itinerary，按 seq 确定顺序）：

    check_in      -> manual_review | waiting
    call_next     -> waiting  => called      （占用一个容量名额，超时自动失效）
    confirm       -> called   => serving
    complete      -> serving  => waiting | done | manual_review
    settle        -> called   => waiting     （过号，保留原号码等待再次叫号）
    pause/resume  -> waiting  <=> paused     （仅等候中可暂停，恢复后保留原号码）
    handover      -> serving  => serving|waiting （唯一允许的服务中转区方式）
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

from night_market_foundation.audit import append_event, canonical_json, digest
from night_market_foundation.clock import Clock, SystemClock
from night_market_foundation.errors import (
    ConflictError,
    NotFoundError,
    PermissionDenied,
    ValidationError,
)
from night_market_foundation.service import IDENTIFIER
from night_market_foundation.storage import Database

from .schema import ensure_schema


HIGH_RISK_FLAGS = frozenset({
    "chest_pain",
    "breathing_difficulty",
    "syncope",
    "severe_bleeding",
    "acute_severe_pain",
    "altered_consciousness",
})

ZONE_STATUSES = frozenset({"open", "paused", "closed"})
EXPERT_STATUSES = frozenset({"on_duty", "off_duty"})
DEFAULT_CALL_TTL_SECONDS = 300
MAX_CALL_TTL_SECONDS = 3600


class DispatchService:
    """协调区域容量、当班专家资格与参与者行程的分流服务。

    写方法返回 ``(decision, replayed)``：``decision`` 是持久化的决定内容，
    同一 ``request_id`` 重放时原样返回；``replayed`` 标识本次是否为重放。
    """

    def __init__(self, database: Database, clock: Clock | None = None) -> None:
        self.database = database
        self.clock = clock or SystemClock()
        ensure_schema(database.connection)

    # ---- 基础工具 ----

    def _now(self) -> datetime:
        return self.clock.now().astimezone(timezone.utc)

    @staticmethod
    def _fmt(value: datetime) -> str:
        return value.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")

    @staticmethod
    def _parse(value: str) -> datetime:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))

    def _identifier(self, value: str, field: str) -> str:
        value = str(value).strip()
        if not IDENTIFIER.fullmatch(value):
            raise ValidationError(f"{field} 格式无效")
        return value

    def _text(self, value: str, field: str, limit: int = 200) -> str:
        value = str(value).strip()
        if not value or len(value) > limit:
            raise ValidationError(f"{field} 不能为空且不能超过 {limit} 个字符")
        return value

    def _string_list(self, value: Any, field: str, *, allow_empty: bool = True) -> list[str]:
        if not isinstance(value, (list, tuple)):
            raise ValidationError(f"{field} 必须是字符串数组")
        items: list[str] = []
        for entry in value:
            entry = self._text(entry, field, 80)
            if entry not in items:
                items.append(entry)
        if not items and not allow_empty:
            raise ValidationError(f"{field} 不能为空数组")
        return items

    def _capacity(self, value: Any) -> int:
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ValidationError("capacity 必须是非负整数")
        return value

    def _actor(self, connection, actor_id: str):
        row = connection.execute("SELECT * FROM actors WHERE actor_id=?", (actor_id,)).fetchone()
        if row is None:
            raise NotFoundError("操作者不存在")
        if not row["active"]:
            raise PermissionDenied("操作者已停用")
        return row

    def _require(self, actor, *roles: str) -> None:
        if actor["role"] not in roles:
            raise PermissionDenied("当前角色不能执行该动作")

    def _site(self, connection, site_id: str):
        row = connection.execute("SELECT * FROM sites WHERE site_id=?", (site_id,)).fetchone()
        if row is None:
            raise NotFoundError("场所不存在")
        return row

    def _check_site_scope(self, actor, site) -> None:
        if actor["role"] != "admin" and actor["organization_id"] != site["organization_id"]:
            raise PermissionDenied("不能操作其他组织的场所")

    # ---- 幂等 ----

    def _receipt(self, connection, request_id: str, action: str, payload: dict[str, Any]):
        """查找既有回执；编号相同而内容不同则报告冲突。"""

        row = connection.execute(
            "SELECT * FROM request_receipts WHERE request_id=?", (request_id,)
        ).fetchone()
        if row is None:
            return None
        if row["action"] != action or row["payload_hash"] != digest(payload):
            raise ConflictError("request_id 已被不同内容使用")
        return json.loads(row["response_json"])

    def _store_receipt(self, connection, *, request_id: str, action: str, payload: dict[str, Any],
                       resource_type: str, resource_id: str, response: dict[str, Any]) -> None:
        connection.execute(
            "INSERT INTO request_receipts(request_id,action,payload_hash,resource_type,resource_id,response_json,created_at) "
            "VALUES(?,?,?,?,?,?,?)",
            (request_id, action, digest(payload), resource_type, resource_id,
             canonical_json(response), self._fmt(self._now())),
        )

    # ---- 状态版本 ----

    def _state_version(self, connection, site_id: str) -> int:
        row = connection.execute(
            "SELECT version FROM dispatch_state_versions WHERE site_id=?", (site_id,)
        ).fetchone()
        return row["version"] if row else 0

    def _bump_state_version(self, connection, site_id: str) -> int:
        connection.execute(
            "INSERT INTO dispatch_state_versions(site_id,version) VALUES(?,1) "
            "ON CONFLICT(site_id) DO UPDATE SET version=version+1",
            (site_id,),
        )
        return self._state_version(connection, site_id)

    # ---- 区域、专家、参与者读取 ----

    def _zone(self, connection, zone_id: str):
        row = connection.execute(
            "SELECT * FROM dispatch_zones WHERE zone_id=?", (zone_id,)
        ).fetchone()
        if row is None:
            raise NotFoundError("区域不存在")
        return row

    def _expert(self, connection, expert_id: str):
        row = connection.execute(
            "SELECT * FROM dispatch_experts WHERE expert_id=?", (expert_id,)
        ).fetchone()
        if row is None:
            raise NotFoundError("专家不存在")
        return row

    def _participant(self, connection, participant_id: str):
        row = connection.execute(
            "SELECT * FROM dispatch_participants WHERE participant_id=?", (participant_id,)
        ).fetchone()
        if row is None:
            raise NotFoundError("参与者不存在")
        return row

    def _zone_load(self, connection, zone_id: str) -> dict[str, int]:
        row = connection.execute(
            "SELECT "
            "COALESCE(SUM(CASE WHEN status='waiting' THEN 1 ELSE 0 END),0) AS waiting, "
            "COALESCE(SUM(CASE WHEN status='called' THEN 1 ELSE 0 END),0) AS called, "
            "COALESCE(SUM(CASE WHEN status='serving' THEN 1 ELSE 0 END),0) AS serving, "
            "COALESCE(SUM(CASE WHEN status='paused' THEN 1 ELSE 0 END),0) AS paused "
            "FROM dispatch_participants WHERE current_zone_id=?",
            (zone_id,),
        ).fetchone()
        return {"waiting": row["waiting"], "called": row["called"],
                "serving": row["serving"], "paused": row["paused"]}

    def _zone_has_qualified_expert(self, connection, zone_id: str, service_type: str) -> bool:
        rows = connection.execute(
            "SELECT qualifications_json FROM dispatch_experts WHERE zone_id=? AND status='on_duty'",
            (zone_id,),
        ).fetchall()
        return any(service_type in json.loads(row["qualifications_json"]) for row in rows)

    def _zone_viable(self, connection, zone_row, service_type: str) -> bool:
        return (
            zone_row["status"] == "open"
            and zone_row["capacity"] > 0
            and zone_row["service_type"] == service_type
            and self._zone_has_qualified_expert(connection, zone_row["zone_id"], service_type)
        )

    def _choose_zone(self, connection, site_id: str, service_type: str):
        """在可用区域中确定性选优：先有空位，再负载率低，再编号字典序。"""

        rows = connection.execute(
            "SELECT * FROM dispatch_zones WHERE site_id=? AND service_type=?",
            (site_id, service_type),
        ).fetchall()
        viable = [row for row in rows if self._zone_viable(connection, row, service_type)]
        if not viable:
            return None

        def rank(row):
            load = self._zone_load(connection, row["zone_id"])
            occupied = load["called"] + load["serving"]
            free = row["capacity"] - occupied
            ratio = (occupied + load["waiting"]) / row["capacity"]
            return (0 if free > 0 else 1, ratio, row["zone_id"])

        return min(viable, key=rank)

    def _next_pending_request(self, participant) -> str | None:
        completed = set(json.loads(participant["completed_json"]))
        for item in json.loads(participant["requests_json"]):
            if item not in completed:
                return item
        return None

    # ---- 行程与去向 ----

    def _record_itinerary(self, connection, *, participant_id: str, event_type: str,
                          from_status: str | None = None, to_status: str | None = None,
                          zone_id: str | None = None, detail: dict[str, Any] | None = None,
                          based_on_version: int | None = None) -> None:
        connection.execute(
            "INSERT INTO dispatch_itinerary(participant_id,event_type,from_status,to_status,zone_id,"
            "detail_json,based_on_version,created_at) VALUES(?,?,?,?,?,?,?,?)",
            (participant_id, event_type, from_status, to_status, zone_id,
             canonical_json(detail or {}), based_on_version, self._fmt(self._now())),
        )

    def _assign(self, connection, *, site_id: str, participant_id: str, zone_id: str,
                reason: str, with_queue_number: bool = True) -> dict[str, Any]:
        """记录一次去向决定，返回所依据的状态版本与队列号。"""

        version = self._state_version(connection, site_id)
        queue_number = None
        if with_queue_number:
            row = connection.execute(
                "SELECT MAX(queue_number) AS top FROM dispatch_assignments WHERE zone_id=?",
                (zone_id,),
            ).fetchone()
            queue_number = (row["top"] or 0) + 1
        connection.execute(
            "INSERT INTO dispatch_assignments(assignment_id,participant_id,zone_id,queue_number,"
            "based_on_version,reason,created_at) VALUES(?,?,?,?,?,?,?)",
            (uuid.uuid4().hex, participant_id, zone_id, queue_number, version, reason,
             self._fmt(self._now())),
        )
        connection.execute(
            "UPDATE dispatch_participants SET current_zone_id=?, queue_number=?, updated_at=? "
            "WHERE participant_id=?",
            (zone_id, queue_number, self._fmt(self._now()), participant_id),
        )
        return {"zone_id": zone_id, "queue_number": queue_number, "based_on_version": version}

    # ---- 区域管理 ----

    def register_zone(self, *, request_id: str, actor_id: str, site_id: str, zone_id: str,
                      name: str, service_type: str, capacity: int) -> tuple[dict[str, Any], bool]:
        payload = {"actor_id": actor_id, "site_id": site_id, "zone_id": zone_id, "name": name,
                   "service_type": service_type, "capacity": capacity}
        action = "dispatch_register_zone"
        with self.database.transaction(immediate=True) as connection:
            request_id = self._identifier(request_id, "request_id")
            replay = self._receipt(connection, request_id, action, payload)
            if replay is not None:
                return replay, True
            actor = self._actor(connection, actor_id)
            self._require(actor, "admin", "operator")
            site = self._site(connection, site_id)
            self._check_site_scope(actor, site)
            zone_id = self._identifier(zone_id, "zone_id")
            name = self._text(name, "name", 80)
            service_type = self._text(service_type, "service_type", 40)
            capacity = self._capacity(capacity)
            now = self._fmt(self._now())
            try:
                connection.execute(
                    "INSERT INTO dispatch_zones(zone_id,site_id,name,service_type,capacity,status,created_at,updated_at) "
                    "VALUES(?,?,?,?,?,'open',?,?)",
                    (zone_id, site_id, name, service_type, capacity, now, now),
                )
            except Exception as exc:
                raise ConflictError("区域编号已经存在") from exc
            version = self._bump_state_version(connection, site_id)
            append_event(connection, actor_id=actor_id, action="dispatch.zone_registered",
                         resource_type="dispatch_zone", resource_id=zone_id,
                         detail={"site_id": site_id, "name": name, "service_type": service_type,
                                 "capacity": capacity, "state_version": version},
                         occurred_at=now)
            response = {"zone_id": zone_id, "site_id": site_id, "name": name,
                        "service_type": service_type, "capacity": capacity,
                        "status": "open", "state_version": version}
            self._store_receipt(connection, request_id=request_id, action=action, payload=payload,
                                resource_type="dispatch_zone", resource_id=zone_id, response=response)
            return response, False

    def update_zone_capacity(self, *, request_id: str, actor_id: str, zone_id: str,
                             capacity: int) -> tuple[dict[str, Any], bool]:
        payload = {"actor_id": actor_id, "zone_id": zone_id, "capacity": capacity}
        action = "dispatch_update_zone_capacity"
        with self.database.transaction(immediate=True) as connection:
            request_id = self._identifier(request_id, "request_id")
            replay = self._receipt(connection, request_id, action, payload)
            if replay is not None:
                return replay, True
            actor = self._actor(connection, actor_id)
            self._require(actor, "admin", "operator")
            zone = self._zone(connection, zone_id)
            site = self._site(connection, zone["site_id"])
            self._check_site_scope(actor, site)
            capacity = self._capacity(capacity)
            now = self._fmt(self._now())
            connection.execute(
                "UPDATE dispatch_zones SET capacity=?, updated_at=? WHERE zone_id=?",
                (capacity, now, zone_id),
            )
            version = self._bump_state_version(connection, site["site_id"])
            append_event(connection, actor_id=actor_id, action="dispatch.zone_capacity_updated",
                         resource_type="dispatch_zone", resource_id=zone_id,
                         detail={"old_capacity": zone["capacity"], "capacity": capacity,
                                 "state_version": version},
                         occurred_at=now)
            response = {"zone_id": zone_id, "capacity": capacity, "state_version": version}
            self._store_receipt(connection, request_id=request_id, action=action, payload=payload,
                                resource_type="dispatch_zone", resource_id=zone_id, response=response)
            return response, False

    def set_zone_status(self, *, request_id: str, actor_id: str, zone_id: str,
                        status: str) -> tuple[dict[str, Any], bool]:
        payload = {"actor_id": actor_id, "zone_id": zone_id, "status": status}
        action = "dispatch_set_zone_status"
        with self.database.transaction(immediate=True) as connection:
            request_id = self._identifier(request_id, "request_id")
            replay = self._receipt(connection, request_id, action, payload)
            if replay is not None:
                return replay, True
            actor = self._actor(connection, actor_id)
            self._require(actor, "admin", "operator")
            zone = self._zone(connection, zone_id)
            site = self._site(connection, zone["site_id"])
            self._check_site_scope(actor, site)
            if status not in ZONE_STATUSES:
                raise ValidationError("status 不在允许范围内")
            now = self._fmt(self._now())
            connection.execute(
                "UPDATE dispatch_zones SET status=?, updated_at=? WHERE zone_id=?",
                (status, now, zone_id),
            )
            version = self._bump_state_version(connection, site["site_id"])
            append_event(connection, actor_id=actor_id, action="dispatch.zone_status_changed",
                         resource_type="dispatch_zone", resource_id=zone_id,
                         detail={"old_status": zone["status"], "status": status,
                                 "state_version": version},
                         occurred_at=now)
            response = {"zone_id": zone_id, "status": status, "state_version": version}
            self._store_receipt(connection, request_id=request_id, action=action, payload=payload,
                                resource_type="dispatch_zone", resource_id=zone_id, response=response)
            return response, False

    # ---- 专家管理 ----

    def register_expert(self, *, request_id: str, actor_id: str, site_id: str, expert_id: str,
                        name: str, qualifications: list[str],
                        zone_id: str | None = None) -> tuple[dict[str, Any], bool]:
        payload = {"actor_id": actor_id, "site_id": site_id, "expert_id": expert_id, "name": name,
                   "qualifications": qualifications, "zone_id": zone_id}
        action = "dispatch_register_expert"
        with self.database.transaction(immediate=True) as connection:
            request_id = self._identifier(request_id, "request_id")
            replay = self._receipt(connection, request_id, action, payload)
            if replay is not None:
                return replay, True
            actor = self._actor(connection, actor_id)
            self._require(actor, "admin", "operator")
            site = self._site(connection, site_id)
            self._check_site_scope(actor, site)
            expert_id = self._identifier(expert_id, "expert_id")
            name = self._text(name, "name", 80)
            qualifications = self._string_list(qualifications, "qualifications", allow_empty=False)
            if zone_id is not None:
                zone = self._zone(connection, zone_id)
                if zone["site_id"] != site_id:
                    raise ValidationError("区域不属于该场所")
            now = self._fmt(self._now())
            try:
                connection.execute(
                    "INSERT INTO dispatch_experts(expert_id,site_id,name,qualifications_json,zone_id,status,"
                    "created_at,updated_at) VALUES(?,?,?,?,?,'on_duty',?,?)",
                    (expert_id, site_id, name, canonical_json(qualifications), zone_id, now, now),
                )
            except Exception as exc:
                raise ConflictError("专家编号已经存在") from exc
            version = self._bump_state_version(connection, site_id)
            append_event(connection, actor_id=actor_id, action="dispatch.expert_registered",
                         resource_type="dispatch_expert", resource_id=expert_id,
                         detail={"site_id": site_id, "name": name, "qualifications": qualifications,
                                 "zone_id": zone_id, "state_version": version},
                         occurred_at=now)
            response = {"expert_id": expert_id, "site_id": site_id, "name": name,
                        "qualifications": qualifications, "zone_id": zone_id,
                        "status": "on_duty", "state_version": version}
            self._store_receipt(connection, request_id=request_id, action=action, payload=payload,
                                resource_type="dispatch_expert", resource_id=expert_id, response=response)
            return response, False

    def assign_expert(self, *, request_id: str, actor_id: str, expert_id: str,
                      zone_id: str | None = None) -> tuple[dict[str, Any], bool]:
        """专家换岗：调整在岗区域并推进状态版本，等候者去向由重算决定。"""

        payload = {"actor_id": actor_id, "expert_id": expert_id, "zone_id": zone_id}
        action = "dispatch_assign_expert"
        with self.database.transaction(immediate=True) as connection:
            request_id = self._identifier(request_id, "request_id")
            replay = self._receipt(connection, request_id, action, payload)
            if replay is not None:
                return replay, True
            actor = self._actor(connection, actor_id)
            self._require(actor, "admin", "operator")
            expert = self._expert(connection, expert_id)
            site = self._site(connection, expert["site_id"])
            self._check_site_scope(actor, site)
            if zone_id is not None:
                zone = self._zone(connection, zone_id)
                if zone["site_id"] != expert["site_id"]:
                    raise ValidationError("专家与区域不属于同一场所")
            now = self._fmt(self._now())
            previous = expert["zone_id"]
            connection.execute(
                "UPDATE dispatch_experts SET zone_id=?, updated_at=? WHERE expert_id=?",
                (zone_id, now, expert_id),
            )
            version = self._bump_state_version(connection, expert["site_id"])
            append_event(connection, actor_id=actor_id, action="dispatch.expert_reassigned",
                         resource_type="dispatch_expert", resource_id=expert_id,
                         detail={"from_zone_id": previous, "to_zone_id": zone_id,
                                 "state_version": version},
                         occurred_at=now)
            response = {"expert_id": expert_id, "zone_id": zone_id,
                        "previous_zone_id": previous, "state_version": version}
            self._store_receipt(connection, request_id=request_id, action=action, payload=payload,
                                resource_type="dispatch_expert", resource_id=expert_id, response=response)
            return response, False

    def set_expert_status(self, *, request_id: str, actor_id: str, expert_id: str,
                          status: str) -> tuple[dict[str, Any], bool]:
        payload = {"actor_id": actor_id, "expert_id": expert_id, "status": status}
        action = "dispatch_set_expert_status"
        with self.database.transaction(immediate=True) as connection:
            request_id = self._identifier(request_id, "request_id")
            replay = self._receipt(connection, request_id, action, payload)
            if replay is not None:
                return replay, True
            actor = self._actor(connection, actor_id)
            self._require(actor, "admin", "operator")
            expert = self._expert(connection, expert_id)
            site = self._site(connection, expert["site_id"])
            self._check_site_scope(actor, site)
            if status not in EXPERT_STATUSES:
                raise ValidationError("status 不在允许范围内")
            now = self._fmt(self._now())
            connection.execute(
                "UPDATE dispatch_experts SET status=?, updated_at=? WHERE expert_id=?",
                (status, now, expert_id),
            )
            version = self._bump_state_version(connection, expert["site_id"])
            append_event(connection, actor_id=actor_id, action="dispatch.expert_status_changed",
                         resource_type="dispatch_expert", resource_id=expert_id,
                         detail={"old_status": expert["status"], "status": status,
                                 "state_version": version},
                         occurred_at=now)
            response = {"expert_id": expert_id, "status": status, "state_version": version}
            self._store_receipt(connection, request_id=request_id, action=action, payload=payload,
                                resource_type="dispatch_expert", resource_id=expert_id, response=response)
            return response, False

    # ---- 参与者入场与分流 ----

    def check_in_participant(self, *, request_id: str, actor_id: str, site_id: str,
                             participant_id: str, alias: str, requests: list[str],
                             contraindications: list[str] | None = None,
                             risk_flags: list[str] | None = None) -> tuple[dict[str, Any], bool]:
        """登记入场诉求与禁忌提示，完成风险筛查后给出首个去向。

        高风险陈述一律转人工处理；系统只输出分流去向，不生成诊断内容。
        """

        payload = {"actor_id": actor_id, "site_id": site_id, "participant_id": participant_id,
                   "alias": alias, "requests": requests, "contraindications": contraindications,
                   "risk_flags": risk_flags}
        action = "dispatch_check_in"
        with self.database.transaction(immediate=True) as connection:
            request_id = self._identifier(request_id, "request_id")
            replay = self._receipt(connection, request_id, action, payload)
            if replay is not None:
                return replay, True
            actor = self._actor(connection, actor_id)
            self._require(actor, "admin", "operator")
            site = self._site(connection, site_id)
            self._check_site_scope(actor, site)
            participant_id = self._identifier(participant_id, "participant_id")
            alias = self._text(alias, "alias", 80)
            requests = self._string_list(requests, "requests", allow_empty=False)
            contraindications = self._string_list(contraindications or [], "contraindications")
            risk_flags = self._string_list(risk_flags or [], "risk_flags")
            registered = {row["service_type"] for row in connection.execute(
                "SELECT DISTINCT service_type FROM dispatch_zones WHERE site_id=?", (site_id,))}
            for item in requests:
                if item not in registered:
                    raise ValidationError(f"诉求项目未登记: {item}")
            now = self._fmt(self._now())
            high_risk = sorted(set(risk_flags) & HIGH_RISK_FLAGS)
            status = "manual_review" if high_risk else "waiting"
            try:
                connection.execute(
                    "INSERT INTO dispatch_participants(participant_id,site_id,alias,requests_json,"
                    "contraindications_json,risk_flags_json,status,current_zone_id,queue_number,"
                    "called_at,call_expires_at,completed_json,screened,created_at,updated_at) "
                    "VALUES(?,?,?,?,?,?,?,NULL,NULL,NULL,NULL,?,?,?,?)",
                    (participant_id, site_id, alias, canonical_json(requests),
                     canonical_json(contraindications), canonical_json(risk_flags), status,
                     canonical_json([]), 0 if high_risk else 1, now, now),
                )
            except Exception as exc:
                raise ConflictError("参与者编号已经存在") from exc
            if high_risk:
                self._record_itinerary(
                    connection, participant_id=participant_id, event_type="escalated_to_manual",
                    to_status="manual_review",
                    detail={"reason": "高风险陈述，转人工处理", "risk_flags": high_risk,
                            "requests": requests, "contraindications": contraindications})
                append_event(connection, actor_id=actor_id, action="dispatch.participant_escalated",
                             resource_type="dispatch_participant", resource_id=participant_id,
                             detail={"site_id": site_id, "risk_flags": high_risk}, occurred_at=now)
                response = {"participant_id": participant_id, "status": "manual_review",
                            "route": "manual_review", "risk_flags": high_risk, "screened": False}
            else:
                chosen = None
                chosen_request = None
                for item in requests:
                    chosen = self._choose_zone(connection, site_id, item)
                    if chosen is not None:
                        chosen_request = item
                        break
                if chosen is None:
                    raise ValidationError("当前没有可接待诉求项目的区域")
                assignment = self._assign(connection, site_id=site_id, participant_id=participant_id,
                                          zone_id=chosen["zone_id"], reason="check_in")
                self._record_itinerary(
                    connection, participant_id=participant_id, event_type="checked_in",
                    to_status="waiting", zone_id=chosen["zone_id"],
                    detail={"requests": requests, "contraindications": contraindications,
                            "risk_flags": risk_flags})
                self._record_itinerary(
                    connection, participant_id=participant_id, event_type="assigned",
                    to_status="waiting", zone_id=chosen["zone_id"],
                    detail={"service_type": chosen_request,
                            "queue_number": assignment["queue_number"],
                            "zone_capacity": chosen["capacity"], "reason": "check_in"},
                    based_on_version=assignment["based_on_version"])
                append_event(connection, actor_id=actor_id, action="dispatch.participant_checked_in",
                             resource_type="dispatch_participant", resource_id=participant_id,
                             detail={"site_id": site_id, "zone_id": chosen["zone_id"],
                                     "queue_number": assignment["queue_number"]},
                             occurred_at=now)
                response = {"participant_id": participant_id, "status": "waiting", "route": "zone",
                            "zone_id": chosen["zone_id"], "queue_number": assignment["queue_number"],
                            "based_on_version": assignment["based_on_version"], "screened": True}
            self._store_receipt(connection, request_id=request_id, action=action, payload=payload,
                                resource_type="dispatch_participant", resource_id=participant_id,
                                response=response)
            return response, False

    # ---- 叫号、到场与完成 ----

    def call_next(self, *, request_id: str, actor_id: str, zone_id: str,
                  ttl_seconds: int = DEFAULT_CALL_TTL_SECONDS) -> tuple[dict[str, Any], bool]:
        """按队列号顺序叫号，产生一个带超时时间的占用权。"""

        payload = {"actor_id": actor_id, "zone_id": zone_id, "ttl_seconds": ttl_seconds}
        action = "dispatch_call_next"
        with self.database.transaction(immediate=True) as connection:
            request_id = self._identifier(request_id, "request_id")
            replay = self._receipt(connection, request_id, action, payload)
            if replay is not None:
                return replay, True
            actor = self._actor(connection, actor_id)
            self._require(actor, "admin", "operator")
            zone = self._zone(connection, zone_id)
            site = self._site(connection, zone["site_id"])
            self._check_site_scope(actor, site)
            if (isinstance(ttl_seconds, bool) or not isinstance(ttl_seconds, int)
                    or not 1 <= ttl_seconds <= MAX_CALL_TTL_SECONDS):
                raise ValidationError(f"ttl_seconds 必须是 1 到 {MAX_CALL_TTL_SECONDS} 之间的整数")
            if zone["status"] != "open":
                raise ConflictError("区域当前未开放叫号")
            load = self._zone_load(connection, zone_id)
            if load["called"] + load["serving"] >= zone["capacity"]:
                raise ConflictError("区域容量已满，暂时无法叫号")
            row = connection.execute(
                "SELECT * FROM dispatch_participants WHERE current_zone_id=? AND status='waiting' "
                "ORDER BY queue_number, participant_id LIMIT 1",
                (zone_id,),
            ).fetchone()
            if row is None:
                raise NotFoundError("该区域没有等候中的参与者")
            now_dt = self._now()
            called_at = self._fmt(now_dt)
            expires_at = self._fmt(now_dt + timedelta(seconds=ttl_seconds))
            connection.execute(
                "UPDATE dispatch_participants SET status='called', called_at=?, call_expires_at=?, "
                "updated_at=? WHERE participant_id=?",
                (called_at, expires_at, called_at, row["participant_id"]),
            )
            self._record_itinerary(connection, participant_id=row["participant_id"],
                                   event_type="called", from_status="waiting", to_status="called",
                                   zone_id=zone_id,
                                   detail={"queue_number": row["queue_number"],
                                           "expires_at": expires_at})
            append_event(connection, actor_id=actor_id, action="dispatch.number_called",
                         resource_type="dispatch_zone", resource_id=zone_id,
                         detail={"participant_id": row["participant_id"],
                                 "queue_number": row["queue_number"], "expires_at": expires_at},
                         occurred_at=called_at)
            response = {"zone_id": zone_id, "participant_id": row["participant_id"],
                        "queue_number": row["queue_number"], "called_at": called_at,
                        "expires_at": expires_at}
            self._store_receipt(connection, request_id=request_id, action=action, payload=payload,
                                resource_type="dispatch_call", resource_id=row["participant_id"],
                                response=response)
            return response, False

    def confirm_arrival(self, *, request_id: str, actor_id: str,
                        participant_id: str) -> tuple[dict[str, Any], bool]:
        """已叫号参与者在占用权有效期内到场，进入服务中。"""

        payload = {"actor_id": actor_id, "participant_id": participant_id}
        action = "dispatch_confirm_arrival"
        with self.database.transaction(immediate=True) as connection:
            request_id = self._identifier(request_id, "request_id")
            replay = self._receipt(connection, request_id, action, payload)
            if replay is not None:
                return replay, True
            actor = self._actor(connection, actor_id)
            self._require(actor, "admin", "operator")
            participant = self._participant(connection, participant_id)
            site = self._site(connection, participant["site_id"])
            self._check_site_scope(actor, site)
            if participant["status"] != "called":
                raise ConflictError("参与者当前不在已叫号状态")
            if self._now() > self._parse(participant["call_expires_at"]):
                raise ConflictError("叫号占用已超时释放，请先结算过号")
            now = self._fmt(self._now())
            connection.execute(
                "UPDATE dispatch_participants SET status='serving', updated_at=? WHERE participant_id=?",
                (now, participant_id),
            )
            self._record_itinerary(connection, participant_id=participant_id,
                                   event_type="service_started", from_status="called",
                                   to_status="serving", zone_id=participant["current_zone_id"],
                                   detail={"queue_number": participant["queue_number"]})
            append_event(connection, actor_id=actor_id, action="dispatch.service_started",
                         resource_type="dispatch_participant", resource_id=participant_id,
                         detail={"zone_id": participant["current_zone_id"]}, occurred_at=now)
            response = {"participant_id": participant_id, "status": "serving",
                        "zone_id": participant["current_zone_id"]}
            self._store_receipt(connection, request_id=request_id, action=action, payload=payload,
                                resource_type="dispatch_participant", resource_id=participant_id,
                                response=response)
            return response, False

    def complete_service(self, *, request_id: str, actor_id: str,
                         participant_id: str) -> tuple[dict[str, Any], bool]:
        """完成当前项目：记入已接受项目，并按剩余诉求决定下一去向。"""

        payload = {"actor_id": actor_id, "participant_id": participant_id}
        action = "dispatch_complete_service"
        with self.database.transaction(immediate=True) as connection:
            request_id = self._identifier(request_id, "request_id")
            replay = self._receipt(connection, request_id, action, payload)
            if replay is not None:
                return replay, True
            actor = self._actor(connection, actor_id)
            self._require(actor, "admin", "operator")
            participant = self._participant(connection, participant_id)
            site = self._site(connection, participant["site_id"])
            self._check_site_scope(actor, site)
            if participant["status"] != "serving":
                raise ConflictError("仅服务中的参与者可以完成当前项目")
            zone = self._zone(connection, participant["current_zone_id"])
            service_type = zone["service_type"]
            completed = json.loads(participant["completed_json"])
            if service_type not in completed:
                completed.append(service_type)
            requests = json.loads(participant["requests_json"])
            next_request = next((item for item in requests if item not in completed), None)
            now = self._fmt(self._now())
            self._record_itinerary(connection, participant_id=participant_id,
                                   event_type="service_completed", from_status="serving",
                                   zone_id=zone["zone_id"],
                                   detail={"service_type": service_type, "completed": completed})
            if next_request is None:
                connection.execute(
                    "UPDATE dispatch_participants SET status='done', completed_json=?, called_at=NULL, "
                    "call_expires_at=NULL, updated_at=? WHERE participant_id=?",
                    (canonical_json(completed), now, participant_id),
                )
                self._record_itinerary(connection, participant_id=participant_id,
                                       event_type="journey_completed", to_status="done",
                                       detail={"completed": completed})
                response = {"participant_id": participant_id, "status": "done",
                            "route": "done", "completed": completed}
            else:
                target = self._choose_zone(connection, participant["site_id"], next_request)
                if target is None:
                    connection.execute(
                        "UPDATE dispatch_participants SET status='manual_review', completed_json=?, "
                        "called_at=NULL, call_expires_at=NULL, updated_at=? WHERE participant_id=?",
                        (canonical_json(completed), now, participant_id),
                    )
                    self._record_itinerary(connection, participant_id=participant_id,
                                           event_type="escalated_to_manual",
                                           to_status="manual_review",
                                           detail={"reason": "no_viable_zone",
                                                   "pending_request": next_request})
                    response = {"participant_id": participant_id, "status": "manual_review",
                                "route": "manual_review", "pending_request": next_request,
                                "completed": completed}
                else:
                    assignment = self._assign(connection, site_id=participant["site_id"],
                                              participant_id=participant_id,
                                              zone_id=target["zone_id"], reason="progression")
                    connection.execute(
                        "UPDATE dispatch_participants SET status='waiting', completed_json=?, "
                        "called_at=NULL, call_expires_at=NULL, updated_at=? WHERE participant_id=?",
                        (canonical_json(completed), now, participant_id),
                    )
                    self._record_itinerary(connection, participant_id=participant_id,
                                           event_type="assigned", to_status="waiting",
                                           zone_id=target["zone_id"],
                                           detail={"service_type": next_request,
                                                   "queue_number": assignment["queue_number"],
                                                   "zone_capacity": target["capacity"],
                                                   "reason": "progression"},
                                           based_on_version=assignment["based_on_version"])
                    response = {"participant_id": participant_id, "status": "waiting",
                                "route": "zone", "zone_id": target["zone_id"],
                                "queue_number": assignment["queue_number"],
                                "based_on_version": assignment["based_on_version"],
                                "completed": completed}
            append_event(connection, actor_id=actor_id, action="dispatch.service_completed",
                         resource_type="dispatch_participant", resource_id=participant_id,
                         detail={"zone_id": zone["zone_id"], "service_type": service_type,
                                 "next_status": response["status"]},
                         occurred_at=now)
            self._store_receipt(connection, request_id=request_id, action=action, payload=payload,
                                resource_type="dispatch_participant", resource_id=participant_id,
                                response=response)
            return response, False

    # ---- 过号、暂停、恢复 ----

    def settle_timeouts(self, *, request_id: str, actor_id: str,
                        site_id: str) -> tuple[dict[str, Any], bool]:
        """结算过号：按到期时间与编号顺序释放超时占用权，参与者回到等候。"""

        payload = {"actor_id": actor_id, "site_id": site_id}
        action = "dispatch_settle_timeouts"
        with self.database.transaction(immediate=True) as connection:
            request_id = self._identifier(request_id, "request_id")
            replay = self._receipt(connection, request_id, action, payload)
            if replay is not None:
                return replay, True
            actor = self._actor(connection, actor_id)
            self._require(actor, "admin", "operator")
            site = self._site(connection, site_id)
            self._check_site_scope(actor, site)
            now = self._fmt(self._now())
            rows = connection.execute(
                "SELECT * FROM dispatch_participants WHERE site_id=? AND status='called' "
                "AND call_expires_at<=? ORDER BY call_expires_at, participant_id",
                (site_id, now),
            ).fetchall()
            missed: list[str] = []
            for row in rows:
                connection.execute(
                    "UPDATE dispatch_participants SET status='waiting', called_at=NULL, "
                    "call_expires_at=NULL, updated_at=? WHERE participant_id=?",
                    (now, row["participant_id"]),
                )
                self._record_itinerary(connection, participant_id=row["participant_id"],
                                       event_type="missed_call", from_status="called",
                                       to_status="waiting", zone_id=row["current_zone_id"],
                                       detail={"queue_number": row["queue_number"],
                                               "expired_at": row["call_expires_at"]})
                missed.append(row["participant_id"])
            append_event(connection, actor_id=actor_id, action="dispatch.timeouts_settled",
                         resource_type="site", resource_id=site_id,
                         detail={"missed": missed}, occurred_at=now)
            response = {"site_id": site_id, "missed": missed, "count": len(missed)}
            self._store_receipt(connection, request_id=request_id, action=action, payload=payload,
                                resource_type="site", resource_id=site_id, response=response)
            return response, False

    def pause_participant(self, *, request_id: str, actor_id: str,
                          participant_id: str) -> tuple[dict[str, Any], bool]:
        """暂停等候中的参与者；已叫号的须先结算过号，保证顺序确定。"""

        payload = {"actor_id": actor_id, "participant_id": participant_id}
        action = "dispatch_pause_participant"
        with self.database.transaction(immediate=True) as connection:
            request_id = self._identifier(request_id, "request_id")
            replay = self._receipt(connection, request_id, action, payload)
            if replay is not None:
                return replay, True
            actor = self._actor(connection, actor_id)
            self._require(actor, "admin", "operator")
            participant = self._participant(connection, participant_id)
            site = self._site(connection, participant["site_id"])
            self._check_site_scope(actor, site)
            if participant["status"] != "waiting":
                raise ConflictError("仅等候中的参与者可以暂停")
            now = self._fmt(self._now())
            connection.execute(
                "UPDATE dispatch_participants SET status='paused', updated_at=? WHERE participant_id=?",
                (now, participant_id),
            )
            self._record_itinerary(connection, participant_id=participant_id, event_type="paused",
                                   from_status="waiting", to_status="paused",
                                   zone_id=participant["current_zone_id"],
                                   detail={"queue_number": participant["queue_number"]})
            append_event(connection, actor_id=actor_id, action="dispatch.participant_paused",
                         resource_type="dispatch_participant", resource_id=participant_id,
                         detail={"zone_id": participant["current_zone_id"]}, occurred_at=now)
            response = {"participant_id": participant_id, "status": "paused",
                        "zone_id": participant["current_zone_id"],
                        "queue_number": participant["queue_number"]}
            self._store_receipt(connection, request_id=request_id, action=action, payload=payload,
                                resource_type="dispatch_participant", resource_id=participant_id,
                                response=response)
            return response, False

    def resume_participant(self, *, request_id: str, actor_id: str,
                           participant_id: str) -> tuple[dict[str, Any], bool]:
        """恢复暂停的参与者，保留原队列号重新等候。"""

        payload = {"actor_id": actor_id, "participant_id": participant_id}
        action = "dispatch_resume_participant"
        with self.database.transaction(immediate=True) as connection:
            request_id = self._identifier(request_id, "request_id")
            replay = self._receipt(connection, request_id, action, payload)
            if replay is not None:
                return replay, True
            actor = self._actor(connection, actor_id)
            self._require(actor, "admin", "operator")
            participant = self._participant(connection, participant_id)
            site = self._site(connection, participant["site_id"])
            self._check_site_scope(actor, site)
            if participant["status"] != "paused":
                raise ConflictError("仅已暂停的参与者可以恢复")
            now = self._fmt(self._now())
            connection.execute(
                "UPDATE dispatch_participants SET status='waiting', updated_at=? WHERE participant_id=?",
                (now, participant_id),
            )
            self._record_itinerary(connection, participant_id=participant_id, event_type="resumed",
                                   from_status="paused", to_status="waiting",
                                   zone_id=participant["current_zone_id"],
                                   detail={"queue_number": participant["queue_number"]})
            append_event(connection, actor_id=actor_id, action="dispatch.participant_resumed",
                         resource_type="dispatch_participant", resource_id=participant_id,
                         detail={"zone_id": participant["current_zone_id"]}, occurred_at=now)
            response = {"participant_id": participant_id, "status": "waiting",
                        "zone_id": participant["current_zone_id"],
                        "queue_number": participant["queue_number"]}
            self._store_receipt(connection, request_id=request_id, action=action, payload=payload,
                                resource_type="dispatch_participant", resource_id=participant_id,
                                response=response)
            return response, False

    # ---- 交接转区 ----

    def initiate_handover(self, *, request_id: str, actor_id: str, participant_id: str,
                          to_zone_id: str, reason: str) -> tuple[dict[str, Any], bool]:
        """为服务中的参与者发起交接，这是其在服务中唯一允许的转区方式。"""

        payload = {"actor_id": actor_id, "participant_id": participant_id,
                   "to_zone_id": to_zone_id, "reason": reason}
        action = "dispatch_initiate_handover"
        with self.database.transaction(immediate=True) as connection:
            request_id = self._identifier(request_id, "request_id")
            replay = self._receipt(connection, request_id, action, payload)
            if replay is not None:
                return replay, True
            actor = self._actor(connection, actor_id)
            self._require(actor, "admin", "operator")
            participant = self._participant(connection, participant_id)
            site = self._site(connection, participant["site_id"])
            self._check_site_scope(actor, site)
            if participant["status"] != "serving":
                raise ConflictError("仅服务中的参与者可以发起交接转区")
            pending = connection.execute(
                "SELECT 1 FROM dispatch_handovers WHERE participant_id=? AND status='pending'",
                (participant_id,),
            ).fetchone()
            if pending is not None:
                raise ConflictError("该参与者已有未完成的交接")
            from_zone = self._zone(connection, participant["current_zone_id"])
            to_zone = self._zone(connection, to_zone_id)
            if to_zone["site_id"] != participant["site_id"]:
                raise ValidationError("目标区域不属于同一场所")
            if to_zone["zone_id"] == from_zone["zone_id"]:
                raise ValidationError("目标区域与当前区域相同")
            if to_zone["status"] != "open":
                raise ConflictError("目标区域当前未开放")
            if to_zone["service_type"] != from_zone["service_type"]:
                raise ValidationError("交接目标区域必须提供相同服务项目")
            reason = self._text(reason, "reason", 200)
            now = self._fmt(self._now())
            handover_id = uuid.uuid4().hex
            connection.execute(
                "INSERT INTO dispatch_handovers(handover_id,site_id,participant_id,from_zone_id,"
                "to_zone_id,reason,status,initiated_by,created_at) VALUES(?,?,?,?,?,?, 'pending', ?, ?)",
                (handover_id, participant["site_id"], participant_id, from_zone["zone_id"],
                 to_zone["zone_id"], reason, actor_id, now),
            )
            self._record_itinerary(connection, participant_id=participant_id,
                                   event_type="handover_initiated", zone_id=from_zone["zone_id"],
                                   detail={"handover_id": handover_id,
                                           "to_zone_id": to_zone["zone_id"], "reason": reason})
            append_event(connection, actor_id=actor_id, action="dispatch.handover_initiated",
                         resource_type="dispatch_handover", resource_id=handover_id,
                         detail={"participant_id": participant_id,
                                 "from_zone_id": from_zone["zone_id"],
                                 "to_zone_id": to_zone["zone_id"]},
                         occurred_at=now)
            response = {"handover_id": handover_id, "participant_id": participant_id,
                        "from_zone_id": from_zone["zone_id"], "to_zone_id": to_zone["zone_id"],
                        "status": "pending"}
            self._store_receipt(connection, request_id=request_id, action=action, payload=payload,
                                resource_type="dispatch_handover", resource_id=handover_id,
                                response=response)
            return response, False

    def complete_handover(self, *, request_id: str, actor_id: str,
                          handover_id: str) -> tuple[dict[str, Any], bool]:
        """完成交接：目标区域有空位则继续服务，否则持新号码排队等候。"""

        payload = {"actor_id": actor_id, "handover_id": handover_id}
        action = "dispatch_complete_handover"
        with self.database.transaction(immediate=True) as connection:
            request_id = self._identifier(request_id, "request_id")
            replay = self._receipt(connection, request_id, action, payload)
            if replay is not None:
                return replay, True
            actor = self._actor(connection, actor_id)
            self._require(actor, "admin", "operator")
            handover = connection.execute(
                "SELECT * FROM dispatch_handovers WHERE handover_id=?", (handover_id,)
            ).fetchone()
            if handover is None:
                raise NotFoundError("交接不存在")
            site = self._site(connection, handover["site_id"])
            self._check_site_scope(actor, site)
            if handover["status"] != "pending":
                raise ConflictError("交接已处理")
            participant = self._participant(connection, handover["participant_id"])
            if (participant["status"] != "serving"
                    or participant["current_zone_id"] != handover["from_zone_id"]):
                raise ConflictError("参与者状态已变化，无法完成交接")
            to_zone = self._zone(connection, handover["to_zone_id"])
            if to_zone["status"] != "open":
                raise ConflictError("目标区域当前未开放，请取消交接")
            load = self._zone_load(connection, to_zone["zone_id"])
            direct = load["called"] + load["serving"] < to_zone["capacity"]
            now = self._fmt(self._now())
            assignment = self._assign(connection, site_id=handover["site_id"],
                                      participant_id=handover["participant_id"],
                                      zone_id=to_zone["zone_id"], reason="handover",
                                      with_queue_number=not direct)
            new_status = "serving" if direct else "waiting"
            connection.execute(
                "UPDATE dispatch_participants SET status=?, called_at=NULL, call_expires_at=NULL, "
                "updated_at=? WHERE participant_id=?",
                (new_status, now, handover["participant_id"]),
            )
            connection.execute(
                "UPDATE dispatch_handovers SET status='completed', resolved_by=?, resolved_at=? "
                "WHERE handover_id=?",
                (actor_id, now, handover_id),
            )
            self._record_itinerary(connection, participant_id=handover["participant_id"],
                                   event_type="handover_completed", from_status="serving",
                                   to_status=new_status, zone_id=to_zone["zone_id"],
                                   detail={"handover_id": handover_id,
                                           "from_zone_id": handover["from_zone_id"],
                                           "result": new_status,
                                           "queue_number": assignment["queue_number"]},
                                   based_on_version=assignment["based_on_version"])
            append_event(connection, actor_id=actor_id, action="dispatch.handover_completed",
                         resource_type="dispatch_handover", resource_id=handover_id,
                         detail={"participant_id": handover["participant_id"],
                                 "to_zone_id": to_zone["zone_id"], "result": new_status},
                         occurred_at=now)
            response = {"handover_id": handover_id, "participant_id": handover["participant_id"],
                        "to_zone_id": to_zone["zone_id"], "status": new_status,
                        "queue_number": assignment["queue_number"],
                        "based_on_version": assignment["based_on_version"]}
            self._store_receipt(connection, request_id=request_id, action=action, payload=payload,
                                resource_type="dispatch_handover", resource_id=handover_id,
                                response=response)
            return response, False

    def cancel_handover(self, *, request_id: str, actor_id: str,
                        handover_id: str) -> tuple[dict[str, Any], bool]:
        """取消未完成的交接，参与者留在原区域继续服务。"""

        payload = {"actor_id": actor_id, "handover_id": handover_id}
        action = "dispatch_cancel_handover"
        with self.database.transaction(immediate=True) as connection:
            request_id = self._identifier(request_id, "request_id")
            replay = self._receipt(connection, request_id, action, payload)
            if replay is not None:
                return replay, True
            actor = self._actor(connection, actor_id)
            self._require(actor, "admin", "operator")
            handover = connection.execute(
                "SELECT * FROM dispatch_handovers WHERE handover_id=?", (handover_id,)
            ).fetchone()
            if handover is None:
                raise NotFoundError("交接不存在")
            site = self._site(connection, handover["site_id"])
            self._check_site_scope(actor, site)
            if handover["status"] != "pending":
                raise ConflictError("交接已处理")
            now = self._fmt(self._now())
            connection.execute(
                "UPDATE dispatch_handovers SET status='cancelled', resolved_by=?, resolved_at=? "
                "WHERE handover_id=?",
                (actor_id, now, handover_id),
            )
            self._record_itinerary(connection, participant_id=handover["participant_id"],
                                   event_type="handover_cancelled",
                                   zone_id=handover["from_zone_id"],
                                   detail={"handover_id": handover_id,
                                           "to_zone_id": handover["to_zone_id"]})
            append_event(connection, actor_id=actor_id, action="dispatch.handover_cancelled",
                         resource_type="dispatch_handover", resource_id=handover_id,
                         detail={"participant_id": handover["participant_id"]}, occurred_at=now)
            response = {"handover_id": handover_id, "status": "cancelled"}
            self._store_receipt(connection, request_id=request_id, action=action, payload=payload,
                                resource_type="dispatch_handover", resource_id=handover_id,
                                response=response)
            return response, False

    # ---- 去向重算与人工处理 ----

    def recompute_assignments(self, *, request_id: str, actor_id: str,
                              site_id: str) -> tuple[dict[str, Any], bool]:
        """按当前状态版本重算等候者去向。

        仅处理 waiting 状态的参与者：服务中的只能经由交接转区，已叫号的
        持有占用权，已暂停的保持原状。原区域仍然可用时保持原队列不变，
        已完成风险筛查的群众不会因换岗被迫重新排队。
        """

        payload = {"actor_id": actor_id, "site_id": site_id}
        action = "dispatch_recompute_assignments"
        with self.database.transaction(immediate=True) as connection:
            request_id = self._identifier(request_id, "request_id")
            replay = self._receipt(connection, request_id, action, payload)
            if replay is not None:
                return replay, True
            actor = self._actor(connection, actor_id)
            self._require(actor, "admin", "operator")
            site = self._site(connection, site_id)
            self._check_site_scope(actor, site)
            version = self._state_version(connection, site_id)
            rows = connection.execute(
                "SELECT * FROM dispatch_participants WHERE site_id=? AND status='waiting' "
                "ORDER BY participant_id",
                (site_id,),
            ).fetchall()
            changes: list[dict[str, Any]] = []
            for row in rows:
                next_request = self._next_pending_request(row)
                if next_request is None:
                    continue
                current = None
                if row["current_zone_id"]:
                    current = connection.execute(
                        "SELECT * FROM dispatch_zones WHERE zone_id=?",
                        (row["current_zone_id"],),
                    ).fetchone()
                if current is not None and self._zone_viable(connection, current, next_request):
                    continue
                target = self._choose_zone(connection, site_id, next_request)
                if target is None:
                    continue
                assignment = self._assign(connection, site_id=site_id,
                                          participant_id=row["participant_id"],
                                          zone_id=target["zone_id"], reason="recompute")
                self._record_itinerary(connection, participant_id=row["participant_id"],
                                       event_type="reassigned", from_status="waiting",
                                       to_status="waiting", zone_id=target["zone_id"],
                                       detail={"from_zone_id": row["current_zone_id"],
                                               "to_zone_id": target["zone_id"],
                                               "service_type": next_request,
                                               "queue_number": assignment["queue_number"],
                                               "reason": "recompute"},
                                       based_on_version=version)
                changes.append({"participant_id": row["participant_id"],
                                "from_zone_id": row["current_zone_id"],
                                "to_zone_id": target["zone_id"],
                                "queue_number": assignment["queue_number"]})
            now = self._fmt(self._now())
            append_event(connection, actor_id=actor_id, action="dispatch.assignments_recomputed",
                         resource_type="site", resource_id=site_id,
                         detail={"state_version": version, "changed": len(changes)},
                         occurred_at=now)
            response = {"site_id": site_id, "based_on_version": version,
                        "examined": len(rows), "changes": changes}
            self._store_receipt(connection, request_id=request_id, action=action, payload=payload,
                                resource_type="site", resource_id=site_id, response=response)
            return response, False

    def resolve_manual_review(self, *, request_id: str, actor_id: str,
                              participant_id: str) -> tuple[dict[str, Any], bool]:
        """人工处理完成：按当前容量与专家状态把参与者接回正常流程。"""

        payload = {"actor_id": actor_id, "participant_id": participant_id}
        action = "dispatch_resolve_manual_review"
        with self.database.transaction(immediate=True) as connection:
            request_id = self._identifier(request_id, "request_id")
            replay = self._receipt(connection, request_id, action, payload)
            if replay is not None:
                return replay, True
            actor = self._actor(connection, actor_id)
            self._require(actor, "admin", "operator")
            participant = self._participant(connection, participant_id)
            site = self._site(connection, participant["site_id"])
            self._check_site_scope(actor, site)
            if participant["status"] != "manual_review":
                raise ConflictError("参与者不在人工处理状态")
            next_request = self._next_pending_request(participant)
            now = self._fmt(self._now())
            if next_request is None:
                connection.execute(
                    "UPDATE dispatch_participants SET status='done', screened=1, updated_at=? "
                    "WHERE participant_id=?",
                    (now, participant_id),
                )
                self._record_itinerary(connection, participant_id=participant_id,
                                       event_type="manual_review_resolved", to_status="done",
                                       detail={"note": "人工处理完成，无剩余诉求"})
                response = {"participant_id": participant_id, "status": "done", "route": "done"}
            else:
                target = self._choose_zone(connection, participant["site_id"], next_request)
                if target is None:
                    raise ConflictError("当前仍没有可接待诉求项目的区域")
                assignment = self._assign(connection, site_id=participant["site_id"],
                                          participant_id=participant_id,
                                          zone_id=target["zone_id"], reason="manual_resolve")
                connection.execute(
                    "UPDATE dispatch_participants SET status='waiting', screened=1, updated_at=? "
                    "WHERE participant_id=?",
                    (now, participant_id),
                )
                self._record_itinerary(connection, participant_id=participant_id,
                                       event_type="manual_review_resolved", to_status="waiting",
                                       detail={"note": "人工处理完成，回到队列"})
                self._record_itinerary(connection, participant_id=participant_id,
                                       event_type="assigned", to_status="waiting",
                                       zone_id=target["zone_id"],
                                       detail={"service_type": next_request,
                                               "queue_number": assignment["queue_number"],
                                               "zone_capacity": target["capacity"],
                                               "reason": "manual_resolve"},
                                       based_on_version=assignment["based_on_version"])
                response = {"participant_id": participant_id, "status": "waiting",
                            "route": "zone", "zone_id": target["zone_id"],
                            "queue_number": assignment["queue_number"],
                            "based_on_version": assignment["based_on_version"]}
            append_event(connection, actor_id=actor_id, action="dispatch.manual_review_resolved",
                         resource_type="dispatch_participant", resource_id=participant_id,
                         detail={"next_status": response["status"]}, occurred_at=now)
            self._store_receipt(connection, request_id=request_id, action=action, payload=payload,
                                resource_type="dispatch_participant", resource_id=participant_id,
                                response=response)
            return response, False

    # ---- 协调员查询 ----

    def get_zone_pressure(self, site_id: str) -> dict[str, Any]:
        """返回各区当前压力：容量、各状态人数、空位与负载率。"""

        connection = self.database.connection
        self._site(connection, site_id)
        zones = []
        for zone in connection.execute(
                "SELECT * FROM dispatch_zones WHERE site_id=? ORDER BY zone_id", (site_id,)):
            load = self._zone_load(connection, zone["zone_id"])
            experts = connection.execute(
                "SELECT COUNT(*) AS count FROM dispatch_experts WHERE zone_id=? AND status='on_duty'",
                (zone["zone_id"],),
            ).fetchone()["count"]
            occupied = load["called"] + load["serving"]
            total = occupied + load["waiting"]
            zones.append({
                "zone_id": zone["zone_id"],
                "name": zone["name"],
                "service_type": zone["service_type"],
                "status": zone["status"],
                "capacity": zone["capacity"],
                "waiting": load["waiting"],
                "called": load["called"],
                "serving": load["serving"],
                "paused": load["paused"],
                "occupied": occupied,
                "free_slots": max(zone["capacity"] - occupied, 0),
                "load_ratio": round(total / zone["capacity"], 4) if zone["capacity"] else None,
                "on_duty_experts": experts,
            })
        return {"site_id": site_id, "state_version": self._state_version(connection, site_id),
                "zones": zones}

    def get_participant(self, participant_id: str) -> dict[str, Any]:
        """返回参与者现状、当前去向所依据的状态版本与未完成的交接。"""

        connection = self.database.connection
        row = self._participant(connection, participant_id)
        assignment = connection.execute(
            "SELECT * FROM dispatch_assignments WHERE participant_id=? "
            "ORDER BY created_at DESC, rowid DESC LIMIT 1",
            (participant_id,),
        ).fetchone()
        handover = connection.execute(
            "SELECT * FROM dispatch_handovers WHERE participant_id=? AND status='pending' "
            "ORDER BY created_at DESC LIMIT 1",
            (participant_id,),
        ).fetchone()
        participant = {
            "participant_id": row["participant_id"],
            "site_id": row["site_id"],
            "alias": row["alias"],
            "requests": json.loads(row["requests_json"]),
            "contraindications": json.loads(row["contraindications_json"]),
            "risk_flags": json.loads(row["risk_flags_json"]),
            "status": row["status"],
            "current_zone_id": row["current_zone_id"],
            "queue_number": row["queue_number"],
            "called_at": row["called_at"],
            "call_expires_at": row["call_expires_at"],
            "completed": json.loads(row["completed_json"]),
            "screened": bool(row["screened"]),
        }
        return {
            "participant": participant,
            "assignment": self._assignment_dict(assignment) if assignment else None,
            "pending_handover": self._handover_dict(handover) if handover else None,
            "state_version": self._state_version(connection, row["site_id"]),
        }

    def get_itinerary(self, participant_id: str) -> list[dict[str, Any]]:
        """返回参与者从入场至今的完整行程，按确定顺序排列。"""

        connection = self.database.connection
        self._participant(connection, participant_id)
        items = []
        for row in connection.execute(
                "SELECT * FROM dispatch_itinerary WHERE participant_id=? ORDER BY seq",
                (participant_id,)):
            items.append({
                "seq": row["seq"],
                "event_type": row["event_type"],
                "from_status": row["from_status"],
                "to_status": row["to_status"],
                "zone_id": row["zone_id"],
                "detail": json.loads(row["detail_json"]),
                "based_on_version": row["based_on_version"],
                "created_at": row["created_at"],
            })
        return items

    def list_pending_handovers(self, site_id: str) -> list[dict[str, Any]]:
        """列出场所内尚未完成的交接。"""

        connection = self.database.connection
        self._site(connection, site_id)
        return [self._handover_dict(row) for row in connection.execute(
            "SELECT * FROM dispatch_handovers WHERE site_id=? AND status='pending' "
            "ORDER BY created_at, handover_id", (site_id,))]

    @staticmethod
    def _assignment_dict(row) -> dict[str, Any]:
        return {
            "assignment_id": row["assignment_id"],
            "zone_id": row["zone_id"],
            "queue_number": row["queue_number"],
            "based_on_version": row["based_on_version"],
            "reason": row["reason"],
            "created_at": row["created_at"],
        }

    @staticmethod
    def _handover_dict(row) -> dict[str, Any]:
        return {
            "handover_id": row["handover_id"],
            "participant_id": row["participant_id"],
            "from_zone_id": row["from_zone_id"],
            "to_zone_id": row["to_zone_id"],
            "reason": row["reason"],
            "status": row["status"],
            "initiated_by": row["initiated_by"],
            "resolved_by": row["resolved_by"],
            "created_at": row["created_at"],
            "resolved_at": row["resolved_at"],
        }
