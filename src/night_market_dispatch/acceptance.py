"""运行分流中枢的离线端到端验收，覆盖重启后的流程恢复。"""

from __future__ import annotations

import json
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

from night_market_foundation.service import DomainService
from night_market_foundation.storage import Database

from .service import DispatchService


class StepClock:
    """可步进的验收时钟。"""

    def __init__(self, value: datetime) -> None:
        self.value = value

    def now(self) -> datetime:
        return self.value

    def advance(self, seconds: float) -> None:
        self.value = self.value + timedelta(seconds=seconds)


def run() -> dict[str, object]:
    """执行一条完整的分流链并返回结果。"""

    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / "dispatch.sqlite3"
        clock = StepClock(datetime(2026, 9, 26, 10, 0, tzinfo=timezone.utc))
        database = Database(path)
        foundation = DomainService(database, clock)
        dispatch = DispatchService(database, clock)

        foundation.register_organization(request_id="acc-org", actor_id="bootstrap",
                                         organization_id="org-jm", name="荆门中医文化夜市组委会")
        foundation.register_actor(request_id="acc-admin", actor_id="bootstrap",
                                  new_actor_id="admin-1", display_name="系统管理员",
                                  role="admin", organization_id="org-jm")
        foundation.register_actor(request_id="acc-op", actor_id="admin-1", new_actor_id="coord-1",
                                  display_name="现场协调员", role="operator", organization_id="org-jm")
        foundation.register_site(request_id="acc-site", actor_id="coord-1", site_id="site-night",
                                 organization_id="org-jm", name="夜市主会场",
                                 timezone_name="Asia/Shanghai")

        dispatch.register_zone(request_id="acc-z1", actor_id="coord-1", site_id="site-night",
                               zone_id="zone-consult", name="义诊区", service_type="consultation",
                               capacity=2)
        dispatch.register_zone(request_id="acc-z2", actor_id="coord-1", site_id="site-night",
                               zone_id="zone-massage", name="推拿体验区", service_type="massage",
                               capacity=1)
        dispatch.register_zone(request_id="acc-z3", actor_id="coord-1", site_id="site-night",
                               zone_id="zone-culture", name="文化讲解区", service_type="culture",
                               capacity=5)
        dispatch.register_expert(request_id="acc-e1", actor_id="coord-1", site_id="site-night",
                                 expert_id="exp-consult", name="张医师",
                                 qualifications=["consultation"], zone_id="zone-consult")
        dispatch.register_expert(request_id="acc-e2", actor_id="coord-1", site_id="site-night",
                                 expert_id="exp-massage", name="李师傅",
                                 qualifications=["massage"], zone_id="zone-massage")
        dispatch.register_expert(request_id="acc-e3", actor_id="coord-1", site_id="site-night",
                                 expert_id="exp-culture", name="王老师",
                                 qualifications=["culture"], zone_id="zone-culture")

        p1, _ = dispatch.check_in_participant(request_id="acc-p1", actor_id="coord-1",
                                              site_id="site-night", participant_id="p-001",
                                              alias="市民甲", requests=["consultation"],
                                              contraindications=["hypertension"])
        dispatch.check_in_participant(request_id="acc-p2", actor_id="coord-1", site_id="site-night",
                                      participant_id="p-002", alias="市民乙",
                                      requests=["massage", "culture"])
        p3, _ = dispatch.check_in_participant(request_id="acc-p3", actor_id="coord-1",
                                              site_id="site-night", participant_id="p-003",
                                              alias="市民丙", requests=["culture"],
                                              risk_flags=["chest_pain"])
        assert p1["route"] == "zone" and p1["zone_id"] == "zone-consult"
        assert p3["route"] == "manual_review" and "zone_id" not in p3

        dispatch.call_next(request_id="acc-c1", actor_id="coord-1", zone_id="zone-consult",
                           ttl_seconds=120)
        dispatch.confirm_arrival(request_id="acc-a1", actor_id="coord-1", participant_id="p-001")
        done1, _ = dispatch.complete_service(request_id="acc-f1", actor_id="coord-1",
                                             participant_id="p-001")
        assert done1["status"] == "done"

        dispatch.call_next(request_id="acc-c2", actor_id="coord-1", zone_id="zone-massage",
                           ttl_seconds=60)
        clock.advance(120)
        settled, _ = dispatch.settle_timeouts(request_id="acc-s1", actor_id="coord-1",
                                              site_id="site-night")
        assert settled["missed"] == ["p-002"]

        dispatch.call_next(request_id="acc-c3", actor_id="coord-1", zone_id="zone-massage",
                           ttl_seconds=300)
        dispatch.confirm_arrival(request_id="acc-a2", actor_id="coord-1", participant_id="p-002")
        prog, _ = dispatch.complete_service(request_id="acc-f2", actor_id="coord-1",
                                            participant_id="p-002")
        assert prog["status"] == "waiting" and prog["zone_id"] == "zone-culture"

        resolved, _ = dispatch.resolve_manual_review(request_id="acc-r1", actor_id="coord-1",
                                                     participant_id="p-003")
        assert resolved["route"] == "zone" and resolved["zone_id"] == "zone-culture"

        dispatch.register_zone(request_id="acc-z4", actor_id="coord-1", site_id="site-night",
                               zone_id="zone-massage-2", name="推拿二区", service_type="massage",
                               capacity=1)
        p4, _ = dispatch.check_in_participant(request_id="acc-p4", actor_id="coord-1",
                                              site_id="site-night", participant_id="p-004",
                                              alias="市民丁", requests=["massage"])
        assert p4["zone_id"] == "zone-massage"
        dispatch.assign_expert(request_id="acc-e2m", actor_id="coord-1", expert_id="exp-massage",
                               zone_id="zone-massage-2")
        rec, _ = dispatch.recompute_assignments(request_id="acc-rc1", actor_id="coord-1",
                                                site_id="site-night")
        assert [c["participant_id"] for c in rec["changes"]] == ["p-004"]
        assert rec["changes"][0]["to_zone_id"] == "zone-massage-2"

        dispatch.call_next(request_id="acc-c4", actor_id="coord-1", zone_id="zone-massage-2",
                           ttl_seconds=300)
        dispatch.confirm_arrival(request_id="acc-a4", actor_id="coord-1", participant_id="p-004")
        handover, _ = dispatch.initiate_handover(request_id="acc-h1", actor_id="coord-1",
                                                 participant_id="p-004",
                                                 to_zone_id="zone-massage", reason="专家临时换岗")
        pending_before = dispatch.list_pending_handovers("site-night")
        database.close()

        # 应用重启：已确认但未结束的流程从持久化记录继续推进。
        clock.advance(30)
        reopened = Database(path)
        foundation2 = DomainService(reopened, clock)
        dispatch2 = DispatchService(reopened, clock)
        replay, replayed = dispatch2.check_in_participant(
            request_id="acc-p1", actor_id="coord-1", site_id="site-night", participant_id="p-001",
            alias="市民甲", requests=["consultation"], contraindications=["hypertension"])
        assert replayed and replay["zone_id"] == "zone-consult"
        pending_after = dispatch2.list_pending_handovers("site-night")
        assert len(pending_after) == 1
        done_ho, _ = dispatch2.complete_handover(request_id="acc-h2", actor_id="coord-1",
                                                 handover_id=handover["handover_id"])
        assert done_ho["status"] == "serving" and done_ho["to_zone_id"] == "zone-massage"
        pressure = dispatch2.get_zone_pressure("site-night")
        itinerary = dispatch2.get_itinerary("p-002")
        valid, event_count = foundation2.verify_audit()
        result = {
            "status": "ok",
            "audit_valid": valid,
            "audit_events": event_count,
            "zones": len(pressure["zones"]),
            "state_version": pressure["state_version"],
            "recomputed": len(rec["changes"]),
            "pending_before_restart": len(pending_before),
            "pending_after_restart": len(pending_after),
            "handover_after_restart": done_ho["status"],
            "itinerary_events": len(itinerary),
            "manual_review_route": p3["route"],
        }
        reopened.close()
        return result


def main() -> int:
    """打印验收结果并设置退出码。"""

    result = run()
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0 if result["status"] == "ok" and result["audit_valid"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
