"""运行服务分流中枢的离线端到端验收。

覆盖：三区与专家排班、风险筛查（高风险转人工、禁忌回避）、幂等重放与冲突、
叫号超时释放与过号重呼、容量/人员变化后的改派、服务中交接、暂停/恢复、
压力与交接查询，以及"重启后从持久化记录继续推进"。
"""

from __future__ import annotations

import json
import tempfile
from datetime import datetime, timezone
from pathlib import Path

from ..audit import verify_chain
from ..errors import ConflictError
from ..clock import FixedClock
from ..storage import Database
from .service import TriageService

START = datetime(2026, 9, 26, 10, 0, tzinfo=timezone.utc)


def _bootstrap(database, clock):
    from ..service import DomainService

    base = DomainService(database, clock)
    base.register_organization(request_id="org", actor_id="bootstrap",
                               organization_id="org-jm", name="荆门中医文化夜市")
    base.register_actor(request_id="admin", actor_id="bootstrap", new_actor_id="admin-1",
                        display_name="管理员", role="admin", organization_id="org-jm")
    base.register_actor(request_id="coord", actor_id="admin-1", new_actor_id="coord-1",
                        display_name="现场协调员", role="operator", organization_id="org-jm")
    base.register_site(request_id="site", actor_id="admin-1", site_id="site-jm",
                       organization_id="org-jm", name="夜市主会场", timezone_name="Asia/Shanghai")
    return base


def run() -> dict[str, object]:
    with tempfile.TemporaryDirectory() as directory:
        db_path = Path(directory) / "triage_acceptance.sqlite3"
        clock = FixedClock(START)
        database = Database(db_path)
        _bootstrap(database, clock)
        triage = TriageService(database, clock, call_timeout_seconds=300)
        coord = "coord-1"
        site = "site-jm"

        # 三个服务区与当班专家
        triage.register_zone(request_id="z-cons", actor_id=coord, site_id=site, zone_id="z-cons",
                             name="义诊区", service_type="consultation", capacity=10, serving_limit=2)
        triage.register_zone(request_id="z-mas", actor_id=coord, site_id=site, zone_id="z-mas",
                             name="推拿体验区", service_type="massage", capacity=10, serving_limit=2)
        triage.register_zone(request_id="z-cul", actor_id=coord, site_id=site, zone_id="z-cul",
                             name="文化讲解区", service_type="culture_talk", capacity=20, serving_limit=5)
        triage.register_expert(request_id="e-cons", actor_id=coord, site_id=site, expert_id="e-cons",
                               display_name="义诊专家", qualifications=["consultation"], zone_id="z-cons")
        triage.register_expert(request_id="e-mas", actor_id=coord, site_id=site, expert_id="e-mas",
                               display_name="推拿师", qualifications=["massage"], zone_id="z-mas")
        triage.register_expert(request_id="e-cul", actor_id=coord, site_id=site, expert_id="e-cul",
                               display_name="讲解员", qualifications=["culture_talk"], zone_id="z-cul")

        # 入场：普通群众、皮肤禁忌群众、高风险陈述群众
        p1 = triage.intake_participant(request_id="in-p1", actor_id=coord, site_id=site,
                                       participant_id="p1")
        p2 = triage.intake_participant(request_id="in-p2", actor_id=coord, site_id=site,
                                       participant_id="p2",
                                       contraindications=["skin_lesion"])
        p3 = triage.intake_participant(request_id="in-p3", actor_id=coord, site_id=site,
                                       participant_id="p3", risk_statements=["chest_pain"])
        assert p1.decision["routing"] == "ready_to_route"
        assert p2.decision["blocked_services"] == ["massage"]
        assert p3.decision["routing"] == "manual_review"

        # 高风险未人工处理前，系统不得自动分流
        try:
            triage.route_participant(request_id="route-p3-blocked", actor_id=coord,
                                     participant_id="p3")
            raise AssertionError("高风险陈述不应自动分流")
        except ConflictError:
            pass
        triage.review_participant(request_id="review-p3", actor_id=coord, participant_id="p3",
                                  note="协调员已人工问询并登记，按普通流程分流")

        # p1 排队到义诊；重放返回同一决定；同号异内容报告冲突
        route_p1 = triage.route_participant(request_id="route-p1", actor_id=coord,
                                            participant_id="p1")
        ticket_p1 = route_p1.decision["ticket_id"]
        replay = triage.route_participant(request_id="route-p1", actor_id=coord,
                                          participant_id="p1")
        assert replay.replayed and replay.decision["ticket_id"] == ticket_p1
        try:
            triage.route_participant(request_id="route-p1", actor_id=coord,
                                     participant_id="p2")
            raise AssertionError("相同 request_id 不同内容必须冲突")
        except ConflictError:
            pass
        triage.route_participant(request_id="route-p2", actor_id=coord, participant_id="p2")
        triage.route_participant(request_id="route-p3", actor_id=coord, participant_id="p3")

        # 叫号 -> 超时未到 -> 过号释放 -> 再次叫号优先重呼 -> 签到进入服务
        call1 = triage.call_next(request_id="call-1", actor_id=coord, zone_id="z-cons")
        assert call1.decision["calls"][0]["ticket_id"] == ticket_p1
        assert call1.decision["calls"][0]["recalled"] is False
        triage.expire_tickets(request_id="expire-nope", actor_id=coord)
        clock._value = datetime(2026, 9, 26, 10, 6, tzinfo=timezone.utc)  # noqa: SLF001
        expired = triage.expire_tickets(request_id="expire-1", actor_id=coord)
        assert len(expired.decision["released"]) == 1
        assert expired.decision["released"][0]["reason"] == "missed"
        expired_replay = triage.expire_tickets(request_id="expire-1", actor_id=coord)
        assert expired_replay.replayed
        call2 = triage.call_next(request_id="call-2", actor_id=coord, zone_id="z-cons")
        assert call2.decision["calls"][0]["ticket_id"] == ticket_p1
        assert call2.decision["calls"][0]["recalled"] is True
        triage.check_in(request_id="checkin-p1", actor_id=coord, ticket_id=ticket_p1)

        # p4 首选推拿；推拿师临时下岗后改派，等待票转到讲解区，服务中的 p1 不动
        triage.intake_participant(request_id="in-p4", actor_id=coord, site_id=site,
                                  participant_id="p4",
                                  preferences=["massage", "culture_talk", "consultation"])
        route_p4 = triage.route_participant(request_id="route-p4", actor_id=coord,
                                            participant_id="p4")
        assert route_p4.decision["zone_id"] == "z-mas"
        triage.update_expert(request_id="expert-off", actor_id="coord-1", expert_id="e-mas",
                             on_duty=False)
        reassign = triage.reassign(request_id="reassign-1", actor_id=coord, site_id=site)
        assert len(reassign.decision["moves"]) == 1
        move = reassign.decision["moves"][0]
        assert move["participant_id"] == "p4" and move["to_zone_id"] == "z-cul"
        assert move["basis_version"] < reassign.decision["state_version"]
        p1_view = triage.get_participant(coord, "p1")
        assert p1_view.current_assignment["state"] == "serving"
        assert p1_view.current_assignment["zone_id"] == "z-cons"

        # 服务中只能经明确交接转区：申请 -> 确认（新区叫号）-> 签到 -> 完成 -> 结束服务
        handshake = triage.request_handshake(request_id="hs-1", actor_id=coord,
                                             participant_id="p1", to_zone_id="z-cul",
                                             reason="群众希望追加文化讲解，专家换岗协调")
        hs_id = handshake.decision["handshake_id"]
        assert len(triage.pending_handshakes(coord, site)) == 1
        confirmed = triage.confirm_handshake(request_id="hs-1-confirm", actor_id=coord,
                                             handshake_id=hs_id)
        new_ticket = confirmed.decision["new_ticket_id"]
        triage.check_in(request_id="checkin-p1-cul", actor_id=coord, ticket_id=new_ticket)
        triage.complete_handshake(request_id="hs-1-done", actor_id=coord, handshake_id=hs_id)
        triage.finish_service(request_id="finish-p1", actor_id=coord, ticket_id=new_ticket)
        p1_view = triage.get_participant(coord, "p1")
        accepted_types = [item["service_type"] for item in p1_view.accepted_services]
        assert accepted_types == ["consultation", "culture_talk"]
        assert p1_view.accepted_services[0]["note"] == "transferred_via_handshake"
        assert p1_view.accepted_services[1]["service_type"] == "culture_talk"
        assert len(triage.pending_handshakes(coord, site)) == 0

        # 暂停/恢复按确定顺序生效
        suspended = triage.suspend_zone(request_id="suspend-mas", actor_id=coord, zone_id="z-mas")
        assert suspended.decision["status"] == "suspended"
        resumed = triage.resume_zone(request_id="resume-mas", actor_id=coord, zone_id="z-mas")
        assert resumed.decision["status"] == "active"

        # 协调员查询：各区压力与状态版本
        pressures = {p.zone_id: p for p in triage.zone_pressures(coord, site)}
        assert set(pressures) == {"z-cons", "z-mas", "z-cul"}
        version_before = triage.current_state_version(coord)
        triage.set_zone_capacity(request_id="cap-cons", actor_id=coord, zone_id="z-cons",
                                 capacity=12, serving_limit=3)
        assert triage.current_state_version(coord) == version_before + 1

        # 行程可追溯：每个事件都带有当时的状态版本
        journey = triage.journey_events(coord, "p1")
        event_types = [event["event_type"] for event in journey]
        assert event_types[:3] == ["intake", "routed", "ticket_called"]
        assert "handshake_confirmed" in event_types and "service_finished" in event_types
        assert all(event["state_version"] is not None for event in journey)

        # 再制造一张候诊票，用于验证重启续推
        triage.intake_participant(request_id="in-p5", actor_id=coord, site_id=site,
                                  participant_id="p5",
                                  preferences=["culture_talk"])
        route_p5 = triage.route_participant(request_id="route-p5", actor_id=coord,
                                            participant_id="p5")
        triage.call_next(request_id="call-p5", actor_id=coord, zone_id="z-cul", max_calls=2)
        _, events_before = verify_chain(triage.database.connection)
        database.close()

        # 模拟应用重启：停机期间叫号到期，恢复时必须补齐释放，在途流程仍可查询
        restart_clock = FixedClock(datetime(2026, 9, 26, 10, 20, tzinfo=timezone.utc))
        restarted_db = Database(db_path)
        restarted = TriageService(restarted_db, restart_clock, call_timeout_seconds=300)
        recovery = restarted.recover("system")
        assert any(item["participant_id"] == "p5" and item["reason"] == "missed"
                   for item in recovery["released"])
        assert recovery["active_tickets"] >= 1  # p2/p3 仍在义诊队列
        p1_after = restarted.get_participant(coord, "p1")
        assert [item["service_type"] for item in p1_after.accepted_services] == [
            "consultation", "culture_talk"]
        valid, event_count = verify_chain(restarted_db.connection)
        restarted_db.close()

        return {"status": "ok", "audit_valid": valid, "audit_events": event_count,
                "audit_grew": event_count > events_before,
                "zones": len(pressures),
                "reassigned": reassign.decision["moves"][0]["to_zone_id"],
                "reassign_basis_version": move["basis_version"],
                "reassign_state_version": reassign.decision["state_version"],
                "p1_accepted": list(p1_view.accepted_services),
                "recovered_releases": len(recovery["released"]),
                "open_handshakes_after_recovery": len(
                    [h for h in recovery["open_handshakes"]])}


def main() -> int:
    result = run()
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0 if result["status"] == "ok" and result["audit_valid"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
