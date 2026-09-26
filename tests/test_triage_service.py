import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
import tempfile

from night_market_foundation.clock import FixedClock
from night_market_foundation.errors import ConflictError, PermissionDenied, ValidationError
from night_market_foundation.service import DomainService
from night_market_foundation.storage import Database
from night_market_foundation.triage.service import TriageService


class MutableFixedClock:
    """可推进的固定时钟，供超时测试使用。"""

    def __init__(self, value):
        self.value = value

    def now(self):
        return self.value

    def advance(self, seconds):
        self.value += timedelta(seconds=seconds)


class TriageTestBase(unittest.TestCase):
    timeout = 300

    def setUp(self):
        self.database = Database()
        self.clock = MutableFixedClock(datetime(2026, 9, 26, 10, 0, tzinfo=timezone.utc))
        base = DomainService(self.database, FixedClock(self.clock.value))
        base.register_organization(request_id="org", actor_id="bootstrap",
                                   organization_id="o1", name="夜市机构")
        base.register_actor(request_id="admin", actor_id="bootstrap", new_actor_id="a1",
                            display_name="管理员", role="admin", organization_id="o1")
        base.register_actor(request_id="coord", actor_id="a1", new_actor_id="op1",
                            display_name="协调员", role="operator", organization_id="o1")
        base.register_actor(request_id="auditor", actor_id="a1", new_actor_id="au1",
                            display_name="审计员", role="auditor", organization_id="o1")
        base.register_site(request_id="site", actor_id="a1", site_id="s1",
                           organization_id="o1", name="夜市", timezone_name="Asia/Shanghai")
        self.service = TriageService(self.database, self.clock, call_timeout_seconds=self.timeout)
        self.coord = "op1"
        self.site = "s1"

    def tearDown(self):
        self.database.close()

    def register_layout(self, cons_cap=10, mas_cap=10, cul_cap=20,
                        cons_serve=2, mas_serve=2, cul_serve=5):
        self.service.register_zone(request_id="zc", actor_id=self.coord, site_id=self.site,
                                   zone_id="z-cons", name="义诊", service_type="consultation",
                                   capacity=cons_cap, serving_limit=cons_serve)
        self.service.register_zone(request_id="zm", actor_id=self.coord, site_id=self.site,
                                   zone_id="z-mas", name="推拿", service_type="massage",
                                   capacity=mas_cap, serving_limit=mas_serve)
        self.service.register_zone(request_id="zk", actor_id=self.coord, site_id=self.site,
                                   zone_id="z-cul", name="讲解", service_type="culture_talk",
                                   capacity=cul_cap, serving_limit=cul_serve)
        self.service.register_expert(request_id="ec", actor_id=self.coord, site_id=self.site,
                                     expert_id="e-cons", display_name="义诊专家",
                                     qualifications=["consultation"], zone_id="z-cons")
        self.service.register_expert(request_id="em", actor_id=self.coord, site_id=self.site,
                                     expert_id="e-mas", display_name="推拿师",
                                     qualifications=["massage"], zone_id="z-mas")
        self.service.register_expert(request_id="ek", actor_id=self.coord, site_id=self.site,
                                     expert_id="e-cul", display_name="讲解员",
                                     qualifications=["culture_talk"], zone_id="z-cul")

    def intake(self, pid, request_id=None, **kwargs):
        return self.service.intake_participant(
            request_id=request_id or f"in-{pid}", actor_id=self.coord, site_id=self.site,
            participant_id=pid, **kwargs)

    def route(self, pid, request_id=None):
        return self.service.route_participant(
            request_id=request_id or f"route-{pid}", actor_id=self.coord, participant_id=pid)

    def call(self, zone=None, request_id=None, max_calls=1):
        return self.service.call_next(
            request_id=request_id or f"call-{zone or 'all'}", actor_id=self.coord,
            zone_id=zone, max_calls=max_calls)


class RiskScreeningTest(TriageTestBase):
    def test_high_risk_statement_goes_manual_and_blocks_routing(self):
        self.register_layout()
        result = self.intake("p1", risk_statements=["chest_pain"])
        self.assertTrue(result.decision["high_risk"])
        self.assertEqual("manual_review", result.decision["routing"])
        with self.assertRaises(ConflictError):
            self.route("p1")

    def test_manual_review_then_route_without_diagnosis(self):
        self.register_layout()
        self.intake("p1", risk_statements=["pregnancy"])
        self.service.review_participant(request_id="rev-p1", actor_id=self.coord,
                                        participant_id="p1", note="已人工问询")
        routed = self.route("p1")
        self.assertEqual("queued", routed.decision["routing"])

    def test_contraindication_avoids_massage(self):
        self.register_layout()
        result = self.intake("p1", contraindications=["skin_lesion"],
                             preferences=["massage", "consultation", "culture_talk"])
        self.assertEqual(["massage"], result.decision["blocked_services"])
        routed = self.route("p1")
        self.assertEqual("z-cons", routed.decision["zone_id"])

    def test_unknown_risk_code_rejected(self):
        self.register_layout()
        with self.assertRaises(ValidationError):
            self.intake("p1", risk_statements=["made_up_code"])


class RoutingAndIdempotencyTest(TriageTestBase):
    def test_replay_returns_same_decision_conflict_on_different_content(self):
        self.register_layout()
        self.intake("p1")
        first = self.route("p1", request_id="req-1")
        replay = self.route("p1", request_id="req-1")
        self.assertFalse(first.replayed)
        self.assertTrue(replay.replayed)
        self.assertEqual(first.decision, replay.decision)
        self.intake("p2", request_id="in-p2")
        with self.assertRaises(ConflictError):
            self.service.route_participant(request_id="req-1", actor_id=self.coord,
                                           participant_id="p2")

    def test_no_capacity_defers_with_basis_version(self):
        self.register_layout(cons_cap=1, mas_cap=1, cul_cap=1,
                             cons_serve=1, mas_serve=1, cul_serve=1)
        # 占满三个区域的容量（waiting 也算占用）
        for pid in ("p1", "p2", "p3"):
            self.intake(pid)
        # p1 义诊、p2 推拿、p3 讲解（按偏好顺序与压力分配）
        r1, r2, r3 = self.route("p1"), self.route("p2"), self.route("p3")
        zones = {r1.decision["zone_id"], r2.decision["zone_id"], r3.decision["zone_id"]}
        self.assertEqual({"z-cons", "z-mas", "z-cul"}, zones)
        self.intake("p4")
        deferred = self.route("p4")
        self.assertEqual("no_capacity", deferred.decision["routing"])
        self.assertIn("state_version", deferred.decision)

    def test_screening_not_repeated_after_terminal_miss(self):
        self.register_layout()
        self.intake("p1")
        ticket = self.route("p1").decision["ticket_id"]
        self.call("z-cons")
        for _ in range(3):  # 超过 MAX_MISS_COUNT 后票终止，但无需重新入场筛查
            self.clock.advance(self.timeout + 1)
            self.service.expire_tickets(request_id=f"exp-{_}", actor_id=self.coord)
            self.call("z-cons", request_id=f"recall-{_}")
        view = self.service.get_participant(self.coord, "p1")
        self.assertEqual("screened", view.status)
        routed = self.service.route_participant(request_id="route-p1-again",
                                                actor_id=self.coord, participant_id="p1")
        self.assertEqual("queued", routed.decision["routing"])


class TicketingTest(TriageTestBase):
    def test_timeout_releases_hold_and_recall_priority(self):
        self.register_layout()
        self.intake("p1")
        self.intake("p2")
        t1 = self.route("p1").decision["ticket_id"]
        t2 = self.route("p2").decision["ticket_id"]
        self.call("z-cons", request_id="c1")
        self.clock.advance(self.timeout + 1)
        expired = self.service.expire_tickets(request_id="exp1", actor_id=self.coord)
        self.assertEqual("missed", expired.decision["released"][0]["reason"])
        # 过号票按 miss_seq 优先于普通 waiting 票重呼
        recalled = self.call("z-cons", request_id="c2")
        self.assertEqual(t1, recalled.decision["calls"][0]["ticket_id"])
        self.assertTrue(recalled.decision["calls"][0]["recalled"])
        # 普通票随后被叫
        normal = self.call("z-cons", request_id="c3", max_calls=2)
        self.assertEqual(t2, normal.decision["calls"][0]["ticket_id"])

    def test_checkin_becomes_serving_and_finish_records_service(self):
        self.register_layout()
        self.intake("p1")
        ticket = self.route("p1").decision["ticket_id"]
        self.call("z-cons")
        self.service.check_in(request_id="ci", actor_id=self.coord, ticket_id=ticket)
        view = self.service.get_participant(self.coord, "p1")
        self.assertEqual("serving", view.current_assignment["state"])
        self.service.finish_service(request_id="fin", actor_id=self.coord, ticket_id=ticket)
        view = self.service.get_participant(self.coord, "p1")
        self.assertIsNone(view.current_assignment)
        self.assertEqual("consultation", view.accepted_services[0]["service_type"])

    def test_suspend_blocks_new_calls_and_resume_restores(self):
        self.register_layout()
        self.intake("p1")
        self.route("p1")
        self.service.suspend_zone(request_id="sus", actor_id=self.coord, zone_id="z-mas")
        calls = self.call("z-mas", request_id="c-blocked")
        self.assertEqual([], calls.decision["calls"])
        self.service.resume_zone(request_id="res", actor_id=self.coord, zone_id="z-mas")
        # p1 在义诊队列，推拿仍无票
        calls = self.call("z-mas", request_id="c-empty")
        self.assertEqual([], calls.decision["calls"])
        pressures = {p.zone_id: p for p in self.service.zone_pressures(self.coord, self.site)}
        self.assertEqual("active", pressures["z-mas"].status)

    def test_suspend_then_expiry_releases_with_reason(self):
        self.register_layout()
        self.intake("p1")
        ticket = self.route("p1").decision["ticket_id"]
        self.call("z-cons")
        self.service.suspend_zone(request_id="sus", actor_id=self.coord, zone_id="z-cons")
        self.clock.advance(121)
        released = self.service.expire_tickets(request_id="exp-sus", actor_id=self.coord)
        self.assertEqual("suspended_timeout", released.decision["released"][0]["reason"])
        self.assertEqual(ticket, released.decision["released"][0]["ticket_id"])


class ReassignAndHandshakeTest(TriageTestBase):
    def test_expert_swap_reassigns_waiting_but_not_serving(self):
        self.register_layout()
        self.intake("p1")
        self.intake("p4", request_id="in-p4",
                    preferences=["massage", "culture_talk", "consultation"])
        t1 = self.route("p1").decision["ticket_id"]
        r4 = self.route("p4")
        self.assertEqual("z-mas", r4.decision["zone_id"])
        self.call("z-cons")
        self.service.check_in(request_id="ci", actor_id=self.coord, ticket_id=t1)
        # 推拿师临时下岗
        self.service.update_expert(request_id="off", actor_id=self.coord, expert_id="e-mas",
                                   on_duty=False)
        result = self.service.reassign(request_id="re", actor_id=self.coord, site_id=self.site)
        self.assertEqual(1, len(result.decision["moves"]))
        move = result.decision["moves"][0]
        self.assertEqual("p4", move["participant_id"])
        self.assertEqual("z-cul", move["to_zone_id"])
        self.assertLess(move["basis_version"], result.decision["state_version"])
        view = self.service.get_participant(self.coord, "p1")
        self.assertEqual("serving", view.current_assignment["state"])

    def test_serving_requires_explicit_handshake_to_transfer(self):
        self.register_layout()
        self.intake("p1")
        t1 = self.route("p1").decision["ticket_id"]
        self.call("z-cons")
        self.service.check_in(request_id="ci", actor_id=self.coord, ticket_id=t1)
        # 直接改派不影响服务中者
        result = self.service.reassign(request_id="re", actor_id=self.coord, site_id=self.site)
        self.assertEqual([], result.decision["moves"])
        hs = self.service.request_handshake(
            request_id="hs", actor_id=self.coord, participant_id="p1",
            to_zone_id="z-cul", reason="追加讲解")
        hs_id = hs.decision["handshake_id"]
        pending = self.service.pending_handshakes(self.coord, self.site)
        self.assertEqual(1, len(pending))
        self.assertEqual("pending", pending[0].status)
        confirmed = self.service.confirm_handshake(request_id="hsc", actor_id=self.coord,
                                                   handshake_id=hs_id)
        new_ticket = confirmed.decision["new_ticket_id"]
        # 未完成前再次确认被拒
        with self.assertRaises(ConflictError):
            self.service.confirm_handshake(request_id="hsc2", actor_id=self.coord,
                                           handshake_id=hs_id)
        self.service.check_in(request_id="ci2", actor_id=self.coord, ticket_id=new_ticket)
        self.service.complete_handshake(request_id="hsd", actor_id=self.coord, handshake_id=hs_id)
        self.service.finish_service(request_id="fin", actor_id=self.coord, ticket_id=new_ticket)
        self.assertEqual([], self.service.pending_handshakes(self.coord, self.site))
        types = [s["service_type"] for s in
                 self.service.get_participant(self.coord, "p1").accepted_services]
        self.assertEqual(["consultation", "culture_talk"], types)

    def test_handshake_rejects_full_target(self):
        self.register_layout(cul_cap=10)
        self.intake("p1")
        t1 = self.route("p1").decision["ticket_id"]
        self.call("z-cons")
        self.service.check_in(request_id="ci", actor_id=self.coord, ticket_id=t1)
        # 用 5 个 serving 占满讲解区服务位
        for i in range(5):
            pid = f"q{i}"
            self.intake(pid, request_id=f"in-{pid}", preferences=["culture_talk"])
            ticket = self.route(pid, request_id=f"route-{pid}").decision["ticket_id"]
        self.call("z-cul", request_id="fill", max_calls=10)
        for i in range(5):
            pid = f"q{i}"
            ticket = self.service.get_participant(self.coord, pid).current_assignment["ticket_id"]
            self.service.check_in(request_id=f"ci-{pid}", actor_id=self.coord, ticket_id=ticket)
        with self.assertRaises(ConflictError):
            self.service.request_handshake(
                request_id="hs", actor_id=self.coord, participant_id="p1",
                to_zone_id="z-cul", reason="满员")


class CoordinatorQueryTest(TriageTestBase):
    def test_pressure_and_version_advance_on_changes(self):
        self.register_layout()
        self.intake("p1")
        self.route("p1")
        pressures = {p.zone_id: p for p in self.service.zone_pressures(self.coord, self.site)}
        self.assertEqual(1, pressures["z-cons"].waiting)
        self.assertEqual(0, pressures["z-mas"].waiting)
        self.assertGreaterEqual(pressures["z-cons"].pressure_ratio, 0.1)
        before = self.service.current_state_version(self.coord)
        self.service.set_zone_capacity(request_id="cap", actor_id=self.coord, zone_id="z-cons",
                                       capacity=15, serving_limit=3)
        self.assertEqual(before + 1, self.service.current_state_version(self.coord))
        pressures = {p.zone_id: p for p in self.service.zone_pressures(self.coord, self.site)}
        self.assertEqual(15, pressures["z-cons"].capacity)

    def test_journey_records_state_versions(self):
        self.register_layout()
        self.intake("p1")
        self.route("p1")
        events = self.service.journey_events(self.coord, "p1")
        self.assertEqual(["intake", "routed"], [e["event_type"] for e in events])
        self.assertTrue(all(e["state_version"] is not None for e in events))

    def test_auditor_cannot_mutate(self):
        self.register_layout()
        with self.assertRaises(PermissionDenied):
            self.service.suspend_zone(request_id="x", actor_id="au1", zone_id="z-cons")


class RecoveryTest(TriageTestBase):
    def test_restart_continues_confirmed_unfinished_flows(self):
        with tempfile.TemporaryDirectory() as directory:
            db_path = Path(directory) / "restart.sqlite3"
            database = Database(db_path)
            clock = MutableFixedClock(datetime(2026, 9, 26, 10, 0, tzinfo=timezone.utc))
            base = DomainService(database, FixedClock(clock.value))
            base.register_organization(request_id="org", actor_id="bootstrap",
                                       organization_id="o1", name="夜市")
            base.register_actor(request_id="admin", actor_id="bootstrap", new_actor_id="a1",
                                display_name="管理员", role="admin", organization_id="o1")
            base.register_actor(request_id="coord", actor_id="a1", new_actor_id="op1",
                                display_name="协调员", role="operator", organization_id="o1")
            base.register_site(request_id="site", actor_id="a1", site_id="s1",
                               organization_id="o1", name="夜市", timezone_name="Asia/Shanghai")
            service = TriageService(database, clock, call_timeout_seconds=300)
            service.register_zone(request_id="zc", actor_id="op1", site_id="s1", zone_id="z-cons",
                                  name="义诊", service_type="consultation", capacity=10,
                                  serving_limit=2)
            service.register_expert(request_id="ec", actor_id="op1", site_id="s1", expert_id="e-cons",
                                    display_name="专家", qualifications=["consultation"],
                                    zone_id="z-cons")
            service.intake_participant(request_id="in", actor_id="op1", site_id="s1",
                                       participant_id="p1")
            ticket = service.route_participant(request_id="rt", actor_id="op1",
                                               participant_id="p1").decision["ticket_id"]
            service.call_next(request_id="call", actor_id="op1", zone_id="z-cons")
            service.check_in(request_id="ci", actor_id="op1", ticket_id=ticket)
            database.close()

            clock.advance(10_000)
            database2 = Database(db_path)
            service2 = TriageService(database2, clock, call_timeout_seconds=300)
            recovery = service2.recover("system")
            # serving 票不受过号影响，重启后仍在服务中，可直接结束服务
            self.assertEqual(1, recovery["active_tickets"])
            view = service2.get_participant("op1", "p1")
            self.assertEqual("serving", view.current_assignment["state"])
            service2.finish_service(request_id="fin", actor_id="op1", ticket_id=ticket)
            database2.close()


if __name__ == "__main__":
    unittest.main()
