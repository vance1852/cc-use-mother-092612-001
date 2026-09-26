import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from night_market_foundation.errors import (
    ConflictError,
    NotFoundError,
    PermissionDenied,
    ValidationError,
)
from night_market_foundation.service import DomainService
from night_market_foundation.storage import Database
from night_market_dispatch.service import DispatchService


class StepClock:
    def __init__(self, value):
        self.value = value

    def now(self):
        return self.value

    def advance(self, seconds):
        self.value = self.value + timedelta(seconds=seconds)


def bootstrap(foundation, dispatch):
    foundation.register_organization(request_id="org", actor_id="bootstrap",
                                     organization_id="o1", name="活动机构")
    foundation.register_actor(request_id="admin", actor_id="bootstrap", new_actor_id="a1",
                              display_name="管理员", role="admin", organization_id="o1")
    foundation.register_actor(request_id="op", actor_id="a1", new_actor_id="op1",
                              display_name="协调员", role="operator", organization_id="o1")
    foundation.register_actor(request_id="auditor", actor_id="a1", new_actor_id="au1",
                              display_name="审计员", role="auditor", organization_id="o1")
    foundation.register_site(request_id="site", actor_id="op1", site_id="s1",
                             organization_id="o1", name="夜市", timezone_name="Asia/Shanghai")
    dispatch.register_zone(request_id="z-consult", actor_id="op1", site_id="s1", zone_id="zc",
                           name="义诊区", service_type="consultation", capacity=1)
    dispatch.register_zone(request_id="z-massage", actor_id="op1", site_id="s1", zone_id="zm",
                           name="推拿区", service_type="massage", capacity=1)
    dispatch.register_zone(request_id="z-culture", actor_id="op1", site_id="s1", zone_id="zl",
                           name="讲解区", service_type="culture", capacity=2)
    dispatch.register_expert(request_id="e-consult", actor_id="op1", site_id="s1", expert_id="ec",
                             name="医师", qualifications=["consultation"], zone_id="zc")
    dispatch.register_expert(request_id="e-massage", actor_id="op1", site_id="s1", expert_id="em",
                             name="推拿师", qualifications=["massage"], zone_id="zm")
    dispatch.register_expert(request_id="e-culture", actor_id="op1", site_id="s1", expert_id="el",
                             name="讲解员", qualifications=["culture"], zone_id="zl")


class DispatchServiceTest(unittest.TestCase):
    def setUp(self):
        self.database = Database()
        self.clock = StepClock(datetime(2026, 9, 26, 10, 0, tzinfo=timezone.utc))
        self.foundation = DomainService(self.database, self.clock)
        self.dispatch = DispatchService(self.database, self.clock)
        bootstrap(self.foundation, self.dispatch)

    def tearDown(self):
        self.database.close()

    def check_in(self, pid, requests, **kwargs):
        return self.dispatch.check_in_participant(
            request_id=f"ci-{pid}", actor_id="op1", site_id="s1",
            participant_id=pid, alias=f"参与者{pid}", requests=requests, **kwargs)

    def call(self, zone_id, key, ttl=300):
        return self.dispatch.call_next(request_id=f"cn-{key}", actor_id="op1",
                                       zone_id=zone_id, ttl_seconds=ttl)

    def arrive(self, pid, key):
        return self.dispatch.confirm_arrival(request_id=f"ca-{key}", actor_id="op1",
                                             participant_id=pid)

    def complete(self, pid, key):
        return self.dispatch.complete_service(request_id=f"cs-{key}", actor_id="op1",
                                              participant_id=pid)

    def test_check_in_assigns_zone_queue_and_records_itinerary(self):
        decision, replayed = self.check_in("p1", ["consultation"],
                                           contraindications=["hypertension"])
        self.assertFalse(replayed)
        self.assertEqual("zc", decision["zone_id"])
        self.assertEqual(1, decision["queue_number"])
        self.assertTrue(decision["screened"])
        view = self.dispatch.get_participant("p1")
        self.assertEqual("waiting", view["participant"]["status"])
        self.assertEqual(["hypertension"], view["participant"]["contraindications"])
        self.assertEqual(decision["based_on_version"], view["assignment"]["based_on_version"])
        self.assertEqual(view["state_version"], view["assignment"]["based_on_version"])
        events = [e["event_type"] for e in self.dispatch.get_itinerary("p1")]
        self.assertEqual(["checked_in", "assigned"], events)

    def test_check_in_rejects_unregistered_request(self):
        with self.assertRaises(ValidationError):
            self.check_in("p2", ["nonexistent"])

    def test_high_risk_goes_to_manual_review_without_zone_or_diagnosis(self):
        decision, _ = self.check_in("p3", ["culture"], risk_flags=["chest_pain", "other"])
        self.assertEqual("manual_review", decision["route"])
        self.assertNotIn("zone_id", decision)
        self.assertNotIn("diagnos", json.dumps(decision).lower())
        view = self.dispatch.get_participant("p3")
        self.assertEqual("manual_review", view["participant"]["status"])
        self.assertIsNone(view["participant"]["current_zone_id"])
        self.assertFalse(view["participant"]["screened"])
        events = self.dispatch.get_itinerary("p3")
        self.assertEqual("escalated_to_manual", events[0]["event_type"])
        pressure = self.dispatch.get_zone_pressure("s1")
        self.assertEqual(0, sum(z["waiting"] for z in pressure["zones"]))

    def test_manual_review_resolve_returns_to_queue(self):
        self.check_in("p4", ["culture"], risk_flags=["syncope"])
        decision, _ = self.dispatch.resolve_manual_review(
            request_id="rm-p4", actor_id="op1", participant_id="p4")
        self.assertEqual("zone", decision["route"])
        self.assertEqual("zl", decision["zone_id"])
        view = self.dispatch.get_participant("p4")
        self.assertTrue(view["participant"]["screened"])
        self.assertEqual("waiting", view["participant"]["status"])

    def test_replay_returns_original_decision(self):
        first, replayed1 = self.check_in("p5", ["culture"])
        again, replayed2 = self.check_in("p5", ["culture"])
        self.assertFalse(replayed1)
        self.assertTrue(replayed2)
        self.assertEqual(first, again)
        call1, _ = self.call("zl", "p5")
        self.arrive("p5", "p5")
        call2, replayed3 = self.call("zl", "p5")
        self.assertTrue(replayed3)
        self.assertEqual(call1, call2)

    def test_same_request_id_with_different_content_conflicts(self):
        self.check_in("p6", ["culture"])
        with self.assertRaises(ConflictError):
            self.dispatch.check_in_participant(
                request_id="ci-p6", actor_id="op1", site_id="s1", participant_id="p6",
                alias="参与者p6", requests=["massage"])

    def test_call_next_orders_by_queue_number_and_respects_capacity(self):
        self.check_in("p7", ["culture"])
        self.check_in("p8", ["culture"])
        self.check_in("p9", ["culture"])
        first, _ = self.call("zl", "1")
        second, _ = self.call("zl", "2")
        self.assertEqual("p7", first["participant_id"])
        self.assertEqual("p8", second["participant_id"])
        with self.assertRaises(ConflictError):
            self.call("zl", "3")
        self.arrive("p7", "1")
        self.complete("p7", "1")
        third, _ = self.call("zl", "3")
        self.assertEqual("p9", third["participant_id"])

    def test_call_next_on_paused_zone_rejected(self):
        self.check_in("p10", ["culture"])
        self.dispatch.set_zone_status(request_id="zs-zl", actor_id="op1",
                                      zone_id="zl", status="paused")
        with self.assertRaises(ConflictError):
            self.call("zl", "p10")
        self.dispatch.set_zone_status(request_id="zs-zl2", actor_id="op1",
                                      zone_id="zl", status="open")
        decision, _ = self.call("zl", "p10")
        self.assertEqual("p10", decision["participant_id"])

    def test_timeout_releases_occupation_in_expiry_order(self):
        self.check_in("p11", ["culture"])
        self.check_in("p12", ["culture"])
        self.call("zl", "p11", ttl=60)
        self.call("zl", "p12", ttl=120)
        self.clock.advance(61)
        settled, _ = self.dispatch.settle_timeouts(request_id="st-1", actor_id="op1", site_id="s1")
        self.assertEqual(["p11"], settled["missed"])
        view = self.dispatch.get_participant("p11")
        self.assertEqual("waiting", view["participant"]["status"])
        self.assertEqual(1, view["participant"]["queue_number"])
        recalled, _ = self.call("zl", "p11-again")
        self.assertEqual("p11", recalled["participant_id"])
        self.clock.advance(301)
        settled2, _ = self.dispatch.settle_timeouts(request_id="st-2", actor_id="op1", site_id="s1")
        self.assertEqual(["p12", "p11"], settled2["missed"])

    def test_expired_occupation_cannot_confirm(self):
        self.check_in("p13", ["massage"])
        self.call("zm", "p13", ttl=60)
        self.clock.advance(61)
        with self.assertRaises(ConflictError):
            self.arrive("p13", "p13")
        self.dispatch.settle_timeouts(request_id="st-p13", actor_id="op1", site_id="s1")
        self.call("zm", "p13-again")
        decision, _ = self.arrive("p13", "p13-again")
        self.assertEqual("serving", decision["status"])

    def test_called_participant_must_settle_before_pause(self):
        self.check_in("p14", ["culture"])
        self.call("zl", "p14", ttl=60)
        with self.assertRaises(ConflictError):
            self.dispatch.pause_participant(request_id="pp-p14", actor_id="op1",
                                            participant_id="p14")
        self.clock.advance(61)
        self.dispatch.settle_timeouts(request_id="st-p14", actor_id="op1", site_id="s1")
        decision, _ = self.dispatch.pause_participant(request_id="pp-p14b", actor_id="op1",
                                                      participant_id="p14")
        self.assertEqual("paused", decision["status"])

    def test_pause_and_resume_keep_queue_number(self):
        self.check_in("p15", ["culture"])
        self.check_in("p16", ["culture"])
        self.dispatch.pause_participant(request_id="pp-p16", actor_id="op1", participant_id="p16")
        called, _ = self.call("zl", "p15")
        self.assertEqual("p15", called["participant_id"])
        resumed, _ = self.dispatch.resume_participant(request_id="pr-p16", actor_id="op1",
                                                      participant_id="p16")
        self.assertEqual("waiting", resumed["status"])
        self.assertEqual(2, resumed["queue_number"])
        with self.assertRaises(ConflictError):
            self.dispatch.resume_participant(request_id="pr-p16b", actor_id="op1",
                                             participant_id="p16")

    def test_serving_participant_only_moves_via_handover(self):
        self.check_in("p17", ["massage"])
        self.call("zm", "p17")
        self.arrive("p17", "p17")
        self.dispatch.set_expert_status(request_id="es-em", actor_id="op1",
                                        expert_id="em", status="off_duty")
        rec, _ = self.dispatch.recompute_assignments(request_id="rc-1", actor_id="op1", site_id="s1")
        self.assertEqual([], rec["changes"])
        view = self.dispatch.get_participant("p17")
        self.assertEqual("serving", view["participant"]["status"])
        self.assertEqual("zm", view["participant"]["current_zone_id"])

    def test_handover_requires_serving_status(self):
        self.check_in("p18", ["massage"])
        self.dispatch.register_zone(request_id="z-m2", actor_id="op1", site_id="s1",
                                    zone_id="zm2", name="推拿二区", service_type="massage",
                                    capacity=1)
        with self.assertRaises(ConflictError):
            self.dispatch.initiate_handover(request_id="hi-p18", actor_id="op1",
                                            participant_id="p18", to_zone_id="zm2",
                                            reason="换岗")

    def test_handover_completes_into_serving_when_capacity_free(self):
        self.check_in("p19", ["massage"])
        self.call("zm", "p19")
        self.arrive("p19", "p19")
        self.dispatch.register_zone(request_id="z-m2", actor_id="op1", site_id="s1",
                                    zone_id="zm2", name="推拿二区", service_type="massage",
                                    capacity=1)
        handover, _ = self.dispatch.initiate_handover(
            request_id="hi-p19", actor_id="op1", participant_id="p19",
            to_zone_id="zm2", reason="专家临时换岗")
        pending = self.dispatch.list_pending_handovers("s1")
        self.assertEqual([handover["handover_id"]], [h["handover_id"] for h in pending])
        done, _ = self.dispatch.complete_handover(request_id="hc-p19", actor_id="op1",
                                                  handover_id=handover["handover_id"])
        self.assertEqual("serving", done["status"])
        self.assertEqual("zm2", done["to_zone_id"])
        self.assertEqual([], self.dispatch.list_pending_handovers("s1"))
        events = [e["event_type"] for e in self.dispatch.get_itinerary("p19")]
        self.assertIn("handover_initiated", events)
        self.assertIn("handover_completed", events)

    def test_handover_completes_into_waiting_when_target_full(self):
        self.check_in("p20", ["massage"])
        self.call("zm", "p20")
        self.arrive("p20", "p20")
        self.dispatch.register_zone(request_id="z-m2", actor_id="op1", site_id="s1",
                                    zone_id="zm2", name="推拿二区", service_type="massage",
                                    capacity=1)
        self.dispatch.assign_expert(request_id="ea-em", actor_id="op1",
                                    expert_id="em", zone_id="zm2")
        self.check_in("p21", ["massage"])
        view = self.dispatch.get_participant("p21")
        self.assertEqual("zm2", view["participant"]["current_zone_id"])
        self.call("zm2", "p21")
        self.arrive("p21", "p21")
        handover, _ = self.dispatch.initiate_handover(
            request_id="hi-p20", actor_id="op1", participant_id="p20",
            to_zone_id="zm2", reason="换岗")
        done, _ = self.dispatch.complete_handover(request_id="hc-p20", actor_id="op1",
                                                  handover_id=handover["handover_id"])
        self.assertEqual("waiting", done["status"])
        self.assertEqual(2, done["queue_number"])

    def test_handover_cancel_keeps_participant(self):
        self.check_in("p22", ["massage"])
        self.call("zm", "p22")
        self.arrive("p22", "p22")
        self.dispatch.register_zone(request_id="z-m2", actor_id="op1", site_id="s1",
                                    zone_id="zm2", name="推拿二区", service_type="massage",
                                    capacity=1)
        handover, _ = self.dispatch.initiate_handover(
            request_id="hi-p22", actor_id="op1", participant_id="p22",
            to_zone_id="zm2", reason="换岗")
        cancelled, _ = self.dispatch.cancel_handover(request_id="hx-p22", actor_id="op1",
                                                     handover_id=handover["handover_id"])
        self.assertEqual("cancelled", cancelled["status"])
        view = self.dispatch.get_participant("p22")
        self.assertEqual("serving", view["participant"]["status"])
        self.assertEqual("zm", view["participant"]["current_zone_id"])
        self.assertEqual([], self.dispatch.list_pending_handovers("s1"))

    def test_second_pending_handover_rejected(self):
        self.check_in("p23", ["massage"])
        self.call("zm", "p23")
        self.arrive("p23", "p23")
        self.dispatch.register_zone(request_id="z-m2", actor_id="op1", site_id="s1",
                                    zone_id="zm2", name="推拿二区", service_type="massage",
                                    capacity=1)
        self.dispatch.initiate_handover(request_id="hi-p23", actor_id="op1",
                                        participant_id="p23", to_zone_id="zm2", reason="换岗")
        with self.assertRaises(ConflictError):
            self.dispatch.initiate_handover(request_id="hi-p23b", actor_id="op1",
                                            participant_id="p23", to_zone_id="zm2", reason="再次")

    def test_handover_requires_same_service_type(self):
        self.check_in("p24", ["massage"])
        self.call("zm", "p24")
        self.arrive("p24", "p24")
        with self.assertRaises(ValidationError):
            self.dispatch.initiate_handover(request_id="hi-p24", actor_id="op1",
                                            participant_id="p24", to_zone_id="zl", reason="换岗")

    def test_recompute_keeps_viable_participants_in_place(self):
        self.check_in("p25", ["culture"])
        self.check_in("p26", ["massage"])
        rec, _ = self.dispatch.recompute_assignments(request_id="rc-2", actor_id="op1", site_id="s1")
        self.assertEqual([], rec["changes"])
        self.assertEqual(2, rec["examined"])
        view = self.dispatch.get_participant("p25")
        self.assertEqual(1, view["participant"]["queue_number"])

    def test_recompute_moves_after_personnel_change_and_records_version(self):
        self.check_in("p27", ["massage"])
        self.dispatch.set_expert_status(request_id="es-em", actor_id="op1",
                                        expert_id="em", status="off_duty")
        rec1, _ = self.dispatch.recompute_assignments(request_id="rc-3", actor_id="op1", site_id="s1")
        self.assertEqual([], rec1["changes"])
        self.dispatch.register_zone(request_id="z-m2", actor_id="op1", site_id="s1",
                                    zone_id="zm2", name="推拿二区", service_type="massage",
                                    capacity=1)
        self.dispatch.set_expert_status(request_id="es-em2", actor_id="op1",
                                        expert_id="em", status="on_duty")
        self.dispatch.assign_expert(request_id="ea-em", actor_id="op1",
                                    expert_id="em", zone_id="zm2")
        rec2, _ = self.dispatch.recompute_assignments(request_id="rc-4", actor_id="op1", site_id="s1")
        self.assertEqual(["p27"], [c["participant_id"] for c in rec2["changes"]])
        self.assertEqual("zm2", rec2["changes"][0]["to_zone_id"])
        view = self.dispatch.get_participant("p27")
        self.assertEqual("waiting", view["participant"]["status"])
        self.assertTrue(view["participant"]["screened"])
        self.assertEqual(rec2["based_on_version"], view["assignment"]["based_on_version"])
        self.assertEqual(view["state_version"], rec2["based_on_version"])
        events = [e for e in self.dispatch.get_itinerary("p27") if e["event_type"] == "reassigned"]
        self.assertEqual(rec2["based_on_version"], events[0]["based_on_version"])

    def test_complete_service_progresses_to_next_request_then_done(self):
        self.check_in("p28", ["massage", "culture"])
        self.call("zm", "p28")
        self.arrive("p28", "p28")
        progressed, _ = self.complete("p28", "p28")
        self.assertEqual("waiting", progressed["status"])
        self.assertEqual("zl", progressed["zone_id"])
        self.assertEqual(["massage"], progressed["completed"])
        self.call("zl", "p28b")
        self.arrive("p28", "p28b")
        done, _ = self.complete("p28", "p28b")
        self.assertEqual("done", done["status"])
        self.assertEqual(["massage", "culture"], done["completed"])

    def test_complete_service_without_viable_zone_goes_to_manual(self):
        self.check_in("p29", ["consultation", "massage"])
        self.call("zc", "p29")
        self.arrive("p29", "p29")
        self.dispatch.set_expert_status(request_id="es-em", actor_id="op1",
                                        expert_id="em", status="off_duty")
        decision, _ = self.complete("p29", "p29")
        self.assertEqual("manual_review", decision["route"])
        self.assertEqual("massage", decision["pending_request"])
        self.dispatch.set_expert_status(request_id="es-em2", actor_id="op1",
                                        expert_id="em", status="on_duty")
        resolved, _ = self.dispatch.resolve_manual_review(
            request_id="rm-p29", actor_id="op1", participant_id="p29")
        self.assertEqual("zm", resolved["zone_id"])

    def test_pressure_reports_load_and_version(self):
        decision, _ = self.check_in("p30", ["culture"])
        self.check_in("p31", ["culture"])
        self.call("zl", "p31")
        pressure = self.dispatch.get_zone_pressure("s1")
        self.assertEqual(decision["based_on_version"], pressure["state_version"])
        zones = {z["zone_id"]: z for z in pressure["zones"]}
        self.assertEqual(1, zones["zl"]["waiting"])
        self.assertEqual(1, zones["zl"]["called"])
        self.assertEqual(1, zones["zl"]["free_slots"])
        self.assertEqual(1, zones["zl"]["on_duty_experts"])
        self.assertEqual(0, zones["zm"]["occupied"])

    def test_auditor_cannot_write(self):
        with self.assertRaises(PermissionDenied):
            self.dispatch.check_in_participant(
                request_id="ci-p32", actor_id="au1", site_id="s1", participant_id="p32",
                alias="参与者p32", requests=["culture"])

    def test_itinerary_is_complete_and_ordered(self):
        self.check_in("p33", ["massage"])
        self.call("zm", "p33")
        self.arrive("p33", "p33")
        self.complete("p33", "p33")
        events = self.dispatch.get_itinerary("p33")
        self.assertEqual(
            ["checked_in", "assigned", "called", "service_started",
             "service_completed", "journey_completed"],
            [e["event_type"] for e in events])
        self.assertEqual(sorted(e["seq"] for e in events), [e["seq"] for e in events])

    def test_unknown_participant_raises_not_found(self):
        with self.assertRaises(NotFoundError):
            self.dispatch.get_participant("missing")


class RestartRecoveryTest(unittest.TestCase):
    def test_confirmed_flows_continue_after_restart(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "market.sqlite3"
            clock = StepClock(datetime(2026, 9, 26, 10, 0, tzinfo=timezone.utc))
            database = Database(path)
            foundation = DomainService(database, clock)
            dispatch = DispatchService(database, clock)
            foundation.register_organization(request_id="org", actor_id="bootstrap",
                                             organization_id="o1", name="活动机构")
            foundation.register_actor(request_id="admin", actor_id="bootstrap", new_actor_id="a1",
                                      display_name="管理员", role="admin", organization_id="o1")
            foundation.register_actor(request_id="op", actor_id="a1", new_actor_id="op1",
                                      display_name="协调员", role="operator", organization_id="o1")
            foundation.register_site(request_id="site", actor_id="op1", site_id="s1",
                                     organization_id="o1", name="夜市",
                                     timezone_name="Asia/Shanghai")
            dispatch.register_zone(request_id="z1", actor_id="op1", site_id="s1", zone_id="zc",
                                   name="义诊区", service_type="consultation", capacity=1)
            dispatch.register_zone(request_id="z2", actor_id="op1", site_id="s1", zone_id="zc2",
                                   name="义诊二区", service_type="consultation", capacity=1)
            dispatch.register_expert(request_id="e1", actor_id="op1", site_id="s1", expert_id="ec",
                                     name="医师", qualifications=["consultation"], zone_id="zc")
            first, _ = dispatch.check_in_participant(
                request_id="ci-p1", actor_id="op1", site_id="s1", participant_id="p1",
                alias="甲", requests=["consultation"])
            dispatch.check_in_participant(request_id="ci-p2", actor_id="op1", site_id="s1",
                                          participant_id="p2", alias="乙",
                                          requests=["consultation"])
            dispatch.call_next(request_id="cn1", actor_id="op1", zone_id="zc", ttl_seconds=300)
            dispatch.confirm_arrival(request_id="ca1", actor_id="op1", participant_id="p1")
            handover, _ = dispatch.initiate_handover(
                request_id="h1", actor_id="op1", participant_id="p1",
                to_zone_id="zc2", reason="专家换岗")
            database.close()

            clock.advance(30)
            reopened = Database(path)
            foundation2 = DomainService(reopened, clock)
            dispatch2 = DispatchService(reopened, clock)
            replay, replayed = dispatch2.check_in_participant(
                request_id="ci-p1", actor_id="op1", site_id="s1", participant_id="p1",
                alias="甲", requests=["consultation"])
            self.assertTrue(replayed)
            self.assertEqual(first, replay)
            pending = dispatch2.list_pending_handovers("s1")
            self.assertEqual(1, len(pending))
            done, _ = dispatch2.complete_handover(request_id="h2", actor_id="op1",
                                                  handover_id=handover["handover_id"])
            self.assertEqual("serving", done["status"])
            self.assertEqual("zc2", done["to_zone_id"])
            call, _ = dispatch2.call_next(request_id="cn2", actor_id="op1",
                                          zone_id="zc", ttl_seconds=300)
            self.assertEqual("p2", call["participant_id"])
            events = [e["event_type"] for e in dispatch2.get_itinerary("p1")]
            self.assertEqual(
                ["checked_in", "assigned", "called", "service_started",
                 "handover_initiated", "handover_completed"], events)
            pressure = dispatch2.get_zone_pressure("s1")
            zones = {z["zone_id"]: z for z in pressure["zones"]}
            self.assertEqual(1, zones["zc2"]["serving"])
            self.assertEqual(1, zones["zc"]["called"])
            valid, _ = foundation2.verify_audit()
            self.assertTrue(valid)
            reopened.close()


if __name__ == "__main__":
    unittest.main()
