import unittest
from datetime import datetime, timedelta, timezone

from night_market_foundation.service import DomainService
from night_market_foundation.storage import Database
from night_market_dispatch.api import route
from night_market_dispatch.service import DispatchService


class StepClock:
    def __init__(self, value):
        self.value = value

    def now(self):
        return self.value

    def advance(self, seconds):
        self.value = self.value + timedelta(seconds=seconds)


class DispatchApiTest(unittest.TestCase):
    def setUp(self):
        self.database = Database()
        self.clock = StepClock(datetime(2026, 9, 26, 10, 0, tzinfo=timezone.utc))
        self.foundation = DomainService(self.database, self.clock)
        self.dispatch = DispatchService(self.database, self.clock)
        self.foundation.register_organization(request_id="org", actor_id="bootstrap",
                                              organization_id="o1", name="活动机构")
        self.foundation.register_actor(request_id="admin", actor_id="bootstrap", new_actor_id="a1",
                                       display_name="管理员", role="admin", organization_id="o1")
        self.foundation.register_actor(request_id="op", actor_id="a1", new_actor_id="op1",
                                       display_name="协调员", role="operator", organization_id="o1")
        self.foundation.register_site(request_id="site", actor_id="op1", site_id="s1",
                                      organization_id="o1", name="夜市",
                                      timezone_name="Asia/Shanghai")
        self.dispatch.register_zone(request_id="z1", actor_id="op1", site_id="s1", zone_id="zl",
                                    name="讲解区", service_type="culture", capacity=2)
        self.dispatch.register_expert(request_id="e1", actor_id="op1", site_id="s1",
                                      expert_id="el", name="讲解员",
                                      qualifications=["culture"], zone_id="zl")

    def tearDown(self):
        self.database.close()

    def post(self, path, body, actor="op1"):
        return route(self.dispatch, self.foundation, "POST", path, body, {"X-Actor-Id": actor})

    def test_check_in_endpoint_and_replay(self):
        body = {"request_id": "r1", "site_id": "s1", "participant_id": "p1",
                "alias": "市民甲", "requests": ["culture"]}
        status, payload = self.post("/dispatch/check-in", body)
        self.assertEqual(201, status)
        self.assertEqual("zl", payload["zone_id"])
        self.assertFalse(payload["replayed"])
        status2, payload2 = self.post("/dispatch/check-in", body)
        self.assertEqual(200, status2)
        self.assertTrue(payload2["replayed"])
        self.assertEqual(payload["zone_id"], payload2["zone_id"])

    def test_conflicting_request_id_returns_409(self):
        body = {"request_id": "r2", "site_id": "s1", "participant_id": "p2",
                "alias": "市民乙", "requests": ["culture"]}
        self.post("/dispatch/check-in", body)
        changed = dict(body, participant_id="p3")
        status, payload = self.post("/dispatch/check-in", changed)
        self.assertEqual(409, status)
        self.assertEqual("conflict", payload["error"])

    def test_pressure_endpoint(self):
        status, payload = route(self.dispatch, self.foundation, "GET",
                                "/dispatch/pressure?site_id=s1", None)
        self.assertEqual(200, status)
        self.assertEqual("s1", payload["site_id"])
        self.assertEqual(1, len(payload["zones"]))
        self.assertEqual("zl", payload["zones"][0]["zone_id"])

    def test_pressure_endpoint_requires_site_id(self):
        status, payload = route(self.dispatch, self.foundation, "GET", "/dispatch/pressure", None)
        self.assertEqual(400, status)
        self.assertEqual("validation_error", payload["error"])

    def test_itinerary_and_participant_endpoints(self):
        self.post("/dispatch/check-in", {"request_id": "r3", "site_id": "s1",
                                         "participant_id": "p4", "alias": "市民丁",
                                         "requests": ["culture"]})
        status, payload = route(self.dispatch, self.foundation, "GET",
                                "/dispatch/itinerary?participant_id=p4", None)
        self.assertEqual(200, status)
        self.assertEqual(["checked_in", "assigned"],
                         [item["event_type"] for item in payload["items"]])
        status2, payload2 = route(self.dispatch, self.foundation, "GET",
                                  "/dispatch/participant?participant_id=p4", None)
        self.assertEqual(200, status2)
        self.assertEqual("waiting", payload2["participant"]["status"])
        self.assertEqual(payload2["state_version"],
                         payload2["assignment"]["based_on_version"])

    def test_pending_handover_endpoint(self):
        status, payload = route(self.dispatch, self.foundation, "GET",
                                "/dispatch/handovers/pending?site_id=s1", None)
        self.assertEqual(200, status)
        self.assertEqual([], payload["items"])

    def test_foundation_health_still_available(self):
        status, payload = route(self.dispatch, self.foundation, "GET", "/health", None)
        self.assertEqual(200, status)
        self.assertEqual("ok", payload["status"])

    def test_unknown_route_returns_404(self):
        status, payload = route(self.dispatch, self.foundation, "GET", "/dispatch/unknown", None)
        self.assertEqual(404, status)
        self.assertEqual("route_not_found", payload["error"])

    def test_missing_actor_returns_404(self):
        status, payload = self.post("/dispatch/check-in",
                                    {"request_id": "r5", "site_id": "s1",
                                     "participant_id": "p6", "alias": "市民己",
                                     "requests": ["culture"]}, actor="")
        self.assertEqual(404, status)


if __name__ == "__main__":
    unittest.main()
