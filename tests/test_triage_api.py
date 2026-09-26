import unittest

from night_market_foundation.api import route
from night_market_foundation.service import DomainService
from night_market_foundation.storage import Database
from night_market_foundation.triage.service import TriageService


class TriageApiTest(unittest.TestCase):
    def setUp(self):
        self.database = Database()
        self.base = DomainService(self.database)
        self.triage = TriageService(self.database)
        self.base.register_organization(request_id="org", actor_id="bootstrap",
                                        organization_id="o1", name="夜市机构")
        self.base.register_actor(request_id="admin", actor_id="bootstrap", new_actor_id="op1",
                                 display_name="协调员", role="operator", organization_id="o1")
        self.base.register_site(request_id="site", actor_id="op1", site_id="s1",
                                organization_id="o1", name="夜市", timezone_name="Asia/Shanghai")
        self.headers = {"X-Actor-Id": "op1"}

    def tearDown(self):
        self.database.close()

    def dispatch(self, method, path, body=None, headers=None):
        return route(self.base, method, path, body or {}, headers or self.headers,
                     triage_service=self.triage)

    def test_zone_registration_and_pressure_query(self):
        status, payload = self.dispatch("POST", "/triage/zones", {
            "request_id": "z1", "site_id": "s1", "zone_id": "z-cons", "name": "义诊",
            "service_type": "consultation", "capacity": 5, "serving_limit": 2})
        self.assertEqual(201, status)
        self.assertEqual("z-cons", payload["resource_id"])
        status, payload = self.dispatch("GET", "/triage/zone-pressures?site_id=s1")
        self.assertEqual(200, status)
        self.assertEqual(1, len(payload["items"]))
        self.assertEqual("z-cons", payload["items"][0]["zone_id"])

    def test_replay_returns_200_and_same_decision(self):
        body = {"request_id": "z1", "site_id": "s1", "zone_id": "z-cons", "name": "义诊",
                "service_type": "consultation", "capacity": 5, "serving_limit": 2}
        first = self.dispatch("POST", "/triage/zones", body)
        second = self.dispatch("POST", "/triage/zones", body)
        self.assertEqual(201, first[0])
        self.assertEqual(200, second[0])
        self.assertTrue(second[1]["replayed"])
        self.assertEqual(first[1]["decision"], second[1]["decision"])

    def test_same_request_id_different_payload_conflicts(self):
        body1 = {"request_id": "z1", "site_id": "s1", "zone_id": "z-cons", "name": "义诊",
                 "service_type": "consultation", "capacity": 5, "serving_limit": 2}
        self.dispatch("POST", "/triage/zones", body1)
        body2 = dict(body1, capacity=9)
        status, payload = self.dispatch("POST", "/triage/zones", body2)
        self.assertEqual(409, status)
        self.assertEqual("conflict", payload["error"])

    def test_unknown_triage_route_404(self):
        status, payload = self.dispatch("GET", "/triage/nope")
        self.assertEqual(404, status)
        self.assertEqual("route_not_found", payload["error"])

    def test_triage_routes_require_actor(self):
        status, payload = self.dispatch("GET", "/triage/zone-pressures?site_id=s1",
                                        headers={"X-Actor-Id": ""})
        self.assertEqual(404, status)

    def test_recover_endpoint(self):
        status, payload = self.dispatch("POST", "/triage/recover", {})
        self.assertEqual(200, status)
        self.assertIn("active_tickets", payload)
        self.assertIn("open_handshakes", payload)


if __name__ == "__main__":
    unittest.main()
