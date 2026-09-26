import unittest

from night_market_dispatch.acceptance import run


class DispatchAcceptanceTest(unittest.TestCase):
    def test_offline_acceptance(self):
        result = run()
        self.assertEqual("ok", result["status"])
        self.assertTrue(result["audit_valid"])
        self.assertEqual(1, result["recomputed"])
        self.assertEqual(1, result["pending_before_restart"])
        self.assertEqual(1, result["pending_after_restart"])
        self.assertEqual("serving", result["handover_after_restart"])
        self.assertEqual("manual_review", result["manual_review_route"])


if __name__ == "__main__":
    unittest.main()
