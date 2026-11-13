"""Owner-poll admission regressions. Fake clocks/health; no real workloads."""
import unittest
from unittest.mock import patch

from client import AdmissionError, Client
from coordinator import APIError, Coordinator, dispatch
from test_coordinator import Clock, Health, MemoryStore


class QueueLivenessTests(unittest.TestCase):
    def setUp(self):
        self.clock, self.health, self.store = Clock(), Health(), MemoryStore()
        self.engine = Coordinator(self.store, self.health, {"healthy_seconds": 0}, clock=self.clock)

    def submit(self, name="request-a", peak=1024):
        return self.engine.submit(dict(request_id=name, job=name, peak_mib=peak,
                                       run_timeout_s=100, wait_timeout_s=1000))

    def test_post_and_status_never_grant_without_owner_poll(self):
        self.assertEqual(self.submit()["state"], "queued")
        self.assertEqual(self.submit()["state"], "queued")
        self.assertIsNone(self.engine.status()["active"])
        self.engine.tick()
        self.assertIsNone(self.engine.state["active"])
        self.assertEqual(self.engine.get("request-a")["state"], "granted")

    def test_abandoned_queue_expires_without_ever_creating_active_orphan(self):
        self.engine.config["healthy_seconds"] = 60
        self.submit()
        for _ in range(20):
            self.clock.advance(5)
            self.engine.tick()
        self.assertEqual(self.engine.get("request-a")["state"], "expired")
        self.assertNotIn("granted_at", self.engine.state["requests"]["request-a"])
        self.assertIsNone(self.engine.state["active"])

    def test_abandoned_head_expires_and_live_older_client_can_poll_into_slot(self):
        self.health.pressure = 2
        self.submit()
        self.submit("request-b")
        for _ in range(7):
            self.clock.advance(5)
            self.assertEqual(self.engine.get("request-b")["state"], "queued")
        self.assertEqual(self.engine.get("request-a")["state"], "expired")
        self.health.pressure = 1
        self.assertEqual(self.engine.get("request-b")["state"], "granted")

    def test_live_fifo_head_is_not_bypassed_by_small_job(self):
        self.health.available = 2048
        self.submit(peak=3072)
        self.submit("request-b", peak=512)
        for _ in range(12):
            self.clock.advance(5)
            self.assertEqual(self.engine.get("request-a")["state"], "queued")
            self.assertEqual(self.engine.get("request-b")["state"], "queued")
        self.health.available = 4096
        self.assertEqual(self.engine.get("request-b")["state"], "queued")
        self.assertEqual(self.engine.get("request-a")["state"], "granted")

    def test_restart_requires_new_owner_poll_and_expires_absent_queued_owner(self):
        self.health.pressure = 2
        self.submit()
        self.clock.advance(300)
        restarted = Coordinator(self.store, self.health, {"healthy_seconds": 0}, clock=self.clock)
        self.health.pressure = 1
        restarted.tick()
        self.assertIsNone(restarted.state["active"])
        for _ in range(7):
            self.clock.advance(5)
            restarted.tick()
        self.assertEqual(restarted.get("request-a")["state"], "expired")

    def test_active_orphan_is_never_expired_by_queue_liveness(self):
        self.submit()
        first = self.engine.get("request-a")
        self.assertEqual(first["state"], "granted")
        self.submit("request-b")
        for _ in range(100):
            self.clock.advance(5)
            self.engine.get("request-b")
        self.assertEqual(self.engine.state["active"], "request-a")
        self.assertEqual(self.engine.get("request-a")["state"], "blocked_orphan")
        self.assertEqual(self.engine.get("request-b")["state"], "queued")


class AcquisitionCleanupTests(unittest.TestCase):
    def test_lost_post_response_cleanup_does_not_poll_and_accidentally_grant(self):
        engine = Coordinator(MemoryStore(), Health(), {"healthy_seconds": 0})
        client = Client("http://fixture.invalid", "/fixture-token-never-read")
        calls = []

        def call(method, path, body=None):
            calls.append((method, path))
            result = dispatch(engine, method, path, body)
            if method == "POST" and path == "/v1/requests":
                raise AdmissionError("synthetic response loss")
            return result

        client.call = call
        with self.assertRaises(AdmissionError):
            client.acquire("fixture", 128, 30)
        request = next(iter(engine.state["requests"].values()))
        self.assertEqual(request["state"], "cancelled")
        self.assertNotIn("granted_at", request)
        self.assertFalse(any(method == "GET" for method, _ in calls))

    def test_lost_grant_response_cleanup_only_releases_never_started_own_work(self):
        engine = Coordinator(MemoryStore(), Health(), {"healthy_seconds": 0})
        client = Client("http://fixture.invalid", "/fixture-token-never-read")
        dropped = False

        def call(method, path, body=None):
            nonlocal dropped
            try:
                result = dispatch(engine, method, path, body)
            except APIError as error:
                raise AdmissionError("HTTP " + str(error.status), status=error.status,
                                     server_error=error.message) from None
            if method == "GET" and result["state"] == "granted" and not dropped:
                dropped = True
                raise AdmissionError("synthetic lost grant response")
            return result

        client.call = call
        with patch("client.time.sleep"), patch("client.subprocess.Popen") as spawn:
            with self.assertRaises(AdmissionError):
                client.acquire("fixture", 128, 30)
            spawn.assert_not_called()
        request = next(iter(engine.state["requests"].values()))
        self.assertTrue(dropped)
        self.assertEqual(request["state"], "cancelled")
        self.assertIsNone(engine.state["active"])


if __name__ == "__main__":
    unittest.main()
