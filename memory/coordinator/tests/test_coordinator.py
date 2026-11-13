import copy
import json
import pathlib
import tempfile
import threading
import time
import types
import unittest
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from http.server import ThreadingHTTPServer
from unittest.mock import Mock, patch

from coordinator import APIError, Coordinator, HealthProbe, StateFile, dispatch, exclusive_state_lock, handler_class, maintain
from bridge import FileBridge, write_message
from client import Client, AdmissionError, run_command


class Clock:
    def __init__(self):
        self.now = 1000.0
    def __call__(self):
        return self.now
    def advance(self, seconds):
        self.now += seconds


class MemoryStore:
    def __init__(self):
        self.value = {"version": 1, "requests": {}, "queue": [], "active": None}
        self.fail = False
    def load(self):
        return copy.deepcopy(self.value)
    def save(self, value):
        if self.fail:
            raise OSError("simulated full disk")
        self.value = copy.deepcopy(value)


class Health:
    def __init__(self):
        self.pressure = 1
        self.available = 8192
        self.swap_in = self.swap_out = 0
        self.maintenance = False
    def get(self):
        return {"pressure_level": self.pressure, "guest_available_mib": self.available,
                "swap_in_mib_s": self.swap_in, "swap_out_mib_s": self.swap_out,
                "maintenance_active": self.maintenance}


class AdmissionTests(unittest.TestCase):
    def setUp(self):
        self.clock, self.store, self.health = Clock(), MemoryStore(), Health()
        self.engine = Coordinator(self.store, self.health, clock=self.clock)

    def request(self, name="request-a", **overrides):
        body = dict(request_id=name, job=name, peak_mib=3072, guest=True,
                    wait_timeout_s=1000, run_timeout_s=100)
        body.update(overrides)
        return self.engine.submit(body)

    def healthy(self, seconds=60):
        for _ in range(int(seconds / 5)):
            self.clock.advance(5)
            self.engine.tick()
            # Simulate real queued clients continuing their per-request polls.
            for key in list(self.engine.state["queue"]):
                self.engine.get(key)

    def grant(self):
        self.request()
        self.healthy()
        return self.engine.get("request-a")

    def test_stable_health_and_guest_peak_plus_reserve(self):
        self.health.available = 3583
        self.request()
        self.healthy()
        self.assertEqual(self.engine.get("request-a")["state"], "queued")
        self.health.available = 3584
        self.assertEqual(self.engine.get("request-a")["state"], "granted")

    def test_pressure_or_unknown_resets_hysteresis(self):
        self.request()
        self.healthy(55)
        self.health.pressure = None
        self.engine.tick()
        self.health.pressure = 1
        self.engine.tick()
        self.healthy(55)
        self.assertEqual(self.engine.get("request-a")["state"], "queued")
        self.healthy(5)
        self.assertEqual(self.engine.get("request-a")["state"], "granted")

    def test_long_unobserved_gap_is_not_healthy_evidence(self):
        self.request()
        self.clock.advance(100)
        self.assertEqual(self.engine.get("request-a")["state"], "queued")

    def test_periodic_observation_allows_short_wait_after_idle_healthy_window(self):
        clock = self.clock
        class StopAfterWindow:
            steps = 0
            def is_set(self):
                return self.steps >= 13
            def wait(self, seconds):
                clock.advance(seconds)
                self.steps += 1
        maintain(self.engine, threading.Lock(), StopAfterWindow())
        self.assertEqual(self.request(wait_timeout_s=5)["state"], "queued")
        self.assertEqual(self.engine.get("request-a")["state"], "granted")

    def test_swap_burst_resets_stable_window_and_unknown_blocks(self):
        self.request()
        self.healthy(55)
        self.health.swap_out = 2
        self.engine.tick()
        self.health.swap_out = None
        self.healthy()
        self.assertEqual(self.engine.get("request-a")["state"], "queued")
        self.health.swap_out = 0.5
        self.engine.tick()
        self.healthy(55)
        self.assertEqual(self.engine.get("request-a")["state"], "queued")
        self.healthy(5)
        self.assertEqual(self.engine.get("request-a")["state"], "granted")

    def test_maintenance_pauses_new_grants_without_revoking_active_owner(self):
        first = self.grant()
        self.request("request-b")
        self.health.maintenance = True
        self.engine.tick()
        self.assertIsNone(self.engine.healthy_since)
        self.assertEqual(self.engine.heartbeat("request-a", {"lease_id": first["lease_id"]})["state"], "granted")
        self.engine.finish("request-a", {"lease_id": first["lease_id"], "stopped": True})
        self.assertEqual(self.engine.get("request-b")["state"], "queued")
        self.assertIn("maintenance", self.engine.get("request-b")["reason"])
        self.health.maintenance = False
        self.engine.tick()
        self.healthy(55)
        self.assertEqual(self.engine.get("request-b")["state"], "queued")
        self.healthy(5)
        self.assertEqual(self.engine.get("request-b")["state"], "granted")

    def test_native_ignores_missing_guest_probe_but_not_pressure(self):
        self.health.available = None
        self.health.pressure = 2
        self.request(guest=False)
        self.healthy()
        self.assertEqual(self.engine.get("request-a")["state"], "queued")
        self.health.pressure = 1
        self.engine.tick()
        self.healthy()
        self.assertEqual(self.engine.get("request-a")["state"], "granted")

    def test_fifo_does_not_skip_large_older_job(self):
        self.health.available = 2048
        self.request()
        self.request("request-b", peak_mib=512)
        self.healthy()
        self.assertIsNone(self.engine.state["active"])
        self.health.available = 4096
        self.engine.tick()
        self.assertIsNone(self.engine.state["active"])
        self.engine.get("request-a")
        self.assertEqual(self.engine.state["active"], "request-a")

    def test_one_slot_then_release_admits_next(self):
        first = self.grant()
        self.request("request-b")
        self.assertEqual(self.engine.get("request-b")["state"], "queued")
        self.engine.finish("request-a", {"lease_id": first["lease_id"], "stopped": True})
        self.assertEqual(self.engine.get("request-b")["state"], "granted")

    def test_heartbeat_expiry_blocks_instead_of_reassigning(self):
        first = self.grant()
        self.request("request-b")
        self.clock.advance(31)
        self.assertEqual(self.engine.get("request-b")["state"], "queued")
        self.assertEqual(self.engine.get("request-a")["state"], "blocked_orphan")
        self.health.pressure = 2
        self.assertEqual(self.engine.heartbeat("request-a", {"lease_id": first["lease_id"]})["state"], "granted")

    def test_runtime_deadline_cannot_be_renewed(self):
        first = self.grant()
        self.clock.advance(101)
        self.assertEqual(self.engine.heartbeat("request-a", {"lease_id": first["lease_id"]})["state"], "blocked_orphan")
        self.assertEqual(self.engine.state["active"], "request-a")

    def test_restart_retains_active_and_queue(self):
        first = self.grant()
        self.request("request-b")
        restarted = Coordinator(self.store, self.health, clock=self.clock)
        self.assertEqual(restarted.get("request-a")["state"], "blocked_orphan")
        self.assertEqual(restarted.get("request-b")["state"], "queued")
        self.assertEqual(restarted.heartbeat("request-a", {"lease_id": first["lease_id"]})["state"], "granted")

    def test_release_requires_identity_and_actual_stop(self):
        first = self.grant()
        for body in [{"stopped": True}, {"lease_id": first["lease_id"]}, {"lease_id": "wrong", "stopped": True}]:
            with self.assertRaises(APIError):
                self.engine.finish("request-a", body)
        self.assertEqual(self.engine.state["active"], "request-a")

    def test_cancel_raced_grant_requires_lease_then_is_idempotent(self):
        first = self.grant()
        with self.assertRaises(APIError):
            self.engine.finish("request-a", {"stopped": True}, cancel=True)
        body = {"lease_id": first["lease_id"], "stopped": True}
        self.assertEqual(self.engine.finish("request-a", body, cancel=True)["state"], "cancelled")
        self.assertEqual(self.engine.finish("request-a", body, cancel=True)["state"], "cancelled")

    def test_queue_deadline_and_cancel(self):
        self.health.pressure = 2
        self.request(wait_timeout_s=10)
        self.request("request-b")
        self.engine.finish("request-b", {}, cancel=True)
        self.clock.advance(10)
        self.assertEqual(self.engine.get("request-a")["state"], "expired")
        self.assertEqual(self.engine.state["queue"], [])

    def test_bounded_queue_and_declared_peak(self):
        self.health.pressure = 2
        self.engine.config["max_queue"] = 1
        self.request()
        with self.assertRaises(APIError) as ctx:
            self.request("request-b")
        self.assertEqual(ctx.exception.status, 429)
        with self.assertRaises(APIError):
            self.request("request-c", peak_mib=3073)

    def test_submission_retry_is_idempotent_and_different_work_rejected(self):
        self.request()
        self.request()
        self.assertEqual(self.engine.state["queue"], ["request-a"])
        with self.assertRaises(APIError) as ctx:
            self.request(peak_mib=1024)
        self.assertEqual(ctx.exception.status, 409)

    def test_failed_persistence_poison_prevents_later_unpersisted_grant(self):
        self.request()
        self.healthy(55)
        self.store.fail = True
        self.clock.advance(5)
        with self.assertRaises(OSError):
            self.engine.get("request-a")
        self.store.fail = False
        with self.assertRaises(APIError) as ctx:
            self.engine.get("request-a")
        self.assertEqual(ctx.exception.status, 503)

    def test_corrupt_state_refuses_startup(self):
        self.store.value = {"version": 1, "active": "missing", "requests": {}, "queue": []}
        with self.assertRaises(ValueError):
            Coordinator(self.store, self.health)

    def test_state_file_permissions_atomic_and_missing_refuses(self):
        with tempfile.TemporaryDirectory() as folder:
            store = StateFile(pathlib.Path(folder) / "state.json")
            with self.assertRaises(FileNotFoundError):
                store.load()
            store.save(self.engine.state)
            self.assertEqual(store.load(), self.engine.state)
            self.assertEqual(store.path.stat().st_mode & 0o777, 0o600)
            with exclusive_state_lock(store.path):
                with self.assertRaises(BlockingIOError):
                    with exclusive_state_lock(store.path):
                        pass

    def test_probe_is_cached_and_fails_closed(self):
        calls = []
        def runner(args, **kwargs):
            calls.append(args)
            if "sysctl" in args[0]:
                return "1"
            if "vm_stat" in args[0]:
                return "Mach Virtual Memory Statistics: (page size of 16384 bytes)\nSwapins: 320.\nSwapouts: 640.\n"
            return "MemAvailable: 4194304 kB\n"
        probe = HealthProbe({"probe_container": "test-container"}, self.clock, runner, monotonic_clock=self.clock)
        self.assertEqual(probe.get()["guest_available_mib"], 4096)
        self.assertIsNone(probe.get()["swap_in_mib_s"])
        probe.get()
        self.assertEqual(len(calls), 3)
        self.clock.advance(5)
        self.assertEqual(probe.get()["swap_in_mib_s"], 0)
        self.assertEqual(len(calls), 5)
        self.clock.advance(10)
        probe.get()
        self.assertEqual(len(calls), 8)
        def fail(*args, **kwargs):
            raise OSError()
        bad = HealthProbe({"probe_container": "test-container"}, self.clock, fail, monotonic_clock=self.clock)
        self.assertEqual(bad.get(), {"pressure_level": None, "guest_available_mib": None,
                                    "swap_in_mib_s": None, "swap_out_mib_s": None,
                                    "maintenance_active": False, "host_observed_mono": self.clock(),
                                    "host_interval_seconds": None, "guest_observed_mono": self.clock(),
                                    "native_control_guard_supported": True, "native_control_guard_active": False,
                                    "native_control_guard_file": None, "native_control_guard_digest": None,
                                    "native_control_guard_identity": None})

    def test_maintenance_file_ttl_missing_invalid_and_unreadable(self):
        with tempfile.TemporaryDirectory() as folder:
            path = pathlib.Path(folder) / "maintenance-until"
            probe = HealthProbe({"maintenance_file": str(path)}, self.clock)
            self.assertFalse(probe._maintenance_active(self.clock()))
            path.write_text(str(self.clock() + 60))
            self.assertTrue(probe._maintenance_active(self.clock()))
            self.clock.advance(60)
            self.assertFalse(probe._maintenance_active(self.clock()))
            for invalid in ("", "not an epoch", "nan", "inf", "-1", "0", "1" * 129):
                path.write_text(invalid)
                self.assertTrue(probe._maintenance_active(self.clock()), invalid)
            with patch("coordinator.pathlib.Path.open", side_effect=PermissionError()):
                self.assertTrue(probe._maintenance_active(self.clock()))

    def test_swap_rate_uses_page_size_and_counter_reset_is_unknown(self):
        counts = [0, 0]
        def runner(args, **kwargs):
            if "sysctl" in args[0]:
                return "1"
            if "vm_stat" in args[0]:
                return f"Mach Virtual Memory Statistics: (page size of 16384 bytes)\nSwapins: {counts[0]}.\nSwapouts: {counts[1]}.\n"
            return "MemAvailable: 4194304 kB\n"
        probe = HealthProbe({"probe_container": "test-container"}, self.clock, runner, monotonic_clock=self.clock)
        probe.get()
        self.clock.advance(5)
        counts[:] = [320, 640]
        value = probe.get()
        self.assertEqual(value["swap_in_mib_s"], 1)
        self.assertEqual(value["swap_out_mib_s"], 2)
        self.clock.advance(5)
        counts[:] = [0, 0]
        self.assertIsNone(probe.get()["swap_in_mib_s"])


class HTTPTests(unittest.TestCase):
    def test_authenticated_parallel_requests_receive_only_one_grant(self):
        with tempfile.TemporaryDirectory() as folder:
            token_file = pathlib.Path(folder) / "token"
            token_file.write_text("temporary-test-token-not-a-deployment-secret")
            engine = Coordinator(MemoryStore(), Health(), {"healthy_seconds": 0})
            server = ThreadingHTTPServer(("127.0.0.1", 0), handler_class(engine, token_file.read_text()))
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                url = f"http://127.0.0.1:{server.server_port}"
                with self.assertRaises(urllib.error.HTTPError) as ctx:
                    urllib.request.urlopen(url + "/v1/status")
                self.assertEqual(ctx.exception.code, 401)
                ctx.exception.close()
                def submit(i):
                    return Client(url, token_file).call("POST", "/v1/requests", dict(
                        request_id=f"request-{i}", job="test", peak_mib=1024, run_timeout_s=60))
                with ThreadPoolExecutor(max_workers=8) as pool:
                    results = list(pool.map(submit, range(8)))
                self.assertTrue(all(r["state"] == "queued" for r in results))
                with ThreadPoolExecutor(max_workers=8) as pool:
                    results = list(pool.map(lambda r: Client(url, token_file).call("GET", "/v1/requests/" + r["request_id"]), results))
                self.assertEqual(sum(r["state"] == "granted" for r in results), 1)
                self.assertEqual(sum(r["state"] == "queued" for r in results), 7)
                active = next(r for r in results if r["state"] == "granted")
                with self.assertRaises(AdmissionError):
                    Client(url, token_file).call("POST", f"/v1/requests/{active['request_id']}/release", {"stopped": True})
            finally:
                server.shutdown()
                server.server_close()
                thread.join()


class SupervisionTests(unittest.TestCase):
    def fixture(self):
        order = []
        lease = Mock()
        lease.info = {"run_deadline": time.time() + 60}
        lease.lost = threading.Event()
        lease.release.side_effect = lambda **kw: order.append("release")
        client = Mock()
        client.acquire.return_value = lease
        args = types.SimpleNamespace(job="fixture", peak_mib=128, run_timeout=60,
            native=False, wait_timeout=60, command=["unused-test-command"])
        process = Mock(pid=123456, returncode=0)
        process.poll.return_value = None
        return order, lease, client, args, process

    def test_heartbeat_loss_stops_group_before_release(self):
        order, lease, client, args, process = self.fixture()
        lease.lost.set()
        # Loss after spawn, rather than the initial before-spawn guard.
        lease.lost.is_set = Mock(side_effect=[False, True])
        with patch("client.subprocess.Popen", return_value=process), \
             patch("client.stop_group", side_effect=lambda p: order.append("stop-group") or True):
            self.assertEqual(run_command(client, args), 124)
        self.assertEqual(order, ["stop-group", "release"])

    def test_runtime_deadline_stops_group_before_release(self):
        order, lease, client, args, process = self.fixture()
        lease.info["run_deadline"] = 1001
        with patch("client.subprocess.Popen", return_value=process), \
             patch("client.time.time", return_value=1000), \
             patch("client.time.monotonic", side_effect=[0, 0, 0, 2, 2]), \
             patch("client.stop_group", side_effect=lambda p: order.append("stop-group") or True):
            self.assertEqual(run_command(client, args), 124)
        self.assertEqual(order, ["stop-group", "release"])

    def test_sigterm_stops_group_before_release(self):
        order, lease, client, args, process = self.fixture()
        handlers = {}
        def register(sig, handler):
            old = handlers.get(sig)
            handlers[sig] = handler
            return old
        def launch(*_args, **_kwargs):
            import signal
            handlers[signal.SIGTERM]()
            return process
        with patch("client.signal.signal", side_effect=register), \
             patch("client.subprocess.Popen", side_effect=launch), \
             patch("client.stop_group", side_effect=lambda p: order.append("stop-group") or True):
            self.assertEqual(run_command(client, args), 130)
        self.assertEqual(order, ["stop-group", "release"])

    def test_unstoppable_group_never_releases_slot(self):
        order, lease, client, args, process = self.fixture()
        lease.lost.is_set = Mock(side_effect=[False, True])
        with patch("client.subprocess.Popen", return_value=process), \
             patch("client.stop_group", return_value=False), patch("client.sys.stderr"):
            self.assertEqual(run_command(client, args), 124)
        self.assertEqual(order, [])

    def test_parent_exit_with_remaining_children_stops_group_before_release(self):
        order, lease, client, args, process = self.fixture()
        process.poll.return_value = 0
        with patch("client.subprocess.Popen", return_value=process), \
             patch("client.process_group_exists", return_value=True), \
             patch("client.stop_group", side_effect=lambda p: order.append("stop-group") or True):
            self.assertEqual(run_command(client, args), 0)
        self.assertEqual(order, ["stop-group", "release"])


class FileBridgeTests(unittest.TestCase):
    def test_file_client_shares_global_slot_and_needs_no_network(self):
        with tempfile.TemporaryDirectory() as folder:
            directory = pathlib.Path(folder)
            token_file = directory / "token"
            token = "temporary-bridge-test-token-no-real-credential"
            token_file.write_text(token)
            engine = Coordinator(MemoryStore(), Health(), {"healthy_seconds": 0})
            bridge = FileBridge(directory, token, lambda m, p, b: dispatch(engine, m, p, b), threading.Lock())
            stop = threading.Event()
            thread = threading.Thread(target=bridge.serve, args=(stop,), daemon=True)
            thread.start()
            try:
                client = Client(directory.as_uri(), token_file, timeout=2)
                with patch("client.urllib.request.build_opener", side_effect=AssertionError("network forbidden")):
                    first = client.call("POST", "/v1/requests", dict(request_id="request-a", job="fixture", peak_mib=1024, run_timeout_s=60))
                    self.assertEqual(first["state"], "queued")
                    first = client.call("GET", "/v1/requests/request-a")
                    second = client.call("POST", "/v1/requests", dict(request_id="request-b", job="fixture", peak_mib=1024, run_timeout_s=60))
                    self.assertEqual(first["state"], "granted")
                    self.assertEqual(second["state"], "queued")
                    client.call("POST", "/v1/requests/request-a/release", {"lease_id": first["lease_id"], "stopped": True})
                    self.assertEqual(client.call("GET", "/v1/requests/request-b")["state"], "granted")
            finally:
                stop.set()
                thread.join()
            self.assertEqual(list(directory.glob("*.request.json")), [])
            self.assertEqual(list(directory.glob("*.response.json")), [])

    def test_bad_signature_and_symlink_never_dispatch(self):
        with tempfile.TemporaryDirectory() as folder:
            directory = pathlib.Path(folder)
            callback = Mock()
            bridge = FileBridge(directory, "right-token", callback, threading.Lock())
            nonce = "a" * 32
            write_message(directory / f"{nonce}.request.json", dict(nonce=nonce, created_at=time.time(),
                          method="GET", path="/v1/status"), "wrong-token")
            bridge.poll_once()
            callback.assert_not_called()
            outside = directory / "private-state"
            outside.write_text("must remain unchanged")
            (directory / f"{nonce}.request.json").symlink_to(outside)
            bridge.poll_once()
            callback.assert_not_called()
            self.assertEqual(outside.read_text(), "must remain unchanged")


if __name__ == "__main__":
    unittest.main()
