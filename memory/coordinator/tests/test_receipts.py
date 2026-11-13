"""Durable CLI ownership proof; temporary files and mocked children only."""
import json
import os
from pathlib import Path
import tempfile
import threading
import time
import types
import unittest
from unittest.mock import Mock, patch

from client import AdmissionError, Client, RunReceipt, run_command


class ReceiptTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.path = Path(self.directory.name) / "receipt.json"
        self.receipt = RunReceipt(self.path)
        self.receipt.lock()
        self.addCleanup(self.receipt.close)
        self.receipt.begin("http://fixture.invalid", "request-a")
        self.client = Mock(url="http://fixture.invalid")
        self.client.cancel_unstarted.return_value = {"request_id": "request-a", "state": "cancelled"}

    def test_not_started_receipt_retries_exact_cleanup_without_polling(self):
        self.receipt.recover(self.client)
        self.client.cancel_unstarted.assert_called_once()
        self.assertEqual(self.client.cancel_unstarted.call_args.args[0], "request-a")
        self.client.call.assert_not_called()
        self.assertFalse(self.path.exists())

    def test_stopped_receipt_survives_lost_cleanup_response_then_recovers(self):
        self.receipt.save("stopped", lease_id="fixture-lease")
        self.client.cancel_unstarted.side_effect = AdmissionError("connection unavailable")
        with self.assertRaises(AdmissionError):
            self.receipt.recover(self.client)
        self.assertEqual(self.receipt.read()["phase"], "stopped")
        self.client.cancel_unstarted.side_effect = None
        self.receipt.recover(self.client)
        self.assertFalse(self.path.exists())

    def test_uncertain_work_never_cancelled_or_polled_even_after_restart(self):
        self.receipt.save("may_have_started", lease_id="fixture-lease")
        self.client.call.return_value = {"requests": [{"request_id": "request-a", "state": "blocked_orphan"}]}
        with self.assertRaises(AdmissionError):
            self.receipt.recover(self.client)
        self.client.call.assert_called_once_with("GET", "/v1/status")
        self.client.cancel_unstarted.assert_not_called()
        self.assertTrue(self.path.exists())

    def test_uncertain_receipt_only_clears_after_matching_terminal_server_proof(self):
        self.receipt.save("may_have_started")
        self.client.call.side_effect = [{"requests": []}, {"request_id": "request-a", "state": "cancelled"}]
        self.receipt.recover(self.client)
        self.client.cancel_unstarted.assert_not_called()
        self.assertFalse(self.path.exists())

    def test_wrong_server_or_wrong_terminal_identity_cannot_clear_receipt(self):
        self.client.url = "http://other.invalid"
        with self.assertRaises(AdmissionError):
            self.receipt.recover(self.client)
        self.client.cancel_unstarted.assert_not_called()
        self.client.url = "http://fixture.invalid"
        self.client.cancel_unstarted.return_value = {"request_id": "request-b", "state": "cancelled"}
        with self.assertRaises(AdmissionError):
            self.receipt.recover(self.client)
        self.assertTrue(self.path.exists())

    def test_second_invocation_and_nonprivate_directory_fail_closed(self):
        with self.assertRaises(AdmissionError):
            RunReceipt(self.path).lock()
        self.assertEqual(self.path.stat().st_mode & 0o777, 0o600)
        os.chmod(self.path.parent, 0o755)
        with self.assertRaises(AdmissionError):
            RunReceipt(self.path.parent / "other.json").lock()
        os.chmod(self.path.parent, 0o700)

    def test_receipt_parent_symlink_is_rejected_without_following_target(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            private = root / "private"
            private.mkdir(mode=0o700)
            link = root / "link"
            link.symlink_to(private, target_is_directory=True)
            receipt = RunReceipt(link / "receipt.json")
            with self.assertRaises(AdmissionError):
                receipt.lock()
            self.assertFalse((private / "receipt.json.lock").exists())

    def test_invalid_fifo_and_duplicate_json_fail_closed(self):
        self.path.unlink()
        os.mkfifo(self.path, 0o600)
        with self.assertRaises(AdmissionError):
            self.receipt.read()
        self.path.unlink()
        self.path.write_text('{"version":1,"version":1}')
        os.chmod(self.path, 0o600)
        with self.assertRaises(AdmissionError):
            self.receipt.read()

    def test_unknown_server_request_can_clear_only_proven_safe_phase(self):
        client = Client("http://fixture.invalid", "/not-read")
        client.call = Mock(side_effect=AdmissionError("unknown", status=404, server_error="unknown request"))
        self.receipt.recover(client)
        self.assertFalse(self.path.exists())
        client.call.assert_called_once_with("POST", "/v1/requests/request-a/cancel", {"lease_id": None, "stopped": True})


class ReceiptSupervisionTests(unittest.TestCase):
    def fixture(self, directory):
        path = Path(directory) / "receipt.json"
        client = Mock(url="http://fixture.invalid")
        lease = Mock()
        lease.info = {"run_deadline": time.time() + 60, "lease_id": "fixture-lease"}
        lease.lost = threading.Event()
        args = types.SimpleNamespace(job="fixture", peak_mib=128, run_timeout=60,
            native=False, wait_timeout=60, command=["never-run"], receipt_file=path)
        def acquire(*_args, **kwargs):
            record = json.loads(path.read_text())
            self.assertEqual(record["phase"], "not_started")
            self.assertEqual(record["request_id"], kwargs["request_id"])
            lease.info["request_id"] = kwargs["request_id"]
            return lease
        client.acquire.side_effect = acquire
        def release(**_kwargs):
            self.assertEqual(json.loads(path.read_text())["phase"], "stopped")
            return {"request_id": lease.info["request_id"], "state": "released"}
        lease.release.side_effect = release
        return path, client, lease, args

    def test_receipt_durable_before_post_spawn_and_release(self):
        with tempfile.TemporaryDirectory() as directory:
            path, client, lease, args = self.fixture(directory)
            proc = Mock(pid=123456, returncode=0)
            proc.poll.return_value = 0
            def spawn(*_args, **_kwargs):
                self.assertEqual(json.loads(path.read_text())["phase"], "may_have_started")
                return proc
            with patch("client.subprocess.Popen", side_effect=spawn), patch("client.process_group_exists", return_value=False):
                self.assertEqual(run_command(client, args), 0)
            self.assertFalse(path.exists())
            lease.release.assert_called_once_with(stopped=True)

    def test_uncertain_spawn_never_acknowledges_work_stopped(self):
        with tempfile.TemporaryDirectory() as directory:
            path, client, lease, args = self.fixture(directory)
            with patch("client.subprocess.Popen", side_effect=OSError("synthetic")), patch("client.sys.stderr"):
                with self.assertRaises(OSError):
                    run_command(client, args)
            self.assertEqual(json.loads(path.read_text())["phase"], "may_have_started")
            lease.release.assert_not_called()

    def test_lost_release_preserves_stopped_receipt(self):
        with tempfile.TemporaryDirectory() as directory:
            path, client, lease, args = self.fixture(directory)
            lease.release.side_effect = AdmissionError("connection unavailable")
            proc = Mock(pid=123456, returncode=0)
            proc.poll.return_value = 0
            with patch("client.subprocess.Popen", return_value=proc), patch("client.process_group_exists", return_value=False), patch("client.sys.stderr"):
                self.assertEqual(run_command(client, args), 0)
            self.assertEqual(json.loads(path.read_text())["phase"], "stopped")


if __name__ == "__main__":
    unittest.main()
