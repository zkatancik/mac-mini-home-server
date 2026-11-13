"""Cleanup must not turn transport/auth failures into owner-poll grants."""
import copy
from pathlib import Path
import tempfile
import threading
import unittest
from unittest.mock import Mock, patch

from client import AdmissionError, Client, RunReceipt
from coordinator import APIError, Coordinator, dispatch
from test_coordinator import Clock, Health, MemoryStore


class CleanupStatusTests(unittest.TestCase):
    def client(self):
        return Client('http://fixture.invalid', '/fixture-token-never-read')

    def test_cancellation_during_wait_cannot_poll_claim_before_failed_cleanup(self):
        clock = Clock()
        engine = Coordinator(MemoryStore(), Health(), {'healthy_seconds': 0},
                             clock=clock, monotonic_clock=clock)
        class CancelDuringWait(threading.Event):
            def wait(self, _timeout=None):
                self.set()
                return True
        cancelled = CancelDuringWait()
        client, calls = self.client(), []
        def call(method, path, body=None):
            calls.append((method, path))
            if path.endswith('/cancel'):
                raise AdmissionError('synthetic cleanup transport failure')
            return dispatch(engine, method, path, body)
        client.call = call
        with self.assertRaises(AdmissionError):
            client.acquire('fixture', 128, 100, cancel=cancelled)
        self.assertIsNone(engine.state['active'], 'cancelled waiter still claimed a lease')
        self.assertFalse(any(method == 'GET' for method, _path in calls))

    def test_transport_failure_during_cancel_cannot_grant_ready_queued_request(self):
        clock = Clock()
        engine = Coordinator(MemoryStore(), Health(), {'healthy_seconds': 0},
                             clock=clock, monotonic_clock=clock)
        engine.submit(dict(request_id='request-a', job='fixture', peak_mib=128,
                           wait_timeout_s=100, run_timeout_s=100))
        client, calls = self.client(), []
        def call(method, path, body=None):
            calls.append((method, path))
            if method == 'POST' and path.endswith('/cancel'):
                raise AdmissionError('synthetic transport failure')
            return dispatch(engine, method, path, body)
        client.call = call
        with self.assertRaises(AdmissionError):
            client.cancel_unstarted('request-a')
        self.assertIsNone(engine.state['active'], 'cleanup GET accidentally claimed the ready request')
        self.assertEqual(calls, [('POST', '/v1/requests/request-a/cancel')])

    def test_nonconflict_failures_preserve_safe_receipts_without_any_get(self):
        errors = [AdmissionError('transport'), AdmissionError('HTTP 409 text without typed status')]
        errors += [AdmissionError('refused', status=status, server_error='refused')
                   for status in (400, 401, 403, 404, 429, 500, 503)]
        for phase in ('not_started', 'stopped'):
            for error in errors:
                with self.subTest(phase=phase, status=error.status), tempfile.TemporaryDirectory() as tmp:
                    receipt = RunReceipt(Path(tmp) / 'receipt.json')
                    receipt.lock()
                    try:
                        receipt.begin('http://fixture.invalid', 'request-a')
                        if phase == 'stopped':
                            receipt.save(phase, lease_id='fixture-known-lease')
                        before = receipt.path.read_bytes()
                        client = self.client()
                        client.call = Mock(side_effect=error)
                        with self.assertRaises(AdmissionError):
                            receipt.recover(client)
                        self.assertEqual(receipt.path.read_bytes(), before)
                        self.assertEqual(client.call.call_count, 1)
                        self.assertEqual(client.call.call_args.args[:2], ('POST', '/v1/requests/request-a/cancel'))
                    finally:
                        receipt.close()

    def test_known_absence_is_safe_only_for_never_started_or_stopped_work(self):
        for phase in ('not_started', 'stopped'):
            with self.subTest(phase=phase), tempfile.TemporaryDirectory() as tmp:
                receipt = RunReceipt(Path(tmp) / 'receipt.json')
                receipt.lock()
                try:
                    receipt.begin('http://fixture.invalid', 'request-a')
                    if phase == 'stopped':
                        receipt.save(phase, lease_id='fixture-known-lease')
                    client = self.client()
                    client.call = Mock(side_effect=AdmissionError('missing', status=404, server_error='unknown request'))
                    receipt.recover(client)
                    self.assertFalse(receipt.path.exists())
                    self.assertEqual(client.call.call_count, 1)
                finally:
                    receipt.close()

    def test_conflict_reconciliation_uses_matching_lease_and_confirms_terminal_result(self):
        client = self.client()
        client.call = Mock(side_effect=[AdmissionError('lease conflict', status=409),
            {'request_id': 'request-a', 'state': 'blocked_orphan', 'lease_id': 'fixture-exact-lease'},
            {'request_id': 'request-a', 'state': 'cancelled'}])
        result = client.cancel_unstarted('request-a')
        self.assertEqual(result['state'], 'cancelled')
        self.assertEqual([(call.args[0], call.args[1]) for call in client.call.call_args_list],
            [('POST', '/v1/requests/request-a/cancel'), ('GET', '/v1/requests/request-a'),
             ('POST', '/v1/requests/request-a/cancel')])
        self.assertEqual(client.call.call_args.args[2], {'lease_id': 'fixture-exact-lease', 'stopped': True})

    def test_conflict_lookup_terminal_or_known_absence_needs_no_second_cancel(self):
        for outcome in ({'request_id': 'request-a', 'state': 'released'},
                        AdmissionError('missing', status=404, server_error='unknown request')):
            with self.subTest(outcome=type(outcome).__name__):
                client = self.client()
                client.call = Mock(side_effect=[AdmissionError('conflict', status=409), outcome])
                self.assertIn(client.cancel_unstarted('request-a')['state'], ('released', 'absent'))
                self.assertEqual(client.call.call_count, 2)

    def test_conflict_lookup_unconfirmed_identity_or_state_cannot_acknowledge_stop(self):
        for known in ({'request_id': 'wrong-id', 'state': 'granted', 'lease_id': 'fixture'},
                      {'request_id': 'request-a', 'state': 'queued'},
                      {'request_id': 'request-a', 'state': 'granted'},
                      {'request_id': 'request-a', 'state': 'blocked_orphan', 'lease_id': ''},
                      {'request_id': 'wrong-id', 'state': 'cancelled'}, None):
            with self.subTest(known=known):
                client = self.client()
                client.call = Mock(side_effect=[AdmissionError('conflict', status=409), known])
                with self.assertRaises(AdmissionError):
                    client.cancel_unstarted('request-a')
                self.assertEqual(client.call.call_count, 2)

    def test_conflict_then_lost_cleanup_retains_exact_receipt_for_retry(self):
        with tempfile.TemporaryDirectory() as tmp:
            receipt = RunReceipt(Path(tmp) / 'receipt.json')
            receipt.lock()
            try:
                receipt.begin('http://fixture.invalid', 'request-a')
                before = receipt.path.read_bytes()
                client = self.client()
                client.call = Mock(side_effect=[AdmissionError('conflict', status=409),
                    {'request_id': 'request-a', 'state': 'granted', 'lease_id': 'fixture-exact-lease'},
                    AdmissionError('lost cancel response')])
                with self.assertRaises(AdmissionError):
                    receipt.recover(client)
                self.assertEqual(receipt.path.read_bytes(), before)
                self.assertEqual(client.call.call_count, 3)
                client.call = Mock(return_value={'request_id': 'request-a', 'state': 'cancelled'})
                receipt.recover(client)
                self.assertFalse(receipt.path.exists())
                self.assertEqual(client.call.call_count, 1)
            finally:
                receipt.close()

    def test_uncertain_receipt_never_cancels_or_clears_from_absence(self):
        with tempfile.TemporaryDirectory() as tmp:
            receipt = RunReceipt(Path(tmp) / 'receipt.json')
            receipt.lock()
            try:
                receipt.begin('http://fixture.invalid', 'request-a')
                receipt.save('may_have_started', lease_id='fixture-active-lease')
                before = receipt.path.read_bytes()
                client = self.client()
                client.call = Mock(side_effect=[{'requests': []},
                    AdmissionError('missing', status=404, server_error='unknown request')])
                with self.assertRaises(AdmissionError):
                    receipt.recover(client)
                self.assertEqual(receipt.path.read_bytes(), before)
                self.assertTrue(all(call.args[0] == 'GET' for call in client.call.call_args_list))
                client.call.reset_mock()
                with self.assertRaises(AdmissionError):
                    client.cancel_unstarted('request-a', receipt.value)
                client.call.assert_not_called()
            finally:
                receipt.close()

    def test_success_response_without_matching_terminal_identity_is_not_cleanup_proof(self):
        for result in ({'request_id': 'other-id', 'state': 'cancelled'},
                       {'request_id': 'request-a', 'state': 'granted'}, None):
            with self.subTest(result=result):
                client = self.client()
                client.call = Mock(return_value=result)
                with self.assertRaises(AdmissionError):
                    client.cancel_unstarted('request-a')
                self.assertEqual(client.call.call_count, 1)


if __name__ == '__main__':
    unittest.main()
