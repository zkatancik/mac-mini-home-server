"""Queued presence is never a grant-capable poll; all workload state is fake."""
import copy
import json
import threading
import unittest
import urllib.error
import urllib.request

from coordinator import APIError, Coordinator, ThreadingHTTPServer, dispatch, handler_class
from test_coordinator import Clock, Health, MemoryStore
from test_warning_policy import MeasuredHealth


class QueuedHeartbeatTests(unittest.TestCase):
    def setUp(self):
        self.clock, self.health, self.store = Clock(), Health(), MemoryStore()
        self.engine = Coordinator(self.store, self.health, {'max_peak_mib': 4096},
                                  clock=self.clock, monotonic_clock=self.clock)

    def submit(self, name='request-a', wait=1000):
        return self.engine.submit(dict(request_id=name, job=name, peak_mib=4096,
                                       wait_timeout_s=wait, run_timeout_s=1000))

    def keep(self, name='request-a', body=None):
        return dispatch(self.engine, 'POST', '/v1/requests/' + name + '/queued-heartbeat',
                        {} if body is None else body)

    def test_presence_retains_original_age_deadline_order_but_ready_slot_needs_get(self):
        first = self.submit()
        self.submit('request-b')
        order = list(self.engine.state['queue'])
        for _ in range(20):
            self.clock.advance(5)
            for key in order:
                self.assertEqual(self.keep(key)['state'], 'queued')
            self.assertIsNone(self.engine.status()['active'])
        current = self.engine.state['requests']['request-a']
        for key in ('created_at', 'queue_deadline', 'peak_mib'):
            self.assertEqual(current[key], first[key])
        self.assertEqual(self.engine.state['queue'], order)
        self.assertEqual(self.engine.get('request-b')['state'], 'queued')
        self.assertEqual(self.engine.get('request-a')['state'], 'granted')

    def test_elapsed_owner_timeout_and_explicit_deadline_are_not_revived(self):
        for wait, elapsed in ((1000, 30), (5, 5)):
            with self.subTest(wait=wait, elapsed=elapsed):
                self.setUp()
                self.submit(wait=wait)
                self.clock.advance(elapsed)
                with self.assertRaises(APIError) as error:
                    self.keep()
                self.assertEqual(error.exception.status, 409)
                self.assertEqual(self.engine.state['requests']['request-a']['state'], 'expired')
                self.assertNotIn('request-a', self.engine.queue_last_seen)
                self.assertIsNone(self.engine.state['active'])

    def test_slow_probe_crossing_deadline_cannot_refresh_expired_presence(self):
        self.submit(wait=2)
        base_get = self.health.get
        def slow_get():
            self.clock.advance(3)
            return base_get()
        self.health.get = slow_get
        with self.assertRaises(APIError) as error:
            self.keep()
        self.assertEqual(error.exception.status, 409)
        self.assertEqual(self.engine.state['requests']['request-a']['state'], 'expired')
        self.assertIsNone(self.engine.state['active'])

    def test_malformed_and_lease_bearing_bodies_are_rejected_without_refresh(self):
        self.submit()
        initial = self.engine.queue_last_seen['request-a']
        self.clock.advance(5)
        for body in (None, [], '', False, {'lease_id': None}, {'lease_id': 'fixture'}, {'extra': 1}):
            with self.subTest(body=body), self.assertRaises(APIError) as error:
                self.engine.queued_heartbeat('request-a', body)
            self.assertEqual(error.exception.status, 400)
            self.assertEqual(self.engine.queue_last_seen['request-a'], initial)
            self.assertIsNone(self.engine.state['active'])

    def test_unknown_terminal_granted_and_lease_bearing_records_are_rejected(self):
        with self.assertRaises(APIError) as error:
            self.keep('unknown-id')
        self.assertEqual(error.exception.status, 404)
        self.submit()
        self.engine.finish('request-a', {}, cancel=True)
        before = copy.deepcopy(self.engine.state)
        with self.assertRaises(APIError):
            self.keep()
        self.assertEqual(self.engine.state, before)
        self.submit('request-b')
        self.engine.config['healthy_seconds'] = 0
        active = self.engine.get('request-b')
        before = copy.deepcopy(self.engine.state)
        with self.assertRaises(APIError):
            self.keep('request-b')
        self.assertEqual(self.engine.state, before)
        self.engine.finish('request-b', {'lease_id': active['lease_id'], 'stopped': True})
        before = copy.deepcopy(self.engine.state)
        with self.assertRaises(APIError):
            self.keep('request-b')
        self.assertEqual(self.engine.state, before)
        self.submit('request-c')
        for key, value in [('lease_id', 'invalid-queued-lease'), ('granted_at', self.clock())]:
            self.engine.state['requests']['request-c'][key] = value
            with self.subTest(key=key), self.assertRaises(APIError):
                self.keep('request-c')
            if key == 'lease_id':
                self.engine.state['requests']['request-c'][key] = None
            else:
                del self.engine.state['requests']['request-c'][key]
        self.assertIsNone(self.engine.state['active'])

    def test_waiting_heartbeat_cannot_extend_or_clear_current_active_lease(self):
        self.engine.config['healthy_seconds'] = 0
        self.submit()
        active = self.engine.get('request-a')
        self.submit('request-b')
        for _ in range(8):
            self.clock.advance(5)
            self.assertEqual(self.keep('request-b')['state'], 'queued')
        current = self.engine.state['requests']['request-a']
        for key in ('lease_id', 'run_deadline', 'last_heartbeat', 'granted_at'):
            self.assertEqual(current[key], active[key])
        self.assertEqual(current['state'], 'blocked_orphan')
        self.assertEqual(self.engine.state['active'], 'request-a')

    def test_restart_pending_presence_cannot_recover_an_active_orphan(self):
        self.engine.config['healthy_seconds'] = 0
        self.submit()
        self.engine.get('request-a')
        queued = self.submit('request-b')
        self.engine = Coordinator(self.store, self.health, self.engine.config,
                                  clock=self.clock, monotonic_clock=self.clock)
        with self.assertRaises(APIError):
            self.keep('request-a')
        self.assertEqual(self.keep('request-b')['state'], 'queued')
        self.assertEqual(self.engine.state['requests']['request-b']['created_at'], queued['created_at'])
        self.assertEqual(self.engine.state['requests']['request-a']['state'], 'blocked_orphan')
        self.assertEqual(self.engine.state['active'], 'request-a')

    def test_quiet_warning_presence_does_not_grant_and_maintenance_never_claims(self):
        health = MeasuredHealth(self.clock)
        self.engine = Coordinator(self.store, health, {'warning_enabled': True, 'max_peak_mib': 4096},
                                  clock=self.clock, monotonic_clock=self.clock)
        self.submit()
        for _ in range(60):
            self.clock.advance(5)
            self.assertEqual(self.keep()['state'], 'queued')
        self.assertTrue(self.engine.warning_window.ready)
        self.assertIsNone(self.engine.state['active'])
        health.maintenance = True
        self.clock.advance(5)
        self.assertEqual(self.keep()['state'], 'queued')
        self.assertEqual(self.engine.get('request-a')['state'], 'queued')
        self.assertFalse(self.engine.warning_window.ready)
        health.maintenance = False
        self.keep()
        for _ in range(60):
            self.clock.advance(5)
            self.keep()
        self.assertIsNone(self.engine.state['active'])
        self.assertEqual(self.engine.get('request-a')['admission_mode'], 'warning')


class QueuedHeartbeatHTTPTests(unittest.TestCase):
    def test_authenticated_empty_object_only_endpoint_never_grants(self):
        clock, health, store = Clock(), Health(), MemoryStore()
        engine = Coordinator(store, health, {'healthy_seconds': 0}, clock=clock, monotonic_clock=clock)
        engine.submit(dict(request_id='request-a', job='fixture', peak_mib=1024,
                           run_timeout_s=100, wait_timeout_s=1000))
        server = ThreadingHTTPServer(('127.0.0.1', 0), handler_class(engine, 'fixture-token'))
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        url = 'http://127.0.0.1:%d/v1/requests/request-a/queued-heartbeat' % server.server_port
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        def request(raw, auth=True):
            headers = {'Content-Type': 'application/json'}
            if auth:
                headers['Authorization'] = 'Bearer fixture-token'
            return opener.open(urllib.request.Request(url, data=raw, headers=headers, method='POST'), timeout=3)
        try:
            for raw, auth, expected in [(b'{}', False, 401), (b'', True, 400),
                    (b'[]', True, 400), (b'{', True, 400), (b'{"lease_id":null}', True, 400)]:
                with self.subTest(raw=raw, auth=auth), self.assertRaises(urllib.error.HTTPError) as error:
                    request(raw, auth)
                self.assertEqual(error.exception.code, expected)
                error.exception.close()
            clock.advance(5)
            with request(b'{}') as response:
                self.assertEqual(response.status, 200)
                self.assertEqual(json.load(response)['state'], 'queued')
            self.assertEqual(engine.queue_last_seen['request-a'], clock())
            self.assertIsNone(engine.state['active'])
            self.assertEqual(engine.get('request-a')['state'], 'granted')
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=3)


if __name__ == '__main__':
    unittest.main()
