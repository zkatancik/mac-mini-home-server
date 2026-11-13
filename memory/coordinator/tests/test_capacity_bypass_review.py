"""Independent capacity bypass regressions; in-memory clocks/state only."""
import unittest

import test_warning_policy as warning_fixtures
from coordinator import APIError, Coordinator


class CapacityBypassReviewTests(unittest.TestCase):
    setUp = warning_fixtures.WarningAdmissionTests.setUp
    submit = warning_fixtures.WarningAdmissionTests.submit
    step = warning_fixtures.WarningAdmissionTests.step
    qualify = warning_fixtures.WarningAdmissionTests.qualify
    stop = warning_fixtures.WarningAdmissionTests.stop

    def restart(self):
        self.engine = Coordinator(self.store, self.health, self.engine.config,
            clock=self.wall, monotonic_clock=self.clock)

    def test_native_warning_head_can_be_bypassed_but_never_warning_granted(self):
        self.submit('native-head', 512, guest=False)
        self.submit('guest-one', 1024)
        self.submit('guest-two', 1024)
        self.submit('guest-three', 1024)
        self.qualify()
        created = self.engine.state['requests']['native-head']['created_at']
        deadline = self.engine.state['requests']['native-head']['queue_deadline']
        for key in ('guest-one', 'guest-two', 'guest-three'):
            result = self.engine.get(key)
            self.assertEqual(result['state'], 'granted')
            self.assertEqual(result['admission_mode'], 'warning')
            self.stop(result)
        self.assertIn('warning_native_unqualified', self.engine.get('native-head')['reason'])
        self.assertEqual(self.engine.state['requests']['native-head']['created_at'], created)
        self.assertEqual(self.engine.state['requests']['native-head']['queue_deadline'], deadline)
        self.assertEqual(self.engine.state['requests']['native-head']['state'], 'queued')
        self.assertEqual(self.engine.capacity_reservation, {})
        self.submit('guest-four', 512)
        self.health.pressure = 1
        self.engine.tick()
        for _ in range(12):
            self.step()
        native = self.engine.get('native-head')
        self.assertEqual(native['state'], 'granted')
        self.assertEqual(native['admission_mode'], 'normal')
        self.assertEqual(self.engine.get('guest-four')['state'], 'queued')

    def test_legacy_native_reservation_and_debt_do_not_delay_warning_guests(self):
        self.submit('native-head', 4096, guest=False)
        self.qualify()
        native = self.engine.state['requests']['native-head']
        native['capacity_bypass_count'] = 2
        self.engine.capacity_reservation['native-head'] = self.clock() + 120
        created, deadline = native['created_at'], native['queue_deadline']
        # More than the persisted-state validator's five-bypass ceiling: a
        # policy-ineligible head must not accrue unbounded capacity debt.
        for index in range(7):
            key = 'guest-%03d' % index
            self.submit(key, 512)
            self.assertIsNone(self.engine.status()['active'])
            result = self.engine.get(key)
            self.assertEqual(result['state'], 'granted')
            self.assertIsNone(result['bypassed_request_id'])
            self.stop(result)
        self.assertEqual(native['capacity_bypass_count'], 2)
        self.assertEqual(self.engine.capacity_reservation, {})
        self.restart()
        native = self.engine.state['requests']['native-head']
        self.assertEqual((native['created_at'], native['queue_deadline']), (created, deadline))
        self.assertEqual(native['capacity_bypass_count'], 2)
        self.assertEqual(native['state'], 'queued')

    def test_native_policy_wait_still_requires_health_capacity_and_owner_poll(self):
        for field, value in [('pressure', 4), ('pressure', None), ('maintenance', True),
                             ('available', None), ('available', 1023), ('swap_out', 1.01)]:
            with self.subTest(field=field, value=value):
                self.setUp()
                self.submit('native-head', 512, guest=False)
                self.submit('guest-one', 512)
                self.qualify()
                self.engine.state['requests']['native-head']['capacity_bypass_count'] = 2
                setattr(self.health, field, value)
                self.step()
                self.assertEqual(self.engine.get('guest-one')['state'], 'queued')
                self.assertIsNone(self.engine.state['active'])

    def test_disabled_bypass_still_preserves_strict_fifo(self):
        self.engine.config['capacity_bypass_enabled'] = False
        self.submit('native-head', 512, guest=False)
        self.submit('guest-one', 512)
        self.qualify()
        self.assertEqual(self.engine.get('guest-one')['state'], 'queued')
        self.assertIsNone(self.engine.state['active'])

    def test_large_guest_behind_native_retains_its_own_capacity_reservation(self):
        self.health.available = 2500
        native = self.submit('native-head', 4096, guest=False)
        large = self.submit('large-guest', 4096)
        self.submit('native-next', 1024, guest=False)
        for key in ('small-one', 'small-two', 'small-three'):
            self.submit(key, 512)
        self.qualify()
        for key in ('small-one', 'small-two'):
            item = self.engine.get(key)
            self.assertEqual(item['state'], 'granted')
            self.assertEqual(item['bypassed_request_id'], 'large-guest')
            self.stop(item)
        self.assertEqual(self.engine.get('small-three')['state'], 'queued')
        self.assertIn('age_reservation', self.engine.get('large-guest')['reason'])
        self.assertNotIn('native-head', self.engine.capacity_reservation)
        self.assertNotIn('native-next', self.engine.capacity_reservation)
        self.assertEqual(self.engine.state['requests']['large-guest']['capacity_bypass_count'], 2)
        self.assertEqual(self.engine.state['requests']['native-head'].get('capacity_bypass_count', 0), 0)
        # A fresh fitting observation serves the large guest during its own
        # reservation, without granting either native profile in warning mode.
        self.health.available = 4608
        granted = self.engine.get('large-guest')
        self.assertEqual(granted['state'], 'granted')
        self.assertEqual(granted['admission_mode'], 'warning')
        self.assertEqual(granted['created_at'], large['created_at'])
        self.stop(granted)
        self.health.pressure = 1
        self.engine.tick()
        for _ in range(12):
            self.step()
        self.assertEqual(self.engine.get('small-three')['state'], 'queued')
        first = self.engine.get('native-head')
        self.assertEqual(first['state'], 'granted')
        self.assertEqual(first['created_at'], native['created_at'])

    def test_all_native_warning_queue_never_grants_or_reserves(self):
        self.submit('native-head', 4096, guest=False)
        self.submit('native-next', 512, guest=False)
        self.engine.state['requests']['native-head']['capacity_bypass_count'] = 2
        self.engine.capacity_reservation['native-head'] = self.clock() + 120
        self.qualify()
        for key in ('native-head', 'native-next'):
            result = self.engine.get(key)
            self.assertEqual(result['state'], 'queued')
            self.assertIn('warning_native_unqualified', result['reason'])
        self.assertIsNone(self.engine.state['active'])
        self.assertEqual(self.engine.capacity_reservation, {})

    def test_large_capacity_head_never_forced_even_after_repeated_reservations(self):
        self.health.available = 4607
        created = self.submit('large-head', 4096)['created_at']
        self.qualify()
        for cycle in range(3):
            for i in range(2):
                key = 'small-%d-%d' % (cycle, i)
                self.submit(key, 512)
                result = self.engine.get(key)
                self.assertEqual(result['state'], 'granted')
                self.stop(result)
            self.assertIn('age_reservation', self.engine.get('large-head')['reason'])
            for _ in range(24):
                self.step()
            self.assertEqual(self.engine.get('large-head')['state'], 'queued')
        self.assertEqual(self.engine.state['requests']['large-head']['created_at'], created)
        self.assertEqual(self.engine.state['requests']['large-head']['peak_mib'], 4096)
        self.health.available = 4608
        self.assertEqual(self.engine.get('large-head')['state'], 'granted')

    def test_oldest_fitting_absent_owner_expires_without_incidental_grant(self):
        self.health.available = 2500
        self.submit('large-head', 4096)
        self.submit('absent-fit', 1024)
        self.submit('younger-fit', 512)
        self.qualify()
        self.engine.status()
        self.engine.tick()
        self.assertIsNone(self.engine.state['active'])
        self.assertEqual(self.engine.get('younger-fit')['state'], 'queued')
        for _ in range(6):
            self.clock.advance(5)
            self.wall.advance(5)
            self.engine.get('large-head')
            result = self.engine.get('younger-fit')
        self.assertEqual(self.engine.state['requests']['absent-fit']['state'], 'expired')
        self.assertNotIn('granted_at', self.engine.state['requests']['absent-fit'])
        self.assertEqual(result['state'], 'granted')

    def test_queued_restart_preserves_ids_counts_and_requalifies_without_auto_grant(self):
        self.health.available = 2500
        self.submit('large-head', 4096)
        self.submit('small-one', 512)
        self.submit('small-two', 512)
        self.submit('small-three', 512)
        self.qualify()
        self.stop(self.engine.get('small-one'))
        self.stop(self.engine.get('small-two'))
        self.step(5)
        queue = list(self.engine.state['queue'])
        created = self.engine.state['requests']['large-head']['created_at']
        self.restart()
        self.assertEqual(self.engine.state['queue'], queue)
        self.assertEqual(self.engine.state['requests']['large-head']['capacity_bypass_count'], 2)
        self.assertEqual(self.engine.state['requests']['large-head']['created_at'], created)
        self.assertFalse(self.engine.warning_window.ready)
        self.assertEqual(self.engine.capacity_reservation, {})
        self.qualify()
        self.assertIsNone(self.engine.state['active'])
        self.assertEqual(self.engine.get('small-three')['state'], 'queued')
        for _ in range(24):
            self.step()
        self.assertIsNone(self.engine.state['active'])
        self.assertEqual(self.engine.get('small-three')['state'], 'granted')

    def test_restarted_active_bypass_remains_orphan_until_exact_owner_confirms_stop(self):
        self.health.available = 2500
        self.submit('large-head', 4096)
        self.submit('small-one', 512)
        self.submit('small-two', 512)
        self.qualify()
        first = self.engine.get('small-one')
        self.restart()
        self.qualify()
        self.assertEqual(self.engine.state['active'], 'small-one')
        self.assertEqual(self.engine.get('small-one')['state'], 'blocked_orphan')
        self.assertEqual(self.engine.get('small-two')['state'], 'queued')
        for body in ({'lease_id': 'wrong', 'stopped': True}, {'lease_id': first['lease_id']}):
            with self.assertRaises(APIError):
                self.engine.finish('small-one', body)
            self.assertEqual(self.engine.state['active'], 'small-one')
            self.assertEqual(self.engine.state['requests']['large-head'].get('capacity_bypass_count', 0), 0)
        self.stop(first)
        self.assertEqual(self.engine.state['requests']['large-head']['capacity_bypass_count'], 1)
        self.assertIsNone(self.engine.state['active'])
        self.assertEqual(self.engine.get('small-two')['state'], 'granted')

    def test_critical_unknown_and_maintenance_block_all_bypass_candidates(self):
        for field, value in [('pressure', 4), ('pressure', None), ('maintenance', True)]:
            with self.subTest(field=field, value=value):
                self.setUp()
                self.health.available = 2500
                self.submit('large-head', 4096)
                self.submit('small-one', 512)
                self.qualify()
                setattr(self.health, field, value)
                self.step()
                self.assertEqual(self.engine.get('small-one')['state'], 'queued')
                self.assertIsNone(self.engine.state['active'])

    def test_unknown_guest_capacity_never_uses_small_declared_peak_as_fallback(self):
        self.submit('large-head', 4096)
        self.submit('small-one', 512)
        self.qualify()
        self.health.available = None
        self.assertEqual(self.engine.get('small-one')['state'], 'queued')
        self.assertIsNone(self.engine.state['active'])

    def test_cancelling_reserved_head_removes_hold_without_granting_successor(self):
        self.health.available = 2500
        self.submit('large-head', 4096)
        self.submit('small-one', 512)
        self.submit('small-two', 512)
        self.submit('small-three', 512)
        self.qualify()
        self.stop(self.engine.get('small-one'))
        self.stop(self.engine.get('small-two'))
        self.assertIn('large-head', self.engine.capacity_reservation)
        self.engine.finish('large-head', {}, cancel=True)
        self.assertNotIn('large-head', self.engine.capacity_reservation)
        self.assertIsNone(self.engine.state['active'])
        self.assertEqual(self.engine.get('small-three')['state'], 'granted')


if __name__ == '__main__':
    unittest.main()
