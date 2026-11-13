"""Offline fixtures only: no live probes, Docker, files, or workload execution."""
import copy
import unittest

from coordinator import Coordinator, HealthProbe
from test_coordinator import Clock, Health, MemoryStore
from warning_policy import WarningWindow


class MeasuredHealth(Health):
    def __init__(self, clock):
        super().__init__()
        self.clock = clock
        self.pressure, self.swap_in, self.swap_out = 2, 0.25, 0.0
        self.freeze = None

    def get(self):
        if self.freeze is not None:
            return copy.deepcopy(self.freeze)
        return {**super().get(), 'host_observed_mono': self.clock(), 'host_interval_seconds': 5,
                'guest_observed_mono': self.clock()}


class WarningAdmissionTests(unittest.TestCase):
    def setUp(self):
        self.clock, self.wall, self.store = Clock(), Clock(), MemoryStore()
        self.health = MeasuredHealth(self.clock)
        self.engine = Coordinator(self.store, self.health, {'warning_enabled': True,
            'capacity_bypass_enabled': True, 'max_peak_mib': 4096},
            clock=self.wall, monotonic_clock=self.clock)

    def submit(self, name='request-a', peak=1536, guest=True):
        return self.engine.submit(dict(request_id=name, job=name, peak_mib=peak,
            guest=guest, run_timeout_s=1200, wait_timeout_s=10000))

    def step(self, seconds=5, poll=False):
        self.clock.advance(seconds)
        self.wall.advance(seconds)
        self.engine.tick()
        for key in list(self.engine.state['queue']):
            if poll:
                self.engine.get(key)
            else:
                # Model a live owner without allowing an incidental grant.
                self.engine.queue_last_seen[key] = self.wall()

    def qualify(self):
        self.engine.tick()
        for _ in range(60):
            self.step()

    def stop(self, item):
        self.engine.finish(item['request_id'], {'lease_id': item['lease_id'], 'stopped': True})

    def test_quiet_warning_admits_only_on_exact_owner_poll_after_full_window(self):
        self.submit()
        for _ in range(59):
            self.step()
        self.assertEqual(self.engine.get('request-a')['state'], 'queued')
        self.step()
        self.assertTrue(self.engine.warning_window.ready)
        self.assertIsNone(self.engine.status()['active'])
        result = self.engine.get('request-a')
        self.assertEqual(result['state'], 'granted')
        self.assertEqual(result['admission_mode'], 'warning')

    def test_4096_profile_uses_real_guest_capacity_not_artificial_small_job_cap(self):
        self.health.available = 4607
        self.submit(peak=4096)
        self.qualify()
        self.assertEqual(self.engine.get('request-a')['state'], 'queued')
        self.health.available = 4608
        self.assertEqual(self.engine.get('request-a')['state'], 'granted')

    def test_critical_unknown_maintenance_missing_or_bursty_swap_clear_window(self):
        mutations = [('pressure', 4), ('pressure', None), ('maintenance', True),
                     ('swap_in', None), ('swap_out', 1.01), ('swap_in', 2.01), ('swap_in', float('nan'))]
        for field, value in mutations:
            with self.subTest(field=field, value=value):
                self.setUp()
                self.submit()
                self.qualify()
                setattr(self.health, field, value)
                self.step()
                self.assertEqual(self.engine.get('request-a')['state'], 'queued')
                self.assertFalse(self.engine.warning_window.ready)
                self.assertEqual(self.engine.warning_window.view().get('observed_seconds', 0), 0)

    def test_mean_limits_and_single_burst_are_distinct(self):
        self.health.swap_in = 0.51  # Below burst limit, above rolling mean limit.
        self.submit()
        self.qualify()
        self.assertFalse(self.engine.warning_window.ready)
        self.assertEqual(self.engine.warning_window.reason, 'warning_swap_average')
        self.health.swap_in = 0.25
        self.qualify()
        self.assertEqual(self.engine.get('request-a')['state'], 'granted')

    def test_cached_reads_and_unobserved_gap_do_not_manufacture_window(self):
        self.submit()
        self.health.freeze = self.health.get()
        for _ in range(70):
            self.step()
        self.assertFalse(self.engine.warning_window.ready)
        self.health.freeze = None
        self.qualify()
        self.step(11)
        self.assertEqual(self.engine.get('request-a')['state'], 'queued')

    def test_wall_time_does_not_advance_warning_observation(self):
        self.submit()
        self.wall.advance(500)
        self.engine.queue_last_seen['request-a'] = self.wall()
        self.engine.tick()
        self.assertFalse(self.engine.warning_window.ready)
        self.assertLess(self.engine.warning_window.view()['observed_seconds'], 300)

    def test_probe_completion_is_not_mistaken_for_a_future_sample(self):
        clock = self.clock
        class DelayedHealth(MeasuredHealth):
            def get(self):
                clock.advance(0.25)
                return super().get()
        self.engine.health = DelayedHealth(clock)
        self.engine.tick()
        self.assertEqual(self.engine.warning_window.view()['observed_seconds'], 0)
        self.assertEqual(self.engine.warning_window.reason, 'warning_health_wait')

    def test_initial_warning_does_not_credit_preceding_critical_interval(self):
        self.health.pressure = 4
        self.submit()
        self.step()
        self.health.pressure = 2
        self.step()
        self.assertEqual(self.engine.warning_window.view()['observed_seconds'], 0)
        for _ in range(59):
            self.step()
        self.assertEqual(self.engine.warning_window.view()['observed_seconds'], 295)
        self.assertEqual(self.engine.get('request-a')['state'], 'queued')
        self.step()
        self.assertEqual(self.engine.get('request-a')['state'], 'granted')

    def test_quiet_normal_warning_oscillation_does_not_starve_both_routes(self):
        self.submit()
        for i in range(60):
            self.health.pressure = 1 if (i // 5) % 2 == 0 else 2
            self.step()
        self.assertTrue(self.engine.warning_window.ready)
        self.health.pressure = 1
        self.assertEqual(self.engine.get('request-a')['state'], 'queued')
        self.health.pressure = 2
        result = self.engine.get('request-a')
        self.assertEqual(result['state'], 'granted')
        self.assertEqual(result['admission_mode'], 'warning')

    def test_implausibly_dense_observations_cannot_grow_history(self):
        window = self.engine.warning_window
        for i in range(1000):
            sample = self.health.get()
            sample.update(host_observed_mono=1000+i*.001, host_interval_seconds=.001)
            window.observe(sample, 1000+i*.001)
        self.assertFalse(window.ready)
        self.assertEqual(len(window.intervals), 0)

    def test_stale_guest_denies_and_native_profile_requires_normal_route(self):
        self.submit(guest=False)
        self.qualify()
        self.assertEqual(self.engine.get('request-a')['state'], 'queued')
        self.assertIn('warning_native_unqualified', self.engine.get('request-a')['reason'])
        self.engine.finish('request-a', {}, cancel=True)
        self.submit('request-b')
        sample = self.health.get()
        sample['guest_observed_mono'] -= 21
        self.health.freeze = sample
        self.assertEqual(self.engine.get('request-b')['state'], 'queued')

    def test_restart_discards_warning_window_and_preserves_active_orphan(self):
        self.submit()
        self.qualify()
        first = self.engine.get('request-a')
        restarted = Coordinator(self.store, self.health, self.engine.config,
                                clock=self.wall, monotonic_clock=self.clock)
        self.assertFalse(restarted.warning_window.ready)
        self.assertEqual(restarted.state['active'], 'request-a')
        self.assertEqual(restarted.get('request-a')['state'], 'blocked_orphan')
        self.assertEqual(restarted.heartbeat('request-a', {'lease_id': first['lease_id']})['state'], 'granted')

    def test_warning_deterioration_never_revokes_existing_lease(self):
        self.submit()
        self.qualify()
        first = self.engine.get('request-a')
        self.submit('request-b')
        self.health.pressure = 4
        self.step()
        self.assertEqual(self.engine.heartbeat('request-a', {'lease_id': first['lease_id']})['state'], 'granted')
        self.assertEqual(self.engine.get('request-b')['state'], 'queued')
        self.assertEqual(self.engine.state['active'], 'request-a')

    def test_oldest_fitting_owner_wins_before_younger_poll(self):
        self.health.available = 2500
        self.submit('request-a', 4096)
        self.submit('request-b', 1536)
        self.submit('request-c', 512)
        self.qualify()
        self.assertEqual(self.engine.get('request-c')['state'], 'queued')
        result = self.engine.get('request-b')
        self.assertEqual(result['state'], 'granted')
        self.assertEqual(result['bypassed_request_id'], 'request-a')

    def test_two_completed_bypasses_then_bounded_reservation_not_forever_block(self):
        self.health.available = 2500
        original = self.submit('request-a', 4096)['created_at']
        self.submit('request-b', 1536)
        self.submit('request-c', 512)
        self.submit('request-d', 512)
        self.qualify()
        first = self.engine.get('request-b')
        self.stop(first)
        second = self.engine.get('request-c')
        self.assertEqual(second['state'], 'granted')
        self.stop(second)
        self.assertEqual(self.engine.state['requests']['request-a']['capacity_bypass_count'], 2)
        self.assertEqual(self.engine.get('request-d')['state'], 'queued')
        self.assertIn('age_reservation', self.engine.get('request-a')['reason'])
        for _ in range(23):
            self.step()
        self.assertEqual(self.engine.get('request-d')['state'], 'queued')
        self.step()
        self.assertEqual(self.engine.get('request-d')['state'], 'granted')
        self.assertEqual(self.engine.state['requests']['request-a']['created_at'], original)
        self.assertIn('last_capacity_deferred_at', self.engine.state['requests']['request-a'])

    def test_old_head_that_fits_wins_immediately_even_during_reservation(self):
        self.health.available = 2500
        self.submit('request-a', 4096)
        self.submit('request-b', 512)
        self.qualify()
        self.engine.state['requests']['request-a']['capacity_bypass_count'] = 2
        self.engine.tick()
        self.health.available = 4608
        self.assertEqual(self.engine.get('request-b')['state'], 'queued')
        self.assertEqual(self.engine.get('request-a')['state'], 'granted')


class MonotonicProbeTests(unittest.TestCase):
    def test_counter_rates_use_monotonic_time_and_reject_long_gaps(self):
        wall, mono = Clock(), Clock()
        counts = [0, 0]
        def runner(args, **kwargs):
            if 'sysctl' in args[0]: return '2'
            if 'vm_stat' in args[0]:
                return f'Mach Virtual Memory Statistics: (page size of 16384 bytes)\nSwapins: {counts[0]}.\nSwapouts: {counts[1]}.\n'
            return 'MemAvailable: 8388608 kB\n'
        probe = HealthProbe({'probe_container': 'fixture'}, wall, runner, monotonic_clock=mono)
        probe.get()
        wall.advance(10000)
        mono.advance(5)
        counts[0] = 320
        observed = probe.get()
        self.assertEqual(observed['swap_in_mib_s'], 1)
        self.assertEqual(observed['host_interval_seconds'], 5)
        mono.advance(11)
        self.assertIsNone(probe.get()['swap_in_mib_s'])


if __name__ == '__main__':
    unittest.main()
