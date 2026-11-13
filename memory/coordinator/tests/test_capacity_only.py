import unittest

from coordinator import Coordinator
from test_coordinator import Clock, MemoryStore
from test_warning_policy import MeasuredHealth


class CapacityOnlyTests(unittest.TestCase):
    def engine(self, **health_fields):
        clock = Clock()
        health = MeasuredHealth(clock)
        for key, value in health_fields.items():
            setattr(health, key, value)
        config = {'host_pressure_admission_enabled': False, 'max_peak_mib': 4096}
        return Coordinator(MemoryStore(), health, config, clock, clock), health, clock

    def submit(self, engine, guest=True, name='job-test-1'):
        engine.submit(dict(request_id=name, job=name, guest=guest, peak_mib=4096, run_timeout_s=120))

    def test_host_telemetry_does_not_veto_and_owner_poll_required(self):
        for pressure in (1, 2, 4, None):
            with self.subTest(pressure=pressure):
                e, _, _ = self.engine(pressure=pressure, swap_in=100, swap_out=None, available=4608)
                self.submit(e)
                self.assertIsNone(e.status()['active'])
                e.queued_heartbeat('job-test-1', {})
                self.assertIsNone(e.status()['active'])
                r = e.get('job-test-1')
                self.assertEqual((r['state'], r['admission_mode']), ('granted', 'capacity_only'))
                self.submit(e, name='job-test-2')
                self.assertEqual(e.get('job-test-2')['state'], 'queued')

    def test_guest_reserve_and_freshness_still_required(self):
        e, h, clock = self.engine(available=4607)
        self.submit(e)
        self.assertIn('below declared', e.get('job-test-1')['reason'])
        h.available = 4608
        for timestamp in (None, clock() - 21, clock() + 1):
            h.freeze = None
            h.freeze = {**h.get(), 'guest_observed_mono': timestamp}
            self.assertIn('stale', e.get('job-test-1')['reason'])
        h.freeze = None
        self.assertEqual(e.get('job-test-1')['state'], 'granted')

    def test_maintenance_still_applies_and_native_does_not_need_guest_probe(self):
        e, h, _ = self.engine(maintenance=True, available=None, pressure=None)
        self.submit(e, guest=False)
        self.assertIn('maintenance', e.get('job-test-1')['reason'])
        h.maintenance = False
        self.assertEqual(e.get('job-test-1')['state'], 'granted')
