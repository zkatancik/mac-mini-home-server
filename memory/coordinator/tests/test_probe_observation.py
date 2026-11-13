"""Offline checks for status sampling and retained reset diagnostics."""
import unittest

from coordinator import Coordinator
from test_coordinator import Clock, MemoryStore
from test_warning_policy import MeasuredHealth


class ProbeObservationTests(unittest.TestCase):
    def setUp(self):
        self.clock = Clock()
        self.health = MeasuredHealth(self.clock)
        self.engine = Coordinator(MemoryStore(), self.health,
            {'warning_enabled': True}, clock=self.clock, monotonic_clock=self.clock)

    def test_status_observes_every_sample_and_never_refreshes_it_twice(self):
        original = self.health.get
        calls = []
        def changing_probe():
            self.clock.advance(5)
            calls.append(self.clock())
            return original()
        self.health.get = changing_probe
        for _ in range(61):
            result = self.engine.status()
            self.assertEqual(result['health']['host_observed_mono'],
                             self.engine.warning_window.last_sample)
        self.assertEqual(len(calls), 61)
        self.assertTrue(result['warning_window']['ready'])
        self.assertIsNone(result['active'])

    def test_burst_reset_evidence_survives_following_healthy_sample(self):
        self.engine.tick()
        self.clock.advance(5)
        self.health.swap_out = 2
        self.engine.tick()
        reset = self.engine.warning_window.view()['last_reset']
        self.assertEqual(reset['reason'], 'warning_swap_burst')
        self.assertEqual(reset['swap_out_mib_s'], 2)
        self.assertEqual(reset['host_interval_seconds'], 5)
        self.clock.advance(5)
        self.health.swap_out = 0
        self.engine.tick()
        result = self.engine.warning_window.view()
        self.assertEqual(result['reason'], 'warning_health_wait')
        self.assertEqual(result['last_reset'], reset)
        self.assertEqual(result['reset_count'], 1)


if __name__ == '__main__':
    unittest.main()
