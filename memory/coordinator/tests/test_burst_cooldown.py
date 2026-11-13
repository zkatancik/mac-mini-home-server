"""Offline policy fixtures with synthetic swap bursts."""
import unittest
from coordinator import Coordinator
from warning_policy import WarningWindow
from test_coordinator import Clock, Health, MemoryStore


CONFIG = dict(warning_enabled=True, warning_seconds=300,
              warning_mean_read_mib_s=.5, warning_mean_write_mib_s=.1,
              warning_burst_read_mib_s=2, warning_burst_write_mib_s=1,
              warning_max_probe_gap_s=10, warning_burst_cooldown_s=60)


class Trace:
    def __init__(self, **config):
        self.window=WarningWindow({**CONFIG, **config});self.now=1000
        self.sample(.16,0)

    def sample(self, read=.16, write=0, seconds=5, advance=False, **fields):
        if advance:self.now+=seconds
        health=dict(pressure_level=2,maintenance_active=False,
                    host_observed_mono=self.now,host_interval_seconds=seconds,
                    swap_in_mib_s=read,swap_out_mib_s=write)
        health.update(fields);self.window.observe(health,self.now)
        return health

    def step(self, read=.16, write=0, seconds=5, **fields):
        return self.sample(read,write,seconds,True,**fields)

    def quiet(self, steps=60):
        for _ in range(steps):self.step()


class BurstCooldownTests(unittest.TestCase):
    def test_default_preserves_existing_full_reset_policy(self):
        t=Trace(warning_burst_cooldown_s=0);t.quiet();t.step(2.125)
        self.assertFalse(t.window.ready);self.assertEqual(len(t.window.intervals),0)
        self.assertEqual(t.window.reason,'warning_swap_burst')

    def test_isolated_read_burst_retains_traffic_and_waits_sixty_fresh_seconds(self):
        t=Trace();t.quiet();t.step(2.125,seconds=5.25)
        self.assertEqual(t.window.summary['observed_seconds'],300)
        expected=(.16*(300-5.25)+2.125*5.25)/300
        self.assertAlmostEqual(t.window.summary['mean_read_mib_s'],expected,places=8)
        self.assertFalse(t.window.ready);self.assertEqual(t.window.reason,'warning_burst_cooldown')
        t.quiet(11);self.assertFalse(t.window.ready)
        t.quiet(1);self.assertTrue(t.window.ready)
        self.assertGreater(t.window.summary['mean_read_mib_s'],.16)

    def test_recurrent_small_reads_do_not_erase_complete_history(self):
        proposed,old=Trace(),Trace(warning_burst_cooldown_s=0)
        for tick in range(1,121):
            rate=2.125 if tick%24==0 else .16
            proposed.step(rate);old.step(rate)
            if tick in (84,108):
                self.assertTrue(proposed.window.ready)
                self.assertFalse(old.window.ready)
        self.assertEqual(proposed.window.summary['observed_seconds'],300)

    def test_write_burst_remains_above_mean_after_cooldown(self):
        for rate,duration in ((12.0,5.5), (29.0,5.25)):
            with self.subTest(rate=rate):
                t=Trace();t.quiet();t.step(write=rate,seconds=duration);t.quiet(12)
                self.assertFalse(t.window.ready)
                self.assertEqual(t.window.reason,'warning_swap_average')
                self.assertAlmostEqual(t.window.summary['mean_write_mib_s'],rate*duration/300)
                t.quiet(46);self.assertFalse(t.window.ready)
                t.quiet(2);self.assertTrue(t.window.ready)

    def test_partial_boundary_clips_burst_traffic_instead_of_dropping_whole_interval(self):
        t=Trace();t.quiet();t.step(write=10,seconds=10)
        t.quiet(59)
        self.assertAlmostEqual(t.window.summary['mean_write_mib_s'],50/300)
        self.assertFalse(t.window.ready)
        t.quiet(1);self.assertTrue(t.window.ready)

    def test_short_cooldown_does_not_replace_full_initial_window(self):
        t=Trace();t.step(3);t.quiet(12)
        self.assertFalse(t.window.ready)
        self.assertEqual(t.window.summary['observed_seconds'],65)
        t.quiet(47);self.assertTrue(t.window.ready)

    def test_cached_reads_never_age_cooldown(self):
        t=Trace();t.quiet();health=t.step(3)
        for extra in (1,2,3,4):t.window.observe(health,t.now+extra)
        self.assertFalse(t.window.ready)
        self.assertEqual(t.window.summary['burst_cooldown_remaining_s'],60)
        self.assertEqual(t.window.summary['observed_seconds'],300)

    def test_hard_resets_still_require_full_fresh_window(self):
        for fields in ({'pressure_level':4},{'pressure_level':None},
                       {'maintenance_active':True},{'swap_in_mib_s':None},
                       {'host_interval_seconds':11},{'host_observed_mono':0}):
            with self.subTest(fields=fields):
                t=Trace();t.quiet();t.step(3);t.step(**fields)
                self.assertFalse(t.window.ready);self.assertFalse(t.window.intervals)
                self.assertIsNone(t.window.last_burst_end)
                t.quiet(60);self.assertFalse(t.window.ready)
                t.quiet(1);self.assertTrue(t.window.ready)

    def test_observation_gap_discards_old_history_and_burst_timer(self):
        t=Trace();t.quiet();t.step(3);t.step(seconds=11)
        self.assertFalse(t.window.ready);self.assertFalse(t.window.intervals)
        self.assertIsNone(t.window.last_burst_end)
        t.quiet(60);self.assertFalse(t.window.ready)
        t.quiet(1);self.assertTrue(t.window.ready)

    def test_read_mean_above_limit_still_denies_without_recent_burst(self):
        t=Trace()
        for _ in range(60):t.step(.51)
        self.assertFalse(t.window.ready);self.assertEqual(t.window.reason,'warning_swap_average')

    def test_invalid_or_shortened_cooldown_configuration_rejected(self):
        for value in (-1,1,59,301,None,True,float('nan'),'60'):
            with self.subTest(value=value),self.assertRaises(ValueError):Trace(warning_burst_cooldown_s=value)

    def test_owner_poll_guest_capacity_and_orphan_rules_unchanged(self):
        clock=Clock()
        class Measured(Health):
            def get(self):
                return {**super().get(),'host_observed_mono':clock(),'host_interval_seconds':5,
                        'guest_observed_mono':clock()}
        health=Measured();health.pressure=2;health.swap_in=.16;health.swap_out=0;health.available=4607
        engine=Coordinator(MemoryStore(),health,{**CONFIG,'max_peak_mib':4096},clock=clock,monotonic_clock=clock)
        engine.submit(dict(request_id='request-a',job='fixture',peak_mib=4096,run_timeout_s=100,wait_timeout_s=1000))
        for _ in range(60):
            clock.advance(5);engine.queued_heartbeat('request-a',{})
        self.assertTrue(engine.warning_window.ready);self.assertIsNone(engine.status()['active'])
        self.assertEqual(engine.get('request-a')['state'],'queued')
        health.available=4608
        grant=engine.get('request-a');self.assertEqual(grant['state'],'granted')
        health.pressure=4
        self.assertEqual(engine.heartbeat('request-a',{'lease_id':grant['lease_id']})['state'],'granted')
        clock.advance(101);engine.tick()
        self.assertEqual(engine.get('request-a')['state'],'blocked_orphan')
        self.assertEqual(engine.state['active'],'request-a')


if __name__=='__main__':unittest.main()
