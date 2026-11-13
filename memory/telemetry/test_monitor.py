import contextlib
import copy
import importlib.machinery
import importlib.util
import io
import json
import os
from pathlib import Path
import tempfile
import subprocess
import sys
import unittest
from unittest import mock

loader = importlib.machinery.SourceFileLoader('monitor', str(Path(__file__).with_name('home-memory.plugin')))
spec = importlib.util.spec_from_loader(loader.name, loader)
m = importlib.util.module_from_spec(spec)
loader.exec_module(m)


class NativeMemoryTests(unittest.TestCase):
    def host(self):
        names = set(m.PAGE_FIELDS.values()) | m.COUNTERS
        vm = 'Mach Virtual Memory Statistics: (page size of 16384 bytes)\n'
        vm += '\n'.join('"%s": %s.' % (name, 64) for name in names)
        raw = ('hw.memsize: 34359738368\nkern.memorystatus_vm_pressure_level: 2\n'
               'vm.swapusage: total = 4096.00M  used = 123.50M  free = 3972.50M (encrypted)\n'
               'kern.boottime: { sec = 1700000000, usec = 0 } Tue Nov 14\n')
        return vm, raw

    def sample(self, monotonic=60):
        host, boot, counters = m.parse_host(*self.host())
        return {'host': host, 'boot_time': boot, 'source_counters': counters,
                'monotonic_seconds': monotonic, 'monotonic_clock': m.MONOTONIC_CLOCK,
                'sample_gap_seconds': None, 'errors': []}

    def test_pages_and_swap_units(self):
        host, boot, counters = m.parse_host(*self.host())
        self.assertEqual(host['compressor_mib'], 1)
        self.assertEqual(host['ram_mib'], 32768)
        self.assertEqual(host['swap_used_mib'], 123.5)
        self.assertEqual(boot, 1700000000)
        self.assertEqual(counters['pages']['Swapins'], 64)

    def test_missing_or_unknown_host_fails_closed(self):
        vm, raw = self.host()
        for a, b in [(vm.replace('Swapins', 'missing'), raw), (vm, raw.replace('level: 2', 'level: 8'))]:
            with self.assertRaises((KeyError, ValueError)):
                m.parse_host(a, b)

    def test_rates_use_page_size_and_monotonic_gap(self):
        old, new = self.sample(), self.sample(120)
        new['source_counters']['pages']['Swapins'] += 3840
        m.add_rates(new, old)
        self.assertEqual(new['host']['swap_in_mib_s'], 1)
        self.assertEqual(new['host']['swap_out_mib_s'], 0)
        self.assertFalse(new['errors'])

    def test_rates_unknown_on_first_gap_reboot_or_counter_reset(self):
        old = self.sample()
        cases = [None, {**old, 'boot_time': 1}, {**old, 'monotonic_seconds': -1000},
                 {**old, 'source_counters': {}}]
        reset = copy.deepcopy(old)
        reset['source_counters']['pages']['Swapins'] = 10000
        cases.append(reset)
        for previous in cases:
            new = self.sample(120)
            m.add_rates(new, previous)
            self.assertIsNone(new['host']['swap_in_mib_s'])
            self.assertTrue(new['errors'])

    def test_codex_direct_and_missing_matches(self):
        bundles = {'/Applications/ChatGPT.app': {'executable': 'Codex', 'version': '1', 'build': '2'}}
        ps = ('10 1 1024 1-01:02:03 /Applications/ChatGPT.app/Contents/MacOS/Codex\n'
              '11 10 512 00:30 /Applications/ChatGPT.app/Contents/Frameworks/Codex Helper\n'
              '12 10 4096 00:30 /usr/bin/other\n')
        top = '10 1G 10M Codex\n11 12M+ 1M Helper\n12 3G 1M Other\n'
        result, errors = m.parse_codex(ps, top, bundles)
        self.assertEqual(result['footprint_mib'], 1036)
        self.assertEqual(result['process_count'], 2)
        self.assertEqual(result['uptime_seconds'], 90123)
        self.assertFalse(errors)
        result, errors = m.parse_codex(ps, '10 1G 10M Codex', bundles)
        self.assertIsNone(result['footprint_mib'])
        self.assertTrue(errors)

    def test_missing_codex_is_not_idle_approval(self):
        result, errors = m.parse_codex('', '', {})
        self.assertIsNone(result['main_pid'])
        self.assertTrue(errors)

    def test_missing_metrics_not_emitted_as_zero(self):
        sample = {'host': {}, 'codex': {}, 'collector': {}, 'quality': 'partial'}
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            m.emit(sample)
        self.assertEqual([x for x in out.getvalue().splitlines() if x.startswith('SET ')], ['SET complete = 0'])

    def test_atomic_latest_and_bounded_owned_history(self):
        with tempfile.TemporaryDirectory() as directory:
            state = Path(directory)
            (state / 'history-20200101.jsonl').write_text('{}\n')
            (state / 'unrelated.json').write_text('keep')
            sample = {'schema_version': 1, 'sampled_epoch': 1700000000, 'host': {'pressure_level': None}}
            m.save(sample, state)
            self.assertEqual(json.loads((state / 'latest.json').read_text()), sample)
            self.assertFalse((state / 'history-20200101.jsonl').exists())
            self.assertEqual((state / 'unrelated.json').read_text(), 'keep')
            self.assertEqual((state / 'latest.json').stat().st_mode & 0o777, 0o600)

    def test_current_day_history_is_bounded_even_with_many_restarts(self):
        with tempfile.TemporaryDirectory() as directory, mock.patch.object(m, 'MAX_HISTORY', 4096):
            state = Path(directory)
            sample = {'schema_version': 1, 'sampled_epoch': 1700000000, 'padding': 'x' * 300}
            for _ in range(30):
                m.save(sample, state)
            files = list(state.glob('history-*.jsonl'))
            self.assertLessEqual(sum(p.stat().st_size for p in files), 4096)
            self.assertTrue(all(json.loads(line) == sample for p in files for line in p.read_text().splitlines()))

    def test_unsafe_state_paths_are_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            state = root / 'state'
            state.mkdir(mode=0o755)
            with self.assertRaises(ValueError):
                m.private_directory(state)
            os.chmod(state, 0o700)
            target = root / 'other'
            target.write_text('untouched')
            (state / 'history').symlink_to(target)
            with self.assertRaises(OSError):
                m.private_open(state / 'history', os.O_WRONLY | os.O_APPEND)
            self.assertEqual(target.read_text(), 'untouched')
            fifo = state / 'fifo'
            os.mkfifo(fifo, 0o600)
            with self.assertRaises(OSError):
                m.private_open(fifo, os.O_WRONLY | os.O_APPEND)

    def test_restart_lock_wait_is_bounded_and_never_admits_two_writers(self):
        with mock.patch.object(m.fcntl, 'flock', side_effect=[BlockingIOError(), None]) as lock, mock.patch.object(m.time, 'sleep') as sleep:
            m.acquire_lock(99)
            self.assertEqual(lock.call_count, 2)
            sleep.assert_called_once_with(1)
        with mock.patch.object(m.fcntl, 'flock', side_effect=BlockingIOError()) as lock, mock.patch.object(m.time, 'sleep'):
            with self.assertRaises(BlockingIOError):
                m.acquire_lock(99)
            self.assertEqual(lock.call_count, 91)

    def test_statsd_missing_values_are_omitted(self):
        sample = {'host': {'pressure_level': 2, 'swap_in_mib_s': None}, 'codex': {}, 'collector': {}, 'quality': 'partial'}
        self.assertEqual(m.statsd_lines(sample), ['home_memory.pressure.level:2|g|#units=level,family=memory,name=level', 'home_memory.collector_quality.complete:0|g|#units=complete,family=collector,name=complete'])

    def test_independent_invocations_use_saved_baseline_and_skip_duplicates(self):
        with tempfile.TemporaryDirectory() as directory:
            state = Path(directory)
            previous = self.sample(10)
            previous.update(schema_version=1, sampled_epoch=1700000000)
            m.save(previous, state)
            with mock.patch.object(m, 'steady_seconds', return_value=30), mock.patch.object(m, 'collect') as collect:
                m.statsd_once(state, 60)
                collect.assert_not_called()
            sample = self.sample(70)
            sample.update(schema_version=1, sampled_epoch=1700000060, codex={}, collector={}, quality='ok')
            with mock.patch.object(m, 'steady_seconds', return_value=70), mock.patch.object(m, 'collect', return_value=sample) as collect, mock.patch.object(m.socket, 'socket'):
                m.statsd_once(state, 60)
                self.assertEqual(collect.call_args.args[0]['monotonic_seconds'], 10)
                self.assertEqual(m.load_previous(state)['monotonic_seconds'], 70)

    def test_clock_is_shared_across_independent_python_processes(self):
        before = m.steady_seconds()
        other = float(subprocess.check_output([sys.executable, '-c', 'import time;print(time.clock_gettime(time.CLOCK_MONOTONIC_RAW))'], timeout=3))
        after = m.steady_seconds()
        self.assertLessEqual(before, other)
        self.assertLessEqual(other, after)

    def test_previous_clock_type_cannot_be_mixed(self):
        old, new = self.sample(), self.sample(120)
        old.pop('monotonic_clock')
        m.add_rates(new, old)
        self.assertIsNone(new['host']['swap_in_mib_s'])
        self.assertTrue(new['errors'])


if __name__ == '__main__':
    unittest.main()
