import contextlib
import importlib.util
import io
import json
import os
import signal
import shutil
import subprocess
import sys
import tempfile
import textwrap
import time
import unittest
from fractions import Fraction
from pathlib import Path
from unittest import mock


MODULE_PATH = Path(__file__).parents[1] / "scripts" / "media-normalizer.py"
SPEC = importlib.util.spec_from_file_location("media_normalizer", MODULE_PATH)
media_normalizer = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
sys.modules[SPEC.name] = media_normalizer
SPEC.loader.exec_module(media_normalizer)


def hvcc(*nal_types):
    data = bytearray(23)
    data[0] = 1
    data[22] = len(nal_types)
    for nal_type in nal_types:
        data.append(nal_type)
        data.extend((0, 1))
        data.extend((0, 1))
        data.append(1)
    return bytes(data)


def hexdump(data):
    lines = []
    for offset in range(0, len(data), 16):
        chunk = data[offset : offset + 16]
        groups = " ".join(chunk[index : index + 2].hex() for index in range(0, len(chunk), 2))
        lines.append("%08x: %-39s  ................" % (offset, groups))
    return "\n".join(lines)


def video_stream(codec="hevc", rate="24000/1001", complete=True, **updates):
    data = hvcc(32, 33, 34) if complete else hvcc()
    stream = {
        "index": 0,
        "codec_type": "video",
        "codec_name": codec,
        "width": 3840,
        "height": 2160,
        "pix_fmt": "yuv420p10le",
        "bits_per_raw_sample": "10",
        "avg_frame_rate": rate,
        "r_frame_rate": rate,
        "color_primaries": "bt709",
        "color_transfer": "bt709",
        "color_space": "bt709",
        "color_range": "tv",
        "extradata": hexdump(data),
        "extradata_size": len(data),
        "disposition": {},
        "tags": {},
    }
    stream.update(updates)
    return stream


def media_probe(video=None, *, duration=120.0, size=300_000_000, extras=None):
    streams = [video or video_stream()]
    streams.extend(extras or [])
    return {
        "streams": streams,
        "format": {
            "duration": str(duration),
            "size": str(size),
            "bit_rate": str(int(size * 8 / duration)),
        },
        "chapters": [],
    }


def settings(root):
    root = Path(root)
    return media_normalizer.Settings(
        state_path=root / "runtime/state.json",
        lock_path=root / "runtime/lock",
        media_mutation_lock_path=root / "runtime/media-mutation.lock",
        rollback_root=root / "rollback",
        ffmpeg="ffmpeg",
        ffprobe="ffprobe",
        sonarr_url="http://sonarr",
        sonarr_api_key="sonarr-secret",
        radarr_url="http://radarr",
        radarr_api_key="radarr-secret",
        tautulli_url="http://tautulli",
        tautulli_config=root / "tautulli.ini",
        plex_plist=root / "plex.plist",
        library_root=root,
        stable_age_seconds=0,
        settle_seconds=0,
        quality_window_seconds=1,
    )


class HvccTests(unittest.TestCase):
    def test_complete_hvcc_requires_vps_sps_and_pps(self):
        self.assertEqual(
            {"vps": 1, "sps": 1, "pps": 1},
            media_normalizer.hvcc_parameter_sets(hvcc(32, 33, 34)),
        )

    def test_empty_hvcc_is_incomplete(self):
        complete, missing = media_normalizer.hvcc_status(video_stream(complete=False))
        self.assertFalse(complete)
        self.assertEqual(["VPS", "SPS", "PPS"], missing)

    def test_each_parameter_set_is_individually_required(self):
        for omitted, expected in ((32, "VPS"), (33, "SPS"), (34, "PPS")):
            with self.subTest(omitted=omitted):
                present = [item for item in (32, 33, 34) if item != omitted]
                complete, missing = media_normalizer.hvcc_status(
                    video_stream(extradata=hexdump(hvcc(*present)))
                )
                self.assertFalse(complete)
                self.assertEqual([expected], missing)

    def test_truncated_hvcc_is_unknown(self):
        stream = video_stream(extradata=hexdump(b"\x01\x02"), extradata_size=2)
        self.assertIsNone(media_normalizer.hvcc_status(stream)[0])


class DetectionPolicyTests(unittest.TestCase):
    def test_supported_hfr_mappings(self):
        cases = {
            Fraction(48, 1): Fraction(24, 1),
            Fraction(50, 1): Fraction(25, 1),
            Fraction(60000, 1001): Fraction(30000, 1001),
            Fraction(60, 1): Fraction(30, 1),
        }
        for source, expected in cases.items():
            with self.subTest(source=source):
                self.assertEqual(expected, media_normalizer.hfr_target(source))

    def test_other_hfr_is_report_only(self):
        probe = media_probe(video_stream(rate="100/3"))
        report = media_normalizer.classify_media(Path("episode.mkv"), probe)
        self.assertEqual("report_only_unsupported_hfr", report["decision"])

    def test_hdr_is_never_selected_for_normalization(self):
        probe = media_probe(
            video_stream(
                complete=False,
                color_transfer="smpte2084",
                color_primaries="bt2020",
            )
        )
        report = media_normalizer.classify_media(Path("episode.mkv"), probe)
        self.assertEqual("report_only_hdr", report["decision"])
        self.assertIn("incomplete_hvcc", report["signatures"])

    def test_av1_sdr_is_normalizable(self):
        stream = video_stream(codec="av1", extradata="", extradata_size=0)
        report = media_normalizer.classify_media(Path("episode.mkv"), media_probe(stream))
        self.assertEqual("normalize", report["decision"])

    def test_resolution_bitrate_ranges_and_clamping(self):
        self.assertEqual((20_000_000, 35_000_000), media_normalizer.bitrate_range(3840, 2160))
        self.assertEqual((8_000_000, 15_000_000), media_normalizer.bitrate_range(1920, 1080))
        self.assertEqual((5_000_000, 10_000_000), media_normalizer.bitrate_range(1280, 720))
        probe = media_probe(size=100_000_000)
        self.assertEqual((20_000_000, 35_000_000), media_normalizer.select_bitrate(probe))

    def test_bitrate_retry_is_capped(self):
        self.assertEqual(12_500_000, media_normalizer.retry_bitrate(10_000_000, 15_000_000))
        self.assertEqual(15_000_000, media_normalizer.retry_bitrate(14_000_000, 15_000_000))
        self.assertIsNone(media_normalizer.retry_bitrate(15_000_000, 15_000_000))


class StateAndLoggingTests(unittest.TestCase):
    def test_signals_stop_admitted_child_before_unlocking_even_when_repeated(self):
        """A real short child proves cleanup survives launchd TERM and repeated INT."""
        for initial_signal in (signal.SIGTERM, signal.SIGINT):
            with self.subTest(signal=initial_signal), tempfile.TemporaryDirectory() as folder:
                root = Path(folder)
                child = root / "ffmpeg"
                child.write_text("#!" + sys.executable + "\n" + textwrap.dedent('''\
                    import os, signal, time
                    from pathlib import Path
                    root = Path(os.environ["SIGNAL_FIXTURE_ROOT"])
                    stopping = False
                    def stop(*_):
                        global stopping
                        stopping = True
                    signal.signal(signal.SIGTERM, stop)
                    (root / "child.pid").write_text(str(os.getpid()))
                    end = time.monotonic() + 5
                    while not stopping and time.monotonic() < end:
                        time.sleep(.01)
                    (root / "child.stopping").touch()
                    time.sleep(.3)
                    (root / "child.stopped").touch()
                    '''))
                child.chmod(0o755)
                wrapper = root / "client.py"
                wrapper.write_text(textwrap.dedent('''\
                    import os, signal, subprocess, sys, time
                    from pathlib import Path
                    root = Path(os.environ["SIGNAL_FIXTURE_ROOT"])
                    stopping = False
                    def stop(*_):
                        global stopping
                        stopping = True
                    signal.signal(signal.SIGTERM, stop)
                    signal.signal(signal.SIGINT, stop)
                    (root / "wrapper.pid").write_text(str(os.getpid()))
                    child = subprocess.Popen(sys.argv[sys.argv.index("--") + 1:], start_new_session=True)
                    while child.poll() is None and not stopping:
                        time.sleep(.01)
                    if child.poll() is None:
                        os.killpg(child.pid, signal.SIGTERM)
                    child.wait()
                    (root / "lease.released").touch()
                    '''))
                token = root / "token"
                token.write_text("fixture only")
                harness = textwrap.dedent('''\
                    import importlib.util, os, sys
                    from pathlib import Path
                    from types import SimpleNamespace
                    spec = importlib.util.spec_from_file_location("normalizer_fixture", sys.argv[1])
                    module = importlib.util.module_from_spec(spec)
                    sys.modules[spec.name] = module
                    spec.loader.exec_module(module)
                    root = Path(os.environ["SIGNAL_FIXTURE_ROOT"])
                    module.load_settings = lambda: SimpleNamespace(
                        lock_path=root / "process.lock", media_mutation_lock_path=root / "mutation.lock",
                        library_root=root)
                    def poll(*args, **kwargs):
                        module.run_process([str(root / "ffmpeg"), "-f", "null", "-"], timeout=10)
                        return 0
                    module.run_poll = poll
                    raise SystemExit(module.main(["poll", "--apply"]))
                    ''')
                env = dict(os.environ, SIGNAL_FIXTURE_ROOT=str(root),
                           MEDIA_NORMALIZER_MEMORY_COORDINATOR_URL="http://fixture.invalid",
                           MEDIA_NORMALIZER_MEMORY_CLIENT_FILE=str(wrapper),
                           MEDIA_NORMALIZER_MEMORY_TOKEN_FILE=str(token))
                process = subprocess.Popen([sys.executable, "-c", harness, str(MODULE_PATH)],
                                           env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                           start_new_session=True)
                try:
                    deadline = time.monotonic() + 3
                    while not (root / "child.pid").exists() and time.monotonic() < deadline:
                        time.sleep(.01)
                    self.assertTrue((root / "child.pid").exists())
                    self.assertTrue((root / "mutation.lock").is_dir())
                    process.send_signal(initial_signal)
                    deadline = time.monotonic() + 2
                    while not (root / "child.stopping").exists() and time.monotonic() < deadline:
                        time.sleep(.01)
                    self.assertTrue((root / "child.stopping").exists())
                    self.assertIsNone(process.poll(), "parent must wait for the physical child")
                    self.assertTrue((root / "mutation.lock").is_dir())
                    process.send_signal(signal.SIGTERM)
                    process.send_signal(signal.SIGINT)
                    process.communicate(timeout=3)
                    self.assertTrue((root / "child.stopped").exists())
                    self.assertTrue((root / "lease.released").exists())
                    self.assertFalse((root / "mutation.lock").exists())
                    expected = 143 if initial_signal == signal.SIGTERM else -signal.SIGINT
                    self.assertEqual(process.returncode, expected)
                    with media_normalizer.process_lock(root / "process.lock"):
                        pass
                finally:
                    # A red regression must not leave its fixture wrapper/child behind.
                    with contextlib.suppress(ProcessLookupError):
                        os.killpg(process.pid, signal.SIGTERM)
                    try:
                        process.communicate(timeout=3)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.communicate()

    def test_shared_media_mutation_lock_is_atomic_and_removed_on_exit(self):
        with tempfile.TemporaryDirectory() as root:
            path = Path(root) / "locks/media.lock"
            with media_normalizer.media_mutation_lock(path):
                self.assertTrue(path.is_dir())
                with self.assertRaises(media_normalizer.MediaMutationLockBusy):
                    with media_normalizer.media_mutation_lock(path):
                        pass
            self.assertFalse(path.exists())

    def test_stale_shared_media_mutation_lock_is_recovered(self):
        with tempfile.TemporaryDirectory() as root:
            path = Path(root) / "locks/media.lock"
            path.mkdir(parents=True)
            old = time.time() - 10
            os.utime(path, (old, old))
            with media_normalizer.media_mutation_lock(path, stale_seconds=1):
                self.assertTrue(path.is_dir())
            self.assertFalse(path.exists())

    def test_atomic_state_recovers_from_backup(self):
        with tempfile.TemporaryDirectory() as root:
            path = Path(root) / "state.json"
            first = media_normalizer.empty_state()
            first["watermarks"]["sonarr"] = "first"
            second = media_normalizer.empty_state()
            second["watermarks"]["sonarr"] = "second"
            media_normalizer.atomic_write_json(path, first)
            media_normalizer.atomic_write_json(path, second)
            path.write_text("not json", encoding="utf-8")
            self.assertEqual("first", media_normalizer.load_state(path)["watermarks"]["sonarr"])

    def test_state_rejects_corrupt_primary_and_backup(self):
        with tempfile.TemporaryDirectory() as root:
            path = Path(root) / "state.json"
            path.write_text("bad", encoding="utf-8")
            path.with_suffix(".json.bak").write_text("also bad", encoding="utf-8")
            with self.assertRaises(media_normalizer.NormalizerError):
                media_normalizer.load_state(path)

    def test_token_redaction(self):
        text = "sonarr-secret api_key=radarr-secret X-Plex-Token=plex-secret"
        result = media_normalizer.redact_text(text, ["sonarr-secret", "radarr-secret"])
        self.assertNotIn("secret", result)
        self.assertEqual(3, result.count("[REDACTED]"))

    def test_subprocess_exception_is_wrapped(self):
        with mock.patch.object(subprocess, "run", side_effect=OSError("gone")):
            with self.assertRaises(media_normalizer.NormalizerError):
                media_normalizer.run_process(["missing"])

    def test_ffmpeg_capture_and_all_input_threads_are_bounded(self):
        process = mock.Mock(returncode=0)
        process.stdout = io.BytesIO(b"a" * 200000)
        process.stderr = io.BytesIO(b"b" * 200000 +
                                   b"\n[Parsed_ssim] SSIM Y:0.99 All:0.990000\n"
                                   b"[Parsed_psnr] PSNR y:42.1 average:42.100000 min:40.0\n")
        process.poll.return_value = 0
        with mock.patch.dict(os.environ, {"MEDIA_NORMALIZER_MEMORY_COORDINATOR_URL": ""}), \
             mock.patch.object(subprocess, "Popen", return_value=process) as launch:
            result = media_normalizer.run_process(["ffmpeg", "-i", "first", "-i", "second", "-f", "null", "-"], timeout=180)
        self.assertLessEqual(len(result.stdout), 65536)
        self.assertLessEqual(len(result.stderr), 65536)
        self.assertEqual(media_normalizer.metric_value(result.stderr, "ssim"), 0.99)
        self.assertEqual(media_normalizer.metric_value(result.stderr, "psnr"), 42.1)
        command = launch.call_args.args[0]
        self.assertEqual(command.count("-threads"), 3)
        self.assertIn("-nostats", command)
        for index, value in enumerate(command):
            if value == "-threads":
                self.assertEqual(command[index + 1], "2")

    def test_ffmpeg_admission_wait_is_separate_from_encode_timeout(self):
        with tempfile.TemporaryDirectory() as root:
            client, token = Path(root) / "client.py", Path(root) / "token"
            client.write_text("# fixture")
            token.write_text("fixture only")
            process = mock.Mock(returncode=0)
            process.stdout, process.stderr = io.BytesIO(b""), io.BytesIO(b"")
            process.poll.return_value = 0
            with mock.patch.dict(os.environ, {"MEDIA_NORMALIZER_MEMORY_COORDINATOR_URL": "http://127.0.0.1:8765",
                    "MEDIA_NORMALIZER_MEMORY_CLIENT_FILE": str(client), "MEDIA_NORMALIZER_MEMORY_TOKEN_FILE": str(token),
                    "MEDIA_NORMALIZER_MEMORY_WAIT_SECONDS": "900"}), \
                 mock.patch.object(subprocess, "Popen", return_value=process) as launch:
                media_normalizer.run_process(["ffmpeg", "-i", "fixture", "-f", "null", "-"], timeout=180)
            command = launch.call_args.args[0]
            self.assertIn("--native", command)
            self.assertEqual(command[command.index("--run-timeout") + 1], "180")
            self.assertEqual(command[command.index("--wait-timeout") + 1], "60")
            process.wait.assert_called_once_with(timeout=300)

    def test_ffmpeg_missing_admission_credentials_fails_closed(self):
        with mock.patch.dict(os.environ, {"MEDIA_NORMALIZER_MEMORY_COORDINATOR_URL": "http://127.0.0.1:8765",
                "MEDIA_NORMALIZER_MEMORY_CLIENT_FILE": "/missing/client.py", "MEDIA_NORMALIZER_MEMORY_TOKEN_FILE": "/missing/token"}), \
             mock.patch.object(subprocess, "Popen") as launch:
            with self.assertRaises(media_normalizer.NormalizerError):
                media_normalizer.run_process(["ffmpeg", "-i", "fixture", "-f", "null", "-"], timeout=180)
            launch.assert_not_called()

    def test_first_real_poll_establishes_watermark_without_enumerating_media(self):
        with tempfile.TemporaryDirectory() as root:
            config = settings(root)
            sonarr = mock.Mock()
            radarr = mock.Mock()
            with mock.patch.object(
                media_normalizer, "arr_clients", return_value={"sonarr": sonarr, "radarr": radarr}
            ):
                self.assertEqual(0, media_normalizer.run_poll(config, dry_run=False))
            state = media_normalizer.load_state(config.state_path)
            self.assertEqual({"sonarr", "radarr"}, set(state["watermarks"]))
            sonarr.managed_media.assert_not_called()
            radarr.managed_media.assert_not_called()

    def test_dry_run_does_not_create_first_watermark(self):
        with tempfile.TemporaryDirectory() as root:
            config = settings(root)
            with mock.patch.object(
                media_normalizer,
                "arr_clients",
                return_value={"sonarr": mock.Mock(), "radarr": mock.Mock()},
            ):
                self.assertEqual(0, media_normalizer.run_poll(config, dry_run=True))
            self.assertFalse(config.state_path.exists())

    def test_failed_future_import_is_recorded_for_retry(self):
        with tempfile.TemporaryDirectory() as root:
            config = settings(root)
            state = media_normalizer.empty_state()
            state["watermarks"] = {
                "sonarr": "2000-01-01T00:00:00Z",
                "radarr": "2000-01-01T00:00:00Z",
            }
            media_normalizer.atomic_write_json(config.state_path, state)
            item = media_normalizer.ManagedMedia(
                service="sonarr",
                owner_id=71,
                file_id=1,
                path=Path(root) / "episode.mkv",
                date_added="2001-01-01T00:00:00Z",
                season_number=4,
            )
            sonarr = mock.Mock()
            sonarr.managed_media.return_value = [item]
            radarr = mock.Mock()
            radarr.managed_media.return_value = []
            with (
                mock.patch.object(
                    media_normalizer,
                    "arr_clients",
                    return_value={"sonarr": sonarr, "radarr": radarr},
                ),
                mock.patch.object(
                    media_normalizer,
                    "normalize_one",
                    side_effect=media_normalizer.SafetyError("active playback"),
                ),
            ):
                self.assertEqual(1, media_normalizer.run_poll(config, dry_run=False))
            record = media_normalizer.load_state(config.state_path)["files"][str(item.path)]
            self.assertEqual("blocked", record["status"])
            self.assertTrue(record["automatic"])

    def test_quality_gate_exhaustion_is_recorded_as_quality_rejected(self):
        with tempfile.TemporaryDirectory() as root:
            config = settings(root)
            state = media_normalizer.empty_state()
            state["watermarks"] = {
                "sonarr": "2000-01-01T00:00:00Z",
                "radarr": "2000-01-01T00:00:00Z",
            }
            media_normalizer.atomic_write_json(config.state_path, state)
            item = media_normalizer.ManagedMedia(
                service="sonarr",
                owner_id=71,
                file_id=1,
                path=Path(root) / "episode.mkv",
                date_added="2001-01-01T00:00:00Z",
                season_number=4,
            )
            sonarr = mock.Mock()
            sonarr.managed_media.return_value = [item]
            radarr = mock.Mock()
            radarr.managed_media.return_value = []
            with (
                mock.patch.object(
                    media_normalizer,
                    "arr_clients",
                    return_value={"sonarr": sonarr, "radarr": radarr},
                ),
                mock.patch.object(
                    media_normalizer,
                    "normalize_one",
                    side_effect=media_normalizer.QualityGateError(
                        "quality gate failed: mean SSIM 0.99170, mean PSNR 38.52 dB"
                    ),
                ),
            ):
                self.assertEqual(1, media_normalizer.run_poll(config, dry_run=False))
            record = media_normalizer.load_state(config.state_path)["files"][str(item.path)]
            self.assertEqual("quality_rejected", record["status"])

    def test_quality_rejected_record_is_not_selected_for_retry_or_backfill(self):
        item = media_normalizer.ManagedMedia(
            service="sonarr",
            owner_id=71,
            file_id=1,
            path=Path("/tv/rejected.mkv"),
            date_added="2001-08-01T00:00:00Z",
        )
        state = media_normalizer.empty_state()
        state["files"][str(item.path)] = {
            "automatic": True,
            "status": "quality_rejected",
        }
        priority, unseen = media_normalizer.select_poll_work(
            [item],
            state,
            {"sonarr": "2001-08-07T16:09:08Z"},
            media_normalizer.parse_datetime("2001-08-08T12:00:00Z"),
        )
        self.assertEqual([], priority)
        self.assertEqual([], unseen)

    def test_all_managed_ignores_normalizer_temp_files(self):
        real = media_normalizer.ManagedMedia(
            service="sonarr",
            owner_id=71,
            file_id=1,
            path=Path("/tv/Episode.mkv"),
            date_added="2001-08-01T00:00:00Z",
        )
        temp = media_normalizer.ManagedMedia(
            service="sonarr",
            owner_id=71,
            file_id=2,
            path=Path("/tv/.Episode.mkv.media-normalizer.12345.tmp.mkv"),
            date_added="2001-08-02T00:00:00Z",
        )
        client = mock.Mock()
        client.managed_media.return_value = [real, temp]
        managed = media_normalizer.all_managed({"sonarr": client})
        self.assertEqual([real.path], [item.path for item in managed])

    def test_pre_watermark_unseen_file_is_backfilled(self):
        with tempfile.TemporaryDirectory() as root:
            config = settings(root)
            state = media_normalizer.empty_state()
            state["watermarks"] = {
                "sonarr": "2001-08-07T16:09:08Z",
                "radarr": "2001-08-07T16:09:08Z",
            }
            media_normalizer.atomic_write_json(config.state_path, state)
            item = media_normalizer.ManagedMedia(
                service="sonarr",
                owner_id=71,
                file_id=1,
                path=Path(root) / "episode.mkv",
                date_added="2001-08-04T22:54:00Z",
                season_number=4,
            )
            sonarr = mock.Mock()
            sonarr.managed_media.return_value = [item]
            radarr = mock.Mock()
            radarr.managed_media.return_value = []

            def fake_normalize(path, settings, state, **kwargs):
                state["files"][str(path)] = {"status": "normalized"}
                return {"status": "normalized"}

            with (
                mock.patch.object(
                    media_normalizer,
                    "arr_clients",
                    return_value={"sonarr": sonarr, "radarr": radarr},
                ),
                mock.patch.object(
                    media_normalizer, "normalize_one", side_effect=fake_normalize
                ) as normalize,
            ):
                self.assertEqual(0, media_normalizer.run_poll(config, dry_run=False))
            normalize.assert_called_once()
            record = media_normalizer.load_state(config.state_path)["files"][str(item.path)]
            self.assertEqual("normalized", record["status"])
            self.assertTrue(record["automatic"])

    def test_backfill_encode_limit_leaves_remaining_unseen(self):
        with tempfile.TemporaryDirectory() as root:
            config = settings(root)
            config.backfill_encode_limit = 1
            state = media_normalizer.empty_state()
            state["watermarks"] = {
                "sonarr": "2001-08-07T16:09:08Z",
                "radarr": "2001-08-07T16:09:08Z",
            }
            media_normalizer.atomic_write_json(config.state_path, state)
            first = media_normalizer.ManagedMedia(
                service="sonarr",
                owner_id=71,
                file_id=1,
                path=Path(root) / "a.mkv",
                date_added="2001-08-04T22:54:00Z",
            )
            second = media_normalizer.ManagedMedia(
                service="sonarr",
                owner_id=71,
                file_id=2,
                path=Path(root) / "b.mkv",
                date_added="2001-08-04T22:54:01Z",
            )
            sonarr = mock.Mock()
            sonarr.managed_media.return_value = [first, second]
            radarr = mock.Mock()
            radarr.managed_media.return_value = []

            def fake_normalize(path, settings, state, **kwargs):
                state["files"][str(path)] = {"status": "normalized"}
                return {"status": "normalized"}

            with (
                mock.patch.object(
                    media_normalizer,
                    "arr_clients",
                    return_value={"sonarr": sonarr, "radarr": radarr},
                ),
                mock.patch.object(
                    media_normalizer, "normalize_one", side_effect=fake_normalize
                ) as normalize,
            ):
                self.assertEqual(0, media_normalizer.run_poll(config, dry_run=False))
            self.assertEqual([second.path], [call.args[0] for call in normalize.call_args_list])
            saved = media_normalizer.load_state(config.state_path)["files"]
            self.assertIn(str(second.path), saved)
            self.assertNotIn(str(first.path), saved)

    def test_replaced_file_is_reinspected_even_when_older_than_watermark(self):
        with tempfile.TemporaryDirectory() as root:
            config = settings(root)
            path = Path(root) / "episode.mkv"
            path.write_bytes(b"current-bytes")
            state = media_normalizer.empty_state()
            state["watermarks"] = {
                "sonarr": "2001-08-07T16:09:08Z",
                "radarr": "2001-08-07T16:09:08Z",
            }
            state["files"][str(path)] = {
                "automatic": True,
                "file_identity": "0:0:1",
                "status": "normalized",
            }
            media_normalizer.atomic_write_json(config.state_path, state)
            item = media_normalizer.ManagedMedia(
                service="sonarr",
                owner_id=71,
                file_id=1,
                path=path,
                date_added="2001-08-04T22:54:00Z",
            )
            sonarr = mock.Mock()
            sonarr.managed_media.return_value = [item]
            radarr = mock.Mock()
            radarr.managed_media.return_value = []

            def fake_normalize(path, settings, state, **kwargs):
                state["files"][str(path)] = {"status": "compatible"}
                return {"status": "compatible"}

            with (
                mock.patch.object(
                    media_normalizer,
                    "arr_clients",
                    return_value={"sonarr": sonarr, "radarr": radarr},
                ),
                mock.patch.object(
                    media_normalizer, "normalize_one", side_effect=fake_normalize
                ) as normalize,
            ):
                self.assertEqual(0, media_normalizer.run_poll(config, dry_run=False))
            normalize.assert_called_once()

    def test_select_poll_work_keeps_new_imports_out_of_backfill(self):
        older = media_normalizer.ManagedMedia(
            service="sonarr",
            owner_id=71,
            file_id=1,
            path=Path("/tv/old.mkv"),
            date_added="2001-08-04T00:00:00Z",
        )
        newer = media_normalizer.ManagedMedia(
            service="sonarr",
            owner_id=71,
            file_id=2,
            path=Path("/tv/new.mkv"),
            date_added="2001-08-08T00:00:00Z",
        )
        state = media_normalizer.empty_state()
        priority, unseen = media_normalizer.select_poll_work(
            [older, newer],
            state,
            {"sonarr": "2001-08-07T16:09:08Z"},
            media_normalizer.parse_datetime("2001-08-08T12:00:00Z"),
        )
        self.assertEqual([newer], priority)
        self.assertEqual([older], unseen)

        older_newer_first, _unseen = media_normalizer.select_poll_work(
            [
                older,
                media_normalizer.ManagedMedia(
                    service="sonarr",
                    owner_id=71,
                    file_id=3,
                    path=Path("/tv/older.mkv"),
                    date_added="2000-01-01T00:00:00Z",
                ),
            ],
            state,
            {"sonarr": "2001-08-07T16:09:08Z"},
            media_normalizer.parse_datetime("2001-08-08T12:00:00Z"),
        )
        self.assertEqual([], older_newer_first)
        self.assertEqual(
            [older.path, Path("/tv/older.mkv")],
            [item.path for item in _unseen],
        )

    def test_season_continues_after_a_safety_error(self):
        with tempfile.TemporaryDirectory() as root:
            config = settings(root)
            first = media_normalizer.ManagedMedia(
                service="sonarr",
                owner_id=71,
                file_id=1,
                path=Path(root) / "e05.mkv",
                date_added="2001-08-04T22:54:00Z",
                season_number=4,
            )
            second = media_normalizer.ManagedMedia(
                service="sonarr",
                owner_id=71,
                file_id=2,
                path=Path(root) / "e06.mkv",
                date_added="2001-08-04T22:54:01Z",
                season_number=4,
            )
            sonarr = mock.Mock()
            sonarr.sonarr_season.return_value = [first, second]
            with (
                mock.patch.object(
                    media_normalizer,
                    "arr_clients",
                    return_value={"sonarr": sonarr, "radarr": mock.Mock()},
                ),
                mock.patch.object(
                    media_normalizer,
                    "normalize_one",
                    side_effect=[
                        media_normalizer.SafetyError("file is open according to lsof"),
                        {"status": "normalized"},
                    ],
                ) as normalize,
            ):
                self.assertEqual(1, media_normalizer.run_normalize_season(config, 71, 4, dry_run=False))
            self.assertEqual(2, normalize.call_count)
            record = media_normalizer.load_state(config.state_path)["files"][str(first.path)]
            self.assertTrue(record["automatic"])


class ArrTests(unittest.TestCase):
    def test_sonarr_media_polling_translates_container_paths(self):
        client = media_normalizer.ArrClient("sonarr", "http://sonarr", "key")

        def fake_get(endpoint, query=None):
            if endpoint == "series":
                return [{"id": 71, "path": "/data/Media/tv/Show"}]
            return [
                {
                    "id": 9,
                    "seriesId": 71,
                    "path": "/data/Media/tv/Show/Season 4/Episode.mkv",
                    "dateAdded": "2001-08-07T01:02:03Z",
                    "seasonNumber": 4,
                }
            ]

        with mock.patch.object(client, "get", side_effect=fake_get), mock.patch.dict(
            os.environ, {"MEDIA_NORMALIZER_PATH_MAPPINGS": "/data=/example/library"}
        ):
            result = client.managed_media()
        self.assertEqual(1, len(result))
        self.assertEqual(
            Path("/example/library/Media/tv/Show/Season 4/Episode.mkv"), result[0].path
        )



class ReplacementSafetyTests(unittest.TestCase):
    def test_hardlinked_replacement_never_changes_download_inode_or_bytes(self):
        with tempfile.TemporaryDirectory() as root:
            root = Path(root)
            library = root / "Episode.mkv"
            download = root / "Download.mkv"
            library.write_bytes(b"original torrent payload")
            os.link(library, download)
            original_inode = download.stat().st_ino
            output = media_normalizer.temporary_output(library)
            output.write_bytes(b"normalized library payload")
            media_normalizer.replace_atomically(output, library)
            self.assertEqual(b"original torrent payload", download.read_bytes())
            self.assertEqual(original_inode, download.stat().st_ino)
            self.assertEqual(b"normalized library payload", library.read_bytes())
            self.assertNotEqual(original_inode, library.stat().st_ino)

    def test_rollback_hardlink_is_atomic_source_preservation(self):
        with tempfile.TemporaryDirectory() as root:
            root = Path(root)
            source = root / "source.mkv"
            destination = root / "rollback" / "source.mkv"
            source.write_bytes(b"payload")
            result = media_normalizer.create_rollback(source, destination)
            self.assertEqual("hardlink", result["method"])
            self.assertEqual(source.stat().st_ino, destination.stat().st_ino)

    def test_insufficient_space_is_blocked(self):
        usage = mock.Mock(free=99)
        with mock.patch.object(media_normalizer.shutil, "disk_usage", return_value=usage):
            with self.assertRaises(media_normalizer.SafetyError):
                media_normalizer.require_free_space(Path("/tmp"), 100)

    def test_lsof_activity_is_detected(self):
        result = subprocess.CompletedProcess([], 0, stdout="p123\n", stderr="")
        with mock.patch.object(media_normalizer, "run_process", return_value=result):
            self.assertTrue(media_normalizer.lsof_active(Path("episode.mkv")))

    def test_hardlinked_open_file_is_blocked_by_lsof(self):
        with tempfile.TemporaryDirectory() as root:
            root = Path(root)
            library = root / "Episode.mkv"
            download = root / "Download.mkv"
            library.write_bytes(b"payload")
            os.link(library, download)
            config = settings(root)
            with (
                mock.patch.object(media_normalizer, "lsof_active", return_value=True),
                mock.patch.object(media_normalizer, "tautulli_active", return_value=False),
                self.assertRaises(media_normalizer.SafetyError),
            ):
                media_normalizer.require_inactive(library, config)

    def test_unique_open_file_is_blocked_by_lsof(self):
        with tempfile.TemporaryDirectory() as root:
            path = Path(root) / "Episode.mkv"
            path.write_bytes(b"payload")
            config = settings(root)
            with (
                mock.patch.object(media_normalizer, "lsof_active", return_value=True),
                mock.patch.object(media_normalizer, "tautulli_active", return_value=False),
            ):
                with self.assertRaises(media_normalizer.SafetyError):
                    media_normalizer.require_inactive(path, config)

    def test_playback_is_blocked_even_when_the_file_is_hardlinked(self):
        with tempfile.TemporaryDirectory() as root:
            root = Path(root)
            library = root / "Episode.mkv"
            download = root / "Download.mkv"
            library.write_bytes(b"payload")
            os.link(library, download)
            config = settings(root)
            with mock.patch.object(media_normalizer, "tautulli_active", return_value=True):
                with self.assertRaises(media_normalizer.SafetyError) as raised:
                    media_normalizer.require_inactive(library, config)
            self.assertIn("Tautulli", str(raised.exception))

    def test_encode_command_preserves_non_default_subtitle_disposition(self):
        subtitle = {
            "index": 1,
            "codec_type": "subtitle",
            "codec_name": "subrip",
            "disposition": {
                "default": 0,
                "original": 1,
                "hearing_impaired": 1,
                "dub": 0,
            },
            "tags": {"language": "eng", "title": "English (SDH)"},
        }
        probe = media_probe(video_stream(complete=False), extras=[subtitle])
        command = media_normalizer.encode_command(
            Path("in.mkv"),
            Path("out.mkv"),
            probe,
            {"signatures": ["incomplete_hvcc"], "frame_rate": "24000/1001"},
            8_000_000,
            "ffmpeg",
        )
        index = command.index("-disposition:s:0")
        self.assertEqual("original+hearing_impaired", command[index + 1])
        self.assertEqual("passthrough", command[command.index("-default_mode") + 1])

    def test_unexpected_temporary_path_cannot_replace_library(self):
        with tempfile.TemporaryDirectory() as root:
            destination = Path(root) / "episode.mkv"
            output = Path(root) / "other.mkv"
            destination.touch()
            output.touch()
            with self.assertRaises(media_normalizer.SafetyError):
                media_normalizer.replace_atomically(output, destination)

    def test_mp4_encode_keeps_container_and_cover_art(self):
        cover = {
            "index": 2,
            "codec_type": "video",
            "codec_name": "mjpeg",
            "disposition": {"attached_pic": 1},
        }
        source = Path("special.mp4")
        output = media_normalizer.temporary_output(source)
        command = media_normalizer.encode_command(
            source,
            output,
            media_probe(video_stream(codec="h264", rate="50/1"), extras=[cover]),
            {"signatures": ["hfr"], "frame_rate": "50/1"},
            8_000_000,
            "ffmpeg",
        )
        self.assertEqual(".mp4", output.suffix)
        self.assertEqual("mp4", command[command.index("-f") + 1])
        self.assertEqual("hvc1", command[command.index("-tag:v:0") + 1])
        self.assertEqual("+faststart", command[command.index("-movflags") + 1])
        self.assertEqual("attached_pic", command[command.index("-disposition:v:1") + 1])
        self.assertNotIn("-default_mode", command)

    def test_mp4_reaches_encoder_and_preserves_source_on_interruption(self):
        with tempfile.TemporaryDirectory() as root:
            path = Path(root) / "special.mp4"
            path.write_bytes(b"original")
            config = settings(root)
            state = media_normalizer.empty_state()
            report = {"decision": "normalize", "signatures": ["hfr"], "frame_rate": "50/1"}
            with (
                mock.patch.object(media_normalizer, "inspect_path", return_value=report),
                mock.patch.object(media_normalizer, "probe_media", return_value=media_probe()),
                mock.patch.object(media_normalizer, "require_stable"),
                mock.patch.object(media_normalizer, "require_inactive"),
                mock.patch.object(media_normalizer, "require_free_space"),
                mock.patch.object(media_normalizer, "run_process", side_effect=KeyboardInterrupt),
            ):
                with self.assertRaises(KeyboardInterrupt):
                    media_normalizer.normalize_one(path, config, state, dry_run=False)
            self.assertEqual(b"original", path.read_bytes())
            record = state["files"][str(path.resolve())]
            self.assertEqual("interrupted", record["status"])
            self.assertEqual(b"original", Path(record["rollback"]["path"]).read_bytes())

    def test_idempotent_compatible_file_does_not_encode(self):
        with tempfile.TemporaryDirectory() as root:
            root = Path(root)
            path = root / "episode.mkv"
            path.touch()
            report = {"decision": "compatible", "signatures": [], "file_identity": "1:2:3"}
            state = media_normalizer.empty_state()
            with (
                mock.patch.object(media_normalizer, "inspect_path", return_value=report),
                mock.patch.object(media_normalizer, "run_process") as runner,
            ):
                result = media_normalizer.normalize_one(
                    path, settings(root), state, dry_run=False
                )
            self.assertEqual("compatible", result["status"])
            runner.assert_not_called()

    def test_interrupted_encode_keeps_original_and_records_rollback(self):
        with tempfile.TemporaryDirectory() as root:
            root = Path(root)
            path = root / "episode.mkv"
            path.write_bytes(b"original")
            config = settings(root)
            report = {
                "decision": "normalize",
                "signatures": ["incomplete_hvcc"],
                "frame_rate": "24000/1001",
                "file_identity": "1:2:3",
            }
            probe = media_probe(video_stream(complete=False))
            state = media_normalizer.empty_state()
            with (
                mock.patch.object(media_normalizer, "inspect_path", return_value=report),
                mock.patch.object(media_normalizer, "probe_media", return_value=probe),
                mock.patch.object(media_normalizer, "require_stable"),
                mock.patch.object(media_normalizer, "require_inactive"),
                mock.patch.object(media_normalizer, "require_free_space"),
                mock.patch.object(media_normalizer, "run_process", side_effect=KeyboardInterrupt),
            ):
                with self.assertRaises(KeyboardInterrupt):
                    media_normalizer.normalize_one(path, config, state, dry_run=False)
            self.assertEqual(b"original", path.read_bytes())
            self.assertEqual("interrupted", state["files"][str(path.resolve())]["status"])
            rollback = Path(state["files"][str(path.resolve())]["rollback"]["path"])
            self.assertEqual(b"original", rollback.read_bytes())


class StreamValidationTests(unittest.TestCase):
    def test_quality_windows_align_hfr_reference_and_output_frames(self):
        self.assertEqual(
            [0.0, 2.0, 4.04],
            media_normalizer.quality_windows(5.04, 1, Fraction(25)),
        )

    def test_quality_windows_preserve_ntsc_alignment_at_millisecond_precision(self):
        self.assertEqual(
            [0.0, 2.002, 4.004],
            media_normalizer.quality_windows(5.04, 1, Fraction(30000, 1001)),
        )

    def test_cover_art_must_survive_normalization(self):
        cover = {
            "codec_type": "video",
            "codec_name": "mjpeg",
            "disposition": {"attached_pic": 1},
        }
        source = media_probe(extras=[cover])
        output = media_probe()
        with self.assertRaises(media_normalizer.ValidationError):
            media_normalizer.compare_streams(
                source, output, {"signatures": [], "frame_rate": "24000/1001"}
            )

    @unittest.skipUnless(shutil.which("ffmpeg"), "ffmpeg is required for the timing regression")
    def test_lossless_hfr_conversions_compare_identical_frames(self):
        for rate, target in (("50/1", "25/1"), ("60000/1001", "30000/1001")):
            with self.subTest(rate=rate), tempfile.TemporaryDirectory() as root:
                config = settings(root)
                config.ffmpeg = shutil.which("ffmpeg")
                source, output = Path(root) / "source.mkv", Path(root) / "output.mkv"
                fixture = (
                    "nullsrc=s=64x64:r=%s:d=5.04,"
                    "geq=lum=mod(X*17+Y*13+N*37\\,256):cb=128:cr=128" % rate
                )
                media_normalizer.run_process(
                    [
                        config.ffmpeg, "-v", "error", "-f", "lavfi", "-i", fixture,
                        "-pix_fmt", "yuv420p", "-c:v", "ffv1", str(source),
                    ]
                )
                media_normalizer.run_process(
                    [
                        config.ffmpeg, "-v", "error", "-i", str(source),
                        "-vf", "fps=" + target, "-c:v", "ffv1", str(output),
                    ]
                )
                probe = media_probe(
                    video_stream(
                        codec="ffv1", rate=rate, pix_fmt="yuv420p", bits_per_raw_sample="8"
                    ),
                    duration=5.04,
                )
                metrics = media_normalizer.quality_metrics(
                    source,
                    output,
                    probe,
                    {"signatures": ["hfr"], "frame_rate": rate},
                    config,
                )
                for window in metrics["windows"]:
                    self.assertAlmostEqual(1.0, window["ssim"], places=6)

    def test_audio_subtitles_chapters_and_dispositions_are_preserved(self):
        audio = {
            "codec_type": "audio",
            "codec_name": "eac3",
            "tags": {"language": "eng"},
            "disposition": {"default": 1, "forced": 0},
        }
        subtitle = {
            "codec_type": "subtitle",
            "codec_name": "subrip",
            "tags": {"language": "eng"},
            "disposition": {"default": 0, "forced": 1},
        }
        source = media_probe(video_stream(complete=False), extras=[audio, subtitle])
        output = media_probe(video_stream(complete=True), extras=[dict(audio), dict(subtitle)])
        source["chapters"] = [{"id": 1}]
        output["chapters"] = [{"id": 2}]
        report = {
            "frame_rate": "24000/1001",
            "signatures": ["incomplete_hvcc"],
        }
        media_normalizer.compare_streams(source, output, report)

    def test_quality_gate_requires_both_thresholds(self):
        media_normalizer.validate_quality({"mean_ssim": 0.98, "mean_psnr": 40.0})
        media_normalizer.validate_quality({"mean_ssim": 0.98, "mean_psnr": 37.0})
        with self.assertRaises(media_normalizer.ValidationError):
            media_normalizer.validate_quality({"mean_ssim": 0.979, "mean_psnr": 50.0})
        with self.assertRaises(media_normalizer.ValidationError):
            media_normalizer.validate_quality({"mean_ssim": 0.99, "mean_psnr": 36.9})


if __name__ == "__main__":
    unittest.main()
