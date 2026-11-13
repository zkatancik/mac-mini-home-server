"""Run filesystem and supervisor checks with synthetic state and owned subprocesses."""

import argparse
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

HOST = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(HOST))

import docker_storage_location as storage
import startup_check_supervisor as supervisor


class StorageTests(unittest.TestCase):
    def test_rejects_ambiguous_paths(self):
        for value in ("relative", "/", "/base/../data", "//base/data"):
            with self.subTest(value=value), self.assertRaises(argparse.ArgumentTypeError):
                storage.absolute_path(value)
        for value in ("", ".", "/containers", "containers/../other"):
            with self.subTest(value=value), self.assertRaises(argparse.ArgumentTypeError):
                storage.relative_directory(value)

    def test_reads_bounded_docker_settings_without_following_links(self):
        with tempfile.TemporaryDirectory() as temporary:
            home = Path(temporary).resolve()
            directory = home / storage.SETTINGS_DIR
            directory.mkdir(parents=True)
            settings = directory / "settings-store.json"
            expected = {"AutoStart": False, "DataFolder": "/Volumes/Example/containers"}
            settings.write_text(json.dumps(expected))
            device = home.stat().st_dev
            with storage.absolute_directory(home) as fd:
                self.assertEqual(storage.read_docker_settings(fd, device, os.getuid()), expected)
                settings.chmod(0o666)
                with self.assertRaises(ValueError):
                    storage.read_docker_settings(fd, device, os.getuid())
                settings.chmod(0o600)
                settings.write_bytes(b" " * (storage.SETTINGS_LIMIT + 1))
                with self.assertRaises(ValueError):
                    storage.read_docker_settings(fd, device, os.getuid())
                settings.rename(settings.with_suffix(".bak"))
                settings.symlink_to(settings.with_suffix(".bak"))
                with self.assertRaises(OSError):
                    storage.read_docker_settings(fd, device, os.getuid())

    def test_internal_fallback_directory_is_refused(self):
        with tempfile.TemporaryDirectory() as temporary:
            mount = Path(temporary).resolve()
            (mount / "containers").mkdir()
            (mount / "containers/Docker.raw").write_bytes(b"fixture")
            with self.assertRaisesRegex(ValueError, "mounted filesystem"):
                storage.check_external_disk(mount, Path("containers"), mount.stat().st_dev, os.getuid())

    def test_settings_mismatch_never_touches_external_storage(self):
        with tempfile.TemporaryDirectory() as temporary:
            home = Path(temporary).resolve()
            args = argparse.Namespace(
                external_mount=Path("/Volumes/Example"),
                disk_directory=Path("containers"),
                readiness_helper=Path("/Library/PrivilegedHelperTools/local.example.ready"),
            )
            with (
                patch.object(storage, "DATA_ROOT", home),
                patch.object(storage, "ACCOUNT_HOME", home),
                patch.object(storage, "worker_phase"),
                patch.object(storage, "read_docker_settings", return_value={"AutoStart": False}),
                patch.object(storage, "check_readiness_helper") as readiness,
                patch.object(storage, "check_external_disk") as disk,
            ):
                with self.assertRaisesRegex(ValueError, "expected the configured"):
                    storage.check_storage(args)
                readiness.assert_not_called()
                disk.assert_not_called()

    def test_readiness_helper_inherits_only_trusted_environment_and_owner_marker(self):
        helper = Path("/Library/PrivilegedHelperTools/local.example.ready")
        with (
            patch.object(storage, "absolute_directory"),
            patch.object(storage.os, "stat", return_value=os.stat_result((0o100755, 0, 0, 1, 0, 0, 0, 0, 0, 0))),
            patch.object(storage, "worker_lock_fd", return_value=42),
            patch.dict(os.environ, {"STARTUP_CHECK_RUN_ID": "fixture-run", "PRIVATE_TEST_VALUE": "fixture-value"}),
            patch.object(storage.subprocess, "run") as run,
        ):
            storage.check_readiness_helper(helper)
        self.assertEqual(run.call_args.args, ([str(helper), "--check"],))
        self.assertEqual(run.call_args.kwargs["pass_fds"], (42,))
        self.assertEqual(run.call_args.kwargs["env"]["STARTUP_CHECK_RUN_ID"], "fixture-run")
        self.assertNotIn("PRIVATE_TEST_VALUE", run.call_args.kwargs["env"])


@unittest.skipUnless(sys.platform == "darwin", "uses Darwin process metadata")
class SupervisorTests(unittest.TestCase):
    def test_isolated_worker_drains_and_retries_after_deadline(self):
        with tempfile.TemporaryDirectory() as temporary:
            state = Path(temporary).resolve() / "state"
            program = (
                "import sys, time; "
                f"sys.path.insert(0, {str(HOST)!r}); "
                "import startup_check_supervisor as supervisor; "
                "sys.exit(supervisor.worker_entry(lambda: time.sleep(float(sys.argv[1]))))"
            )
            command = [sys.executable, "-c", program]
            first = supervisor.supervise([*command, "0"], state_dir=state, timeout_seconds=4)
            self.assertTrue(first["ready"], first)
            self.assertTrue(first["physical_cleanup_confirmed"], first)
            expired = supervisor.supervise([*command, "20"], state_dir=state, timeout_seconds=1)
            self.assertFalse(expired["ready"], expired)
            self.assertLess(expired["elapsed_seconds"], 6, expired)
            retry = supervisor.supervise([*command, "0"], state_dir=state, timeout_seconds=4)
            self.assertTrue(retry["ready"], retry)
            self.assertTrue(retry["physical_cleanup_confirmed"], retry)
