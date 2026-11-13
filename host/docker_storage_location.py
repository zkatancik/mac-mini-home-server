#!/usr/bin/env python3
"""Check that Docker Desktop is configured to use an existing external disk."""

import argparse
from contextlib import contextmanager
import json
import os
from pathlib import Path
import stat
import subprocess
import sys

from startup_check_supervisor import (
    ACCOUNT_HOME,
    ENV,
    supervise,
    worker_entry,
    worker_lock_fd,
    worker_phase,
)

DATA_ROOT = Path("/System/Volumes/Data")
SETTINGS_DIR = Path("Library/Group Containers/group.com.docker")
SETTINGS_LIMIT = 128 * 1024
DIRECTORY_FLAGS = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC


def absolute_path(value):
    path = Path(value)
    if path.anchor != "/" or path == Path("/") or ".." in path.parts:
        raise argparse.ArgumentTypeError("expected an absolute path without parent traversal")
    return path


def relative_directory(value):
    path = Path(value)
    if path.is_absolute() or not path.parts or ".." in path.parts:
        raise argparse.ArgumentTypeError("expected a relative directory inside the external mount")
    return path


@contextmanager
def descend(root_fd, parts, *, owner=None, device=None):
    """Hold each directory by descriptor and reject symlinks."""
    current = os.dup(root_fd)
    try:
        for part in parts:
            child = os.open(part, DIRECTORY_FLAGS, dir_fd=current)
            os.close(current)
            current = child
            info = os.fstat(current)
            if owner is not None and info.st_uid != owner:
                raise ValueError("unexpected directory owner")
            if owner == 0 and info.st_mode & 0o022:
                raise ValueError("root-owned directory is writable by another user")
            if device is not None and info.st_dev != device:
                raise ValueError("unexpected directory device")
        yield current
    finally:
        os.close(current)


@contextmanager
def absolute_directory(path, *, owner=None):
    root_fd = os.open("/", DIRECTORY_FLAGS)
    try:
        with descend(root_fd, path.parts[1:], owner=owner) as fd:
            yield fd
    finally:
        os.close(root_fd)


def read_docker_settings(home_fd, device, uid):
    with descend(home_fd, SETTINGS_DIR.parts, owner=uid, device=device) as directory:
        fd = os.open(
            "settings-store.json", os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK,
            dir_fd=directory,
        )
        with os.fdopen(fd, "rb") as source:
            info = os.fstat(source.fileno())
            if (
                not stat.S_ISREG(info.st_mode) or info.st_dev != device or info.st_uid != uid
                or info.st_nlink != 1 or info.st_size > SETTINGS_LIMIT or info.st_mode & 0o022
            ):
                raise ValueError("unsafe Docker Desktop settings")
            raw = source.read(SETTINGS_LIMIT + 1)
    if len(raw) > SETTINGS_LIMIT:
        raise ValueError("Docker Desktop settings exceed 128 KiB")
    settings = json.loads(raw)
    if not isinstance(settings, dict):
        raise ValueError("invalid Docker Desktop settings")
    return settings


def check_readiness_helper(path):
    with absolute_directory(path.parent, owner=0) as directory:
        info = os.stat(path.name, dir_fd=directory, follow_symlinks=False)
        if (
            not stat.S_ISREG(info.st_mode) or info.st_uid != 0
            or info.st_mode & 0o022 or not info.st_mode & 0o111
        ):
            raise ValueError("readiness helper must be root-owned, executable and non-writable by others")
    # The supervisor uses the inherited run marker to verify group membership.
    helper_env = {**ENV, "STARTUP_CHECK_RUN_ID": os.environ["STARTUP_CHECK_RUN_ID"]}
    subprocess.run(
        [str(path), "--check"], check=True, timeout=20, env=helper_env, cwd="/",
        stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        pass_fds=(worker_lock_fd(),),
    )


def check_external_disk(mount, folder, data_device, uid):
    with absolute_directory(mount.parent) as parent:
        with descend(parent, (mount.name,)) as mount_fd:
            device = os.fstat(mount_fd).st_dev
            if device in (data_device, os.fstat(parent).st_dev):
                raise ValueError("external directory is not on its own mounted filesystem")
            with descend(mount_fd, folder.parts, owner=uid, device=device) as directory:
                info = os.stat("Docker.raw", dir_fd=directory, follow_symlinks=False)
                if (
                    not stat.S_ISREG(info.st_mode) or info.st_uid != uid or info.st_dev != device
                    or info.st_nlink != 1 or info.st_size <= 0
                ):
                    raise ValueError("expected an existing regular Docker.raw disk on the external mount")


def check_storage(args):
    worker_phase("opening_internal_root")
    device = os.stat(DATA_ROOT).st_dev
    uid = os.getuid()
    with absolute_directory(ACCOUNT_HOME) as home_fd:
        home = os.fstat(home_fd)
        if home.st_dev != device or home.st_uid != uid:
            raise ValueError("expected the invoking account's home on the internal Data volume")
        worker_phase("opening_docker_settings")
        settings = read_docker_settings(home_fd, device, uid)
    if (
        settings.get("DataFolder") != str(args.external_mount / args.disk_directory)
        or settings.get("AutoStart") is not False
    ):
        raise ValueError("expected the configured external Docker disk folder and AutoStart=false")
    worker_phase("checking_pool_helper")
    check_readiness_helper(args.readiness_helper)
    worker_phase("checking_external_disk")
    check_external_disk(args.external_mount, args.disk_directory, device, uid)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--external-mount", type=absolute_path, required=True)
    parser.add_argument("--disk-directory", type=relative_directory, required=True)
    parser.add_argument("--readiness-helper", type=absolute_path, required=True)
    parser.add_argument("--startup-worker", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args()
    if sys.platform != "darwin" or os.getuid() == 0:
        parser.error("run this check on macOS as the Docker Desktop login account")
    if args.startup_worker:
        return worker_entry(lambda: check_storage(args))
    command = [sys.executable, str(Path(__file__).resolve()), "--startup-worker", *sys.argv[1:]]
    result = supervise(command)
    ready = result["ready"] and result["physical_cleanup_confirmed"]
    print(json.dumps({"status": "ready" if ready else "deferred", "reason": result["reason"]}))
    return 0 if ready else 1


if __name__ == "__main__":
    sys.exit(main())
