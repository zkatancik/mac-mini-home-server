"""Guarded atomic state writes; retry never adopts another writer's ownership."""
import hashlib
import json
import os
import pathlib
import stat
import tempfile

MAX_BYTES = 4 * 1024 * 1024


class PersistenceConflict(ValueError):
    """Identity/content conflict needs explicit owner reconciliation, not retries."""


def encode(state):
    raw = json.dumps(state, separators=(",", ":"), allow_nan=False).encode()
    if len(raw) > MAX_BYTES:
        raise PersistenceConflict("state exceeds bound")
    return raw


def directory_identity(path):
    info = path.lstat()
    if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.geteuid() or info.st_mode & 0o077:
        raise PersistenceConflict("state directory ownership changed")
    return info.st_dev, info.st_ino


def regular(info):
    if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid()
            or info.st_mode & 0o077 or info.st_nlink != 1 or info.st_size > MAX_BYTES):
        raise PersistenceConflict("state file ownership changed")


def stamp(info, raw):
    return info.st_dev, info.st_ino, len(raw), hashlib.sha256(raw).hexdigest()


class StateOwnership:
    """The process-lifetime flock descriptor must still name the original lock."""
    def __init__(self, state_path, fd):
        self.path = pathlib.Path(str(state_path) + ".lock")
        self.fd = fd
        self.parent = directory_identity(self.path.parent)
        info = os.fstat(fd)
        regular(info)
        self.identity = info.st_dev, info.st_ino
        self.check()

    def check(self):
        try:
            parent = directory_identity(self.path.parent)
            opened, named = os.fstat(self.fd), self.path.lstat()
        except FileNotFoundError:
            raise PersistenceConflict("state lifetime lock disappeared") from None
        if parent != self.parent:
            raise PersistenceConflict("state parent replaced")
        for info in (opened, named):
            regular(info)
            if (info.st_dev, info.st_ino) != self.identity:
                raise PersistenceConflict("state lifetime lock replaced")


class StateFile:
    def __init__(self, path, ownership=None):
        self.path = pathlib.Path(path)
        self.ownership = ownership
        self.parent = directory_identity(self.path.parent)
        self.known = self.visible = self.attempted = None
        self.loaded = False

    def _check(self):
        if directory_identity(self.path.parent) != self.parent:
            raise PersistenceConflict("state parent replaced")
        if self.ownership is not None:
            self.ownership.check()

    def _read(self):
        regular(self.path.lstat())
        fd = os.open(self.path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        with os.fdopen(fd, "rb") as stream:
            info = os.fstat(stream.fileno())
            regular(info)
            raw = stream.read(MAX_BYTES + 1)
        if len(raw) > MAX_BYTES:
            raise PersistenceConflict("state exceeds bound")
        return raw, stamp(info, raw)

    def load(self):
        self._check()
        raw, identity = self._read()
        value = json.loads(raw)
        self.known, self.visible, self.attempted, self.loaded = identity, None, None, True
        return value

    def _check_current(self):
        self._check()
        try:
            _, current = self._read()
        except FileNotFoundError:
            if self.known is None and not self.loaded:
                return  # Only the explicit first initialization can create it.
            raise PersistenceConflict("state disappeared") from None
        if current not in (self.known, self.visible, self.attempted):
            raise PersistenceConflict("state replaced or modified")
        return current

    def save(self, state):
        self._write(encode(state))

    def retry_frozen(self, raw):
        if self.ownership is None:
            raise PersistenceConflict("persistence retry requires lifetime lock proof")
        self._write(raw)

    def _write(self, raw):
        self._check_current()
        fd, name = tempfile.mkstemp(prefix=".admission-", dir=self.path.parent)
        try:
            with os.fdopen(fd, "wb") as stream:
                os.fchmod(stream.fileno(), 0o600)
                stream.write(raw)
                stream.flush()
                os.fsync(stream.fileno())
                candidate = stamp(os.fstat(stream.fileno()), raw)
            current = self._check_current()
            # Track the candidate before rename: an error may happen after the
            # rename became visible but before directory fsync confirms durability.
            # A previous rename may be visible but not directory-synced. Keep
            # that owned identity if this next rename fails before taking effect.
            # Merely observing it does not acknowledge durability or clear failure.
            self.visible, self.attempted = current, candidate
            os.replace(name, self.path)
            directory = os.open(self.path.parent, os.O_RDONLY | os.O_NOFOLLOW)
            try:
                if (os.fstat(directory).st_dev, os.fstat(directory).st_ino) != self.parent:
                    raise PersistenceConflict("state parent replaced during save")
                os.fsync(directory)
            finally:
                os.close(directory)
            if self._check_current() != candidate:
                raise PersistenceConflict("written candidate no longer current")
            self.known, self.visible, self.attempted, self.loaded = candidate, None, None, True
        finally:
            if os.path.exists(name):
                os.unlink(name)
