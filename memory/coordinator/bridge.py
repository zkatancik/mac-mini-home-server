"""Authenticated file RPC for a network-isolated, explicitly mounted client."""
import hashlib
import hmac
import itertools
import json
import os
import pathlib
import re
import stat
import tempfile
import time


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def read_regular(path, maximum=65536):
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(fd, "rb") as stream:
        info = os.fstat(stream.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_size > maximum:
            raise ValueError("invalid bridge file")
        return json.loads(stream.read(maximum + 1))


def write_message(path, payload, token):
    value = {"payload": payload, "signature": hmac.new(token.encode(), encoded(payload), hashlib.sha256).hexdigest()}
    fd, temporary = tempfile.mkstemp(prefix=".bridge-", dir=path.parent)
    try:
        with os.fdopen(fd, "wb") as stream:
            os.fchmod(stream.fileno(), 0o644)  # Exact private mount crosses host/container UIDs.
            stream.write(encoded(value))
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


class FileBridge:
    def __init__(self, directory, token, callback, lock):
        self.directory, self.token, self.callback, self.lock = pathlib.Path(directory), token, callback, lock
        if not self.directory.is_dir() or self.directory.is_symlink():
            raise ValueError("bridge directory must already exist and not be a symlink")

    def poll_once(self):
        processed = 0
        with os.scandir(self.directory) as entries:
            for entry in itertools.islice(entries, 256):
                match = re.fullmatch(r"([a-f0-9]{32})\.(request|response)\.json", entry.name)
                if not match or not entry.is_file(follow_symlinks=False):
                    continue
                path = pathlib.Path(entry.path)
                if match[2] == "response":
                    try:
                        if time.time() - entry.stat(follow_symlinks=False).st_mtime > 120:
                            path.unlink(missing_ok=True)
                    except OSError:
                        pass
                    continue
                if processed >= 32:
                    break
                processed += 1
                nonce = match[1]
                try:
                    message = read_regular(path, 16384)
                    payload, signature = message["payload"], message["signature"]
                    if not isinstance(payload, dict):
                        raise ValueError("invalid request payload")
                    expected = hmac.new(self.token.encode(), encoded(payload), hashlib.sha256).hexdigest()
                    if not isinstance(signature, str) or not hmac.compare_digest(signature, expected):
                        raise ValueError("bad authentication")
                    if payload.get("nonce") != nonce or abs(time.time() - payload["created_at"]) > 60:
                        raise ValueError("invalid request identity or age")
                    method, endpoint, body = payload["method"], payload["path"], payload.get("body") or {}
                    if method not in {"GET", "POST"} or not isinstance(endpoint, str) or not isinstance(body, dict):
                        raise ValueError("invalid request")
                    try:
                        with self.lock:
                            result = self.callback(method, endpoint, body)
                        response = {"nonce": nonce, "status": 200, "body": result}
                    except Exception as exc:
                        response = {"nonce": nonce, "status": getattr(exc, "status", 503),
                                    "body": {"error": getattr(exc, "message", "admission unavailable")}}
                    write_message(self.directory / f"{nonce}.response.json", response, self.token)
                except (OSError, ValueError, KeyError, TypeError):
                    pass  # Invalid/untrusted files never become commands or engine requests.
                finally:
                    path.unlink(missing_ok=True)

    def serve(self, stop):
        while not stop.is_set():
            try:
                self.poll_once()
            except OSError:
                pass  # Broken bridge fails closed; HTTP and persisted active slot remain.
            stop.wait(0.2)
