#!/usr/bin/env python3
"""Vendorable stdlib admission client and supervised-process CLI.

The server only grants permission. Clients must stop actual work before release.
"""
import argparse
import fcntl
import hashlib
import hmac
import itertools
import json
import os
import pathlib
import signal
import stat
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid


class AdmissionError(RuntimeError):
    def __init__(self, message, *, status=None, server_error=None):
        super().__init__(message)
        self.status, self.server_error = status, server_error


class Client:
    def __init__(self, url, token_file, timeout=8):
        self.url = url.rstrip("/")
        self.token_file = pathlib.Path(token_file)
        self.timeout = timeout

    def call(self, method, path, body=None):
        try:
            token = self.token_file.read_text().strip()
        except OSError:
            raise AdmissionError("admission token file unavailable") from None
        if self.url.startswith("file://"):
            return self._file_call(method, path, body, token)
        request = urllib.request.Request(self.url + path, method=method,
            data=None if body is None else json.dumps(body).encode(),
            headers={"Authorization": "Bearer " + token, "Content-Type": "application/json"})
        # Do not use environment HTTP proxies for local/host.docker.internal control traffic.
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        try:
            with opener.open(request, timeout=self.timeout) as response:
                result = json.load(response)
        except urllib.error.HTTPError as exc:
            try:
                message = json.loads(exc.read(16384)).get("error", "admission rejected")
            except Exception:
                message = "admission rejected"
            finally:
                exc.close()
            raise AdmissionError(f"HTTP {exc.code}: {message}", status=exc.code, server_error=message) from None
        except (OSError, ValueError) as exc:
            raise AdmissionError("admission connection or response unavailable") from exc
        if not isinstance(result, dict):
            raise AdmissionError("invalid admission response")
        return result

    def _file_call(self, method, path, body, token):
        parsed = urllib.parse.urlparse(self.url)
        if parsed.netloc not in {"", "localhost"}:
            raise AdmissionError("invalid admission bridge URL")
        directory = pathlib.Path(urllib.parse.unquote(parsed.path))
        nonce = uuid.uuid4().hex
        request_path, response_path = directory / f"{nonce}.request.json", directory / f"{nonce}.response.json"
        def encoded(value):
            return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
        payload = dict(nonce=nonce, method=method, path=path, body=body, created_at=time.time())
        message = dict(payload=payload, signature=hmac.new(token.encode(), encoded(payload), hashlib.sha256).hexdigest())
        temporary = None
        try:
            # Bound files left by this transport without opening arbitrary entries.
            if sum(1 for _ in itertools.islice(directory.iterdir(), 128)) >= 128:
                raise AdmissionError("admission bridge busy")
            fd, temporary = tempfile.mkstemp(prefix=".bridge-", dir=directory)
            with os.fdopen(fd, "wb") as stream:
                os.fchmod(stream.fileno(), 0o644)
                stream.write(encoded(message))
            os.replace(temporary, request_path)
            end = time.monotonic() + self.timeout
            while time.monotonic() < end:
                try:
                    fd = os.open(response_path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
                    with os.fdopen(fd, "rb") as stream:
                        info = os.fstat(stream.fileno())
                        if not stat.S_ISREG(info.st_mode) or info.st_size > 65536:
                            raise AdmissionError("invalid bridge response")
                        response = json.load(stream)
                    value = response["payload"]
                    expected = hmac.new(token.encode(), encoded(value), hashlib.sha256).hexdigest()
                    if not hmac.compare_digest(response["signature"], expected) or value["nonce"] != nonce:
                        raise AdmissionError("bridge response authentication failed")
                    if value["status"] != 200:
                        message = value['body'].get('error', 'admission rejected')
                        raise AdmissionError(f"HTTP {value['status']}: {message}", status=value['status'], server_error=message)
                    return value["body"]
                except FileNotFoundError:
                    time.sleep(0.05)
            raise AdmissionError("admission bridge unavailable")
        except (OSError, ValueError, KeyError, TypeError) as exc:
            raise AdmissionError("admission bridge unavailable or invalid") from exc
        finally:
            for candidate in (request_path, response_path, pathlib.Path(temporary) if temporary else None):
                if candidate is not None:
                    try:
                        candidate.unlink(missing_ok=True)
                    except OSError:
                        pass

    def cancel_unstarted(self, request_id, info=None):
        """Only before work starts, or with a durable receipt proving it stopped."""
        if info is not None and info.get("phase") == "may_have_started":
            raise AdmissionError("Uncertain workload cannot be cancelled as unstarted")
        endpoint = "/v1/requests/" + request_id
        try:
            # Queued cancellation needs no lease. Do this before GET, which is
            # the owner's polling/claim operation and may grant a live queue head.
            result = self.call("POST", endpoint + "/cancel",
                               {"lease_id": (info or {}).get("lease_id"), "stopped": True})
        except AdmissionError as error:
            if error.status == 404 and error.server_error == "unknown request":
                return {"request_id": request_id, "state": "absent"}
            if error.status != 409:
                raise  # Transport/auth/server failures never justify a claim.
            # Only an explicit lease conflict can conceal a raced/lost grant.
            # The caller's never-started/stopped proof authorizes reconciliation.
            try:
                known = self.call("GET", endpoint)
            except AdmissionError as lookup_error:
                if lookup_error.status == 404 and lookup_error.server_error == "unknown request":
                    return {"request_id": request_id, "state": "absent"}
                raise
            if not isinstance(known, dict) or known.get("request_id") != request_id:
                raise AdmissionError("Cleanup identity was not confirmed")
            if known.get("state") in {"cancelled", "released", "expired"}:
                return known
            lease_id = known.get("lease_id")
            if known.get("state") not in {"granted", "blocked_orphan"} or not isinstance(lease_id, str) or not lease_id:
                raise AdmissionError("Cleanup lease was not confirmed")
            result = self.call("POST", endpoint + "/cancel",
                               {"lease_id": lease_id, "stopped": True})
        if (not isinstance(result, dict) or result.get("request_id") != request_id
                or result.get("state") not in {"cancelled", "released", "expired"}):
            raise AdmissionError("Cleanup terminal identity was not confirmed")
        return result

    def acquire(self, job, peak_mib, run_timeout_s, *, guest=True, wait_timeout_s=86400, cancel=None, request_id=None):
        request_id = request_id or str(uuid.uuid4())
        endpoint = "/v1/requests/" + request_id
        body = dict(request_id=request_id, job=job, peak_mib=peak_mib, run_timeout_s=run_timeout_s,
                    guest=guest, wait_timeout_s=wait_timeout_s)
        result = None
        deadline = time.monotonic() + wait_timeout_s
        try:
            result = self.call("POST", "/v1/requests", body)
            while True:
                if cancel is not None and cancel.is_set():
                    raise AdmissionError("admission wait cancelled")
                if result.get("state") == "granted":
                    return Lease(self, result)
                if result.get("state") != "queued":
                    raise AdmissionError("admission did not grant work: " + str(result.get("state")))
                if time.monotonic() >= deadline:
                    raise AdmissionError("admission wait deadline reached")
                if cancel is not None:
                    cancel.wait(2)
                else:
                    time.sleep(2)
                if cancel is not None and cancel.is_set():
                    raise AdmissionError("admission wait cancelled")
                result = self.call("GET", endpoint)
        except BaseException:
            try:
                self.cancel_unstarted(request_id, result)
            except Exception:
                pass  # Never start work; an unreachable grant remains safely blocked.
            raise


class Lease:
    def __init__(self, client, info):
        self.client, self.info = client, info
        self.endpoint = "/v1/requests/" + info["request_id"]
        self.lost = threading.Event()
        self._stop = threading.Event()
        self._thread = None

    def start_heartbeat(self):
        if self._thread is not None:
            raise AdmissionError("heartbeat already started")

        def beat():
            while not self._stop.wait(self.info["heartbeat_interval_s"]):
                try:
                    value = self.client.call("POST", self.endpoint + "/heartbeat", {"lease_id": self.info["lease_id"]})
                    if value.get("state") != "granted":
                        raise AdmissionError("lease is no longer granted")
                except Exception:
                    self.lost.set()
                    return
        self._thread = threading.Thread(target=beat, daemon=True)
        self._thread.start()

    def release(self, *, stopped):
        if stopped is not True:
            raise AdmissionError("release requires actual work to have stopped")
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=self.client.timeout + 1)
            if self._thread.is_alive():
                raise AdmissionError("heartbeat still in flight; retain lease for explicit resolution")
        return self.client.call("POST", self.endpoint + "/release", {"lease_id": self.info["lease_id"], "stopped": True})


class RunReceipt:
    """Private per-command lifecycle proof, locked for the entire CLI invocation.

    A not_started/stopped receipt can be retried safely. may_have_started never
    authorizes cancellation; only an already terminal server record clears it.
    """
    TERMINAL = {"released", "cancelled", "expired"}

    def __init__(self, path):
        self.path = pathlib.Path(path)
        self.lock_fd = None
        self.value = None

    def lock(self):
        try:
            parent = self.path.parent.lstat()
            if not stat.S_ISDIR(parent.st_mode) or parent.st_uid != os.geteuid() or parent.st_mode & 0o077:
                raise AdmissionError("Lifecycle receipt directory must be private and owned by this user")
            self.lock_fd = os.open(str(self.path) + ".lock", os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW | os.O_NONBLOCK, 0o600)
            self._check_file(os.fstat(self.lock_fd))
            fcntl.flock(self.lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            self.close()
            raise AdmissionError("Lifecycle receipt unavailable or another invocation owns it") from None
        except BaseException:
            self.close()
            raise

    @staticmethod
    def _check_file(info):
        if not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid() or info.st_mode & 0o077 or info.st_nlink != 1:
            raise AdmissionError("Lifecycle receipt must be a private regular file owned by this user")

    def read(self):
        try:
            fd = os.open(self.path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        except FileNotFoundError:
            return None
        try:
            with os.fdopen(fd, "rb") as stream:
                self._check_file(os.fstat(stream.fileno()))
                raw = stream.read(16385)
            if len(raw) > 16384:
                raise ValueError()
            def unique(pairs):
                result = {}
                for key, value in pairs:
                    if key in result:
                        raise ValueError()
                    result[key] = value
                return result
            value = json.loads(raw, object_pairs_hook=unique)
            if (type(value.get("version")) is not int or value["version"] != 1
                    or value.get("phase") not in {"not_started", "may_have_started", "stopped"}
                    or not isinstance(value.get("request_id"), str)
                    or not 8 <= len(value["request_id"]) <= 80
                    or not all(c.isascii() and (c.isalnum() or c in "-_") for c in value["request_id"])
                    or not isinstance(value.get("url"), str)
                    or value.get("lease_id") is not None and not isinstance(value["lease_id"], str)):
                raise ValueError()
            self.value = value
            return value
        except (OSError, ValueError, TypeError, AttributeError, KeyError):
            raise AdmissionError("Lifecycle receipt is invalid; explicit owner inspection required") from None

    def save(self, phase, *, lease_id=None):
        value = {**self.value, "phase": phase, "updated_at": time.time()}
        if lease_id is not None:
            value["lease_id"] = lease_id
        fd, temporary = tempfile.mkstemp(prefix=".batch-receipt-", dir=self.path.parent)
        try:
            with os.fdopen(fd, "w") as stream:
                os.fchmod(stream.fileno(), 0o600)
                json.dump(value, stream, allow_nan=False)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, self.path)
            self._sync_directory()
            self.value = value
        finally:
            if os.path.exists(temporary):
                os.unlink(temporary)

    def begin(self, url, request_id):
        self.value = {"version": 1, "url": url, "request_id": request_id, "lease_id": None}
        self.save("not_started")  # Durable before POST can create a server request.

    def _sync_directory(self):
        fd = os.open(self.path.parent, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)

    def clear(self):
        self.path.unlink()
        self._sync_directory()
        self.value = None

    @staticmethod
    def confirmed(result, request_id, *, allow_absent=False):
        states = RunReceipt.TERMINAL | ({"absent"} if allow_absent else set())
        if not isinstance(result, dict) or result.get("request_id") != request_id or result.get("state") not in states:
            raise AdmissionError("Lifecycle cleanup confirmation did not match this request")

    def recover(self, client):
        value = self.read()
        if value is None:
            return
        if value["url"] != client.url:
            raise AdmissionError("Lifecycle receipt belongs to another coordinator; explicit inspection required")
        request_id = value["request_id"]
        if value["phase"] == "may_have_started":
            # Do not poll/claim a queued request, heartbeat, cancel, or infer death
            # from elapsed time. Only an already terminal record permits cleanup.
            status = client.call("GET", "/v1/status")
            requests = status.get("requests")
            if not isinstance(requests, list) or any(not isinstance(r, dict) or r.get("request_id") == request_id for r in requests):
                raise AdmissionError("Previous workload completion is unconfirmed; explicit owner resolution required")
            result = client.call("GET", "/v1/requests/" + request_id)
            self.confirmed(result, request_id)
        else:
            # Only these durable phases prove this invocation cannot still work.
            result = client.cancel_unstarted(request_id, value)
            self.confirmed(result, request_id, allow_absent=True)
        self.clear()

    def close(self):
        if self.lock_fd is not None:
            os.close(self.lock_fd)
            self.lock_fd = None


def process_group_exists(pgid):
    try:
        os.killpg(pgid, 0)
        return True
    except ProcessLookupError:
        return False


def stop_group(proc, grace=10):
    """Wait for the owned process group, not just its parent, before freeing slot."""
    for sig, seconds in [(signal.SIGTERM, grace), (signal.SIGKILL, 5)]:
        try:
            os.killpg(proc.pid, sig)
        except ProcessLookupError:
            pass
        end = time.monotonic() + seconds
        while time.monotonic() < end:
            proc.poll()  # Reap parent; orphan zombies can still keep the group present.
            if not process_group_exists(proc.pid):
                return True
            time.sleep(0.1)
    return not process_group_exists(proc.pid)


def write_status(path, value):
    if path is None:
        return
    path = pathlib.Path(path)
    fd, name = tempfile.mkstemp(prefix=".batch-status-", dir=path.parent)
    try:
        with os.fdopen(fd, "w") as stream:
            json.dump(value, stream)
        os.replace(name, path)
    finally:
        if os.path.exists(name):
            os.unlink(name)


def run_command(client, args):
    cancelled = threading.Event()
    previous = {}
    for sig in (signal.SIGTERM, signal.SIGINT):
        previous[sig] = signal.signal(sig, lambda *_: cancelled.set())
    lease, proc, stopped, started = None, None, True, None
    receipt, receipt_current = None, False
    status_path = getattr(args, "status_file", None)
    try:
        acquire_extra = {}
        if getattr(args, "receipt_file", None) is not None:
            receipt = RunReceipt(args.receipt_file)
            receipt.lock()
            receipt.recover(client)
            request_id = str(uuid.uuid4())
            receipt.begin(client.url, request_id)
            receipt_current = True
            acquire_extra["request_id"] = request_id
        lease = client.acquire(args.job, args.peak_mib, args.run_timeout, guest=not args.native,
                               wait_timeout_s=args.wait_timeout, cancel=cancelled, **acquire_extra)
        if receipt is not None:
            receipt.save("not_started", lease_id=lease.info["lease_id"])
        if cancelled.is_set():
            return 130
        lease.start_heartbeat()
        deadline = time.monotonic() + max(0, lease.info["run_deadline"] - time.time())
        if time.monotonic() >= deadline or lease.lost.is_set():
            return 124
        started = time.monotonic()
        write_status(status_path, {"state": "running", "started_monotonic": started})
        if receipt is not None:
            receipt.save("may_have_started")  # Durable BEFORE the child can exist.
        stopped = False  # A spawn exception must not become a false stop acknowledgement.
        proc = subprocess.Popen(args.command, start_new_session=True)
        while proc.poll() is None:
            if cancelled.is_set() or lease.lost.is_set() or time.monotonic() >= deadline:
                stopped = stop_group(proc)
                return 124 if not cancelled.is_set() else 130
            time.sleep(0.2)
        result = proc.returncode
        stopped = not process_group_exists(proc.pid)
        if not stopped:
            stopped = stop_group(proc)
        return result if result >= 0 else 128 - result
    finally:
        if proc is not None and not stopped:
            stopped = stop_group(proc)
        if started is not None:
            try:
                write_status(status_path, {"state": "stopped" if stopped else "unconfirmed",
                    "started_monotonic": started, "elapsed_seconds": time.monotonic() - started})
            except OSError:
                print("Could not write batch timing status.", file=sys.stderr)
        if receipt_current and stopped:
            try:
                receipt.save("stopped")  # Only after the actual process group is gone.
            except OSError:
                print("Could not persist stopped lifecycle receipt; uncertain phase retained.", file=sys.stderr)
        if lease is not None:
            if stopped:
                try:
                    result = lease.release(stopped=True)
                    if receipt_current:
                        receipt.confirmed(result, lease.info["request_id"])
                        receipt.clear()
                except Exception:
                    print("Work stopped; release unconfirmed. Coordinator retains slot for explicit resolution.", file=sys.stderr)
            else:
                print("Owned process group did not stop; lease retained. Explicit operator resolution required.", file=sys.stderr)
        elif receipt_current:
            try:
                receipt.recover(client)  # Acquire never returned; no child could have started.
            except Exception:
                print("Acquisition cleanup unconfirmed; lifecycle receipt retained for retry.", file=sys.stderr)
        if receipt is not None:
            receipt.close()
        for sig, handler in previous.items():
            signal.signal(sig, handler)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", required=True)
    parser.add_argument("--token-file", required=True)
    subs = parser.add_subparsers(dest="action", required=True)
    subs.add_parser("status")
    resolve = subs.add_parser("resolve", help="explicit operator acknowledgement after checking workload is stopped")
    resolve.add_argument("request_id")
    resolve.add_argument("--confirm-workload-stopped", action="store_true", required=True)
    run = subs.add_parser("run")
    run.add_argument("--job", required=True)
    run.add_argument("--peak-mib", required=True, type=int)
    run.add_argument("--run-timeout", required=True, type=int)
    run.add_argument("--wait-timeout", type=int, default=86400)
    run.add_argument("--native", action="store_true")
    run.add_argument("--status-file", type=pathlib.Path, help="optional credential-free timing metadata for a local supervisor")
    run.add_argument("--receipt-file", type=pathlib.Path, help="optional private persistent lifecycle receipt; parent directory must already exist")
    run.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    client = Client(args.url, args.token_file)
    try:
        if args.action == "status":
            print(json.dumps(client.call("GET", "/v1/status"), indent=2))
        elif args.action == "resolve":
            path = "/v1/requests/" + args.request_id
            info = client.call("GET", path)
            value = client.call("POST", path + "/cancel", {"lease_id": info.get("lease_id"), "stopped": True})
            print(json.dumps(value, indent=2))
        else:
            if args.command[:1] == ["--"]:
                args.command = args.command[1:]
            if not args.command:
                parser.error("run requires a command after --")
            return run_command(client, args)
    except AdmissionError as exc:
        print(str(exc), file=sys.stderr)
        return 75
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
