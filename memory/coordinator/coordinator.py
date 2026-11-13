#!/usr/bin/env python3
"""Small, fail-closed admission service. It never executes or kills client jobs."""
import argparse
import contextlib
import fcntl
import hashlib
import hmac
import json
import math
import os
import pathlib
import re
import secrets
import stat
import subprocess
import tempfile
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from warning_policy import WarningWindow, finite
from persistence_recovery import StateFile, StateOwnership, PersistenceConflict, encode


class APIError(Exception):
    def __init__(self, status, message):
        self.status, self.message = status, message


def number(value, name, minimum, maximum):
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not minimum <= value <= maximum:
        raise APIError(400, f"invalid {name}")
    return value


class HealthProbe:
    """Cached, bounded read-only probes; absence of evidence never grants a slot."""
    def __init__(self, config, clock=time.time, runner=subprocess.check_output, monotonic_clock=time.monotonic):
        self.config, self.clock, self.runner = config, clock, runner
        self.monotonic_clock = monotonic_clock
        self.host_cache = self.guest_cache = None
        self.last_vm = None

    def _run(self, args):
        return self.runner(args, text=True, stderr=subprocess.DEVNULL, timeout=2).strip()

    def _maintenance_active(self, now):
        path = self.config.get("maintenance_file")
        if not path:
            return False
        try:
            with pathlib.Path(path).open() as stream:
                value = stream.read(129)
            if len(value) > 128:
                return True
            deadline = float(value.strip())
            if not math.isfinite(deadline) or deadline <= 0:
                return True
            return deadline > now
        except FileNotFoundError:
            return False
        except (OSError, ValueError, UnicodeError):
            return True

    def _native_control_guard(self):
        """Optional durable hold: presence blocks admission, with no TTL.

        Only a valid private file yields an ownership digest/identity. Invalid
        or unreadable presence remains a hold; no content (including a nonce)
        is exposed through health. Unconfigured installations keep old policy.
        """
        configured = "native_control_guard_file" in self.config
        path = self.config.get("native_control_guard_file")
        result = {"native_control_guard_supported": True,
                  "native_control_guard_file": path if isinstance(path, str) else None,
                  "native_control_guard_active": configured,
                  "native_control_guard_digest": None,
                  "native_control_guard_identity": None}
        if not configured:
            return result
        if not isinstance(path, str) or not path or not os.path.isabs(path):
            return result
        try:
            before = os.lstat(path)
        except FileNotFoundError:
            result["native_control_guard_active"] = False
            return result
        except (OSError, ValueError, UnicodeError):
            return result
        if (not stat.S_ISREG(before.st_mode) or before.st_uid != os.getuid()
                or before.st_mode & 0o077 or before.st_size > 4096):
            return result
        descriptor = None
        try:
            descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
            opened = os.fstat(descriptor)
            if (not stat.S_ISREG(opened.st_mode) or opened.st_uid != os.getuid()
                    or opened.st_mode & 0o077 or opened.st_size > 4096
                    or (opened.st_dev, opened.st_ino) != (before.st_dev, before.st_ino)):
                return result
            raw = bytearray()
            while len(raw) <= 4096:
                chunk = os.read(descriptor, 4097 - len(raw))
                if not chunk:
                    break
                raw.extend(chunk)
            after = os.fstat(descriptor)
            current = os.lstat(path)
            identity = lambda info: (info.st_dev, info.st_ino, info.st_size,
                                     info.st_mtime_ns, info.st_ctime_ns, info.st_mode, info.st_uid)
            if len(raw) > 4096 or identity(opened) != identity(after) or identity(after) != identity(current):
                return result

            def unique_object(pairs):
                value = {}
                for key, item in pairs:
                    if key in value:
                        raise ValueError("duplicate guard field")
                    value[key] = item
                return value

            def invalid_constant(_value):
                raise ValueError("nonfinite guard value")

            value = json.loads(bytes(raw).decode("utf-8"), object_pairs_hook=unique_object,
                               parse_constant=invalid_constant)
            required = {"version", "scope", "run_id", "manifest_sha256", "fence_identity"}
            # Recovery metadata is digest-bound, never proof of execution,
            # expiry or physical cleanup for admission.
            optional = {"nonce", "job_label", "job_pgid", "job_start",
                        "work_deadline_epoch", "work_deadline_raw", "work_deadline_clock", "run_directory", "boot_session_uuid"}
            if (not isinstance(value, dict) or not required.issubset(value)
                    or set(value) - required - optional
                    or type(value["version"]) is not int or value["version"] != 1
                    or value["scope"] != "synthetic-native-harness-control"
                    or not isinstance(value["run_id"], str)
                    or re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}", value["run_id"]) is None
                    or not isinstance(value["manifest_sha256"], str)
                    or re.fullmatch(r"[a-f0-9]{64}", value["manifest_sha256"]) is None
                    or not isinstance(value["fence_identity"], list) or len(value["fence_identity"]) != 2
                    or any(type(item) is not int or item < 0 for item in value["fence_identity"])
                    or value["fence_identity"][1] == 0
                    or ("nonce" in value and (not isinstance(value["nonce"], str)
                                              or not 1 <= len(value["nonce"]) <= 256))):
                return result
            result["native_control_guard_digest"] = hashlib.sha256(raw).hexdigest()
            result["native_control_guard_identity"] = [after.st_dev, after.st_ino]
            return result
        except (OSError, ValueError, UnicodeError, TypeError, RecursionError):
            return result
        finally:
            if descriptor is not None:
                os.close(descriptor)

    def get(self):
        now, mono = self.clock(), self.monotonic_clock()
        if self.host_cache is None or mono - self.host_cache[0] >= 5 or mono < self.host_cache[0]:
            try:
                level = int(self._run(["/usr/sbin/sysctl", "-n", "kern.memorystatus_vm_pressure_level"]))
                vm = self._run(["/usr/bin/vm_stat"])
                page_size = int(re.search(r"page size of (\d+)", vm)[1])
                counts = [int(re.search(r"^" + key + r":\s+(\d+)\.", vm, re.M)[1]) for key in ("Swapins", "Swapouts")]
                rates, interval = [None, None], None
                if self.last_vm is not None:
                    before, old, old_page_size = self.last_vm
                    if 0 < mono - before <= 10 and page_size == old_page_size and all(a >= b for a, b in zip(counts, old)):
                        interval = mono - before
                        rates = [(a - b) * page_size / 2**20 / interval for a, b in zip(counts, old)]
                self.last_vm = (mono, counts, page_size)
                self.host_cache = (mono, {"pressure_level": level, "swap_in_mib_s": rates[0], "swap_out_mib_s": rates[1],
                                         "host_observed_mono": mono, "host_interval_seconds": interval})
            except Exception:
                self.last_vm = None
                self.host_cache = (mono, {"pressure_level": None, "swap_in_mib_s": None, "swap_out_mib_s": None,
                                         "host_observed_mono": mono, "host_interval_seconds": None})
        if self.guest_cache is None or mono - self.guest_cache[0] >= 15 or mono < self.guest_cache[0]:
            try:
                container = self.config.get("probe_container")
                if not container:
                    raise ValueError("guest probe not configured")
                raw = self._run([self.config.get("docker_path", "/usr/local/bin/docker"),
                                 "exec", container, "cat", "/proc/meminfo"])
                match = re.search(r"^MemAvailable:\s+(\d+)\s+kB$", raw, re.M)
                self.guest_cache = (mono, {"guest_available_mib": int(match[1]) / 1024, "guest_observed_mono": mono})
            except Exception:
                self.guest_cache = (mono, {"guest_available_mib": None, "guest_observed_mono": mono})
        # Check the maintenance fence on every admission read, independently of
        # cached pressure/guest probes, so a freshly written hold takes effect now.
        guard = self._native_control_guard()
        return {**self.host_cache[1], **self.guest_cache[1], **guard,
                "maintenance_active": self._maintenance_active(now) or guard["native_control_guard_active"]}


class Coordinator:
    TERMINAL = {"released", "cancelled", "expired"}

    def __init__(self, store, health, config=None, clock=time.time, monotonic_clock=time.monotonic):
        self.store, self.health, self.clock = store, health, clock
        self.monotonic_clock = monotonic_clock
        self.config = {"healthy_seconds": 60, "max_peak_mib": 3072, "guest_reserve_mib": 512,
                       "max_swap_mib_s": 1, "host_pressure_admission_enabled": True,
                       "max_queue": 32, "lease_timeout_s": 30, "heartbeat_interval_s": 5,
                       "queued_owner_timeout_s": 30,
                       "max_wait_s": 86400, "max_run_s": 86400,
                       "warning_enabled": False, "warning_seconds": 300,
                       "warning_mean_read_mib_s": 0.5, "warning_mean_write_mib_s": 0.1,
                       "warning_burst_read_mib_s": 2, "warning_burst_write_mib_s": 1,
                       "warning_max_probe_gap_s": 10, "capacity_bypass_enabled": False,
                       "capacity_max_bypasses": 2, "capacity_reservation_s": 120, **(config or {})}
        number(self.config["queued_owner_timeout_s"], "queued_owner_timeout_s", 5, 300)
        for key in ("warning_enabled", "capacity_bypass_enabled", "host_pressure_admission_enabled"):
            if type(self.config[key]) is not bool:
                raise ValueError("invalid policy switch")
        for key, low, high in (("warning_seconds", 60, 3600), ("warning_mean_read_mib_s", 0, 1),
                ("warning_mean_write_mib_s", 0, 1), ("warning_burst_read_mib_s", 0, 10),
                ("warning_burst_write_mib_s", 0, 10), ("warning_max_probe_gap_s", 5, 10),
                ("capacity_max_bypasses", 1, 5), ("capacity_reservation_s", 30, 600)):
            number(self.config[key], key, low, high)
        self.warning_window = WarningWindow(self.config)
        self.capacity_reservation = {}
        self.healthy_since = None
        self.last_tick = None
        self.failed = False
        self.failed_snapshot = None
        self.recovery_blocked = None
        self.recovery_next = None
        self.recovery_attempts = 0
        self.state = store.load()
        self._validate_state()
        # Queue polling is deliberately volatile. After restart, live clients
        # have a fresh bounded grace to poll; persisted queue entries alone never
        # prove an owner is present and can never acquire the active slot.
        self.queue_last_seen = {key: self.clock() for key in self.state["queue"]}
        if self.state["active"]:
            request = self.state["requests"][self.state["active"]]
            request.update(state="blocked_orphan", reason="coordinator restarted; owner heartbeat required")
            self.store.save(self.state)

    def _validate_state(self):
        s = self.state
        if not isinstance(s, dict) or type(s.get("version")) is not int or s.get("version") != 1 or "active" not in s or not isinstance(s.get("requests"), dict) or not isinstance(s.get("queue"), list):
            raise ValueError("invalid admission state; explicit recovery required")
        for key, value in s["requests"].items():
            if not isinstance(value, dict) or value.get("request_id") != key or value.get("state") not in self.TERMINAL | {"queued", "granted", "blocked_orphan"}:
                raise ValueError("invalid admission request record")
            bypasses = value.get("capacity_bypass_count", 0)
            if type(bypasses) is not int or not 0 <= bypasses <= 5:
                raise ValueError("invalid stored capacity fairness counter")
            required = {"queue_deadline", "peak_mib", "guest", "run_timeout_s"} if value["state"] == "queued" else set()
            if value["state"] in {"granted", "blocked_orphan"}:
                required = {"lease_id", "run_deadline", "last_heartbeat"}
            if not required.issubset(value):
                raise ValueError("incomplete active or queued admission record")
            numeric = {"queue_deadline", "peak_mib", "run_timeout_s"} if value["state"] == "queued" else set()
            if value["state"] in {"granted", "blocked_orphan"}:
                numeric = {"run_deadline", "last_heartbeat"}
                if not isinstance(value["lease_id"], str) or not value["lease_id"]:
                    raise ValueError("invalid stored lease identity")
            if any(isinstance(value[field], bool) or not isinstance(value[field], (int, float)) or not math.isfinite(value[field]) or value[field] <= 0 for field in numeric):
                raise ValueError("invalid stored admission budget or deadline")
            if value["state"] == "queued" and not isinstance(value["guest"], bool):
                raise ValueError("invalid stored guest flag")
            if value["state"] == "queued" and (value.get("lease_id") or any(
                    field in value for field in ("granted_at", "run_deadline", "last_heartbeat"))):
                raise ValueError("queued request contains previous active ownership")
        active = s.get("active")
        live = [k for k, v in s["requests"].items() if v.get("state") in {"granted", "blocked_orphan"}]
        if live != ([active] if active else []):
            raise ValueError("inconsistent active admission state")
        if len(s["queue"]) != len(set(s["queue"])) or any(
                k not in s["requests"] or s["requests"][k].get("state") != "queued" for k in s["queue"]):
            raise ValueError("inconsistent admission queue")
        for k, v in s["requests"].items():
            if v.get("state") == "queued" and k not in s["queue"]:
                raise ValueError("unqueued pending request")

    def _save(self):
        terminal = sorted((v for v in self.state["requests"].values() if v["state"] in self.TERMINAL),
                          key=lambda v: v.get("finished_at", 0))
        for item in terminal[:-128]:
            del self.state["requests"][item["request_id"]]
        try:
            self.store.save(self.state)
        except Exception as error:
            self.failed = True
            self.healthy_since = self.last_tick = None
            self.warning_window.reset("persistence_failure")
            try:
                self._validate_state()
                self.failed_snapshot = encode(self.state)
            except Exception:
                self.recovery_blocked = "invalid_frozen_state"
            if isinstance(error, PersistenceConflict):
                self.recovery_blocked = "state_ownership_conflict"
            now = self.monotonic_clock()
            self.recovery_next = now + 30 if finite(now) else None
            raise

    def retry_persistence(self):
        """Maintenance-only, under the engine lock; never observes or grants work."""
        if not self.failed or self.recovery_blocked is not None:
            return False
        now = self.monotonic_clock()
        if not finite(now):
            self.recovery_next = None
            return False
        if self.recovery_next is None or now < self.recovery_next - 30:
            self.recovery_next = now + 30
            return False
        if now < self.recovery_next:
            return False
        self.recovery_next = now + 30
        self.recovery_attempts += 1
        try:
            self._validate_state()
            if self.failed_snapshot is None or encode(self.state) != self.failed_snapshot:
                raise PersistenceConflict("frozen state changed")
            self.store.retry_frozen(self.failed_snapshot)
        except (PersistenceConflict, ValueError, TypeError):
            self.recovery_blocked = "state_ownership_conflict"
            return False
        except Exception:
            return False  # Transient I/O failure: same state, bounded later retry.
        self.failed = False
        self.failed_snapshot = None
        self.recovery_next = None
        self.healthy_since = self.last_tick = None
        self.warning_window.reset("persistence_recovered")
        # No new deadline, queue age, presence, lease or grace period is invented.
        # A subsequent ordinary tick processes overdue ownership before a claim.
        return True

    def _request(self, request_id):
        if self.failed:
            raise APIError(503, "durable state unavailable; retain owned cleanup identity")
        try:
            return self.state["requests"][request_id]
        except KeyError:
            raise APIError(404, "unknown request")

    def _view(self, request):
        return {**request, "heartbeat_interval_s": self.config["heartbeat_interval_s"],
                "lease_timeout_s": self.config["lease_timeout_s"]}

    def tick(self, owner_poll=None):
        if self.failed:
            raise APIError(503, "durable state unavailable; retain owned cleanup identity")
        now, mono, changed = self.clock(), self.monotonic_clock(), False
        if self.last_tick is None or now - self.last_tick > 10 or now < self.last_tick:
            self.healthy_since = None
        self.last_tick = now
        health = self.health.get()
        mono = self.monotonic_clock()  # Freshness must be checked after the probe finishes.
        self.warning_window.observe(health, mono)
        maintenance = health.get("maintenance_active", False) is not False
        swap_ok = all(isinstance(health.get(k), (int, float)) and 0 <= health[k] <= self.config["max_swap_mib_s"]
                      for k in ("swap_in_mib_s", "swap_out_mib_s"))
        if not maintenance and health.get("pressure_level") == 1 and swap_ok:
            if self.healthy_since is None or now < self.healthy_since:
                self.healthy_since = now
        else:
            self.healthy_since = None
        for key in list(self.state["queue"]):
            item = self._request(key)
            last_seen = self.queue_last_seen[key]
            if now < last_seen:
                self.queue_last_seen[key] = last_seen = now
            if now >= item["queue_deadline"] or now - last_seen >= self.config["queued_owner_timeout_s"]:
                reason = "queue deadline reached" if now >= item["queue_deadline"] else "queued owner stopped polling before work was granted"
                item.update(state="expired", reason=reason, finished_at=now)
                self.state["queue"].remove(key)
                self.queue_last_seen.pop(key, None)
                changed = True
        self.capacity_reservation = {key: value for key, value in self.capacity_reservation.items() if key in self.state["queue"]}
        active = self.state["active"]
        if active:
            item = self._request(active)
            if now >= item["run_deadline"]:
                reason = "runtime deadline reached; confirm workload stopped before release"
            elif now - item["last_heartbeat"] >= self.config["lease_timeout_s"]:
                reason = "owner heartbeat expired; confirm workload stopped before release"
            else:
                reason = None
            if reason and (item["state"] != "blocked_orphan" or item.get("reason") != reason):
                item.update(state="blocked_orphan", reason=reason)
                changed = True
        elif self.state["queue"]:
            item = self._request(self.state["queue"][0])
            mode = None
            if maintenance:
                reason = "maintenance window active or maintenance file invalid/unreadable"
            elif not self.config["host_pressure_admission_enabled"]:
                mode, reason = "capacity_only", None
            elif self.healthy_since is not None and now - self.healthy_since >= self.config["healthy_seconds"]:
                mode, reason = "normal", None
            elif health.get("pressure_level") == 2 and self.warning_window.ready:
                mode, reason = "warning", None
            elif self.healthy_since is not None:
                reason = "waiting for stable normal host pressure"
            else:
                reason = self.warning_window.reason if self.config["warning_enabled"] else "host pressure or swap activity elevated or unavailable"
            bypassed = None
            if mode is not None:
                item, reason, bypassed = self._candidate(health, mono, mode)
                if reason is None and owner_poll != item["request_id"]:
                    reason = "waiting for the queued owner's current poll"
            if reason:
                item["reason"] = reason
            else:
                self.state["queue"].remove(item["request_id"])
                self.queue_last_seen.pop(item["request_id"], None)
                self.state["active"] = item["request_id"]
                item.update(state="granted", reason=None, lease_id=secrets.token_hex(24),
                            granted_at=now, last_heartbeat=now, run_deadline=now + item["run_timeout_s"],
                            admission_mode=mode, bypassed_request_id=bypassed)
                changed = True
        if changed:
            self._save()
        return health

    def _capacity_reason(self, item, health, mono, mode):
        if item["peak_mib"] > self.config["max_peak_mib"]:
            return "declared peak exceeds the current configured batch budget"
        if mode == "warning" and not item["guest"]:
            return "warning_native_unqualified: host headroom has not been independently qualified"
        if item["guest"]:
            available = health.get("guest_available_mib")
            if not finite(available) or available < 0:
                return "guest available-memory probe unavailable"
            if mode in ("warning", "capacity_only") and (not finite(health.get("guest_observed_mono")) or
                    not 0 <= mono - health["guest_observed_mono"] <= 20):
                return "guest available-memory probe unavailable or stale"
            if available < item["peak_mib"] + self.config["guest_reserve_mib"]:
                return "guest available memory below declared peak plus reserve"
        return None

    def _candidate(self, health, mono, mode):
        candidates = self.state["queue"]
        if mode == "warning" and self.config["capacity_bypass_enabled"]:
            # Native profiles cannot become eligible by reserving guest memory.
            # Keep their durable FIFO positions for normal admission, and apply
            # capacity fairness to the oldest currently eligible guest instead.
            candidates = []
            for key in self.state["queue"]:
                item = self._request(key)
                if item["guest"]:
                    candidates.append(key)
                else:
                    item["reason"] = self._capacity_reason(item, health, mono, mode)
                    self.capacity_reservation.pop(key, None)
            if not candidates:
                head = self._request(self.state["queue"][0])
                return head, head["reason"], None
        head = self._request(candidates[0])
        reason = self._capacity_reason(head, health, mono, mode)
        if reason is None:
            self.capacity_reservation.pop(head["request_id"], None)
        if reason is None or not self.config["capacity_bypass_enabled"]:
            return head, reason, None
        head["reason"] = reason
        if head.get("capacity_bypass_count", 0) >= self.config["capacity_max_bypasses"]:
            deadline = self.capacity_reservation.setdefault(head["request_id"], mono + self.config["capacity_reservation_s"])
            if mono < deadline:
                return head, "age_reservation: waiting briefly for the oldest job's capacity", None
            self.capacity_reservation.pop(head["request_id"], None)
            head["capacity_bypass_count"] = 0
            head["last_capacity_deferred_at"] = self.clock()
            head["reason"] = "capacity_deferred: oldest job still does not fit; original queue age retained"
            self._save()
        for key in candidates[1:]:
            item = self._request(key)
            if self._capacity_reason(item, health, mono, mode) is None:
                return item, None, head["request_id"]
        return head, reason, None

    def submit(self, body):
        request_id, job = body.get("request_id"), body.get("job")
        if not isinstance(request_id, str) or not re.fullmatch(r"[A-Za-z0-9_-]{8,80}", request_id):
            raise APIError(400, "invalid request_id")
        if not isinstance(job, str) or not 1 <= len(job) <= 100 or any(ord(c) < 32 for c in job):
            raise APIError(400, "invalid job")
        peak = number(body.get("peak_mib"), "peak_mib", 1, self.config["max_peak_mib"])
        wait = number(body.get("wait_timeout_s", 86400), "wait_timeout_s", 1, self.config["max_wait_s"])
        run = number(body.get("run_timeout_s"), "run_timeout_s", 1, self.config["max_run_s"])
        guest = body.get("guest", True)
        if not isinstance(guest, bool):
            raise APIError(400, "invalid guest flag")
        fields = dict(request_id=request_id, job=job, peak_mib=peak, guest=guest,
                      wait_timeout_s=wait, run_timeout_s=run)
        self.tick()
        existing = self.state["requests"].get(request_id)
        if existing:
            if any(existing.get(k) != v for k, v in fields.items()):
                raise APIError(409, "request_id already used for different work")
            return self._view(existing)
        if len(self.state["queue"]) >= self.config["max_queue"]:
            raise APIError(429, "admission queue full")
        now = self.clock()
        item = {**fields, "state": "queued", "created_at": now, "queue_deadline": now + wait,
                "lease_id": None, "reason": "queued behind earlier work"}
        self.state["requests"][request_id] = item
        self.state["queue"].append(request_id)
        self.queue_last_seen[request_id] = now
        self._save()
        self.tick()
        return self._view(item)

    def get(self, request_id):
        item = self._request(request_id)
        if item["state"] == "queued":
            self.queue_last_seen[request_id] = self.clock()
        self.tick(owner_poll=request_id)
        return self._view(self._request(request_id))

    def queued_heartbeat(self, request_id, body):
        """Keep a pending owner present without providing a grant-capable poll."""
        if type(body) is not dict or body:
            raise APIError(400, "queued heartbeat requires an empty JSON object")
        item = self._request(request_id)
        if (item["state"] != "queued" or item.get("lease_id") is not None
                or any(key in item for key in ("granted_at", "run_deadline", "last_heartbeat"))):
            raise APIError(409, "only an unexpired queued request may send a queued heartbeat")
        # Expire an already absent owner/deadline before refreshing presence.
        # No owner_poll is supplied, so even a ready FIFO head cannot acquire.
        self.tick()
        item = self._request(request_id)
        if item["state"] != "queued":
            raise APIError(409, "queued request expired before heartbeat")
        now = self.clock()
        if (now >= item["queue_deadline"] or
                now - self.queue_last_seen[request_id] >= self.config["queued_owner_timeout_s"]):
            self.tick()  # A slow probe must not let elapsed deadlines revive.
            raise APIError(409, "queued request expired before heartbeat")
        self.queue_last_seen[request_id] = now
        return self._view(item)

    def heartbeat(self, request_id, body):
        self.tick()
        item = self._request(request_id)
        self._owner(item, body)
        if self.state["active"] != request_id:
            raise APIError(409, "request no longer owns the slot")
        if self.clock() < item["run_deadline"]:
            recovered = item["state"] != "granted"
            item.update(state="granted", reason=None, last_heartbeat=self.clock())
            # Slot ownership and the immutable deadline are already durable.
            # Restart always requires owner recovery, so ordinary heartbeats
            # need not rewrite/fsync the entire bounded history every 5 seconds.
            if recovered:
                self._save()
        return self._view(item)

    @staticmethod
    def _owner(item, body):
        supplied = body.get("lease_id")
        if not isinstance(supplied, str) or not item.get("lease_id") or not hmac.compare_digest(supplied, item["lease_id"]):
            raise APIError(409, "lease identity mismatch")

    def finish(self, request_id, body, cancel=False):
        self.tick()
        item = self._request(request_id)
        if item["state"] in self.TERMINAL:
            return self._view(item)
        if self.state["active"] == request_id:
            self._owner(item, body)
            if body.get("stopped") is not True:
                raise APIError(409, "must confirm actual workload stopped")
            self.state["active"] = None
            # Count completed/confirmed-stopped bypasses, not time while work is
            # still active. A following reservation therefore starts after work.
            older = self.state["requests"].get(item.get("bypassed_request_id"))
            if older and older["state"] == "queued":
                older["capacity_bypass_count"] = older.get("capacity_bypass_count", 0) + 1
        elif item["state"] == "queued" and cancel:
            self.state["queue"].remove(request_id)
            self.queue_last_seen.pop(request_id, None)
        else:
            raise APIError(409, "only an active lease can be released")
        item.update(state="cancelled" if cancel else "released", finished_at=self.clock(), reason=None)
        self._save()  # Persist release before any replacement grant.
        self.tick()
        return self._view(item)

    def status(self):
        health = self.tick()
        return {"active": self.state["active"], "queue": list(self.state["queue"]),
                "healthy_since": self.healthy_since, "health": health,
                "host_pressure_admission_enabled": self.config["host_pressure_admission_enabled"],
                "warning_window": self.warning_window.view(),
                "requests": [self._view(v) for v in self.state["requests"].values() if v["state"] not in self.TERMINAL]}


def dispatch(engine, method, endpoint, body):
    path = endpoint.strip("/").split("/")
    if method == "GET" and path == ["v1", "status"]:
        return engine.status()
    if method == "POST" and path == ["v1", "requests"]:
        return engine.submit(body)
    if len(path) >= 3 and path[:2] == ["v1", "requests"]:
        if method == "GET" and len(path) == 3:
            return engine.get(path[2])
        if method == "POST" and len(path) == 4 and path[3] == "heartbeat":
            return engine.heartbeat(path[2], body)
        if method == "POST" and len(path) == 4 and path[3] == "queued-heartbeat":
            return engine.queued_heartbeat(path[2], body)
        if method == "POST" and len(path) == 4 and path[3] in {"release", "cancel"}:
            return engine.finish(path[2], body, cancel=path[3] == "cancel")
    raise APIError(404, "unknown endpoint")


def maintain(engine, lock, stop):
    """Observe stable health even when clients use short nonblocking queue waits."""
    while not stop.is_set():
        try:
            with lock:
                if engine.failed:
                    engine.retry_persistence()
                else:
                    engine.tick()
        except Exception:
            pass  # Probe/state failures block grants; ownership is never discarded.
        stop.wait(5)


def handler_class(engine, token, lock=None):
    lock = lock or threading.Lock()

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_args):
            pass  # Never log auth, request bodies, URLs or job names.

        def setup(self):
            super().setup()
            self.connection.settimeout(10)

        def respond(self, status, body):
            data = json.dumps(body, allow_nan=False).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def handle_api(self):
            try:
                if not hmac.compare_digest(self.headers.get("Authorization", ""), "Bearer " + token):
                    raise APIError(401, "authentication required")
                body = {}
                if self.command == "POST":
                    try:
                        length = int(self.headers.get("Content-Length", "0"))
                        if not 0 < length <= 16384:
                            raise ValueError()
                        body = json.loads(self.rfile.read(length))
                        if not isinstance(body, dict):
                            raise ValueError()
                    except (ValueError, json.JSONDecodeError):
                        raise APIError(400, "invalid JSON body")
                with lock:
                    result = dispatch(engine, self.command, self.path, body)
                self.respond(200, result)
            except APIError as exc:
                self.respond(exc.status, {"error": exc.message})
            except Exception:
                # Includes failed durable writes; no success/grant is returned.
                self.respond(503, {"error": "admission unavailable; retain or stop owned work safely"})

        do_GET = do_POST = handle_api

    return Handler


@contextlib.contextmanager
def exclusive_state_lock(path):
    path = pathlib.Path(str(path) + ".lock")
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    fd = os.open(path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW | os.O_NONBLOCK, 0o600)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        yield StateOwnership(pathlib.Path(str(path)[:-5]), fd)
    finally:
        os.close(fd)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=pathlib.Path, required=True)
    parser.add_argument("--initialize-state", action="store_true", help="one-time empty-state creation; exits without serving")
    args = parser.parse_args()
    config = json.loads(args.config.read_text())
    token = pathlib.Path(config["token_file"]).read_text().strip()
    if len(token) < 32 or any(c.isspace() for c in token):
        raise ValueError("token file must contain one strong token of at least 32 characters")
    with exclusive_state_lock(config["state_file"]) as ownership:
        if args.initialize_state:
            if pathlib.Path(config["state_file"]).exists():
                raise ValueError("refusing to overwrite existing state")
            StateFile(config["state_file"], ownership=ownership).save({"version": 1, "requests": {}, "queue": [], "active": None})
            return
        engine = Coordinator(StateFile(config["state_file"], ownership=ownership), HealthProbe(config), config)
        lock = threading.Lock()
        server = ThreadingHTTPServer((config.get("listen_host", "127.0.0.1"), config.get("port", 8765)), handler_class(engine, token, lock))
        server.daemon_threads = True
        stop_bridge = threading.Event()
        threading.Thread(target=maintain, args=(engine, lock, stop_bridge), daemon=True).start()
        if config.get("bridge_dir"):
            from bridge import FileBridge
            bridge = FileBridge(config["bridge_dir"], token, lambda method, path, body: dispatch(engine, method, path, body), lock)
            threading.Thread(target=bridge.serve, args=(stop_bridge,), daemon=True).start()
        try:
            server.serve_forever(poll_interval=0.5)
        finally:
            stop_bridge.set()
            server.server_close()


if __name__ == "__main__":
    main()
