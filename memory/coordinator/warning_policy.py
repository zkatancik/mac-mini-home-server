"""Offline candidate policy. No I/O, work execution, or lease management."""
from collections import deque
import math


def finite(value):
    return not isinstance(value, bool) and isinstance(value, (int, float)) and math.isfinite(value)


class WarningWindow:
    def __init__(self, config):
        self.config = config
        self.burst_cooldown = config.get("warning_burst_cooldown_s", 0)
        if (not finite(self.burst_cooldown)
                or self.burst_cooldown != 0 and not 60 <= self.burst_cooldown <= 300):
            raise ValueError("warning_burst_cooldown_s must be 0 or 60..300")
        self.last_burst_end = None
        self.intervals = deque()
        self.last_call = self.last_sample = None
        self.ready = False
        self.reason = "warning_health_wait"
        self.summary = {}
        self.reset_count = 0
        self.last_reset = None
        self.observation = {}

    def reset(self, reason):
        self.reset_count += 1
        self.last_reset = {"reason": reason, "previous_sample_mono": self.last_sample,
                           **self.observation}
        self.intervals.clear()
        self.last_burst_end = None
        self.last_sample = None
        self.ready = False
        self.reason = reason
        self.summary = {}

    def observe(self, health, now):
        # Keep one bounded, numeric reset record. A healthy observation may
        # replace the current reason, but must not erase the cause of a reset.
        self.observation = {key: value if finite(value) else None for key, value in {
            "observed_mono": now,
            **{key: health.get(key) for key in ("host_observed_mono", "host_interval_seconds",
                "swap_in_mib_s", "swap_out_mib_s", "pressure_level")}}.items()}
        self.observation["maintenance_active"] = health.get("maintenance_active", False) is not False
        gap_limit = self.config["warning_max_probe_gap_s"]
        if not finite(now):
            self.reset("warning_clock_unknown")
            return
        if self.last_call is not None and not 0 <= now - self.last_call <= gap_limit:
            self.reset("warning_observation_gap")
        self.last_call = now
        if not self.config["warning_enabled"]:
            self.reset("warning_disabled")
            return
        if health.get("maintenance_active", False) is not False:
            self.reset("maintenance")
            return
        if type(health.get("pressure_level")) is not int or health["pressure_level"] not in (1, 2):
            self.reset("warning_pressure_unqualified")
            return
        end, duration = health.get("host_observed_mono"), health.get("host_interval_seconds")
        incoming, outgoing = health.get("swap_in_mib_s"), health.get("swap_out_mib_s")
        if (not all(finite(x) for x in (end, duration, incoming, outgoing))
                or not 0 <= now - end <= gap_limit or not 1 <= duration <= gap_limit
                or min(incoming, outgoing) < 0):
            self.reset("warning_measurement_unknown_or_stale")
            return
        if self.last_sample == end:
            return  # Cached repeated reads cannot manufacture observed time.
        if self.last_sample is not None and (
                not 0 < end - self.last_sample <= gap_limit or abs(end - self.last_sample - duration) > 0.25):
            self.reset("warning_counter_interval_gap")
        burst = incoming > self.config["warning_burst_read_mib_s"] or outgoing > self.config["warning_burst_write_mib_s"]
        if burst:
            if self.burst_cooldown == 0:
                self.reset("warning_swap_burst")
                self.last_sample = end
                return
            # A recent burst blocks new admission. Its complete measured traffic
            # remains in the rolling mean; it is not deleted or replaced by zero.
            self.last_burst_end = end
        if self.last_sample is None:
            # Pressure before this observation is unknown. Do not credit its
            # preceding counter interval after startup, critical, or a gap.
            self.last_sample = end
            self.ready = False
            self.reason = "warning_health_wait"
            self.summary = {"observed_seconds": 0, "mean_read_mib_s": None,
                            "mean_write_mib_s": None, "interval_count": 0}
            return
        self.last_sample = end
        self.intervals.append((end - duration, end, incoming, outgoing))
        cutoff = end - self.config["warning_seconds"]
        while self.intervals and self.intervals[0][1] <= cutoff:
            self.intervals.popleft()
        if len(self.intervals) > int(self.config["warning_seconds"]) + 2:
            self.reset("warning_sample_frequency_invalid")
            self.last_sample = end
            return
        observed = read_total = write_total = 0.0
        for start, finish, read, write in self.intervals:
            weight = finish - max(start, cutoff)
            observed += weight
            read_total += read * weight
            write_total += write * weight
        mean_read = read_total / observed
        mean_write = write_total / observed
        full = observed >= self.config["warning_seconds"] - 0.001
        quiet = mean_read <= self.config["warning_mean_read_mib_s"] and mean_write <= self.config["warning_mean_write_mib_s"]
        cooldown_remaining = (max(0.0, self.last_burst_end + self.burst_cooldown - end)
                              if self.last_burst_end is not None else 0.0)
        self.ready = full and quiet and cooldown_remaining == 0
        self.reason = ("warning_ready" if self.ready else "warning_health_wait" if not full
                       else "warning_swap_average" if not quiet else "warning_burst_cooldown")
        self.summary = {"observed_seconds": round(observed, 3), "mean_read_mib_s": mean_read,
                        "mean_write_mib_s": mean_write, "interval_count": len(self.intervals)}
        if self.burst_cooldown:
            self.summary.update(last_burst_end_mono=self.last_burst_end,
                                burst_cooldown_remaining_s=cooldown_remaining)

    def view(self):
        return {"ready": self.ready, "reason": self.reason, **self.summary,
                "reset_count": self.reset_count,
                "last_reset": dict(self.last_reset) if self.last_reset is not None else None}
