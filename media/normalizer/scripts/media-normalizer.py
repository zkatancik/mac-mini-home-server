#!/usr/bin/env python3
"""Audit and normalize media that is incompatible with the Plex Apple TV client.

The worker is intentionally host-native: it uses the Mac's VideoToolbox encoder
and keeps runtime state outside Git. Scheduled polls always handle new Arr
imports, then gradually inspect older library files that were never recorded so
pre-existing Apple TV incompatibilities are not stranded behind the first-run
watermark.
"""

from __future__ import annotations

import argparse
import configparser
import contextlib
from collections import deque
import datetime as dt
import fcntl
import hashlib
import json
import math
import os
import plistlib
import re
import shutil
import signal
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from fractions import Fraction
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional, Sequence, Tuple


STATE_VERSION = 1
VIDEO_EXTENSIONS = {".mkv", ".mp4", ".m4v", ".avi", ".ts"}
HDR_TRANSFERS = {"smpte2084", "arib-std-b67"}
HDR_PRIMARIES = {"bt2020"}
RETRYABLE_STATUSES = {"blocked", "error", "interrupted", "encoding", "capacity_deferred"}
ADMISSION_RETRY_STATUSES = {"capacity_deferred", "interrupted"}
MAX_LOCKED_ADMISSION_WAIT_SECONDS = 60
# quality_rejected is deliberately not retryable: the source failed the quality
# gate at the bitrate ceiling, so re-encoding again can never succeed.
ENCODED_POLL_STATUSES = {"normalized", "would_normalize"}
TEMP_MARKER = ".media-normalizer."
DISPOSITION_FLAGS = (
    "default",
    "dub",
    "original",
    "comment",
    "lyrics",
    "karaoke",
    "forced",
    "hearing_impaired",
    "visual_impaired",
    "clean_effects",
    "attached_pic",
    "timed_thumbnails",
    "captions",
    "descriptions",
    "dependent",
    "still_image",
)
NORMALIZABLE_SIGNATURES = {"av1", "hfr", "incomplete_hvcc"}
MEDIA_MUTATION_LOCK_STALE_SECONDS = int(
    os.environ.get("MEDIA_MUTATION_LOCK_STALE_SECONDS", "300")
)


class NormalizerError(RuntimeError):
    """Base error for expected worker failures."""


class TransientMediaProcessError(NormalizerError):
    """Admission or process interruption is not evidence of damaged media."""

    def __init__(self, message: str, *, status: str) -> None:
        super().__init__(message)
        self.status = status


class SafetyError(NormalizerError):
    """A replacement safety precondition was not met."""


class ValidationError(NormalizerError):
    """An encoded output failed a compatibility or quality gate."""


class QualityGateError(ValidationError):
    """The quality gate failed at the bitrate ceiling; retrying cannot help."""


class LockBusy(NormalizerError):
    """Another normalizer process owns the lock."""


class MediaMutationLockBusy(NormalizerError):
    """Another worker is mutating the shared media library."""


@dataclass
class Settings:
    state_path: Path
    lock_path: Path
    media_mutation_lock_path: Path
    rollback_root: Path
    ffmpeg: str
    ffprobe: str
    sonarr_url: str
    sonarr_api_key: str
    radarr_url: str
    radarr_api_key: str
    tautulli_url: str
    tautulli_config: Path
    plex_plist: Path
    library_root: Optional[Path] = None
    stable_age_seconds: int = 120
    settle_seconds: float = 2.0
    rollback_days: int = 30
    quality_window_seconds: int = 10
    backfill_inspect_limit: int = 200
    backfill_encode_limit: int = 20


@dataclass
class ManagedMedia:
    service: str
    owner_id: int
    file_id: int
    path: Path
    date_added: str
    season_number: Optional[int] = None


def utc_now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def isoformat(value: dt.datetime) -> str:
    return value.astimezone(dt.timezone.utc).isoformat().replace("+00:00", "Z")


def parse_datetime(value: str) -> dt.datetime:
    if not value:
        return dt.datetime.fromtimestamp(0, tz=dt.timezone.utc)
    parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    return parsed.astimezone(dt.timezone.utc)


def load_dotenv(path: Path) -> Dict[str, str]:
    """Parse dotenv values without executing the file in a shell."""
    values: Dict[str, str] = {}
    if not path.exists():
        return values
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[7:].lstrip()
        if "=" not in line:
            continue
        key, raw_value = line.split("=", 1)
        key = key.strip()
        value = raw_value.strip()
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", key):
            continue
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
            quote = value[0]
            value = value[1:-1]
            if quote == '"':
                value = bytes(value, "utf-8").decode("unicode_escape")
        values[key] = value
    return values


def load_settings() -> Settings:
    root = Path.home() / ".config" / "home-server-media-normalizer"
    env_path = Path(os.environ.get("MEDIA_NORMALIZER_ENV", str(root / ".env")))
    env = load_dotenv(env_path)
    runtime = Path(
        os.environ.get("MEDIA_NORMALIZER_STATE_DIR", str(root / "runtime"))
    )
    library_root = os.environ.get("MEDIA_NORMALIZER_LIBRARY_ROOT")
    return Settings(
        state_path=runtime / "state.json",
        lock_path=runtime / "normalizer.lock",
        media_mutation_lock_path=Path(
            os.environ.get(
                "MEDIA_MUTATION_LOCK_PATH",
                str(runtime / "media-library-mutation.lock"),
            )
        ),
        rollback_root=Path(
            os.environ.get(
                "MEDIA_NORMALIZER_ROLLBACK_ROOT",
                str(runtime / "rollback"),
            )
        ),
        ffmpeg=env.get("MEDIA_NORMALIZER_FFMPEG") or os.environ.get("MEDIA_NORMALIZER_FFMPEG", "ffmpeg"),
        ffprobe=env.get("MEDIA_NORMALIZER_FFPROBE") or os.environ.get("MEDIA_NORMALIZER_FFPROBE", "ffprobe"),
        sonarr_url=os.environ.get(
            "MEDIA_NORMALIZER_SONARR_URL", "http://127.0.0.1:8989"
        ).rstrip("/"),
        sonarr_api_key=env.get("SONARR_API_KEY", os.environ.get("SONARR_API_KEY", "")),
        radarr_url=os.environ.get(
            "MEDIA_NORMALIZER_RADARR_URL", "http://127.0.0.1:7878"
        ).rstrip("/"),
        radarr_api_key=env.get("RADARR_API_KEY", os.environ.get("RADARR_API_KEY", "")),
        tautulli_url=os.environ.get(
            "MEDIA_NORMALIZER_TAUTULLI_URL", "http://127.0.0.1:8181"
        ).rstrip("/"),
        tautulli_config=Path(
            os.environ.get(
                "MEDIA_NORMALIZER_TAUTULLI_CONFIG", str(root / "tautulli.ini")
            )
        ),
        plex_plist=Path(
            os.environ.get(
                "MEDIA_NORMALIZER_PLEX_PLIST",
                str(Path.home() / "Library/Preferences/com.plexapp.plexmediaserver.plist"),
            )
        ),
        library_root=Path(library_root) if library_root else None,
        stable_age_seconds=int(os.environ.get("MEDIA_NORMALIZER_STABLE_AGE", "120")),
        settle_seconds=float(os.environ.get("MEDIA_NORMALIZER_SETTLE_SECONDS", "2")),
        rollback_days=int(os.environ.get("MEDIA_NORMALIZER_ROLLBACK_DAYS", "30")),
        quality_window_seconds=int(
            os.environ.get("MEDIA_NORMALIZER_QUALITY_WINDOW", "10")
        ),
        backfill_inspect_limit=max(
            0, int(os.environ.get("MEDIA_NORMALIZER_BACKFILL_INSPECT", "200"))
        ),
        backfill_encode_limit=max(
            0, int(os.environ.get("MEDIA_NORMALIZER_BACKFILL_ENCODE", "20"))
        ),
    )


def redact_text(value: str, secrets: Iterable[str]) -> str:
    redacted = value
    for secret in secrets:
        if secret and len(secret) >= 4:
            redacted = redacted.replace(secret, "[REDACTED]")
    redacted = re.sub(
        r"(?i)(apikey|api_key|token|x-plex-token)([=:\s]+)[^&\s\"']+",
        r"\1\2[REDACTED]",
        redacted,
    )
    return redacted


def log(message: str, settings: Optional[Settings] = None, *, stderr: bool = False) -> None:
    secrets: List[str] = []
    if settings:
        secrets = [settings.sonarr_api_key, settings.radarr_api_key]
    timestamp = isoformat(utc_now())
    print(
        "%s %s" % (timestamp, redact_text(message, secrets)),
        file=sys.stderr if stderr else sys.stdout,
        flush=True,
    )


def empty_state() -> Dict[str, Any]:
    return {"version": STATE_VERSION, "watermarks": {}, "files": {}, "rollbacks": []}


def valid_state(value: Any) -> bool:
    return (
        isinstance(value, dict)
        and value.get("version") == STATE_VERSION
        and isinstance(value.get("watermarks"), dict)
        and isinstance(value.get("files"), dict)
        and isinstance(value.get("rollbacks"), list)
        and ("pending_poll" not in value or (
            isinstance(value["pending_poll"], dict)
            and value["pending_poll"].get("version") == 1
            and isinstance(value["pending_poll"].get("completed"), dict)
            and all(isinstance(key, str) and isinstance(identity, str)
                    for key, identity in value["pending_poll"]["completed"].items())
        ))
    )


def load_state(path: Path) -> Dict[str, Any]:
    errors: List[str] = []
    for candidate in (path, path.with_suffix(path.suffix + ".bak")):
        if not candidate.exists():
            continue
        try:
            value = json.loads(candidate.read_text(encoding="utf-8"))
            if not valid_state(value):
                raise ValueError("unsupported or incomplete state")
            return value
        except (OSError, ValueError, json.JSONDecodeError) as exc:
            errors.append("%s: %s" % (candidate, exc))
    if errors:
        raise NormalizerError("unable to recover normalizer state: " + "; ".join(errors))
    return empty_state()


def atomic_write_json(path: Path, value: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(".%s.%d.tmp" % (path.name, os.getpid()))
    backup = path.with_suffix(path.suffix + ".bak")
    backup_temp = backup.with_name(".%s.%d.tmp" % (backup.name, os.getpid()))
    try:
        if path.exists():
            shutil.copy2(path, backup_temp)
            os.replace(backup_temp, backup)
        with temp.open("w", encoding="utf-8") as handle:
            json.dump(value, handle, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp, path)
        directory_fd = os.open(str(path.parent), os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        for leftover in (temp, backup_temp):
            with contextlib.suppress(FileNotFoundError):
                leftover.unlink()


@contextlib.contextmanager
def process_lock(path: Path) -> Iterator[None]:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+") as handle:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise LockBusy("another media-normalizer process is already running") from exc
        handle.seek(0)
        handle.truncate()
        handle.write(str(os.getpid()))
        handle.flush()
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


@contextlib.contextmanager
def media_mutation_lock(
    path: Path, stale_seconds: int = MEDIA_MUTATION_LOCK_STALE_SECONDS
) -> Iterator[None]:
    """Coordinate host and container mutations through an atomic directory."""
    path.parent.mkdir(parents=True, exist_ok=True)
    acquired = False
    for _ in range(2):
        try:
            path.mkdir()
            acquired = True
            break
        except FileExistsError as exc:
            try:
                age = time.time() - path.stat().st_mtime
                if stale_seconds <= 0 or age <= stale_seconds:
                    raise MediaMutationLockBusy(
                        "another worker is mutating the media library"
                    ) from exc
                path.rmdir()
            except MediaMutationLockBusy:
                raise
            except (FileNotFoundError, OSError) as cleanup_exc:
                raise MediaMutationLockBusy(
                    "another worker is mutating the media library"
                ) from cleanup_exc
    if not acquired:
        raise MediaMutationLockBusy("another worker is mutating the media library")

    stop_heartbeat = threading.Event()
    interval = max(1.0, min(30.0, stale_seconds / 3 if stale_seconds > 0 else 30.0))

    def heartbeat() -> None:
        while not stop_heartbeat.wait(interval):
            with contextlib.suppress(FileNotFoundError):
                os.utime(path, None)

    heartbeat_thread = threading.Thread(target=heartbeat, daemon=True)
    heartbeat_thread.start()
    try:
        yield
    finally:
        stop_heartbeat.set()
        heartbeat_thread.join(timeout=interval + 1)
        with contextlib.suppress(FileNotFoundError):
            path.rmdir()


def run_process(
    command: Sequence[str],
    *,
    timeout: Optional[float] = None,
    check: bool = True,
) -> subprocess.CompletedProcess:
    if Path(command[0]).name == "ffmpeg":
        return run_bounded_ffmpeg(command, timeout=timeout, check=check)
    try:
        result = subprocess.run(
            list(command),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=timeout,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise NormalizerError("subprocess failed: %s" % exc) from exc
    if check and result.returncode != 0:
        detail = (result.stderr or result.stdout).strip()[-4000:]
        raise NormalizerError(
            "command exited %d (%s): %s"
            % (result.returncode, Path(command[0]).name, detail)
        )
    return result


@contextlib.contextmanager
def _defer_stop_signals() -> Iterator[None]:
    """Finish joining owned work before honoring a new stop request."""
    if threading.current_thread() is not threading.main_thread():
        yield
        return
    pending: List[int] = []
    unwinding = sys.exc_info()[0] is not None
    previous = {}

    def defer(received: int, _frame: Any) -> None:
        if not pending:
            pending.append(received)

    for sig in (signal.SIGTERM, signal.SIGINT):
        previous[sig] = signal.signal(sig, defer)
    try:
        yield
    finally:
        for sig, handler in previous.items():
            signal.signal(sig, handler)
    if pending and not unwinding:
        handler = previous[pending[0]]
        if callable(handler):
            handler(pending[0], None)
            return
        if handler == signal.SIG_IGN:
            return
        if pending[0] == signal.SIGINT:
            raise KeyboardInterrupt
        raise SystemExit(128 + pending[0])


def run_bounded_ffmpeg(
    command: Sequence[str], *, timeout: Optional[float], check: bool
) -> subprocess.CompletedProcess:
    """Bound decoder/filter threads and output capture; admit each native batch."""
    bounded = [command[0], "-nostats", "-filter_threads", "2", "-filter_complex_threads", "2"]
    for argument in command[1:-1]:
        if argument == "-i":
            bounded.extend(["-threads", "2"])
        bounded.append(argument)
    bounded.extend(["-threads", "2", command[-1]])
    endpoint = os.environ.get("MEDIA_NORMALIZER_MEMORY_COORDINATOR_URL", "")
    wrapped = bool(endpoint)
    wall_timeout = timeout
    if wrapped:
        client = Path(os.environ.get("MEDIA_NORMALIZER_MEMORY_CLIENT_FILE", ""))
        token = Path(os.environ.get("MEDIA_NORMALIZER_MEMORY_TOKEN_FILE", ""))
        if not client.is_file() or not token.is_file():
            raise NormalizerError("media memory admission requires client and token files")
        try:
            peak = int(os.environ.get("MEDIA_NORMALIZER_MEMORY_PEAK_MIB", "1536"))
            wait = int(os.environ.get("MEDIA_NORMALIZER_MEMORY_WAIT_SECONDS", "60"))
            runtime = int(math.ceil(timeout if timeout is not None else 7200))
            if not 1 <= peak <= 3072 or not 1 <= wait <= 86400 or not 1 <= runtime <= 86400:
                raise ValueError("out of bounds")
            # The library mutation lock spans this transaction. Never park it
            # for a day while waiting for admission; the durable poll is retried.
            wait = min(wait, MAX_LOCKED_ADMISSION_WAIT_SECONDS)
        except ValueError as exc:
            raise NormalizerError("invalid media memory admission budget") from exc
        # One persistent receipt/lock spans all serial probes and encodes. The
        # installer prepares this private parent; the client rejects unsafe/missing
        # ownership or permissions rather than silently repairing them here.
        receipt = Path.home() / ".config" / "home-server-media-normalizer" / "memory-admission.json"
        bounded = [sys.executable, str(client), "--url", endpoint, "--token-file", str(token),
                   "run", "--job", "native-media-normalizer", "--peak-mib", str(peak),
                   "--run-timeout", str(runtime), "--wait-timeout", str(wait),
                   "--receipt-file", str(receipt), "--native", "--", *bounded]
        wall_timeout = wait + runtime + 60  # Admission wait does not consume encode time.

    process = None
    readers = []
    tails = [deque(maxlen=8), deque(maxlen=8)]  # At most 64 KiB retained per stream.

    def read_tail(stream, tail):
        try:
            while True:
                chunk = stream.read(8192)
                if not chunk:
                    return
                tail.append(chunk)
        finally:
            stream.close()

    def stop_owned():
        if process is not None and process.poll() is None:
            process.terminate()  # Wrapper needs time to stop/join its child and release.
            try:
                process.wait(timeout=40 if wrapped else 5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()

    try:
        process = subprocess.Popen(bounded, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        for stream, tail in zip((process.stdout, process.stderr), tails):
            reader = threading.Thread(target=read_tail, args=(stream, tail), daemon=True)
            reader.start()
            readers.append(reader)
        process.wait(timeout=wall_timeout)
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise NormalizerError("ffmpeg subprocess failed: %s" % type(exc).__name__) from exc
    finally:
        with _defer_stop_signals():
            stop_owned()
            for reader in readers:
                reader.join(timeout=5)
    result = subprocess.CompletedProcess(list(command), process.returncode,
        b"".join(tails[0]).decode("utf-8", errors="replace"),
        b"".join(tails[1]).decode("utf-8", errors="replace"))
    # The admission CLI shares its exit-code channel with the child. Reserved
    # wrapper results are therefore conservatively retryable, even if a child
    # theoretically returned the same value. Never turn that ambiguity into a
    # permanent media-quality verdict. Unwrapped ordinary decoder codes retain
    # their existing interpretation, and check=False only suppresses decoder errors.
    if wrapped and result.returncode == 75:
        raise TransientMediaProcessError(
            "media memory admission deferred (exit 75)", status="capacity_deferred"
        )
    if result.returncode < 0 or (wrapped and result.returncode in {124, 130, 137, 143}):
        raise TransientMediaProcessError(
            "media process interrupted or admission lease ended (exit %d)" % result.returncode,
            status="interrupted",
        )
    if check and result.returncode != 0:
        raise NormalizerError("command exited %d (ffmpeg): %s" % (
            result.returncode, (result.stderr or result.stdout).strip()[-4000:]))
    return result


def api_path_to_host(value: str) -> Path:
    mappings = []
    for item in filter(None, os.environ.get("MEDIA_NORMALIZER_PATH_MAPPINGS", "").split(",")):
        source, separator, destination = item.partition("=")
        if not separator or not Path(source).is_absolute() or not Path(destination).is_absolute():
            raise NormalizerError("path mappings must use absolute source and destination paths")
        mappings.append((source.rstrip("/") + "/", destination.rstrip("/") + "/"))
    for source, destination in mappings:
        if value.startswith(source):
            return Path(destination + value[len(source) :])
    return Path(value)


class ArrClient:
    def __init__(self, service: str, base_url: str, api_key: str):
        self.service = service
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key

    def request(
        self,
        method: str,
        endpoint: str,
        payload: Optional[Dict[str, Any]] = None,
        query: Optional[Dict[str, Any]] = None,
    ) -> Any:
        if not self.api_key:
            raise NormalizerError("%s API key is missing" % self.service)
        url = "%s/api/v3/%s" % (self.base_url, endpoint.lstrip("/"))
        if query:
            url += "?" + urllib.parse.urlencode(query)
        data = None
        headers = {"X-Api-Key": self.api_key, "Accept": "application/json"}
        if payload is not None:
            data = json.dumps(payload).encode("utf-8")
            headers["Content-Type"] = "application/json"
        request = urllib.request.Request(url, data=data, headers=headers, method=method)
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                raw = response.read()
        except (urllib.error.URLError, TimeoutError) as exc:
            raise NormalizerError("%s API request failed: %s" % (self.service, exc)) from exc
        return json.loads(raw) if raw else None

    def get(self, endpoint: str, query: Optional[Dict[str, Any]] = None) -> Any:
        return self.request("GET", endpoint, query=query)

    def post(self, endpoint: str, payload: Dict[str, Any]) -> Any:
        return self.request("POST", endpoint, payload=payload)

    def managed_media(self) -> List[ManagedMedia]:
        found: List[ManagedMedia] = []
        if self.service == "sonarr":
            for series in self.get("series"):
                series_id = int(series["id"])
                for media in self.get("episodefile", {"seriesId": series_id}):
                    raw_path = media.get("path")
                    if not raw_path:
                        raw_path = str(Path(series["path"]) / media["relativePath"])
                    found.append(
                        ManagedMedia(
                            service="sonarr",
                            owner_id=series_id,
                            file_id=int(media["id"]),
                            path=api_path_to_host(raw_path),
                            date_added=media.get("dateAdded", ""),
                            season_number=(
                                int(media["seasonNumber"])
                                if media.get("seasonNumber") is not None
                                else None
                            ),
                        )
                    )
            return found

        for movie in self.get("movie"):
            media = movie.get("movieFile") or {}
            if not media or not movie.get("hasFile"):
                continue
            raw_path = media.get("path")
            if not raw_path:
                raw_path = str(Path(movie["path"]) / media["relativePath"])
            found.append(
                ManagedMedia(
                    service="radarr",
                    owner_id=int(movie["id"]),
                    file_id=int(media["id"]),
                    path=api_path_to_host(raw_path),
                    date_added=media.get("dateAdded", ""),
                )
            )
        return found

    def sonarr_season(self, series_id: int, season: int) -> List[ManagedMedia]:
        if self.service != "sonarr":
            raise NormalizerError("season selection requires Sonarr")
        series = self.get("series/%d" % series_id)
        episodes = self.get("episode", {"seriesId": series_id})
        file_seasons = {
            int(item["episodeFileId"]): int(item["seasonNumber"])
            for item in episodes
            if item.get("hasFile") and item.get("episodeFileId")
        }
        found: List[ManagedMedia] = []
        for media in self.get("episodefile", {"seriesId": series_id}):
            season_number = media.get("seasonNumber")
            if season_number is None:
                season_number = file_seasons.get(int(media["id"]))
            if season_number != season:
                continue
            raw_path = media.get("path") or str(
                Path(series["path"]) / media["relativePath"]
            )
            found.append(
                ManagedMedia(
                    service="sonarr",
                    owner_id=series_id,
                    file_id=int(media["id"]),
                    path=api_path_to_host(raw_path),
                    date_added=media.get("dateAdded", ""),
                    season_number=season,
                )
            )
        return sorted(found, key=lambda item: str(item.path))

    def rescan(self, owner_id: int) -> None:
        if self.service == "sonarr":
            self.post("command", {"name": "RescanSeries", "seriesId": owner_id})
        else:
            self.post("command", {"name": "RescanMovie", "movieId": owner_id})


def arr_clients(settings: Settings) -> Dict[str, ArrClient]:
    return {
        "sonarr": ArrClient("sonarr", settings.sonarr_url, settings.sonarr_api_key),
        "radarr": ArrClient("radarr", settings.radarr_url, settings.radarr_api_key),
    }


def parse_fraction(value: Any) -> Optional[Fraction]:
    if value in (None, "", "0/0", "N/A"):
        return None
    try:
        return Fraction(str(value))
    except (ValueError, ZeroDivisionError):
        return None


def rate_string(rate: Optional[Fraction]) -> Optional[str]:
    if rate is None:
        return None
    return "%d/%d" % (rate.numerator, rate.denominator)


def hfr_target(rate: Optional[Fraction]) -> Optional[Fraction]:
    if rate is None:
        return None
    candidates = [
        (Fraction(48, 1), Fraction(24, 1)),
        (Fraction(50, 1), Fraction(25, 1)),
        (Fraction(60000, 1001), Fraction(30000, 1001)),
        (Fraction(60, 1), Fraction(30, 1)),
    ]
    for source, target in candidates:
        if abs(float(rate - source)) <= 0.01:
            return target
    return None


def extradata_bytes(value: str) -> bytes:
    chunks: List[str] = []
    for line in (value or "").splitlines():
        if ":" not in line:
            continue
        payload = line.split(":", 1)[1]
        hex_column = re.split(r"\s{2,}", payload.strip(), maxsplit=1)[0]
        chunks.extend(re.findall(r"[0-9A-Fa-f]{2}", hex_column))
    return bytes.fromhex("".join(chunks)) if chunks else b""


def hvcc_parameter_sets(data: bytes) -> Optional[Dict[str, int]]:
    """Return HEVC hvcC VPS/SPS/PPS counts, or None for an unknown record."""
    if len(data) < 23 or data[0] != 1:
        return None
    offset = 23
    counts = {"vps": 0, "sps": 0, "pps": 0}
    names = {32: "vps", 33: "sps", 34: "pps"}
    try:
        for _ in range(data[22]):
            nal_type = data[offset] & 0x3F
            offset += 1
            number = int.from_bytes(data[offset : offset + 2], "big")
            offset += 2
            for _ in range(number):
                length = int.from_bytes(data[offset : offset + 2], "big")
                offset += 2
                if length <= 0 or offset + length > len(data):
                    return None
                if nal_type in names:
                    counts[names[nal_type]] += 1
                offset += length
    except (IndexError, ValueError):
        return None
    return counts


def hvcc_status(video: Dict[str, Any]) -> Tuple[Optional[bool], List[str]]:
    if video.get("codec_name") not in {"hevc", "h265"}:
        return None, []
    counts = hvcc_parameter_sets(extradata_bytes(video.get("extradata", "")))
    if counts is None:
        return None, ["unparseable hvcC configuration"]
    missing = [name.upper() for name in ("vps", "sps", "pps") if counts[name] == 0]
    return not missing, missing


def probe_media(path: Path, ffprobe: str) -> Dict[str, Any]:
    result = run_process(
        [
            ffprobe,
            "-v",
            "error",
            "-show_streams",
            "-show_format",
            "-show_chapters",
            "-show_data",
            "-of",
            "json",
            str(path),
        ],
        timeout=180,
    )
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise NormalizerError("ffprobe returned invalid JSON for %s" % path) from exc


def primary_video(probe: Dict[str, Any]) -> Dict[str, Any]:
    videos = [stream for stream in probe.get("streams", []) if stream.get("codec_type") == "video"]
    if not videos:
        raise NormalizerError("media has no video stream")
    return videos[0]


def media_duration(probe: Dict[str, Any]) -> float:
    values = [probe.get("format", {}).get("duration"), primary_video(probe).get("duration")]
    for value in values:
        try:
            if value not in (None, "N/A"):
                return float(value)
        except (TypeError, ValueError):
            pass
    raise NormalizerError("media duration is unavailable")


def video_rate(video: Dict[str, Any]) -> Optional[Fraction]:
    return parse_fraction(video.get("avg_frame_rate")) or parse_fraction(video.get("r_frame_rate"))


def bit_depth(video: Dict[str, Any]) -> int:
    raw = video.get("bits_per_raw_sample")
    if raw not in (None, "", "N/A"):
        with contextlib.suppress(ValueError):
            return int(raw)
    match = re.search(r"(?:p|yuv\d{3}p)(10|12|16)(?:le|be)?", video.get("pix_fmt", ""))
    return int(match.group(1)) if match else 8


def hdr_reasons(path: Path, video: Dict[str, Any]) -> List[str]:
    reasons: List[str] = []
    transfer = str(video.get("color_transfer", "")).lower()
    primaries = str(video.get("color_primaries", "")).lower()
    if transfer in HDR_TRANSFERS:
        reasons.append("HDR transfer %s" % transfer)
    if primaries in HDR_PRIMARIES:
        reasons.append("HDR primaries %s" % primaries)
    side_data = json.dumps(video.get("side_data_list", []), sort_keys=True).lower()
    if any(marker in side_data for marker in ("dovi", "dolby vision", "mastering display", "content light")):
        reasons.append("HDR or Dolby Vision side data")
    tags = json.dumps(video.get("tags", {}), sort_keys=True)
    label = "%s %s" % (path.name, tags)
    if re.search(r"(?i)\b(?:dovi|dolby[ ._-]?vision|dv|hdr10\+?|hlg|pq)\b", label):
        reasons.append("HDR or Dolby Vision label")
    return sorted(set(reasons))


def classify_media(path: Path, probe: Dict[str, Any]) -> Dict[str, Any]:
    video = primary_video(probe)
    codec = str(video.get("codec_name", "unknown")).lower()
    rate = video_rate(video)
    signatures: List[str] = []
    details: List[str] = []
    if codec in {"av1", "av01"}:
        signatures.append("av1")
    if rate is not None and float(rate) > 30.01:
        signatures.append("hfr")
        if hfr_target(rate) is None:
            details.append("unsupported HFR rate %s" % rate_string(rate))
    hvcc_complete, hvcc_detail = hvcc_status(video)
    if hvcc_complete is False:
        signatures.append("incomplete_hvcc")
        details.append("hvcC missing %s" % ", ".join(hvcc_detail))
    elif hvcc_complete is None and codec in {"hevc", "h265"}:
        details.extend(hvcc_detail)
    hdr = hdr_reasons(path, video)
    decision = "compatible"
    if signatures:
        if hdr:
            decision = "report_only_hdr"
        elif "hfr" in signatures and hfr_target(rate) is None:
            decision = "report_only_unsupported_hfr"
        else:
            decision = "normalize"
    elif hvcc_complete is None and codec in {"hevc", "h265"}:
        decision = "report_only_unknown_hevc_configuration"
    return {
        "path": str(path),
        "codec": codec,
        "width": int(video.get("width") or 0),
        "height": int(video.get("height") or 0),
        "frame_rate": rate_string(rate),
        "frame_rate_float": float(rate) if rate is not None else None,
        "bit_depth": bit_depth(video),
        "color_primaries": video.get("color_primaries"),
        "color_transfer": video.get("color_transfer"),
        "color_space": video.get("color_space"),
        "signatures": signatures,
        "details": details,
        "hdr_reasons": hdr,
        "decision": decision,
    }


def sample_positions(duration: float, window: float = 5.0) -> List[float]:
    maximum = max(0.0, duration - window)
    values = [0.0, max(0.0, duration / 2.0 - window / 2.0), maximum]
    result: List[float] = []
    for value in values:
        rounded = round(min(value, maximum), 3)
        if rounded not in result:
            result.append(rounded)
    return result


def sample_decode(path: Path, probe: Dict[str, Any], ffmpeg: str) -> List[Dict[str, Any]]:
    duration = media_duration(probe)
    results: List[Dict[str, Any]] = []
    for position in sample_positions(duration):
        command = [
            ffmpeg,
            "-hide_banner",
            "-nostdin",
            "-v",
            "error",
            "-xerror",
            "-ss",
            "%.3f" % position,
            "-t",
            "5",
            "-i",
            str(path),
            "-map",
            "0:v:0",
            "-an",
            "-sn",
            "-f",
            "null",
            "-",
        ]
        result = run_process(command, timeout=180, check=False)
        results.append(
            {
                "position": position,
                "ok": result.returncode == 0,
                "error": result.stderr.strip()[-1000:] if result.returncode else "",
            }
        )
        if result.returncode:
            break  # A definite decoder failure already rules out replacement.
    return results


def inspect_path(path: Path, settings: Settings, *, decode_candidates: bool = True) -> Dict[str, Any]:
    if not path.is_file():
        raise NormalizerError("media file does not exist: %s" % path)
    probe = probe_media(path, settings.ffprobe)
    report = classify_media(path, probe)
    stat = path.stat()
    report["file_identity"] = "%d:%d:%d" % (stat.st_dev, stat.st_ino, stat.st_size)
    report["size"] = stat.st_size
    report["duration"] = media_duration(probe)
    if decode_candidates and report["decision"] == "normalize":
        samples = sample_decode(path, probe, settings.ffmpeg)
        report["decode_samples"] = samples
        if not all(sample["ok"] for sample in samples):
            report["decision"] = "report_only_decode_failure"
    return report


def bitrate_range(width: int, height: int) -> Tuple[int, int]:
    if width >= 3000 or height >= 2000:
        return 20_000_000, 35_000_000
    if width >= 1900 or height >= 1000:
        return 8_000_000, 15_000_000
    return 5_000_000, 10_000_000


def source_video_bitrate(probe: Dict[str, Any]) -> int:
    video = primary_video(probe)
    candidates = [video.get("bit_rate"), probe.get("format", {}).get("bit_rate")]
    for candidate in candidates:
        try:
            value = int(candidate)
            if value > 0:
                return value
        except (TypeError, ValueError):
            pass
    duration = media_duration(probe)
    size = int(probe.get("format", {}).get("size") or 0)
    return int(size * 8 / duration) if size and duration else 0


def select_bitrate(probe: Dict[str, Any]) -> Tuple[int, int]:
    video = primary_video(probe)
    floor, ceiling = bitrate_range(int(video.get("width") or 0), int(video.get("height") or 0))
    source = source_video_bitrate(probe)
    return min(ceiling, max(floor, source)), ceiling


def retry_bitrate(current: int, ceiling: int) -> Optional[int]:
    if current >= ceiling:
        return None
    return min(ceiling, int(math.ceil(current * 1.25 / 1000.0) * 1000))


def output_frame_rate(report: Dict[str, Any]) -> Fraction:
    source = parse_fraction(report.get("frame_rate"))
    if "hfr" in report.get("signatures", []):
        target = hfr_target(source)
        if target is None:
            raise ValidationError("unsupported HFR rate %s" % report.get("frame_rate"))
        return target
    if source is None:
        raise ValidationError("source frame rate is unknown")
    return source


def stream_inventory(probe: Dict[str, Any]) -> Dict[str, Any]:
    inventory: Dict[str, Any] = {"chapters": len(probe.get("chapters", []))}
    main_video = primary_video(probe)
    for kind in ("audio", "subtitle", "attachment", "data", "video"):
        entries = []
        for stream in probe.get("streams", []):
            if stream.get("codec_type") != kind:
                continue
            if kind == "video" and stream is main_video:
                continue
            entries.append(
                {
                    "codec": stream.get("codec_name"),
                    "tags": {
                        name: stream.get("tags", {}).get(name)
                        for name in ("language", "title", "filename", "mimetype")
                        if stream.get("tags", {}).get(name) is not None
                    },
                    "disposition": stream.get("disposition", {}),
                }
            )
        inventory[kind] = entries
    return inventory


def disposition_arg(stream: Dict[str, Any]) -> str:
    disposition = stream.get("disposition") or {}
    names = [name for name in DISPOSITION_FLAGS if disposition.get(name)]
    return "+".join(names) if names else "0"


def append_disposition_args(command: List[str], probe: Dict[str, Any]) -> None:
    counts = {"video": 0, "audio": 0, "subtitle": 0}
    specifiers = {"video": "v", "audio": "a", "subtitle": "s"}
    for stream in probe.get("streams", []):
        kind = stream.get("codec_type")
        if kind not in counts:
            continue
        command.extend(
            [
                "-disposition:%s:%d" % (specifiers[kind], counts[kind]),
                disposition_arg(stream),
            ]
        )
        counts[kind] += 1


def is_relative_to(path: Path, parent: Path) -> bool:
    try:
        path.resolve().relative_to(parent.resolve())
        return True
    except ValueError:
        return False


def require_stable(path: Path, settings: Settings) -> os.stat_result:
    first = path.stat()
    age = time.time() - first.st_mtime
    if age < settings.stable_age_seconds:
        raise SafetyError("file is only %.0f seconds old" % age)
    if settings.settle_seconds > 0:
        time.sleep(settings.settle_seconds)
    second = path.stat()
    if (first.st_dev, first.st_ino, first.st_size, first.st_mtime_ns) != (
        second.st_dev,
        second.st_ino,
        second.st_size,
        second.st_mtime_ns,
    ):
        raise SafetyError("file changed during the stability check")
    return second


def lsof_active(path: Path) -> bool:
    lsof = shutil.which("lsof") or "/usr/sbin/lsof"
    result = run_process([lsof, "-F", "p", "--", str(path)], timeout=30, check=False)
    if result.returncode not in (0, 1) or result.stderr.strip():
        raise SafetyError("unable to verify whether the media file is open")
    return result.returncode == 0 and bool(result.stdout.strip())


def tautulli_api_key(path: Path) -> Optional[str]:
    if not path.is_file():
        return None
    parser = configparser.ConfigParser(interpolation=None)
    parser.read(path)
    for section in parser.sections():
        for key in ("api_key", "apikey"):
            value = parser.get(section, key, fallback="").strip()
            if value:
                return value
    return None


def tautulli_active(path: Path, settings: Settings) -> bool:
    api_key = tautulli_api_key(settings.tautulli_config)
    if not api_key:
        return False
    query = urllib.parse.urlencode({"apikey": api_key, "cmd": "get_activity"})
    url = "%s/api/v2?%s" % (settings.tautulli_url, query)
    try:
        with urllib.request.urlopen(url, timeout=15) as response:
            payload = json.loads(response.read())
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
        raise SafetyError("unable to verify Tautulli playback activity: %s" % exc) from exc
    sessions = payload.get("response", {}).get("data", {}).get("sessions", [])
    target = str(path.resolve())
    for session in sessions:
        candidate = session.get("file") or session.get("full_path")
        if candidate and str(api_path_to_host(candidate).resolve()) == target:
            return True
    return False


def require_inactive(path: Path, settings: Settings) -> None:
    if tautulli_active(path, settings):
        raise SafetyError("file is in an active Plex/Tautulli session")
    if lsof_active(path):
        raise SafetyError("file is open according to lsof")


def estimated_output_bytes(probe: Dict[str, Any], bitrate: int) -> int:
    duration = media_duration(probe)
    source_total = int(probe.get("format", {}).get("bit_rate") or bitrate)
    source_video = max(0, source_video_bitrate(probe))
    non_video = max(0, source_total - source_video)
    return int(duration * (bitrate + non_video) / 8 * 1.2) + 1024**3


def require_free_space(directory: Path, required: int) -> None:
    free = shutil.disk_usage(directory).free
    if free < required:
        raise SafetyError(
            "insufficient free space: need %.1f GiB, have %.1f GiB"
            % (required / 1024**3, free / 1024**3)
        )


def rollback_path(source: Path, settings: Settings) -> Path:
    digest = hashlib.sha256(str(source).encode("utf-8")).hexdigest()[:12]
    timestamp = utc_now().strftime("%Y%m%dT%H%M%SZ")
    return settings.rollback_root / (timestamp + "-" + digest) / source.name


def create_rollback(source: Path, destination: Path) -> Dict[str, Any]:
    destination.parent.mkdir(parents=True, exist_ok=False)
    before = source.stat()
    method = "hardlink"
    try:
        os.link(source, destination)
    except OSError:
        method = "copy"
        require_free_space(destination.parent, before.st_size + 1024**3)
        shutil.copy2(source, destination)
    after = destination.stat()
    if after.st_size != before.st_size:
        raise SafetyError("rollback copy size does not match source")
    if method == "hardlink" and (after.st_dev, after.st_ino) != (before.st_dev, before.st_ino):
        raise SafetyError("rollback hardlink does not reference the source inode")
    return {
        "path": str(destination),
        "method": method,
        "inode": after.st_ino,
        "device": after.st_dev,
        "created_at": isoformat(utc_now()),
    }


def temporary_output(path: Path) -> Path:
    return path.parent / (
        ".%s%s%d.tmp%s" % (path.name, TEMP_MARKER, os.getpid(), path.suffix.lower())
    )


def encode_command(
    source: Path,
    output: Path,
    probe: Dict[str, Any],
    report: Dict[str, Any],
    bitrate: int,
    ffmpeg: str,
) -> List[str]:
    video = primary_video(probe)
    depth = bit_depth(video)
    pixel_format = "p010le" if depth > 8 else "yuv420p"
    profile = "main10" if depth > 8 else "main"
    filters: List[str] = []
    if "hfr" in report.get("signatures", []):
        filters.append("fps=%s" % rate_string(output_frame_rate(report)))
    filters.append("format=%s" % pixel_format)
    command = [
        ffmpeg,
        "-hide_banner",
        "-nostdin",
        "-y",
        "-i",
        str(source),
        "-map",
        "0",
        "-map_metadata",
        "0",
        "-map_chapters",
        "0",
        "-c",
        "copy",
        "-c:v:0",
        "hevc_videotoolbox",
        "-profile:v:0",
        profile,
        "-pix_fmt:v:0",
        pixel_format,
        "-b:v:0",
        str(bitrate),
        "-spatial_aq:v:0",
        "1",
        "-prio_speed:v:0",
        "0",
        "-realtime:v:0",
        "0",
        "-filter:v:0",
        ",".join(filters),
        "-fps_mode:v:0",
        "cfr",
    ]
    append_disposition_args(command, probe)
    color_options = {
        "color_primaries": "-color_primaries:v:0",
        "color_transfer": "-color_trc:v:0",
        "color_space": "-colorspace:v:0",
        "color_range": "-color_range:v:0",
    }
    for field, option in color_options.items():
        value = video.get(field)
        if value and value != "unknown":
            command.extend([option, str(value)])
    command.extend(["-max_muxing_queue_size", "4096"])
    if source.suffix.lower() == ".mp4":
        command.extend(["-f", "mp4", "-tag:v:0", "hvc1", "-movflags", "+faststart"])
    else:
        command.extend(["-f", "matroska", "-default_mode", "passthrough"])
    command.append(str(output))
    return command


def full_decode(path: Path, ffmpeg: str) -> None:
    command = [
        ffmpeg,
        "-hide_banner",
        "-nostdin",
        "-v",
        "error",
        "-xerror",
        "-hwaccel",
        "videotoolbox",
        "-i",
        str(path),
        "-map",
        "0:v:0",
        "-an",
        "-sn",
        "-f",
        "null",
        "-",
    ]
    run_process(command, timeout=7200)


def compare_streams(
    source_probe: Dict[str, Any],
    output_probe: Dict[str, Any],
    report: Dict[str, Any],
) -> None:
    source_video = primary_video(source_probe)
    output_video = primary_video(output_probe)
    if output_video.get("codec_name") not in {"hevc", "h265"}:
        raise ValidationError("output video is not HEVC")
    complete, missing = hvcc_status(output_video)
    if complete is not True:
        raise ValidationError("output hvcC is incomplete: %s" % ", ".join(missing))
    for field in ("width", "height"):
        if int(source_video.get(field) or 0) != int(output_video.get(field) or 0):
            raise ValidationError("output %s changed" % field)
    expected_rate = output_frame_rate(report)
    actual_rate = video_rate(output_video)
    if actual_rate is None or abs(float(actual_rate - expected_rate)) > 0.01:
        raise ValidationError(
            "output frame rate %s does not match %s"
            % (rate_string(actual_rate), rate_string(expected_rate))
        )
    if bit_depth(source_video) != bit_depth(output_video):
        raise ValidationError("output bit depth changed")
    for field in ("color_primaries", "color_transfer", "color_space", "color_range"):
        source_value = source_video.get(field)
        if source_value and source_value != "unknown" and output_video.get(field) != source_value:
            raise ValidationError("output %s changed" % field)
    for tag in ("language", "title"):
        if source_video.get("tags", {}).get(tag) != output_video.get("tags", {}).get(tag):
            raise ValidationError("output video %s tag changed" % tag)
    if source_video.get("disposition", {}) != output_video.get("disposition", {}):
        raise ValidationError("output video disposition changed")
    if stream_inventory(source_probe) != stream_inventory(output_probe):
        raise ValidationError(
            "audio, subtitle, attachment, data, secondary video, or chapter metadata changed"
        )
    duration_difference = abs(media_duration(source_probe) - media_duration(output_probe))
    if duration_difference > max(1.0, 2.0 / float(expected_rate)):
        raise ValidationError("output duration changed by %.3f seconds" % duration_difference)


def metric_value(text: str, kind: str) -> float:
    pattern = r"All:([0-9.]+)" if kind == "ssim" else r"average:([0-9.]+|inf)"
    matches = re.findall(pattern, text)
    if not matches:
        raise ValidationError("unable to parse %s metric" % kind.upper())
    return 100.0 if matches[-1] == "inf" else float(matches[-1])


def quality_windows(duration: float, window: int, frame_rate: Fraction) -> List[float]:
    maximum = max(0.0, duration - window)
    values = [0.0, max(0.0, duration / 2.0 - window / 2.0), maximum]
    # Independent input seeks must start on the same output-frame boundary.
    # A half-frame seek changes the HFR downsampling phase and compares
    # different pictures. Use boundaries exactly representable in milliseconds
    # (also the Matroska timestamp precision), including NTSC frame rates.
    step = Fraction(frame_rate.denominator, math.gcd(frame_rate.numerator, 1000))
    result: List[float] = []
    for value in values:
        aligned = int(Fraction(str(min(value, maximum))) / step) * step
        rounded = round(float(aligned), 3)
        if rounded not in result:
            result.append(rounded)
    return result


def quality_metrics(
    source: Path,
    output: Path,
    source_probe: Dict[str, Any],
    report: Dict[str, Any],
    settings: Settings,
) -> Dict[str, Any]:
    depth = bit_depth(primary_video(source_probe))
    pixel_format = "yuv420p10le" if depth > 8 else "yuv420p"
    output_rate = output_frame_rate(report)
    reference_filters: List[str] = []
    if "hfr" in report.get("signatures", []):
        reference_filters.append("fps=%s" % rate_string(output_rate))
    # Container timestamps can round an NTSC frame to milliseconds while the
    # reference fps filter uses an exact rational clock. Rebase both samples
    # onto the same frame clock so framesync does not reuse the previous frame.
    output_filters = [
        "settb=expr=%s" % rate_string(1 / output_rate),
        "setpts=N",
        "format=%s" % pixel_format,
    ]
    reference_filters.extend(output_filters)
    windows: List[Dict[str, Any]] = []
    duration = media_duration(source_probe)
    for position in quality_windows(
        duration, settings.quality_window_seconds, output_rate
    ):
        values: Dict[str, Any] = {"position": position}
        for metric in ("ssim", "psnr"):
            graph = "[0:v:0]%s[ref];[1:v:0]%s[dist];[ref][dist]%s" % (
                ",".join(reference_filters),
                ",".join(output_filters),
                metric,
            )
            command = [
                settings.ffmpeg,
                "-hide_banner",
                "-nostdin",
                "-v",
                "info",
                "-ss",
                "%.3f" % position,
                "-t",
                str(settings.quality_window_seconds),
                "-i",
                str(source),
                "-ss",
                "%.3f" % position,
                "-t",
                str(settings.quality_window_seconds),
                "-i",
                str(output),
                "-filter_complex",
                graph,
                "-an",
                "-sn",
                "-f",
                "null",
                "-",
            ]
            result = run_process(command, timeout=1800)
            values[metric] = metric_value(result.stderr, metric)
        windows.append(values)
    mean_ssim = sum(item["ssim"] for item in windows) / len(windows)
    mean_psnr = sum(item["psnr"] for item in windows) / len(windows)
    return {"windows": windows, "mean_ssim": mean_ssim, "mean_psnr": mean_psnr}


def validate_quality(metrics: Dict[str, Any]) -> None:
    # PSNR floor is 37 rather than 40: grainy sources (e.g. Tires) encode with
    # visually transparent SSIM >= 0.98 but cannot reach 40 dB even at the
    # bitrate ceiling, and rejecting them leaves the incompatible original.
    if metrics["mean_ssim"] < 0.98 or metrics["mean_psnr"] < 37.0:
        raise ValidationError(
            "quality gate failed: mean SSIM %.5f, mean PSNR %.2f dB"
            % (metrics["mean_ssim"], metrics["mean_psnr"])
        )


def plex_refresh(path: Path, settings: Settings) -> Dict[str, Any]:
    if not settings.plex_plist.is_file():
        return {"status": "skipped", "reason": "Plex preferences not found"}
    try:
        with settings.plex_plist.open("rb") as handle:
            preferences = plistlib.load(handle)
        token = preferences.get("PlexOnlineToken")
    except (OSError, plistlib.InvalidFileException) as exc:
        return {"status": "skipped", "reason": "unable to read Plex preferences: %s" % exc}
    if not token:
        return {"status": "skipped", "reason": "Plex token not found"}
    sections_url = "http://127.0.0.1:32400/library/sections?" + urllib.parse.urlencode(
        {"X-Plex-Token": token}
    )
    try:
        with urllib.request.urlopen(sections_url, timeout=20) as response:
            root = ET.fromstring(response.read())
        matches: List[Tuple[int, int]] = []
        for directory in root.findall("Directory"):
            key = directory.get("key")
            if not key:
                continue
            for location in directory.findall("Location"):
                location_path = location.get("path")
                if location_path and is_relative_to(path, Path(location_path)):
                    matches.append((len(location_path), int(key)))
        if not matches:
            return {"status": "skipped", "reason": "no Plex library contains the path"}
        section = max(matches)[1]
        query = urllib.parse.urlencode(
            {"path": str(path.parent), "X-Plex-Token": token}
        )
        request = urllib.request.Request(
            "http://127.0.0.1:32400/library/sections/%d/refresh?%s" % (section, query),
            method="GET",
        )
        with urllib.request.urlopen(request, timeout=30):
            pass
        return {"status": "requested", "section": section}
    except (urllib.error.URLError, TimeoutError, ET.ParseError) as exc:
        return {"status": "failed", "reason": str(exc)}


def replace_atomically(output: Path, destination: Path) -> None:
    if output.parent != destination.parent:
        raise SafetyError("temporary output is not in the destination directory")
    if not output.name.startswith("." + destination.name + TEMP_MARKER):
        raise SafetyError("refusing to replace from an unexpected temporary path")
    os.replace(output, destination)


def record_for(report: Dict[str, Any], status: str, **extra: Any) -> Dict[str, Any]:
    value = {
        "updated_at": isoformat(utc_now()),
        "file_identity": report.get("file_identity"),
        "signature": report.get("signatures", []),
        "decision": report.get("decision"),
        "status": status,
        "metrics": report,
    }
    value.update(extra)
    return value


def normalize_one(
    path: Path,
    settings: Settings,
    state: Dict[str, Any],
    *,
    dry_run: bool,
    managed: Optional[ManagedMedia] = None,
    clients: Optional[Dict[str, ArrClient]] = None,
) -> Dict[str, Any]:
    path = path.resolve()
    if not dry_run and (settings.library_root is None or not is_relative_to(path, settings.library_root)):
        raise SafetyError("media replacement requires a path within the configured library root")
    try:
        report = inspect_path(path, settings)
    except (TransientMediaProcessError, KeyboardInterrupt, SystemExit) as exc:
        # Inspection precedes rollback/encoding. Persist retryability here too,
        # including direct normalize/season calls that do not use process_poll_item.
        if not dry_run:
            status = exc.status if isinstance(exc, TransientMediaProcessError) else "interrupted"
            deferred = {"decision": "inspection_deferred", "file_identity": current_file_identity(path)}
            state["files"][str(path)] = record_for(deferred, status, error=str(exc))
            atomic_write_json(settings.state_path, state)
        raise
    result: Dict[str, Any] = {"path": str(path), "inspection": report}
    if report["decision"] != "normalize":
        result["status"] = report["decision"]
        if not dry_run:
            state["files"][str(path)] = record_for(report, result["status"])
        return result
    if dry_run:
        result["status"] = "would_normalize"
        return result
    try:
        if path.suffix.lower() not in {".mkv", ".mp4"}:
            raise SafetyError("automatic replacement requires an MKV or MP4 destination")
        source_probe = probe_media(path, settings.ffprobe)
        bitrate, ceiling = select_bitrate(source_probe)
        require_stable(path, settings)
        require_inactive(path, settings)
        require_free_space(path.parent, estimated_output_bytes(source_probe, ceiling))
        rollback = create_rollback(path, rollback_path(path, settings))
    except (NormalizerError, OSError) as exc:
        state["files"][str(path)] = record_for(
            report,
            "blocked" if isinstance(exc, SafetyError) else "error",
            error=str(exc),
        )
        atomic_write_json(settings.state_path, state)
        raise
    original_stat = path.stat()
    output = temporary_output(path)
    attempts: List[Dict[str, Any]] = []
    state["files"][str(path)] = record_for(
        report,
        "encoding",
        rollback=rollback,
        original_inode=original_stat.st_ino,
        original_device=original_stat.st_dev,
    )
    state["rollbacks"].append(dict(rollback, source_path=str(path)))
    atomic_write_json(settings.state_path, state)
    try:
        while True:
            with contextlib.suppress(FileNotFoundError):
                output.unlink()
            log("encoding %s at %.3f Mb/s" % (path, bitrate / 1_000_000), settings)
            started = time.monotonic()
            run_process(
                encode_command(path, output, source_probe, report, bitrate, settings.ffmpeg),
                timeout=24 * 3600,
            )
            output_probe = probe_media(output, settings.ffprobe)
            compare_streams(source_probe, output_probe, report)
            full_decode(output, settings.ffmpeg)
            metrics = quality_metrics(path, output, source_probe, report, settings)
            attempt = {
                "bitrate": bitrate,
                "elapsed_seconds": round(time.monotonic() - started, 3),
                "output_size": output.stat().st_size,
                "quality": metrics,
            }
            attempts.append(attempt)
            try:
                validate_quality(metrics)
                break
            except ValidationError as exc:
                next_bitrate = retry_bitrate(bitrate, ceiling)
                if next_bitrate is None:
                    raise QualityGateError(str(exc)) from exc
                bitrate = next_bitrate
                log("quality gate requested retry at %.3f Mb/s" % (bitrate / 1_000_000), settings)

        require_inactive(path, settings)
        current_stat = path.stat()
        if (current_stat.st_dev, current_stat.st_ino, current_stat.st_size) != (
            original_stat.st_dev,
            original_stat.st_ino,
            original_stat.st_size,
        ):
            raise SafetyError("library file changed before atomic replacement")
        output.chmod(original_stat.st_mode & 0o777)
        replace_atomically(output, path)
        replacement_stat = path.stat()
        if (replacement_stat.st_dev, replacement_stat.st_ino) == (
            original_stat.st_dev,
            original_stat.st_ino,
        ):
            raise SafetyError("replacement unexpectedly reused the source inode")
        arr_scan: Dict[str, Any] = {"status": "skipped"}
        if managed and clients:
            clients[managed.service].rescan(managed.owner_id)
            arr_scan = {"status": "requested", "service": managed.service, "id": managed.owner_id}
        plex_scan = plex_refresh(path, settings)
        result.update(
            {
                "status": "normalized",
                "rollback": rollback,
                "attempts": attempts,
                "output": {
                    "path": str(path),
                    "inode": replacement_stat.st_ino,
                    "device": replacement_stat.st_dev,
                    "size": replacement_stat.st_size,
                },
                "arr_scan": arr_scan,
                "plex_scan": plex_scan,
            }
        )
        state["files"][str(path)] = record_for(
            report,
            "normalized",
            rollback=rollback,
            attempts=attempts,
            output=result["output"],
            arr_scan=arr_scan,
            plex_scan=plex_scan,
            file_identity="%d:%d:%d"
            % (replacement_stat.st_dev, replacement_stat.st_ino, replacement_stat.st_size),
        )
        atomic_write_json(settings.state_path, state)
        return result
    except BaseException as exc:
        if isinstance(exc, (KeyboardInterrupt, SystemExit)):
            status = "interrupted"
        elif isinstance(exc, TransientMediaProcessError):
            status = exc.status
        elif isinstance(exc, QualityGateError):
            status = "quality_rejected"
        else:
            status = "error"
        state["files"][str(path)] = record_for(
            report,
            status,
            rollback=rollback,
            attempts=attempts,
            error=str(exc),
        )
        atomic_write_json(settings.state_path, state)
        raise
    finally:
        with contextlib.suppress(FileNotFoundError):
            output.unlink()


def cleanup_rollbacks(state: Dict[str, Any], settings: Settings) -> List[str]:
    cutoff = utc_now() - dt.timedelta(days=settings.rollback_days)
    removed: List[str] = []
    for rollback in state.get("rollbacks", []):
        raw_path = rollback.get("path")
        created = rollback.get("created_at")
        if (
            not raw_path
            or not created
            or rollback.get("retired_at")
            or parse_datetime(created) > cutoff
        ):
            continue
        path = Path(raw_path)
        if not is_relative_to(path, settings.rollback_root):
            continue
        with contextlib.suppress(FileNotFoundError):
            path.unlink()
            removed.append(str(path))
        with contextlib.suppress(OSError):
            path.parent.rmdir()
        rollback["retired_at"] = isoformat(utc_now())
    return removed


def all_managed(clients: Dict[str, ArrClient]) -> List[ManagedMedia]:
    found: List[ManagedMedia] = []
    for client in clients.values():
        found.extend(client.managed_media())
    unique: Dict[str, ManagedMedia] = {}
    for item in found:
        if TEMP_MARKER in item.path.name:
            # Sonarr/Radarr can register our in-flight temp outputs as episode
            # files if a library scan races an encode; never treat them as work.
            continue
        if item.path.suffix.lower() in VIDEO_EXTENSIONS:
            unique[str(item.path)] = item
    return sorted(unique.values(), key=lambda item: str(item.path))


def find_managed(path: Path, clients: Dict[str, ArrClient]) -> Optional[ManagedMedia]:
    target = str(path.resolve())
    return next((item for item in all_managed(clients) if str(item.path.resolve()) == target), None)


def run_audit(settings: Settings, *, json_output: bool) -> int:
    clients = arr_clients(settings)
    reports: List[Dict[str, Any]] = []
    failures = 0
    for item in all_managed(clients):
        try:
            report = inspect_path(item.path, settings)
            report.update({"service": item.service, "owner_id": item.owner_id, "file_id": item.file_id})
        except NormalizerError as exc:
            failures += 1
            report = {
                "path": str(item.path),
                "service": item.service,
                "decision": "audit_error",
                "error": str(exc),
            }
        reports.append(report)
        if not json_output and report["decision"] != "compatible":
            print("%-42s %s" % (report["decision"], report["path"]))
    if json_output:
        json.dump(
            {
                "generated_at": isoformat(utc_now()),
                "count": len(reports),
                "failures": failures,
                "reports": reports,
            },
            sys.stdout,
            indent=2,
            sort_keys=True,
        )
        print()
    else:
        findings = sum(1 for report in reports if report["decision"] != "compatible")
        print("Audited %d files; %d findings; %d probe failures." % (len(reports), findings, failures))
    return 0


def current_file_identity(path: Path) -> Optional[str]:
    try:
        stat = path.stat()
    except OSError:
        return None
    return "%d:%d:%d" % (stat.st_dev, stat.st_ino, stat.st_size)


def current_file_stamp(path: Path) -> Optional[str]:
    try:
        stat = path.stat()
    except OSError:
        return None
    return "%d:%d:%d:%d:%d" % (
        stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns)


def mark_automatic(state: Dict[str, Any], item: ManagedMedia) -> None:
    record = state["files"].setdefault(str(item.path), {})
    record["automatic"] = True
    record["service"] = item.service
    record["date_added"] = item.date_added


def select_poll_work(
    items: Sequence[ManagedMedia],
    state: Dict[str, Any],
    watermarks: Dict[str, str],
    cycle: dt.datetime,
) -> Tuple[List[ManagedMedia], List[ManagedMedia]]:
    """Split Arr media into watermark/retry/replaced work versus unseen backfill."""
    priority: Dict[str, ManagedMedia] = {}
    unseen: List[ManagedMedia] = []
    for item in items:
        key = str(item.path)
        added = parse_datetime(item.date_added)
        watermark = parse_datetime(watermarks.get(item.service, ""))
        record = state["files"].get(key)
        imported = watermark < added <= cycle
        if record:
            retry = record.get("status") in RETRYABLE_STATUSES and record.get(
                "automatic", True
            )
            recorded_identity = record.get("file_identity")
            current_identity = current_file_identity(item.path)
            replaced = bool(
                recorded_identity
                and current_identity
                and current_identity != recorded_identity
            )
            if imported or retry or replaced:
                priority[key] = item
            continue
        if imported:
            priority[key] = item
        else:
            unseen.append(item)
    return (
        sorted(priority.values(), key=lambda item: (item.date_added, str(item.path))),
        sorted(
            unseen,
            key=lambda item: (-parse_datetime(item.date_added).timestamp(), str(item.path)),
        ),
    )


def process_poll_item(
    item: ManagedMedia,
    settings: Settings,
    state: Dict[str, Any],
    clients: Dict[str, ArrClient],
    *,
    dry_run: bool,
    source: str,
) -> Tuple[str, bool]:
    try:
        result = normalize_one(
            item.path,
            settings,
            state,
            dry_run=dry_run,
            managed=item,
            clients=clients,
        )
        if not dry_run:
            mark_automatic(state, item)
            atomic_write_json(settings.state_path, state)
        log("%s result: %s %s" % (source, result["status"], item.path), settings)
        return result["status"], False
    except (NormalizerError, OSError) as exc:
        if isinstance(exc, TransientMediaProcessError):
            status = exc.status
        elif isinstance(exc, QualityGateError):
            status = "quality_rejected"
        elif isinstance(exc, SafetyError):
            status = "blocked"
        else:
            status = "error"
        record = state["files"].setdefault(str(item.path), {})
        record.update(
            {
                "updated_at": isoformat(utc_now()),
                "status": status,
                "error": str(exc),
                "automatic": True,
                "service": item.service,
                "date_added": item.date_added,
            }
        )
        if not dry_run:
            atomic_write_json(settings.state_path, state)
        deferred = isinstance(exc, TransientMediaProcessError)
        log("%s %s for %s: %s" % (source, "deferred" if deferred else "failed", item.path, exc),
            settings, stderr=not deferred)
        return record["status"], True


def run_poll(settings: Settings, *, dry_run: bool) -> int:
    state = load_state(settings.state_path)
    clients = arr_clients(settings)
    cycle = utc_now()
    if not state["watermarks"]:
        if dry_run:
            log("dry run: first real poll would establish the import watermark", settings)
        else:
            state["watermarks"] = {service: isoformat(cycle) for service in clients}
            state["initialized_at"] = isoformat(cycle)
            atomic_write_json(settings.state_path, state)
            log(
                "established first-run watermark; later polls backfill older incompatible media",
                settings,
            )
        return 0

    priority, unseen = select_poll_work(
        all_managed(clients),
        state,
        state["watermarks"],
        cycle,
    )
    completed = state.get("pending_poll", {}).get("completed", {})
    def already_completed(item: ManagedMedia) -> bool:
        identity = current_file_stamp(item.path)
        return identity is not None and completed.get(str(item.path)) == identity
    priority = [item for item in priority if not already_completed(item)]
    unseen = [item for item in unseen if not already_completed(item)]
    if not dry_run:
        # Retain the entire unfinished poll, including not-yet-visited imports.
        # Completion and its watermark are committed together below.
        state["pending_poll"] = {"version": 1, "completed": completed}
        atomic_write_json(settings.state_path, state)
    def checkpoint(item: ManagedMedia, before_stamp: Optional[str]) -> None:
        if not dry_run:
            stamp = current_file_stamp(item.path)
            result_identity = state["files"].get(str(item.path), {}).get("file_identity")
            if (stamp is not None and stamp == before_stamp
                    and ":".join(stamp.split(":")[:3]) == result_identity):
                completed[str(item.path)] = stamp
            else:
                completed.pop(str(item.path), None)
            atomic_write_json(settings.state_path, state)
    backfill: List[ManagedMedia] = []
    inspected = 0
    backfill_encoded = 0
    failures = 0
    for item in priority:
        before_stamp = current_file_stamp(item.path)
        _status, failed = process_poll_item(
            item,
            settings,
            state,
            clients,
            dry_run=dry_run,
            source="poll",
        )
        failures += int(failed)
        if _status in ADMISSION_RETRY_STATUSES:
            return 75
        checkpoint(item, before_stamp)

    for item in unseen:
        if inspected >= settings.backfill_inspect_limit:
            break
        if backfill_encoded >= settings.backfill_encode_limit:
            break
        inspected += 1
        backfill.append(item)
        before_stamp = current_file_stamp(item.path)
        status, failed = process_poll_item(
            item,
            settings,
            state,
            clients,
            dry_run=dry_run,
            source="backfill",
        )
        failures += int(failed)
        if status in ADMISSION_RETRY_STATUSES:
            return 75
        checkpoint(item, before_stamp)
        if status in ENCODED_POLL_STATUSES:
            backfill_encoded += 1

    if not dry_run:
        state["watermarks"] = {service: isoformat(cycle) for service in clients}
        removed = cleanup_rollbacks(state, settings)
        state.pop("pending_poll", None)
        atomic_write_json(settings.state_path, state)
        if removed:
            log(
                "retired %d rollback links older than %d days"
                % (len(removed), settings.rollback_days),
                settings,
            )
    log(
        "poll complete: %d import(s), %d backfill(s), %d failure(s)"
        % (len(priority), len(backfill), failures),
        settings,
    )
    return 1 if failures else 0


def deferred_poll_pending(state: Dict[str, Any]) -> bool:
    return "pending_poll" in state or any(
        record.get("status") in ADMISSION_RETRY_STATUSES and record.get("automatic", True)
        for record in state["files"].values() if isinstance(record, dict)
    )


def run_normalize_path(settings: Settings, path: Path, *, dry_run: bool) -> int:
    state = load_state(settings.state_path)
    clients = arr_clients(settings)
    managed = find_managed(path, clients)
    try:
        result = normalize_one(
            path,
            settings,
            state,
            dry_run=dry_run,
            managed=managed,
            clients=clients,
        )
    except (NormalizerError, OSError) as exc:
        log("normalize failed for %s: %s" % (path, exc), settings, stderr=True)
        return 75 if isinstance(exc, TransientMediaProcessError) else 1
    if not dry_run:
        atomic_write_json(settings.state_path, state)
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


def run_normalize_season(
    settings: Settings, series_id: int, season: int, *, dry_run: bool
) -> int:
    state = load_state(settings.state_path)
    clients = arr_clients(settings)
    files = clients["sonarr"].sonarr_season(series_id, season)
    failures = 0
    for item in files:
        try:
            result = normalize_one(
                item.path,
                settings,
                state,
                dry_run=dry_run,
                managed=item,
                clients=clients,
            )
            if not dry_run:
                mark_automatic(state, item)
            log("season result: %s %s" % (result["status"], item.path), settings)
        except (NormalizerError, OSError) as exc:
            failures += 1
            if not dry_run:
                mark_automatic(state, item)
            log("season failed for %s: %s" % (item.path, exc), settings, stderr=True)
            if isinstance(exc, TransientMediaProcessError):
                if not dry_run:
                    atomic_write_json(settings.state_path, state)
                return 75
    if not dry_run:
        atomic_write_json(settings.state_path, state)
    log("season complete: %d file(s), %d failure(s)" % (len(files), failures), settings)
    return 1 if failures else 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    audit = subparsers.add_parser("audit", help="inspect all Arr-managed media")
    audit.add_argument("--json", action="store_true", help="emit a machine-readable report")
    poll = subparsers.add_parser(
        "poll",
        help="process new Arr imports, then backfill older unrecorded incompatible media",
    )
    poll.add_argument("--apply", action="store_true", help="allow media replacement and write state")
    retry = subparsers.add_parser("retry-deferred", help="resume an unfinished automatic poll")
    retry.add_argument("--apply", action="store_true", required=True)
    normalize = subparsers.add_parser("normalize", help="explicitly normalize existing media")
    selection = normalize.add_mutually_exclusive_group(required=True)
    selection.add_argument("--path", type=Path)
    selection.add_argument("--sonarr-series-id", type=int)
    normalize.add_argument("--season", type=int)
    normalize.add_argument("--apply", action="store_true", help="allow media replacement and write state")
    return parser


def _main(argv: Optional[Sequence[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    if args.command == "normalize" and args.sonarr_series_id is not None and args.season is None:
        raise SystemExit("--season is required with --sonarr-series-id")
    if args.command == "normalize" and args.path is not None and args.season is not None:
        raise SystemExit("--season is only valid with --sonarr-series-id")
    settings = load_settings()
    try:
        if getattr(args, "apply", False):
            root = settings.library_root
            if root is None or not root.is_absolute() or root.resolve() == Path("/") or not root.is_dir():
                raise SafetyError("set MEDIA_NORMALIZER_LIBRARY_ROOT to an absolute media directory")
        with process_lock(settings.lock_path):
            if args.command == "retry-deferred":
                if not deferred_poll_pending(load_state(settings.state_path)):
                    return 0
                if not os.environ.get("MEDIA_NORMALIZER_MEMORY_COORDINATOR_URL"):
                    raise SafetyError("automatic deferred retry requires memory admission")
                with media_mutation_lock(settings.media_mutation_lock_path):
                    return run_poll(settings, dry_run=False)
            if args.command == "audit":
                return run_audit(settings, json_output=args.json)
            if args.command == "poll" and not args.apply:
                return run_poll(settings, dry_run=True)
            if args.command == "normalize" and not args.apply:
                if args.path is not None:
                    return run_normalize_path(settings, args.path, dry_run=True)
                return run_normalize_season(settings, args.sonarr_series_id, args.season, dry_run=True)
            with media_mutation_lock(settings.media_mutation_lock_path):
                if args.command == "poll":
                    return run_poll(settings, dry_run=False)
                if args.path is not None:
                    return run_normalize_path(settings, args.path, dry_run=False)
                return run_normalize_season(
                    settings,
                    args.sonarr_series_id,
                    args.season,
                    dry_run=False,
                )
    except (LockBusy, MediaMutationLockBusy) as exc:
        log(str(exc), settings, stderr=True)
        return 75
    except (NormalizerError, OSError) as exc:
        log("fatal: %s" % exc, settings, stderr=True)
        return 1


def main(argv: Optional[Sequence[str]] = None) -> int:
    stopping = False

    def request_stop(received: int, _frame: Any) -> None:
        nonlocal stopping
        if stopping:
            return  # Repeated signals must not interrupt child/lock cleanup.
        stopping = True
        if received == signal.SIGINT:
            raise KeyboardInterrupt
        raise SystemExit(128 + received)

    previous = {}
    for sig in (signal.SIGTERM, signal.SIGINT):
        previous[sig] = signal.signal(sig, request_stop)
    try:
        return _main(argv)
    finally:
        for sig, handler in previous.items():
            signal.signal(sig, handler)


if __name__ == "__main__":
    os.umask(0o077)
    raise SystemExit(main())
