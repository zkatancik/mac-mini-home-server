#!/usr/bin/env python3
"""Preview or prune aged, dangling Docker images explicitly marked disposable."""

import argparse
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import re
import stat
import subprocess
import sys
import time


LABEL = "io.example.home-server.disposable=true"
IMAGE_ID = re.compile(r"sha256:[0-9a-f]{64}")
CONTAINER_ID = re.compile(r"[0-9a-f]{64}")
MAX_RECORDS = 200
INSPECT_FIELDS = '{"created":{{json .Created}},"size":{{json .Size}},"tags":{{json .RepoTags}}}'


def bounded_ids(output, pattern):
    values = sorted(set(output.split()))
    if len(values) > MAX_RECORDS or any(not pattern.fullmatch(value) for value in values):
        raise ValueError("unexpected or oversized Docker inventory")
    return values


def preview(cli, cutoff):
    containers = bounded_ids(cli("container", "ls", "-aq", "--no-trunc"), CONTAINER_ID)
    used = {cli("container", "inspect", "--format", "{{.Image}}", cid).strip() for cid in containers}
    if any(not IMAGE_ID.fullmatch(image) for image in used):
        raise ValueError("unexpected container image reference")
    dangling = bounded_ids(cli("image", "ls", "-q", "--no-trunc", "--filter", "dangling=true",
                               "--filter", f"label={LABEL}"), IMAGE_ID)
    matches = []
    for image in dangling:
        if image in used:
            continue
        fields = json.loads(cli("image", "inspect", "--format", INSPECT_FIELDS, image))
        created = datetime.fromisoformat(fields["created"].replace("Z", "+00:00"))
        if created.tzinfo is None or type(fields["size"]) is not int or fields["size"] < 0:
            raise ValueError("unexpected image timestamp or size")
        tags = fields["tags"] or []
        if created < cutoff and all(tag == "<none>:<none>" for tag in tags):
            matches.append((image, fields["size"]))
    return {
        "candidate_count": len(matches),
        "candidate_images": [image for image, _ in matches],
        "summed_logical_image_bytes": sum(size for _, size in matches),
    }


def prune(cli, cutoff):
    # Native prune rechecks dangling state and container references in the daemon.
    cli("image", "prune", "--force", "--filter", f"label={LABEL}", "--filter", f"until={cutoff}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--socket", type=Path, required=True, help="absolute path to the local Docker Unix socket")
    parser.add_argument("--minimum-age-days", type=int, default=14)
    parser.add_argument("--apply", action="store_true", help="perform native prune after preview")
    args = parser.parse_args()
    if not args.socket.is_absolute() or not 1 <= args.minimum_age_days <= 3650:
        parser.error("require an absolute socket path and minimum age from 1 to 3650 days")
    try:
        socket = args.socket.resolve(strict=True)
        socket_info = socket.stat()
        if not stat.S_ISSOCK(socket_info.st_mode):
            raise ValueError("endpoint must be a local Unix socket")
        endpoint = f"unix://{socket}"
        env = {key: value for key, value in os.environ.items()
               if not key.startswith(("DOCKER_", "BUILDKIT_", "BUILDX_"))}
        deadline = time.monotonic() + 120

        def cli(*argv):
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise ValueError("Docker operation exceeded its time budget")
            current = socket.stat()
            if (current.st_dev, current.st_ino) != (socket_info.st_dev, socket_info.st_ino):
                raise ValueError("Docker socket changed during this operation")
            result = subprocess.run(["docker", "--host", endpoint, *argv], env=env,
                                    capture_output=True, text=True, timeout=min(remaining, 60))
            if result.returncode:
                raise ValueError(f"Docker {argv[0]} {argv[1]} failed")
            return result.stdout

        cutoff = datetime.now(timezone.utc) - timedelta(days=args.minimum_age_days)
        cutoff = cutoff.replace(microsecond=0)
        report = preview(cli, cutoff)
        report.update(mode="apply" if args.apply else "dry-run", cutoff=cutoff.isoformat(), prune_requested=False)
        if args.apply and report["candidate_count"]:
            prune(cli, report["cutoff"])
            report["prune_requested"] = True
        print(json.dumps(report, indent=2))
        return 0
    except subprocess.TimeoutExpired:
        print("Docker command timed out. Inspect the engine before retrying.", file=sys.stderr)
    except (KeyError, OSError, TypeError, ValueError) as exc:
        print(f"Docker retention failed: {exc}", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
