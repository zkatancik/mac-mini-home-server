#!/usr/bin/env python3
"""Run serial Nextcloud cron through the memory admission client."""

import argparse
import json
from pathlib import Path
import signal
import subprocess
import sys
import threading


def positive_integer(value):
    number = int(value)
    if number < 1:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return number


def run_loop(command, interval):
    stopping = threading.Event()
    signals = (signal.SIGTERM, signal.SIGINT)
    handlers = {sig: signal.signal(sig, lambda *_: stopping.set()) for sig in signals}
    try:
        while not stopping.is_set():
            with subprocess.Popen(command) as child:
                while child.poll() is None and not stopping.wait(0.2):
                    pass
                if stopping.is_set() and child.poll() is None:
                    # The admission client owns PHP and releases after group cleanup.
                    child.terminate()
                result = child.wait()
            if result and not stopping.is_set():
                print(f"Cron invocation exited {result}. Retrying in {interval} seconds.", flush=True)
            stopping.wait(interval)
    finally:
        for sig, handler in handlers.items():
            signal.signal(sig, handler)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run", action="store_true", help="start the loop; default prints a plan")
    parser.add_argument("--client", type=Path, required=True)
    parser.add_argument("--url", required=True)
    parser.add_argument("--token-file", type=Path, required=True)
    parser.add_argument("--receipt-file", type=Path, required=True)
    parser.add_argument("--cron-file", type=Path, required=True)
    parser.add_argument("--peak-mib", type=positive_integer, required=True)
    parser.add_argument("--run-timeout", type=positive_integer, required=True)
    parser.add_argument("--wait-timeout", type=positive_integer, required=True)
    parser.add_argument("--interval", type=positive_integer, required=True)
    args = parser.parse_args()
    files = (args.client, args.token_file, args.receipt_file, args.cron_file)
    if any(not path.is_absolute() for path in files):
        parser.error("client, token, receipt and cron paths must be absolute")
    if not args.run:
        print(json.dumps({
            "mode": "dry-run", "job": "nextcloud-cron", "peak_mib": args.peak_mib,
            "run_timeout_seconds": args.run_timeout, "wait_timeout_seconds": args.wait_timeout,
            "interval_seconds": args.interval,
        }, indent=2))
        return 0
    command = [
        sys.executable, str(args.client), "--url", args.url, "--token-file", str(args.token_file),
        "run", "--job", "nextcloud-cron", "--peak-mib", str(args.peak_mib),
        "--run-timeout", str(args.run_timeout), "--wait-timeout", str(args.wait_timeout),
        "--receipt-file", str(args.receipt_file), "--", "php", "-f", str(args.cron_file),
    ]
    run_loop(command, args.interval)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
