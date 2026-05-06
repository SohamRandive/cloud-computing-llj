#!/usr/bin/env python3
"""Run all ShopStream data generators in parallel."""

import signal
import subprocess
import sys
from pathlib import Path

BASE = Path(__file__).parent

PRODUCERS = [
    "clickstream_producer.py",
    "purchases_producer.py",
    "reviews_producer.py",
    "social_buzz_producer.py",
]

procs = []


def _shutdown(sig, frame):
    print("\n[run_all] Shutting down all producers...")
    for p in procs:
        p.terminate()
    for p in procs:
        p.wait()
    print("[run_all] All stopped.")
    sys.exit(0)


signal.signal(signal.SIGINT,  _shutdown)
signal.signal(signal.SIGTERM, _shutdown)

print("[run_all] Starting ShopStream data generators...")
for script in PRODUCERS:
    p = subprocess.Popen([sys.executable, str(BASE / script)])
    procs.append(p)
    print(f"  ▶  {script}  (pid {p.pid})")

print(f"\n[run_all] {len(procs)} producers running. Press Ctrl+C to stop all.\n")
for p in procs:
    p.wait()
