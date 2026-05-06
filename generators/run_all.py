#!/usr/bin/env python3

import signal
import subprocess
import sys
from pathlib import Path

GENERATORS = [
    "generators/web_logs_producer.py",
    "generators/transactions_producer.py",
    "generators/reviews_producer.py",
    "generators/social_media_producer.py",
]

processes = []

def shutdown(sig, frame):
    print("\n\n🛑 Shutting down all producers...")
    for p in processes:
        p.terminate()
    for p in processes:
        p.wait()
    print("✅ All producers stopped.")
    sys.exit(0)

signal.signal(signal.SIGINT,  shutdown)
signal.signal(signal.SIGTERM, shutdown)

if __name__ == "__main__":
    print("🚀 Starting all 4 data generators...\n")
    root = Path(__file__).parent.parent

    for script in GENERATORS:
        p = subprocess.Popen(
            [sys.executable, str(root / script)],
            cwd=str(root),
        )
        processes.append(p)
        print(f"  ▶ Started: {script}  (PID {p.pid})")

    print("\n  All producers running. Press Ctrl+C to stop all.\n")
    print("  Watch messages at → http://localhost:8080  (Kafka UI)\n")

    # wait for all (or until Ctrl+C)
    for p in processes:
        p.wait()
