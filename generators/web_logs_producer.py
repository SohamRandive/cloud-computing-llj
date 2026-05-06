#!/usr/bin/env python3

import json
import random
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

# ── allow import from project root ───────────────────
sys.path.insert(0, str(Path(__file__).parent.parent))
from config.settings import (
    CUSTOMER_IDS, DEVICES, EVENT_TYPES, PAGES,
    PRODUCT_IDS, PRODUCER_CONFIG, REFERRERS, TOPICS, fake,
)
from kafka import KafkaProducer

# ── Config ────────────────────────────────────────────
TOPIC        = TOPICS["web_logs"]
RATE_PER_SEC = 10       # target messages per second
SLEEP        = 1.0 / RATE_PER_SEC

# ── Producer ──────────────────────────────────────────
producer = KafkaProducer(
    **PRODUCER_CONFIG,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8"),
)

def generate_web_log() -> dict:
    event_type  = random.choice(EVENT_TYPES)
    customer_id = random.choice(CUSTOMER_IDS) if random.random() > 0.2 else None

    # build base IP then mask last octet (PII simulation)
    raw_ip = fake.ipv4_private()
    parts  = raw_ip.split(".")
    masked_ip = f"{parts[0]}.{parts[1]}.{parts[2]}.xxx"

    event = {
        "session_id":  str(uuid.uuid4()),
        "customer_id": customer_id,
        "event_type":  event_type,
        "url":         random.choice(PAGES),
        "referrer":    random.choice(REFERRERS),
        "device":      random.choice(DEVICES),
        "search_term": fake.word() if event_type == "search" else None,
        "product_id":  random.choice(PRODUCT_IDS)
                       if event_type in ("add_to_cart", "remove_from_cart") else None,
        "duration_ms": random.randint(500, 30000),
        "ip_address":  masked_ip,
        "timestamp":   datetime.now(timezone.utc).isoformat(),
    }
    return event

def main():
    print(f"🌐 Web Logs Producer started → topic: '{TOPIC}' @ {RATE_PER_SEC} msg/sec")
    print("   Press Ctrl+C to stop.\n")

    sent = 0
    try:
        while True:
            event = generate_web_log()

            # partition key = customer_id (or 'anonymous')
            # ensures all events for same customer go to same partition
            key = event["customer_id"] or "anonymous"

            producer.send(TOPIC, key=key, value=event)
            sent += 1

            if sent % 50 == 0:
                producer.flush()
                print(f"  ✔ {sent} web log events sent | latest: {event['event_type']} "
                      f"by {event['customer_id'] or 'anon'} on {event['device']}")

            time.sleep(SLEEP)

    except KeyboardInterrupt:
        producer.flush()
        print(f"\n  Stopped. Total sent: {sent}")

if __name__ == "__main__":
    main()
