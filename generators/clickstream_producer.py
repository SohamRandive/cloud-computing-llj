#!/usr/bin/env python3
"""Clickstream producer — simulates user browsing events on ShopStream."""

import json
import random
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))
from config.settings import (
    CLICK_EVENTS, PLATFORMS, PRODUCER_CONFIG, SITE_PAGES,
    SKU_IDS, TOPICS, TRAFFIC_SRC, USER_IDS,
)
from kafka import KafkaProducer

producer = KafkaProducer(
    **PRODUCER_CONFIG,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

TOPIC = TOPICS["clickstream"]
sent  = 0

print(f"[clickstream] Publishing to '{TOPIC}'. Ctrl+C to stop.")

try:
    while True:
        uid = random.choice(USER_IDS)
        event = {
            "session_id":   str(uuid.uuid4()),
            "user_id":      uid,
            "event_type":   random.choice(CLICK_EVENTS),
            "page":         random.choice(SITE_PAGES),
            "platform":     random.choice(PLATFORMS),
            "traffic_src":  random.choice(TRAFFIC_SRC),
            "keyword":      random.choice(["shoes", "laptop", "dress", "phone", "book",
                                           "watch", "bag", "headphones", "perfume", "jacket"])
                            if random.random() < 0.3 else None,
            "sku":          random.choice(SKU_IDS) if random.random() < 0.4 else None,
            "dwell_ms":     random.randint(200, 120000),
            "ip":           f"10.{random.randint(0,255)}.{random.randint(0,255)}.x",
            "timestamp":    datetime.now(timezone.utc).isoformat(),
        }
        producer.send(TOPIC, value=event)
        sent += 1
        if sent % 500 == 0:
            print(f"[clickstream] {sent} events sent")
        time.sleep(random.uniform(0.05, 0.15))

except KeyboardInterrupt:
    producer.flush()
    print(f"[clickstream] Stopped — {sent} events sent.")
