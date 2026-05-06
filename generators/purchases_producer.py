#!/usr/bin/env python3
"""Purchases producer — simulates orders, payments and returns on ShopStream."""

import json
import random
import string
import time
from datetime import datetime, timezone
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))
from config.settings import (
    PAYMENT_METHODS, PRODUCER_CONFIG, PRODUCTS,
    PURCHASE_STATUSES, TOPICS, USER_IDS,
)
from kafka import KafkaProducer

producer = KafkaProducer(
    **PRODUCER_CONFIG,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

TOPIC = TOPICS["purchases"]
sent  = 0


def _order_id():
    return "ORD-" + "".join(random.choices(string.ascii_uppercase + string.digits, k=8))


def _make_order(uid: str) -> dict:
    num_items = random.randint(1, 5)
    items = []
    for _ in range(num_items):
        p   = random.choice(PRODUCTS)
        qty = random.randint(1, 3)
        items.append({
            "sku":       p["sku"],
            "title":     p["title"],
            "category":  p["category"],
            "qty":       qty,
            "unit_price": p["price"],
            "subtotal":  round(p["price"] * qty, 2),
        })

    total   = round(sum(i["subtotal"] for i in items), 2)
    disc    = random.choice([0, 5, 10, 15, 20])
    final   = round(total * (1 - disc / 100), 2)
    status  = random.choice(PURCHASE_STATUSES)

    return {
        "order_id":       _order_id(),
        "user_id":        uid,
        "status":         status,
        "items":          items,
        "total_amount":   total,
        "discount_pct":   disc,
        "final_amount":   final,
        "payment_method": random.choice(PAYMENT_METHODS),
        "payment_status": random.choice(["success", "success", "success", "failed", "pending"]),
        "ship_country":   "IN",
        "is_return":      status == "returned",
        "timestamp":      datetime.now(timezone.utc).isoformat(),
    }


print(f"[purchases] Publishing to '{TOPIC}'. Ctrl+C to stop.")

try:
    while True:
        uid = random.choice(USER_IDS)
        order = _make_order(uid)
        producer.send(TOPIC, value=order)
        sent += 1
        if sent % 100 == 0:
            print(f"[purchases] {sent} orders sent")
        time.sleep(random.uniform(0.2, 0.5))

except KeyboardInterrupt:
    producer.flush()
    print(f"[purchases] Stopped — {sent} orders sent.")
