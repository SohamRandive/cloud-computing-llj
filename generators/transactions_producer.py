#!/usr/bin/env python3

import json
import random
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from config.settings import (
    CUSTOMERS, ORDER_STATUSES, PAYMENT_METHODS,
    PRODUCTS, PRODUCER_CONFIG, TOPICS,
)
from kafka import KafkaProducer

TOPIC        = TOPICS["transactions"]
RATE_PER_SEC = 3
SLEEP        = 1.0 / RATE_PER_SEC

producer = KafkaProducer(
    **PRODUCER_CONFIG,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8"),
)

# build a customer lookup for fast access
CUSTOMER_MAP = {c["customer_id"]: c for c in CUSTOMERS}

def generate_transaction() -> dict:
    customer = random.choice(CUSTOMERS)
    cid      = customer["customer_id"]

    # 1–5 line items per order
    n_items = random.randint(1, 5)
    chosen  = random.sample(PRODUCTS, n_items)

    items = []
    for product in chosen:
        qty       = random.randint(1, 4)
        unit_price = product["price"]
        items.append({
            "product_id":  product["product_id"],
            "name":        product["name"],
            "category":    product["category"],
            "qty":         qty,
            "unit_price":  unit_price,
            "subtotal":    round(qty * unit_price, 2),
        })

    total_amount  = round(sum(i["subtotal"] for i in items), 2)
    discount_pct  = random.choice([0, 0, 0, 5, 10, 15, 20, 25, 30])  # 0 most common
    final_amount  = round(total_amount * (1 - discount_pct / 100), 2)

    status         = random.choices(
        ORDER_STATUSES,
        weights=[20, 20, 20, 25, 10, 5],   # delivered most common, cancelled rare
        k=1
    )[0]
    payment_status = "success" if status != "cancelled" else random.choice(["success", "failed"])

    return {
        "order_id":        f"ORD-{uuid.uuid4().hex[:8].upper()}",
        "customer_id":     cid,
        "timestamp":       datetime.now(timezone.utc).isoformat(),
        "status":          status,
        "items":           items,
        "total_amount":    total_amount,
        "discount_pct":    discount_pct,
        "final_amount":    final_amount,
        "payment_method":  random.choice(PAYMENT_METHODS),
        "payment_status":  payment_status,
        "shipping_country": customer["country"],
        "is_return":       status == "returned",
    }

def main():
    print(f"💳 Transactions Producer started → topic: '{TOPIC}' @ {RATE_PER_SEC} msg/sec")
    print("   Press Ctrl+C to stop.\n")

    sent = 0
    try:
        while True:
            txn = generate_transaction()
            producer.send(TOPIC, key=txn["customer_id"], value=txn)
            sent += 1

            if sent % 20 == 0:
                producer.flush()
                print(f"  ✔ {sent} transactions sent | latest: {txn['order_id']} "
                      f"| {txn['customer_id']} | ₹{txn['final_amount']} | {txn['status']}")

            time.sleep(SLEEP)

    except KeyboardInterrupt:
        producer.flush()
        print(f"\n  Stopped. Total sent: {sent}")

if __name__ == "__main__":
    main()
