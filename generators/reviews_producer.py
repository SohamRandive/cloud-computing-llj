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
    CUSTOMER_IDS, PRODUCTS, PRODUCER_CONFIG,
    REVIEW_TEXTS, TOPICS,
)
from kafka import KafkaProducer
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

TOPIC        = TOPICS["reviews"]
RATE_PER_SEC = 2
SLEEP        = 1.0 / RATE_PER_SEC

analyzer = SentimentIntensityAnalyzer()

producer = KafkaProducer(
    **PRODUCER_CONFIG,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8"),
)

def sentiment_label(compound: float) -> str:
    """Convert VADER compound score to categorical label."""
    if compound >= 0.05:
        return "positive"
    elif compound <= -0.05:
        return "negative"
    return "neutral"

def rating_from_sentiment(compound: float) -> int:
    """
    Map compound score to star rating with some noise.
    compound in [0.05, 1.0]  → skew toward 4–5 stars
    compound in [-0.05, 0.05] → skew toward 3 stars
    compound in [-1.0, -0.05] → skew toward 1–2 stars
    """
    if compound >= 0.05:
        return random.choices([3, 4, 5], weights=[10, 35, 55])[0]
    elif compound <= -0.05:
        return random.choices([1, 2, 3], weights=[55, 35, 10])[0]
    return 3

def generate_review() -> dict:
    product     = random.choice(PRODUCTS)
    review_text = random.choice(REVIEW_TEXTS)
    scores      = analyzer.polarity_scores(review_text)
    compound    = round(scores["compound"], 4)

    return {
        "review_id":        f"REV-{uuid.uuid4().hex[:8].upper()}",
        "customer_id":      random.choice(CUSTOMER_IDS),
        "product_id":       product["product_id"],
        "product_category": product["category"],
        "timestamp":        datetime.now(timezone.utc).isoformat(),
        "rating":           rating_from_sentiment(compound),
        "review_text":      review_text,
        "verified_purchase": random.random() > 0.3,   # 70% verified
        "helpful_votes":    random.randint(0, 50),
        "sentiment_label":  sentiment_label(compound),
        "sentiment_score":  compound,
    }

def main():
    print(f"⭐ Reviews Producer started → topic: '{TOPIC}' @ {RATE_PER_SEC} msg/sec")
    print("   Press Ctrl+C to stop.\n")

    sent = 0
    try:
        while True:
            review = generate_review()
            producer.send(TOPIC, key=review["customer_id"], value=review)
            sent += 1

            if sent % 10 == 0:
                producer.flush()
                print(f"  ✔ {sent} reviews sent | latest: {review['review_id']} "
                      f"| {review['sentiment_label']:8s} ({review['sentiment_score']:+.3f}) "
                      f"| {review['rating']}★ | {review['product_category']}")

            time.sleep(SLEEP)

    except KeyboardInterrupt:
        producer.flush()
        print(f"\n  Stopped. Total sent: {sent}")

if __name__ == "__main__":
    main()
