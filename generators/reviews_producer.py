#!/usr/bin/env python3
"""Reviews producer — simulates product reviews with VADER sentiment on ShopStream."""

import json
import random
import string
import time
from datetime import datetime, timezone
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))
from config.settings import PRODUCER_CONFIG, REVIEW_TEXTS, SKU_IDS, TOPICS, USER_IDS
from kafka import KafkaProducer
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

analyzer = SentimentIntensityAnalyzer()

producer = KafkaProducer(
    **PRODUCER_CONFIG,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

TOPIC = TOPICS["reviews"]
sent  = 0


def _review_id():
    return "RVW-" + "".join(random.choices(string.ascii_uppercase + string.digits, k=8))


def _sentiment(text: str):
    score = analyzer.polarity_scores(text)["compound"]
    if score >= 0.05:
        return "positive", round(score, 4)
    if score <= -0.05:
        return "negative", round(score, 4)
    return "neutral", round(score, 4)


print(f"[reviews] Publishing to '{TOPIC}'. Ctrl+C to stop.")

try:
    while True:
        text         = random.choice(REVIEW_TEXTS)
        label, score = _sentiment(text)
        rating       = random.choices([1, 2, 3, 4, 5], weights=[5, 10, 15, 35, 35])[0]

        review = {
            "review_id":         _review_id(),
            "user_id":           random.choice(USER_IDS),
            "sku":               random.choice(SKU_IDS),
            "rating":            rating,
            "review_text":       text,
            "verified_purchase": random.random() > 0.2,
            "helpful_votes":     random.randint(0, 80),
            "sentiment_label":   label,
            "sentiment_score":   score,
            "timestamp":         datetime.now(timezone.utc).isoformat(),
        }
        producer.send(TOPIC, value=review)
        sent += 1
        if sent % 100 == 0:
            print(f"[reviews] {sent} reviews sent")
        time.sleep(random.uniform(0.3, 0.7))

except KeyboardInterrupt:
    producer.flush()
    print(f"[reviews] Stopped — {sent} reviews sent.")
