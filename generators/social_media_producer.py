#!/usr/bin/env python3

import json
import random
import re
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from config.settings import (
    CUSTOMER_IDS, PLATFORMS, PRODUCER_CONFIG,
    SOCIAL_TEMPLATES, TOPICS,
)
from kafka import KafkaProducer
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

TOPIC        = TOPICS["social"]
RATE_PER_SEC = 2
SLEEP        = 1.0 / RATE_PER_SEC

analyzer = SentimentIntensityAnalyzer()

producer = KafkaProducer(
    **PRODUCER_CONFIG,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8"),
)

def extract_hashtags(text: str) -> list:
    """Pull #hashtags out of post text."""
    return re.findall(r"#\w+", text)

def sentiment_label(compound: float) -> str:
    if compound >= 0.05:
        return "positive"
    elif compound <= -0.05:
        return "negative"
    return "neutral"

def generate_post() -> dict:
    text       = random.choice(SOCIAL_TEMPLATES)
    scores     = analyzer.polarity_scores(text)
    compound   = round(scores["compound"], 4)
    platform   = random.choice(PLATFORMS)

    # 70% of posts are from known customers, 30% anonymous
    customer_id = random.choice(CUSTOMER_IDS) if random.random() > 0.3 else None

    # engagement roughly correlated with sentiment magnitude
    engagement_multiplier = 1 + abs(compound) * 3
    likes  = int(random.randint(0, 150) * engagement_multiplier)
    shares = int(random.randint(0, 30)  * engagement_multiplier)

    return {
        "post_id":        f"POST-{uuid.uuid4().hex[:8].upper()}",
        "platform":       platform,
        "customer_id":    customer_id,
        "timestamp":      datetime.now(timezone.utc).isoformat(),
        "text":           text,
        "hashtags":       extract_hashtags(text),
        "likes":          likes,
        "shares":         shares,
        "sentiment_label": sentiment_label(compound),
        "sentiment_score": compound,
        "language":       "en",
    }

def main():
    print(f"📱 Social Media Producer started → topic: '{TOPIC}' @ {RATE_PER_SEC} msg/sec")
    print("   Press Ctrl+C to stop.\n")

    sent = 0
    try:
        while True:
            post = generate_post()
            # key = customer_id if known, else random uuid (still needs a key for partitioning)
            key = post["customer_id"] or str(uuid.uuid4())
            producer.send(TOPIC, key=key, value=post)
            sent += 1

            if sent % 10 == 0:
                producer.flush()
                print(f"  ✔ {sent} posts sent | latest: {post['post_id']} "
                      f"| {post['platform']:10s} | {post['sentiment_label']:8s} "
                      f"({post['sentiment_score']:+.3f}) | 👍 {post['likes']}")

            time.sleep(SLEEP)

    except KeyboardInterrupt:
        producer.flush()
        print(f"\n  Stopped. Total sent: {sent}")

if __name__ == "__main__":
    main()
