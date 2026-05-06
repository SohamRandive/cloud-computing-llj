#!/usr/bin/env python3
"""Social Buzz producer — simulates social media posts about ShopStream."""

import json
import random
import string
import time
from datetime import datetime, timezone
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))
from config.settings import PRODUCER_CONFIG, SOCIAL_PLATFORMS, SOCIAL_TEXTS, TOPICS, USER_IDS
from kafka import KafkaProducer
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

analyzer = SentimentIntensityAnalyzer()

producer = KafkaProducer(
    **PRODUCER_CONFIG,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

TOPIC = TOPICS["social"]
sent  = 0

HASHTAG_POOL = [
    "#ShopStream", "#OnlineShopping", "#Haul", "#MustBuy",
    "#Deal", "#Sale", "#Shopping", "#Review", "#Unboxing",
]


def _post_id():
    return "PST-" + "".join(random.choices(string.ascii_uppercase + string.digits, k=8))


def _sentiment(text: str):
    score = analyzer.polarity_scores(text)["compound"]
    if score >= 0.05:
        return "positive", round(score, 4)
    if score <= -0.05:
        return "negative", round(score, 4)
    return "neutral", round(score, 4)


print(f"[social_buzz] Publishing to '{TOPIC}'. Ctrl+C to stop.")

try:
    while True:
        text         = random.choice(SOCIAL_TEXTS)
        label, score = _sentiment(text)

        post = {
            "post_id":         _post_id(),
            "platform":        random.choice(SOCIAL_PLATFORMS),
            "user_id":         random.choice(USER_IDS) if random.random() > 0.15 else None,
            "content":         text,
            "hashtags":        random.sample(HASHTAG_POOL, k=random.randint(1, 4)),
            "likes":           random.randint(0, 3000),
            "shares":          random.randint(0, 500),
            "sentiment_label": label,
            "sentiment_score": score,
            "lang":            "en",
            "timestamp":       datetime.now(timezone.utc).isoformat(),
        }
        producer.send(TOPIC, value=post)
        sent += 1
        if sent % 100 == 0:
            print(f"[social_buzz] {sent} posts sent")
        time.sleep(random.uniform(0.3, 0.7))

except KeyboardInterrupt:
    producer.flush()
    print(f"[social_buzz] Stopped — {sent} posts sent.")
