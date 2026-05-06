#!/usr/bin/env python3
"""
ShopStream — Unified User Profile Builder

Reads silver Parquet (purchases, product_reviews, clickstream, social_buzz),
aggregates per user, then upserts into MongoDB shopstream_profiles.user_profiles.

Modes:
  --mode batch   → chunked bulk upserts (default)
  --mode stream  → one user at a time with small delay
"""

import argparse
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import psycopg2
import psycopg2.extras
from pymongo import MongoClient, UpdateOne
from pymongo.errors import BulkWriteError
from pyspark.sql import SparkSession, functions as F

sys.path.insert(0, str(Path(__file__).parent.parent))
from spark_jobs.spark_utils import SILVER_PATH, get_spark

parser = argparse.ArgumentParser()
parser.add_argument("--mode",         choices=["batch", "stream"], default="batch")
parser.add_argument("--batch-size",   type=int,   default=100)
parser.add_argument("--limit",        type=int,   default=None)
parser.add_argument("--stream-delay", type=float, default=0.05)
args = parser.parse_args()

PG_CONFIG = {
    "host": "localhost", "port": 5432,
    "dbname": "shopstream_gold",
    "user": "shopstream_user", "password": "shopstream_pg_pass",
}
MONGO_URI = "mongodb://admin:shopstream_mongo_pass@localhost:27017/"
MONGO_DB  = "shopstream_profiles"
MONGO_COL = "user_profiles"


def fetch_segments(pg_conn) -> dict:
    print("  📊 Loading user segments from PostgreSQL...")
    cur = pg_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
        SELECT user_id, segment, revenue_30d, order_count_30d,
               last_purchase_date, avg_review_score, churn_score
        FROM user_segments ORDER BY user_id
    """)
    rows = cur.fetchall()
    cur.close()
    print(f"     {len(rows)} segments loaded")
    return {r["user_id"]: dict(r) for r in rows}


def aggregate_silver(spark: SparkSession) -> dict:
    result = {}

    # ── Purchases ─────────────────────────────────────
    print("  ⚡ Aggregating purchases...")
    df = spark.read.parquet(f"{SILVER_PATH}/purchases")
    for row in (
        df.filter(F.col("user_id").isNotNull())
        .groupBy("user_id")
        .agg(
            F.countDistinct("order_id").alias("total_orders"),
            F.round(F.sum("final_amount"), 2).alias("total_spent"),
            F.sum(F.when(F.col("is_return"), 1).otherwise(0)).cast("integer").alias("total_returns"),
            F.max("event_ts").alias("last_purchase_at"),
            F.first("payment_method").alias("preferred_payment"),
            F.sum(F.when(F.col("spend_band") == "low",  1).otherwise(0)).cast("integer").alias("band_low"),
            F.sum(F.when(F.col("spend_band") == "mid",  1).otherwise(0)).cast("integer").alias("band_mid"),
            F.sum(F.when(F.col("spend_band") == "high", 1).otherwise(0)).cast("integer").alias("band_high"),
        ).collect()
    ):
        total = int(row["total_orders"]) or 1
        result.setdefault(row["user_id"], {})["purchases"] = {
            "total_orders":      int(row["total_orders"] or 0),
            "total_spent":       float(row["total_spent"] or 0),
            "total_returns":     int(row["total_returns"] or 0),
            "return_rate":       round((row["total_returns"] or 0) / total, 4),
            "last_purchase_at":  row["last_purchase_at"].isoformat() if row["last_purchase_at"] else None,
            "preferred_payment": row["preferred_payment"],
            "spend_distribution": {
                "low": int(row["band_low"] or 0),
                "mid": int(row["band_mid"] or 0),
                "high": int(row["band_high"] or 0),
            }
        }
    print(f"     {len([k for k,v in result.items() if 'purchases' in v])} users with purchases")

    # ── Reviews ───────────────────────────────────────
    print("  ⚡ Aggregating product reviews...")
    df = spark.read.parquet(f"{SILVER_PATH}/product_reviews")
    for row in (
        df.filter(F.col("user_id").isNotNull())
        .groupBy("user_id")
        .agg(
            F.round(F.avg("rating"), 2).alias("avg_rating"),
            F.count("*").cast("integer").alias("total_reviews"),
            F.round(F.avg("sentiment_score"), 4).alias("avg_sentiment"),
            F.sum(F.when(F.col("sentiment_label") == "positive", 1).otherwise(0)).cast("integer").alias("pos"),
            F.sum(F.when(F.col("sentiment_label") == "neutral",  1).otherwise(0)).cast("integer").alias("neu"),
            F.sum(F.when(F.col("sentiment_label") == "negative", 1).otherwise(0)).cast("integer").alias("neg"),
        ).collect()
    ):
        result.setdefault(row["user_id"], {})["reviews"] = {
            "avg_rating":     float(row["avg_rating"] or 0),
            "total_reviews":  int(row["total_reviews"] or 0),
            "avg_sentiment":  float(row["avg_sentiment"] or 0),
            "breakdown":      {"positive": int(row["pos"] or 0),
                               "neutral":  int(row["neu"] or 0),
                               "negative": int(row["neg"] or 0)},
        }
    print(f"     {len([k for k,v in result.items() if 'reviews' in v])} users with reviews")

    # ── Clickstream ───────────────────────────────────
    print("  ⚡ Aggregating clickstream...")
    df = spark.read.parquet(f"{SILVER_PATH}/clickstream")
    for row in (
        df.filter(F.col("user_id").isNotNull() & (~F.col("is_bot")))
        .groupBy("user_id")
        .agg(
            F.countDistinct("session_id").cast("integer").alias("total_sessions"),
            F.sum(F.when(F.col("event_type") == "view",          1).otherwise(0)).cast("integer").alias("total_views"),
            F.sum(F.when(F.col("is_search"),                     1).otherwise(0)).cast("integer").alias("total_searches"),
            F.sum(F.when(F.col("is_checkout"),                   1).otherwise(0)).cast("integer").alias("total_cart_adds"),
            F.max("event_ts").alias("last_active_at"),
            F.first("platform").alias("top_platform"),
        ).collect()
    ):
        result.setdefault(row["user_id"], {})["browsing"] = {
            "total_sessions": int(row["total_sessions"] or 0),
            "total_views":    int(row["total_views"] or 0),
            "total_searches": int(row["total_searches"] or 0),
            "total_cart_adds":int(row["total_cart_adds"] or 0),
            "last_active_at": row["last_active_at"].isoformat() if row["last_active_at"] else None,
            "top_platform":   row["top_platform"],
        }
    print(f"     {len([k for k,v in result.items() if 'browsing' in v])} users with browsing activity")

    # ── Social ────────────────────────────────────────
    print("  ⚡ Aggregating social buzz...")
    df = spark.read.parquet(f"{SILVER_PATH}/social_buzz")
    for row in (
        df.filter(F.col("user_id").isNotNull())
        .groupBy("user_id")
        .agg(
            F.count("*").cast("integer").alias("post_count"),
            F.round(F.avg("sentiment_score"), 4).alias("avg_sentiment"),
            F.round(F.avg("engagement_score"), 2).alias("avg_engagement"),
            F.sum(F.when(F.col("is_viral"), 1).otherwise(0)).cast("integer").alias("viral_posts"),
            F.collect_set("platform").alias("platforms"),
        ).collect()
    ):
        result.setdefault(row["user_id"], {})["social"] = {
            "post_count":    int(row["post_count"] or 0),
            "avg_sentiment": float(row["avg_sentiment"] or 0),
            "avg_engagement":float(row["avg_engagement"] or 0),
            "viral_posts":   int(row["viral_posts"] or 0),
            "platforms":     list(row["platforms"] or []),
        }
    print(f"     {len([k for k,v in result.items() if 'social' in v])} users with social activity")

    return result


def build_doc(uid: str, silver: dict, seg: dict) -> dict:
    return {
        "user_id":    uid,
        "updated_at": datetime.now(timezone.utc),
        "purchases":  silver.get("purchases", {}),
        "reviews":    silver.get("reviews",   {}),
        "browsing":   silver.get("browsing",  {}),
        "social":     silver.get("social",    {}),
        "segment": {
            "label":              seg.get("segment", "unknown"),
            "churn_score":        float(seg.get("churn_score") or 0),
            "revenue_30d":        float(seg.get("revenue_30d") or 0),
            "order_count_30d":    int(seg.get("order_count_30d") or 0),
            "last_purchase_date": str(seg.get("last_purchase_date")) if seg.get("last_purchase_date") else None,
            "avg_review_score":   float(seg.get("avg_review_score") or 0),
        }
    }


def run_batch(users, silver, segs, col, batch_size):
    print(f"\n  📦 BATCH — {len(users)} users, chunk={batch_size}\n")
    total, chunks = 0, [users[i:i+batch_size] for i in range(0, len(users), batch_size)]
    for idx, chunk in enumerate(chunks, 1):
        ops = [UpdateOne({"user_id": uid}, {"$set": build_doc(uid, silver.get(uid, {}), segs.get(uid, {}))}, upsert=True) for uid in chunk]
        try:
            r = col.bulk_write(ops, ordered=False)
            total += r.upserted_count + r.modified_count
        except BulkWriteError as e:
            print(f"  ⚠️  chunk {idx}: {e.details}")
        pct = idx / len(chunks) * 100
        bar = "█" * int(pct // 5) + "░" * (20 - int(pct // 5))
        print(f"  [{bar}] {idx}/{len(chunks)} | {pct:.0f}% | written: {total}")
    return total


def run_stream(users, silver, segs, col, delay):
    print(f"\n  🌊 STREAM — {len(users)} users. Ctrl+C to stop.\n")
    total = 0
    try:
        for idx, uid in enumerate(users, 1):
            doc = build_doc(uid, silver.get(uid, {}), segs.get(uid, {}))
            col.update_one({"user_id": uid}, {"$set": doc}, upsert=True)
            total += 1
            seg   = doc["segment"]["label"]
            churn = doc["segment"]["churn_score"]
            spent = doc["purchases"].get("total_spent", 0)
            print(f"  ✔ [{idx:5d}/{len(users)}] {uid} | {seg:8s} | churn={churn:.2f} | ₹{spent:,.0f}")
            time.sleep(delay)
    except KeyboardInterrupt:
        print(f"\n  🛑 Stopped at {total} users.")
    return total


if __name__ == "__main__":
    print("=" * 55)
    print("  USER PROFILE BUILDER — ShopStream")
    print(f"  Mode: {args.mode.upper()}")
    print("=" * 55)

    spark   = get_spark("shopstream-profile-builder", shuffle_partitions=4)
    print("\n[1/4] Aggregating silver data...")
    silver  = aggregate_silver(spark)
    spark.stop()

    print("\n[2/4] Loading segments from PostgreSQL...")
    pg       = psycopg2.connect(**PG_CONFIG)
    segs     = fetch_segments(pg)
    pg.close()

    all_users = sorted(set(silver.keys()) | set(segs.keys()))
    if args.limit:
        all_users = all_users[:args.limit]

    print(f"\n[3/4] Connecting to MongoDB...")
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    col    = client[MONGO_DB][MONGO_COL]
    print(f"  Connected. Existing docs: {col.count_documents({})}")

    print(f"\n[4/4] Upserting {len(all_users)} profiles...")
    t0    = time.time()
    total = run_batch(all_users, silver, segs, col, args.batch_size) \
            if args.mode == "batch" \
            else run_stream(all_users, silver, segs, col, args.stream_delay)

    print(f"\n🎉 Done in {time.time()-t0:.1f}s — {total} profiles upserted")
    print(f"   Total in MongoDB: {col.count_documents({})}")

    for seg in col.aggregate([{"$group": {"_id": "$segment.label", "n": {"$sum": 1}}}, {"$sort": {"n": -1}}]):
        print(f"  {seg['_id']:10s}  {seg['n']:5d}  {'█' * (seg['n'] // 20)}")
    client.close()
