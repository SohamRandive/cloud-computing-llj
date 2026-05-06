#!/usr/bin/env python3
"""
Modes:
  --mode batch   → processes all customers in chunks of
                   --batch-size, prints progress per chunk
  --mode stream  → processes one customer at a time with
                   a small delay, simulates live updates

Usage:
  python3 mongodb/customer_view_builder.py                        # batch, 100/chunk
  python3 mongodb/customer_view_builder.py --mode batch --batch-size 200
  python3 mongodb/customer_view_builder.py --mode stream
  python3 mongodb/customer_view_builder.py --limit 50             # test: first 50
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

# ── CLI ───────────────────────────────────────────────
parser = argparse.ArgumentParser(description="Single Customer View builder")
parser.add_argument("--mode", choices=["batch", "stream"], default="batch",
                    help="batch: chunked upserts | stream: one-by-one with delay")
parser.add_argument("--batch-size", type=int, default=100,
                    help="Customers per MongoDB bulk_write batch (default: 100)")
parser.add_argument("--limit", type=int, default=None,
                    help="Process only first N customers (for testing)")
parser.add_argument("--stream-delay", type=float, default=0.05,
                    help="Seconds between each upsert in stream mode (default: 0.05)")
args = parser.parse_args()

# ── Connection config ─────────────────────────────────
PG_CONFIG = {
    "host": "localhost", "port": 5432,
    "dbname": "llj_gold",
    "user": "llj_user", "password": "llj_pg_pass",
}
MONGO_URI = "mongodb://admin:llj_mongo_pass@localhost:27017/"
MONGO_DB  = "llj_cvs"
MONGO_COL = "customer_profiles"

# ─────────────────────────────────────────────────────
#  STEP 1 — PostgreSQL: fetch segment data
# ─────────────────────────────────────────────────────

def fetch_segments(pg_conn) -> dict:
    print("  📊 Fetching customer segments from PostgreSQL...")
    cur = pg_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
        SELECT customer_id, segment, total_spent_30d, order_count_30d,
               last_order_date, avg_review_rating, churn_risk_score
        FROM gold_customer_segments ORDER BY customer_id
    """)
    rows = cur.fetchall()
    cur.close()
    print(f"     {len(rows)} segments loaded")
    return {r["customer_id"]: dict(r) for r in rows}

# ─────────────────────────────────────────────────────
#  STEP 2 — Spark: aggregate all silver sources
# ─────────────────────────────────────────────────────

def aggregate_silver_data(spark: SparkSession) -> dict:
    result = {}

    # ── Transactions ──────────────────────────────────
    print("  ⚡ Aggregating transactions...")
    txn_df = spark.read.parquet(f"{SILVER_PATH}/transactions")
    for row in (
        txn_df.filter(F.col("customer_id").isNotNull())
        .groupBy("customer_id")
        .agg(
            F.countDistinct("order_id").alias("total_orders"),
            F.round(F.sum("final_amount"), 2).alias("total_spent"),
            F.sum(F.when(F.col("is_return"), 1).otherwise(0)).cast("integer").alias("total_returns"),
            F.max("event_timestamp").alias("last_order_at"),
            F.first("payment_method").alias("preferred_payment"),
            F.sum(F.when(F.col("revenue_band") == "low",    1).otherwise(0)).cast("integer").alias("band_low"),
            F.sum(F.when(F.col("revenue_band") == "medium", 1).otherwise(0)).cast("integer").alias("band_medium"),
            F.sum(F.when(F.col("revenue_band") == "high",   1).otherwise(0)).cast("integer").alias("band_high"),
        ).collect()
    ):
        total = int(row["total_orders"]) or 1
        result.setdefault(row["customer_id"], {})["transactions"] = {
            "total_orders":    int(row["total_orders"] or 0),
            "total_spent":     float(row["total_spent"] or 0),
            "total_returns":   int(row["total_returns"] or 0),
            "return_rate":     round((row["total_returns"] or 0) / total, 4),
            "last_order_at":   row["last_order_at"].isoformat() if row["last_order_at"] else None,
            "preferred_payment": row["preferred_payment"],
            "revenue_band_distribution": {
                "low": int(row["band_low"] or 0),
                "medium": int(row["band_medium"] or 0),
                "high": int(row["band_high"] or 0),
            }
        }
    print(f"     {len([k for k,v in result.items() if 'transactions' in v])} customers with transactions")

    # ── Reviews ───────────────────────────────────────
    print("  ⚡ Aggregating reviews...")
    rev_df = spark.read.parquet(f"{SILVER_PATH}/reviews")
    for row in (
        rev_df.filter(F.col("customer_id").isNotNull())
        .groupBy("customer_id")
        .agg(
            F.round(F.avg("rating"), 2).alias("avg_rating"),
            F.count("*").cast("integer").alias("total_reviews"),
            F.round(F.avg("sentiment_score"), 4).alias("avg_sentiment_score"),
            F.sum(F.when(F.col("sentiment_label") == "positive", 1).otherwise(0)).cast("integer").alias("pos_count"),
            F.sum(F.when(F.col("sentiment_label") == "neutral",  1).otherwise(0)).cast("integer").alias("neu_count"),
            F.sum(F.when(F.col("sentiment_label") == "negative", 1).otherwise(0)).cast("integer").alias("neg_count"),
        ).collect()
    ):
        result.setdefault(row["customer_id"], {})["reviews"] = {
            "avg_rating":          float(row["avg_rating"] or 0),
            "total_reviews":       int(row["total_reviews"] or 0),
            "avg_sentiment_score": float(row["avg_sentiment_score"] or 0),
            "sentiment_distribution": {
                "positive": int(row["pos_count"] or 0),
                "neutral":  int(row["neu_count"] or 0),
                "negative": int(row["neg_count"] or 0),
            }
        }
    print(f"     {len([k for k,v in result.items() if 'reviews' in v])} customers with reviews")

    # ── Web Behaviour ─────────────────────────────────
    print("  ⚡ Aggregating web behaviour...")
    web_df = spark.read.parquet(f"{SILVER_PATH}/web_logs")
    for row in (
        web_df.filter(F.col("customer_id").isNotNull() & (~F.col("is_bot")))
        .groupBy("customer_id")
        .agg(
            F.countDistinct("session_id").cast("integer").alias("total_sessions"),
            F.sum(F.when(F.col("event_type") == "click",       1).otherwise(0)).cast("integer").alias("total_clicks"),
            F.sum(F.when(F.col("event_type") == "search",      1).otherwise(0)).cast("integer").alias("total_searches"),
            F.sum(F.when(F.col("event_type") == "add_to_cart", 1).otherwise(0)).cast("integer").alias("total_add_to_cart"),
            F.max("event_timestamp").alias("last_active_at"),
            F.first("device").alias("top_device"),
        ).collect()
    ):
        result.setdefault(row["customer_id"], {})["web_behaviour"] = {
            "total_sessions":    int(row["total_sessions"] or 0),
            "total_clicks":      int(row["total_clicks"] or 0),
            "total_searches":    int(row["total_searches"] or 0),
            "total_add_to_cart": int(row["total_add_to_cart"] or 0),
            "last_active_at":    row["last_active_at"].isoformat() if row["last_active_at"] else None,
            "top_device":        row["top_device"],
        }
    print(f"     {len([k for k,v in result.items() if 'web_behaviour' in v])} customers with web activity")

    # ── Social ────────────────────────────────────────
    print("  ⚡ Aggregating social media...")
    soc_df = spark.read.parquet(f"{SILVER_PATH}/social_media")
    for row in (
        soc_df.filter(F.col("customer_id").isNotNull())
        .groupBy("customer_id")
        .agg(
            F.count("*").cast("integer").alias("post_count"),
            F.round(F.avg("sentiment_score"), 4).alias("avg_sentiment"),
            F.round(F.avg("engagement_score"), 2).alias("avg_engagement_score"),
            F.sum(F.when(F.col("is_high_engagement"), 1).otherwise(0)).cast("integer").alias("high_engagement_posts"),
            F.collect_set("platform").alias("platforms"),
        ).collect()
    ):
        result.setdefault(row["customer_id"], {})["social"] = {
            "post_count":            int(row["post_count"] or 0),
            "avg_sentiment":         float(row["avg_sentiment"] or 0),
            "avg_engagement_score":  float(row["avg_engagement_score"] or 0),
            "high_engagement_posts": int(row["high_engagement_posts"] or 0),
            "platforms":             list(row["platforms"] or []),
        }
    print(f"     {len([k for k,v in result.items() if 'social' in v])} customers with social activity")

    return result

# ─────────────────────────────────────────────────────
#  BUILD DOCUMENT helper
# ─────────────────────────────────────────────────────

def build_document(customer_id: str, silver: dict, seg: dict) -> dict:
    now = datetime.now(timezone.utc)
    return {
        "customer_id":   customer_id,
        "updated_at":    now,
        "transactions":  silver.get("transactions",  {}),
        "reviews":       silver.get("reviews",       {}),
        "web_behaviour": silver.get("web_behaviour", {}),
        "social":        silver.get("social",        {}),
        "segment": {
            "label":             seg.get("segment", "unknown"),
            "churn_risk_score":  float(seg.get("churn_risk_score") or 0),
            "total_spent_30d":   float(seg.get("total_spent_30d") or 0),
            "order_count_30d":   int(seg.get("order_count_30d") or 0),
            "last_order_date":   str(seg.get("last_order_date")) if seg.get("last_order_date") else None,
            "avg_review_rating": float(seg.get("avg_review_rating") or 0),
        }
    }

# ─────────────────────────────────────────────────────
#  BATCH MODE — chunked bulk upserts
# ─────────────────────────────────────────────────────

def run_batch(customer_list, silver_data, segments, mongo_col, batch_size):
    print(f"\n  📦 BATCH MODE — {len(customer_list)} customers, {batch_size} per chunk\n")
    total_upserted = 0
    chunks = [customer_list[i:i+batch_size]
              for i in range(0, len(customer_list), batch_size)]

    for chunk_idx, chunk in enumerate(chunks, 1):
        ops = []
        for cid in chunk:
            doc = build_document(cid, silver_data.get(cid, {}), segments.get(cid, {}))
            ops.append(UpdateOne({"customer_id": cid}, {"$set": doc}, upsert=True))

        try:
            res = mongo_col.bulk_write(ops, ordered=False)
            batch_count = res.upserted_count + res.modified_count
            total_upserted += batch_count
        except BulkWriteError as e:
            print(f"  ⚠️  Chunk {chunk_idx} error: {e.details}")
            batch_count = 0

        pct = chunk_idx / len(chunks) * 100
        bar = "█" * int(pct // 5) + "░" * (20 - int(pct // 5))
        print(f"  [{bar}] Chunk {chunk_idx:3d}/{len(chunks)} "
              f"| {chunk_idx*batch_size:5d}/{len(customer_list)} customers "
              f"| {pct:5.1f}% | written: {total_upserted}")

    return total_upserted

# ─────────────────────────────────────────────────────
#  STREAM MODE — one customer at a time with delay
# ─────────────────────────────────────────────────────

def run_stream(customer_list, silver_data, segments, mongo_col, delay):
    print(f"\n  🌊 STREAM MODE — {len(customer_list)} customers, "
          f"{delay}s delay per record. Ctrl+C to stop.\n")
    total_upserted = 0

    try:
        for idx, cid in enumerate(customer_list, 1):
            doc = build_document(cid, silver_data.get(cid, {}), segments.get(cid, {}))
            mongo_col.update_one({"customer_id": cid}, {"$set": doc}, upsert=True)
            total_upserted += 1

            seg_label = doc["segment"]["label"]
            churn     = doc["segment"]["churn_risk_score"]
            spent     = doc["transactions"].get("total_spent", 0)

            print(f"  ✔ [{idx:5d}/{len(customer_list)}] {cid} "
                  f"| {seg_label:8s} | churn={churn:.2f} | ₹{spent:,.0f}")
            time.sleep(delay)

    except KeyboardInterrupt:
        print(f"\n  🛑 Stopped at {total_upserted} customers.")

    return total_upserted

# ─────────────────────────────────────────────────────
#  SUMMARY
# ─────────────────────────────────────────────────────

def print_summary(mongo_col):
    import json
    print("\n" + "─"*55)
    print("  SEGMENT DISTRIBUTION")
    print("─"*55)
    for seg in mongo_col.aggregate([
        {"$group": {"_id": "$segment.label", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}}
    ]):
        bar = "█" * (seg["count"] // 20)
        print(f"  {seg['_id']:10s} {seg['count']:5d}  {bar}")

    doc = mongo_col.find_one({"segment.label": "vip"}, {"_id": 0})
    if doc:
        def _default(o):
            if isinstance(o, datetime): return o.isoformat()
            raise TypeError
        print("\n  SAMPLE VIP DOCUMENT:")
        print(json.dumps(doc, indent=2, default=_default))

# ─────────────────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 55)
    print("  SINGLE CUSTOMER VIEW BUILDER")
    print(f"  Mode:       {args.mode.upper()}")
    print(f"  Batch size: {args.batch_size}")
    if args.limit:
        print(f"  Limit:      {args.limit} customers")
    print("=" * 55)

    # ── Spark aggregations ────────────────────────────
    spark = get_spark("llj-scv-builder", shuffle_partitions=4)
    print("\n[1/4] Aggregating silver data with Spark...")
    silver_data = aggregate_silver_data(spark)
    spark.stop()
    print("  Spark session closed.")

    # ── PostgreSQL segments ───────────────────────────
    print("\n[2/4] Fetching segments from PostgreSQL...")
    pg_conn  = psycopg2.connect(**PG_CONFIG)
    segments = fetch_segments(pg_conn)
    pg_conn.close()

    # ── Build customer list ───────────────────────────
    all_customers = sorted(set(silver_data.keys()) | set(segments.keys()))
    if args.limit:
        all_customers = all_customers[:args.limit]

    print(f"\n[3/4] Connecting to MongoDB...")
    mongo_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    mongo_col    = mongo_client[MONGO_DB][MONGO_COL]
    print(f"  Connected. Existing docs: {mongo_col.count_documents({})}")

    # ── Run selected mode ─────────────────────────────
    print(f"\n[4/4] Building & upserting {len(all_customers)} documents...")
    t0 = time.time()

    if args.mode == "batch":
        total = run_batch(all_customers, silver_data, segments,
                          mongo_col, args.batch_size)
    else:
        total = run_stream(all_customers, silver_data, segments,
                           mongo_col, args.stream_delay)

    elapsed = time.time() - t0
    print(f"\n🎉 Done in {elapsed:.1f}s — {total} documents upserted")
    print(f"   Total in MongoDB: {mongo_col.count_documents({})}")

    print_summary(mongo_col)
    mongo_client.close()
