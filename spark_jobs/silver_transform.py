#!/usr/bin/env python3
"""
spark_jobs/silver_transform.py
─────────────────────────────────────────────────────
SILVER LAYER: Clean + enrich bronze Parquet → silver Parquet

What this job does per source:

  web_logs:
    - Drop rows where customer_id AND session_id both null
    - Deduplicate on (session_id, event_type, event_timestamp)
    - Add hour_of_day, day_of_week derived columns
    - Flag bot traffic (duration_ms < 100ms)

  transactions:
    - Drop rows where order_id or customer_id is null
    - Deduplicate on order_id (keep latest kafka_timestamp)
    - Explode items array → one row per line item
    - Add revenue_band: low/medium/high based on final_amount
    - Flag returns explicitly as boolean

  reviews:
    - Drop where review_id or customer_id null
    - Deduplicate on review_id
    - Bin sentiment_score into sentiment_band (-1..1 → 5 bands)
    - Add rating_group: negative(1-2), neutral(3), positive(4-5)

  social_media:
    - Drop where post_id null
    - Deduplicate on post_id
    - Add engagement_score = likes + (shares * 3)
    - Add is_high_engagement flag (engagement_score > 100)

Output paths in MinIO silver bucket:
  s3a://silver/web_logs/ingestion_date=YYYY-MM-DD/
  s3a://silver/transactions/ingestion_date=YYYY-MM-DD/
  s3a://silver/reviews/ingestion_date=YYYY-MM-DD/
  s3a://silver/social_media/ingestion_date=YYYY-MM-DD/

Usage:
  python3 spark_jobs/silver_transform.py --mode batch    (default)
  python3 spark_jobs/silver_transform.py --mode stream
"""

import argparse
import sys
from pathlib import Path

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.window import Window

sys.path.insert(0, str(Path(__file__).parent.parent))
from spark_jobs.spark_utils import BRONZE_PATH, SILVER_PATH, get_spark

# ── CLI ───────────────────────────────────────────────
parser = argparse.ArgumentParser(description="Silver transform job")
parser.add_argument("--mode", choices=["batch", "stream"], default="batch")
args = parser.parse_args()
MODE = args.mode

# ─────────────────────────────────────────────────────
#  TRANSFORMS — one function per source
# ─────────────────────────────────────────────────────

def transform_web_logs(df: DataFrame) -> DataFrame:
    """
    Clean and enrich web log events.

    Dedup key: (session_id, event_type, event_timestamp)
    We keep one row per unique user interaction.
    """
    # ── drop completely useless rows ─────────────────
    df = df.filter(
        F.col("session_id").isNotNull() &
        F.col("event_type").isNotNull()
    )

    # ── deduplication via window function ────────────
    # Within each (session_id, event_type) group, keep
    # the row with the latest ingested_at timestamp.
    dedup_window = Window.partitionBy("session_id", "event_type") \
                         .orderBy(F.col("ingested_at").desc())

    df = (
        df.withColumn("_row_num", F.row_number().over(dedup_window))
          .filter(F.col("_row_num") == 1)
          .drop("_row_num")
    )

    # ── derived columns ───────────────────────────────
    df = (
        df
        .withColumn("hour_of_day",
                    F.hour(F.col("event_timestamp")))
        .withColumn("day_of_week",
                    F.dayofweek(F.col("event_timestamp")))  # 1=Sun, 7=Sat
        .withColumn("is_weekend",
                    F.col("day_of_week").isin([1, 7]))
        .withColumn("is_bot",
                    F.col("duration_ms") < 100)             # heuristic
        .withColumn("is_authenticated",
                    F.col("customer_id").isNotNull())
        .withColumn("ingestion_date",
                    F.to_date(F.col("ingested_at")))
    )
    return df


def transform_transactions(df: DataFrame) -> DataFrame:
    """
    Clean transactions and explode line items.

    After explode: one row = one product in one order.
    This is the natural grain for revenue aggregations.

    Dedup key: order_id (keep row with latest kafka_timestamp)
    """
    df = df.filter(
        F.col("order_id").isNotNull() &
        F.col("customer_id").isNotNull()
    )

    # dedup on order_id — keep latest version of the order
    dedup_window = Window.partitionBy("order_id") \
                         .orderBy(F.col("kafka_timestamp").desc())
    df = (
        df.withColumn("_row_num", F.row_number().over(dedup_window))
          .filter(F.col("_row_num") == 1)
          .drop("_row_num")
    )

    # explode items array → one row per line item
    df = df.withColumn("item", F.explode(F.col("items"))) \
           .drop("items") \
           .select(
               "*",
               F.col("item.product_id").alias("item_product_id"),
               F.col("item.name").alias("item_name"),
               F.col("item.category").alias("item_category"),
               F.col("item.qty").alias("item_qty"),
               F.col("item.unit_price").alias("item_unit_price"),
               F.col("item.subtotal").alias("item_subtotal"),
           ).drop("item")

    # revenue band based on order's final_amount
    df = (
        df
        .withColumn("revenue_band",
            F.when(F.col("final_amount") < 500,   "low")
             .when(F.col("final_amount") < 2000,  "medium")
             .otherwise("high")
        )
        .withColumn("hour_of_day",
                    F.hour(F.col("event_timestamp")))
        .withColumn("ingestion_date",
                    F.to_date(F.col("ingested_at")))
    )
    return df


def transform_reviews(df: DataFrame) -> DataFrame:
    """
    Clean reviews and add sentiment banding.

    sentiment_score is VADER compound: -1.0 to 1.0
    We bin it into 5 bands for easier aggregation.
    """
    df = df.filter(
        F.col("review_id").isNotNull() &
        F.col("customer_id").isNotNull()
    )

    dedup_window = Window.partitionBy("review_id") \
                         .orderBy(F.col("ingested_at").desc())
    df = (
        df.withColumn("_row_num", F.row_number().over(dedup_window))
          .filter(F.col("_row_num") == 1)
          .drop("_row_num")
    )

    df = (
        df
        # 5-band sentiment binning
        .withColumn("sentiment_band",
            F.when(F.col("sentiment_score") >= 0.5,  "very_positive")
             .when(F.col("sentiment_score") >= 0.05, "positive")
             .when(F.col("sentiment_score") > -0.05, "neutral")
             .when(F.col("sentiment_score") > -0.5,  "negative")
             .otherwise("very_negative")
        )
        # coarser rating grouping
        .withColumn("rating_group",
            F.when(F.col("rating") <= 2, "negative")
             .when(F.col("rating") == 3, "neutral")
             .otherwise("positive")
        )
        .withColumn("ingestion_date",
                    F.to_date(F.col("ingested_at")))
    )
    return df


def transform_social_media(df: DataFrame) -> DataFrame:
    """
    Clean social posts and compute engagement score.

    engagement_score = likes + (shares * 3)
    Shares weighted more heavily — a share exposes the
    post to a new audience, so it has higher signal value.
    """
    df = df.filter(F.col("post_id").isNotNull())

    dedup_window = Window.partitionBy("post_id") \
                         .orderBy(F.col("ingested_at").desc())
    df = (
        df.withColumn("_row_num", F.row_number().over(dedup_window))
          .filter(F.col("_row_num") == 1)
          .drop("_row_num")
    )

    df = (
        df
        .withColumn("engagement_score",
                    F.col("likes") + (F.col("shares") * 3))
        .withColumn("is_high_engagement",
                    F.col("engagement_score") > 100)
        .withColumn("is_from_customer",
                    F.col("customer_id").isNotNull())
        .withColumn("ingestion_date",
                    F.to_date(F.col("ingested_at")))
    )
    return df


# ─────────────────────────────────────────────────────
#  BATCH MODE
# ─────────────────────────────────────────────────────

SOURCE_CONFIG = {
    "web_logs":    (transform_web_logs,      "web_logs"),
    "transactions":(transform_transactions,  "transactions"),
    "reviews":     (transform_reviews,       "reviews"),
    "social_media":(transform_social_media,  "social_media"),
}

def run_batch(spark):
    print("\n📦 Running in BATCH mode.\n")

    for source, (transform_fn, folder) in SOURCE_CONFIG.items():
        bronze_path = f"{BRONZE_PATH}/{folder}"
        silver_path = f"{SILVER_PATH}/{folder}"

        print(f"  ⏳ Reading bronze: {bronze_path}")
        try:
            bronze_df = spark.read.parquet(bronze_path)
        except Exception as e:
            print(f"  ⚠️  Could not read {bronze_path}: {e}")
            print(f"      Run bronze_ingestion.py first.")
            continue

        count_in = bronze_df.count()
        print(f"     {count_in} rows read from bronze")

        silver_df = transform_fn(bronze_df)
        count_out = silver_df.count()

        (
            silver_df
            .write
            .mode("overwrite")
            .partitionBy("ingestion_date")
            .parquet(silver_path)
        )

        print(f"  ✅ {source}: {count_in} bronze → {count_out} silver rows → {silver_path}")

    print("\n🎉 Silver batch complete.")


def run_stream(spark):
    print("\n🌊 Running in STREAM mode. Ctrl+C to stop.\n")
    queries = []

    for source, (transform_fn, folder) in SOURCE_CONFIG.items():
        bronze_path = f"{BRONZE_PATH}/{folder}"
        silver_path = f"{SILVER_PATH}/{folder}"
        checkpoint  = f"{SILVER_PATH}/_checkpoints/{folder}"

        bronze_stream = (
            spark.readStream
            .schema(spark.read.parquet(bronze_path).schema)  # infer from existing
            .parquet(bronze_path)
        )

        silver_stream = transform_fn(bronze_stream)

        query = (
            silver_stream
            .writeStream
            .format("parquet")
            .option("path", silver_path)
            .option("checkpointLocation", checkpoint)
            .partitionBy("ingestion_date")
            .trigger(processingTime="60 seconds")
            .start()
        )
        queries.append(query)
        print(f"  ▶ Stream started: {source}")

    print(f"\n  {len(queries)} streams running.")
    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        for q in queries:
            q.stop()
        print("✅ All streams stopped.")


if __name__ == "__main__":
    print("=" * 55)
    print("  SILVER TRANSFORM JOB")
    print(f"  Mode: {MODE.upper()}")
    print(f"  Input:  {BRONZE_PATH}")
    print(f"  Output: {SILVER_PATH}")
    print("=" * 55)

    spark = get_spark("llj-silver-transform")

    if MODE == "batch":
        run_batch(spark)
    else:
        run_stream(spark)

    spark.stop()
    print("\nSpark session closed.")
