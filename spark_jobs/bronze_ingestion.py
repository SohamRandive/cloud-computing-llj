#!/usr/bin/env python3
"""
spark_jobs/bronze_ingestion.py
─────────────────────────────────────────────────────
BRONZE LAYER: Raw ingestion from Kafka → MinIO (Parquet)

What this job does:
  1. Reads raw bytes from all 4 Kafka topics
  2. Parses JSON payload using topic-specific schemas
  3. Adds ingestion metadata (ingested_at, source_topic)
  4. Writes partitioned Parquet to MinIO bronze bucket

No business logic here — bronze is the raw landing zone.
The only transforms are:
  - JSON string → typed struct
  - timestamp string → TimestampType
  - add ingestion_date partition column (for efficient reads)

Output paths in MinIO bronze bucket:
  s3a://bronze/web_logs/ingestion_date=YYYY-MM-DD/
  s3a://bronze/transactions/ingestion_date=YYYY-MM-DD/
  s3a://bronze/reviews/ingestion_date=YYYY-MM-DD/
  s3a://bronze/social_media/ingestion_date=YYYY-MM-DD/

Usage:
  python3 spark_jobs/bronze_ingestion.py --mode batch    (default)
  python3 spark_jobs/bronze_ingestion.py --mode stream
"""

import argparse
import sys
from pathlib import Path

from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType, BooleanType, DoubleType, IntegerType,
    LongType, StringType, StructField, StructType, TimestampType,
)

sys.path.insert(0, str(Path(__file__).parent.parent))
from spark_jobs.spark_utils import BRONZE_PATH, KAFKA_BROKER, get_spark

# ── CLI ───────────────────────────────────────────────
parser = argparse.ArgumentParser(description="Bronze ingestion job")
parser.add_argument(
    "--mode", choices=["batch", "stream"], default="batch",
    help="batch: read all existing msgs and exit | stream: run continuously"
)
args = parser.parse_args()
MODE = args.mode

# ─────────────────────────────────────────────────────
#  SCHEMAS — enforce structure on raw JSON at ingest
#  Using StructType means malformed records become null
#  fields rather than crashing the job.
# ─────────────────────────────────────────────────────

WEB_LOG_SCHEMA = StructType([
    StructField("session_id",   StringType(),  True),
    StructField("customer_id",  StringType(),  True),
    StructField("event_type",   StringType(),  True),
    StructField("url",          StringType(),  True),
    StructField("referrer",     StringType(),  True),
    StructField("device",       StringType(),  True),
    StructField("search_term",  StringType(),  True),
    StructField("product_id",   StringType(),  True),
    StructField("duration_ms",  IntegerType(), True),
    StructField("ip_address",   StringType(),  True),
    StructField("timestamp",    StringType(),  True),
])

# Nested item schema for transactions
ITEM_SCHEMA = StructType([
    StructField("product_id",  StringType(),  True),
    StructField("name",        StringType(),  True),
    StructField("category",    StringType(),  True),
    StructField("qty",         IntegerType(), True),
    StructField("unit_price",  DoubleType(),  True),
    StructField("subtotal",    DoubleType(),  True),
])

TRANSACTION_SCHEMA = StructType([
    StructField("order_id",         StringType(),           True),
    StructField("customer_id",      StringType(),           True),
    StructField("timestamp",        StringType(),           True),
    StructField("status",           StringType(),           True),
    StructField("items",            ArrayType(ITEM_SCHEMA), True),
    StructField("total_amount",     DoubleType(),           True),
    StructField("discount_pct",     DoubleType(),           True),
    StructField("final_amount",     DoubleType(),           True),
    StructField("payment_method",   StringType(),           True),
    StructField("payment_status",   StringType(),           True),
    StructField("shipping_country", StringType(),           True),
    StructField("is_return",        BooleanType(),          True),
])

REVIEW_SCHEMA = StructType([
    StructField("review_id",         StringType(),  True),
    StructField("customer_id",       StringType(),  True),
    StructField("product_id",        StringType(),  True),
    StructField("product_category",  StringType(),  True),
    StructField("timestamp",         StringType(),  True),
    StructField("rating",            IntegerType(), True),
    StructField("review_text",       StringType(),  True),
    StructField("verified_purchase", BooleanType(), True),
    StructField("helpful_votes",     IntegerType(), True),
    StructField("sentiment_label",   StringType(),  True),
    StructField("sentiment_score",   DoubleType(),  True),
])

SOCIAL_SCHEMA = StructType([
    StructField("post_id",         StringType(),            True),
    StructField("platform",        StringType(),            True),
    StructField("customer_id",     StringType(),            True),
    StructField("timestamp",       StringType(),            True),
    StructField("text",            StringType(),            True),
    StructField("hashtags",        ArrayType(StringType()), True),
    StructField("likes",           IntegerType(),           True),
    StructField("shares",          IntegerType(),           True),
    StructField("sentiment_label", StringType(),            True),
    StructField("sentiment_score", DoubleType(),            True),
    StructField("language",        StringType(),            True),
])

# Map topic → (output folder name, schema)
TOPIC_CONFIG = {
    "web-logs":    ("web_logs",    WEB_LOG_SCHEMA),
    "transactions":("transactions", TRANSACTION_SCHEMA),
    "reviews":     ("reviews",      REVIEW_SCHEMA),
    "social-media":("social_media", SOCIAL_SCHEMA),
}

# ─────────────────────────────────────────────────────
#  CORE TRANSFORM
#  Kafka gives us: key, value (bytes), topic, partition,
#  offset, timestamp, timestampType
#  We decode value as UTF-8 JSON → parse with schema
# ─────────────────────────────────────────────────────

def parse_kafka_topic(df, schema):
    """
    Decode Kafka value bytes → JSON string → typed struct.
    Adds ingested_at and ingestion_date metadata columns.
    """
    return (
        df.select(
            # decode raw bytes to string
            F.col("value").cast(StringType()).alias("raw_json"),
            F.col("topic"),
            F.col("partition"),
            F.col("offset"),
            # kafka message timestamp (when broker received it)
            F.col("timestamp").alias("kafka_timestamp"),
        )
        # parse JSON into typed struct
        .withColumn("data", F.from_json(F.col("raw_json"), schema))
        # flatten struct fields to top-level columns
        .select("topic", "partition", "offset", "kafka_timestamp",
                "data.*")
        # cast the string timestamp from the payload to proper type
        .withColumn("event_timestamp",
                    F.to_timestamp(F.col("timestamp")))
        .drop("timestamp")
        # ingestion metadata
        .withColumn("ingested_at",
                    F.current_timestamp())
        .withColumn("ingestion_date",
                    F.to_date(F.current_timestamp()))  # partition column
    )

# ─────────────────────────────────────────────────────
#  BATCH MODE
# ─────────────────────────────────────────────────────

def run_batch(spark):
    print("\n📦 Running in BATCH mode — reads all messages then exits.\n")
    total_written = {}

    for topic, (folder, schema) in TOPIC_CONFIG.items():
        print(f"  ⏳ Processing topic: {topic} ...")

        raw_df = (
            spark.read                          # static read
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BROKER)
            .option("subscribe",               topic)
            .option("startingOffsets",         "earliest")
            .option("endingOffsets",           "latest")
            .load()
        )

        count_raw = raw_df.count()
        if count_raw == 0:
            print(f"  ⚠️  No messages in '{topic}' — skipping.")
            continue

        parsed_df = parse_kafka_topic(raw_df, schema)

        out_path = f"{BRONZE_PATH}/{folder}"
        (
            parsed_df
            .write
            .mode("overwrite")
            .partitionBy("ingestion_date")      # always overwrite — full Kafka re-read
            .parquet(out_path)
        )

        count_out = parsed_df.count()
        total_written[topic] = count_out
        print(f"  ✅ {topic}: {count_raw} msgs read → {count_out} rows → {out_path}")

    print(f"\n🎉 Bronze batch complete. Summary: {total_written}")

# ─────────────────────────────────────────────────────
#  STREAM MODE
#  Uses trigger(availableNow=True) which is a one-shot
#  micro-batch: processes everything available right now
#  then stops. Better than trigger(once=True) which is
#  deprecated in Spark 3.4+.
# ─────────────────────────────────────────────────────

def run_stream(spark):
    print("\n🌊 Running in STREAM mode — processes continuously. Ctrl+C to stop.\n")
    queries = []

    for topic, (folder, schema) in TOPIC_CONFIG.items():
        print(f"  ▶ Starting stream for: {topic}")

        raw_stream = (
            spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BROKER)
            .option("subscribe",               topic)
            .option("startingOffsets",         "earliest")
            .option("failOnDataLoss",          "false")
            .load()
        )

        parsed_stream = parse_kafka_topic(raw_stream, schema)

        out_path       = f"{BRONZE_PATH}/{folder}"
        checkpoint_dir = f"{BRONZE_PATH}/_checkpoints/{folder}"

        query = (
            parsed_stream
            .writeStream
            .format("parquet")
            .option("path",             out_path)
            .option("checkpointLocation", checkpoint_dir)
            .partitionBy("ingestion_date")
            .trigger(processingTime="30 seconds")  # micro-batch every 30s
            .start()
        )
        queries.append(query)

    print(f"\n  {len(queries)} streams running. Watch at http://localhost:4040")
    print("  Press Ctrl+C to stop.\n")

    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        print("\n🛑 Stopping all streams...")
        for q in queries:
            q.stop()
        print("✅ All streams stopped.")

# ─────────────────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 55)
    print("  BRONZE INGESTION JOB")
    print(f"  Mode: {MODE.upper()}")
    print(f"  Kafka: {KAFKA_BROKER}")
    print(f"  Output: {BRONZE_PATH}")
    print("=" * 55)

    spark = get_spark("llj-bronze-ingestion")

    if MODE == "batch":
        run_batch(spark)
    else:
        run_stream(spark)

    spark.stop()
    print("\nSpark session closed.")
