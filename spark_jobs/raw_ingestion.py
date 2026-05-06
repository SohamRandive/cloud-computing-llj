#!/usr/bin/env python3
"""
BRONZE LAYER — Kafka → MinIO (raw Parquet)
Topics: clickstream, purchases, product-reviews, social-buzz
"""

import argparse
import sys
from pathlib import Path

from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType, BooleanType, DoubleType, IntegerType,
    StringType, StructField, StructType,
)

sys.path.insert(0, str(Path(__file__).parent.parent))
from spark_jobs.spark_config import BRONZE_PATH, KAFKA_BROKER, get_spark

parser = argparse.ArgumentParser()
parser.add_argument("--mode", choices=["batch", "stream"], default="batch")
args = parser.parse_args()
MODE = args.mode

# ── Schemas ───────────────────────────────────────────

CLICKSTREAM_SCHEMA = StructType([
    StructField("session_id",  StringType(),  True),
    StructField("user_id",     StringType(),  True),
    StructField("event_type",  StringType(),  True),
    StructField("page",        StringType(),  True),
    StructField("platform",    StringType(),  True),
    StructField("traffic_src", StringType(),  True),
    StructField("keyword",     StringType(),  True),
    StructField("sku",         StringType(),  True),
    StructField("dwell_ms",    IntegerType(), True),
    StructField("ip",          StringType(),  True),
    StructField("timestamp",   StringType(),  True),
])

LINE_ITEM_SCHEMA = StructType([
    StructField("sku",        StringType(),  True),
    StructField("title",      StringType(),  True),
    StructField("category",   StringType(),  True),
    StructField("qty",        IntegerType(), True),
    StructField("unit_price", DoubleType(),  True),
    StructField("subtotal",   DoubleType(),  True),
])

PURCHASE_SCHEMA = StructType([
    StructField("order_id",        StringType(),              True),
    StructField("user_id",         StringType(),              True),
    StructField("status",          StringType(),              True),
    StructField("items",           ArrayType(LINE_ITEM_SCHEMA), True),
    StructField("total_amount",    DoubleType(),              True),
    StructField("discount_pct",    DoubleType(),              True),
    StructField("final_amount",    DoubleType(),              True),
    StructField("payment_method",  StringType(),              True),
    StructField("payment_status",  StringType(),              True),
    StructField("ship_country",    StringType(),              True),
    StructField("is_return",       BooleanType(),             True),
    StructField("timestamp",       StringType(),              True),
])

REVIEW_SCHEMA = StructType([
    StructField("review_id",         StringType(),  True),
    StructField("user_id",           StringType(),  True),
    StructField("sku",               StringType(),  True),
    StructField("rating",            IntegerType(), True),
    StructField("review_text",       StringType(),  True),
    StructField("verified_purchase", BooleanType(), True),
    StructField("helpful_votes",     IntegerType(), True),
    StructField("sentiment_label",   StringType(),  True),
    StructField("sentiment_score",   DoubleType(),  True),
    StructField("timestamp",         StringType(),  True),
])

SOCIAL_SCHEMA = StructType([
    StructField("post_id",         StringType(),             True),
    StructField("platform",        StringType(),             True),
    StructField("user_id",         StringType(),             True),
    StructField("content",         StringType(),             True),
    StructField("hashtags",        ArrayType(StringType()),  True),
    StructField("likes",           IntegerType(),            True),
    StructField("shares",          IntegerType(),            True),
    StructField("sentiment_label", StringType(),             True),
    StructField("sentiment_score", DoubleType(),             True),
    StructField("lang",            StringType(),             True),
    StructField("timestamp",       StringType(),             True),
])

TOPIC_CONFIG = {
    "clickstream":    ("clickstream",    CLICKSTREAM_SCHEMA),
    "purchases":      ("purchases",      PURCHASE_SCHEMA),
    "product-reviews":("product_reviews", REVIEW_SCHEMA),
    "social-buzz":    ("social_buzz",    SOCIAL_SCHEMA),
}


def parse_topic(df, schema):
    return (
        df.select(
            F.col("value").cast(StringType()).alias("raw_json"),
            F.col("topic"), F.col("partition"), F.col("offset"),
            F.col("timestamp").alias("kafka_ts"),
        )
        .withColumn("data", F.from_json(F.col("raw_json"), schema))
        .select("topic", "partition", "offset", "kafka_ts", "data.*")
        .withColumn("event_ts",       F.to_timestamp(F.col("timestamp")))
        .drop("timestamp")
        .withColumn("ingested_at",    F.current_timestamp())
        .withColumn("ingestion_date", F.to_date(F.current_timestamp()))
    )


def run_batch(spark):
    print("\n📦 BATCH mode — reads all messages then exits.\n")
    for topic, (folder, schema) in TOPIC_CONFIG.items():
        print(f"  ⏳ {topic} ...")
        raw = (
            spark.read.format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BROKER)
            .option("subscribe", topic)
            .option("startingOffsets", "earliest")
            .option("endingOffsets", "latest")
            .load()
        )
        n = raw.count()
        if n == 0:
            print(f"  ⚠️  No messages in '{topic}' — skipping.")
            continue
        parsed = parse_topic(raw, schema)
        out    = f"{BRONZE_PATH}/{folder}"
        parsed.write.mode("overwrite").partitionBy("ingestion_date").parquet(out)
        print(f"  ✅ {topic}: {n} msgs → {out}")
    print("\n🎉 Bronze complete.")


def run_stream(spark):
    print("\n🌊 STREAM mode. Ctrl+C to stop.\n")
    queries = []
    for topic, (folder, schema) in TOPIC_CONFIG.items():
        raw = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BROKER)
            .option("subscribe", topic)
            .option("startingOffsets", "earliest")
            .option("failOnDataLoss", "false")
            .load()
        )
        parsed = parse_topic(raw, schema)
        out    = f"{BRONZE_PATH}/{folder}"
        ckpt   = f"{BRONZE_PATH}/_checkpoints/{folder}"
        q = (
            parsed.writeStream.format("parquet")
            .option("path", out)
            .option("checkpointLocation", ckpt)
            .partitionBy("ingestion_date")
            .trigger(processingTime="30 seconds")
            .start()
        )
        queries.append(q)
        print(f"  ▶ {topic}")
    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        for q in queries:
            q.stop()
        print("✅ Streams stopped.")


if __name__ == "__main__":
    print("=" * 55)
    print("  BRONZE INGESTION — ShopStream")
    print(f"  Mode: {MODE.upper()}")
    print("=" * 55)
    spark = get_spark("shopstream-bronze")
    if MODE == "batch":
        run_batch(spark)
    else:
        run_stream(spark)
    spark.stop()
