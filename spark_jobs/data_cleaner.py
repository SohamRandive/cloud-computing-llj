#!/usr/bin/env python3
"""
SILVER LAYER — Clean + enrich bronze Parquet → silver Parquet

  clickstream:  drop nulls, dedup on (session_id, event_type),
                add hour_of_day, is_weekend, is_bot, is_search
  purchases:    drop nulls, dedup on order_id, explode line items,
                add spend_band, hour_of_day
  product_reviews: dedup on review_id, add sentiment_band, rating_group
  social_buzz:  dedup on post_id, add engagement_score, is_viral
"""

import argparse
import sys
from pathlib import Path

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.window import Window

sys.path.insert(0, str(Path(__file__).parent.parent))
from spark_jobs.spark_config import BRONZE_PATH, SILVER_PATH, get_spark

parser = argparse.ArgumentParser()
parser.add_argument("--mode", choices=["batch", "stream"], default="batch")
args = parser.parse_args()
MODE = args.mode


def transform_clickstream(df: DataFrame) -> DataFrame:
    df = df.filter(F.col("session_id").isNotNull() & F.col("event_type").isNotNull())
    w  = Window.partitionBy("session_id", "event_type").orderBy(F.col("ingested_at").desc())
    df = df.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")
    return (
        df
        .withColumn("hour_of_day",  F.hour("event_ts"))
        .withColumn("day_of_week",  F.dayofweek("event_ts"))
        .withColumn("is_weekend",   F.col("day_of_week").isin([1, 7]))
        .withColumn("is_bot",       F.col("dwell_ms") < 120)
        .withColumn("is_search",    F.col("event_type") == "search")
        .withColumn("is_checkout",  F.col("event_type") == "add_to_cart")
        .withColumn("ingestion_date", F.to_date("ingested_at"))
    )


def transform_purchases(df: DataFrame) -> DataFrame:
    df = df.filter(F.col("order_id").isNotNull() & F.col("user_id").isNotNull())
    w  = Window.partitionBy("order_id").orderBy(F.col("kafka_ts").desc())
    df = df.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")
    df = (
        df.withColumn("item", F.explode("items")).drop("items")
        .select(
            "*",
            F.col("item.sku").alias("item_sku"),
            F.col("item.title").alias("item_title"),
            F.col("item.category").alias("item_category"),
            F.col("item.qty").alias("item_qty"),
            F.col("item.unit_price").alias("item_unit_price"),
            F.col("item.subtotal").alias("item_subtotal"),
        ).drop("item")
    )
    return (
        df
        .withColumn("spend_band",
            F.when(F.col("final_amount") < 500,  "low")
             .when(F.col("final_amount") < 2500, "mid")
             .otherwise("high"))
        .withColumn("hour_of_day",    F.hour("event_ts"))
        .withColumn("ingestion_date", F.to_date("ingested_at"))
    )


def transform_product_reviews(df: DataFrame) -> DataFrame:
    df = df.filter(F.col("review_id").isNotNull() & F.col("user_id").isNotNull())
    w  = Window.partitionBy("review_id").orderBy(F.col("ingested_at").desc())
    df = df.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")
    return (
        df
        .withColumn("sentiment_band",
            F.when(F.col("sentiment_score") >= 0.5,  "very_positive")
             .when(F.col("sentiment_score") >= 0.05, "positive")
             .when(F.col("sentiment_score") > -0.05, "neutral")
             .when(F.col("sentiment_score") > -0.5,  "negative")
             .otherwise("very_negative"))
        .withColumn("rating_group",
            F.when(F.col("rating") <= 2, "negative")
             .when(F.col("rating") == 3, "neutral")
             .otherwise("positive"))
        .withColumn("ingestion_date", F.to_date("ingested_at"))
    )


def transform_social_buzz(df: DataFrame) -> DataFrame:
    df = df.filter(F.col("post_id").isNotNull())
    w  = Window.partitionBy("post_id").orderBy(F.col("ingested_at").desc())
    df = df.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")
    return (
        df
        .withColumn("engagement_score", F.col("likes") + F.col("shares") * 4)
        .withColumn("is_viral",         F.col("engagement_score") > 200)
        .withColumn("has_user",         F.col("user_id").isNotNull())
        .withColumn("ingestion_date",   F.to_date("ingested_at"))
    )


SOURCE_CONFIG = {
    "clickstream":     (transform_clickstream,     "clickstream"),
    "purchases":       (transform_purchases,       "purchases"),
    "product_reviews": (transform_product_reviews, "product_reviews"),
    "social_buzz":     (transform_social_buzz,     "social_buzz"),
}


def run_batch(spark):
    print("\n📦 BATCH mode.\n")
    for src, (fn, folder) in SOURCE_CONFIG.items():
        bronze = f"{BRONZE_PATH}/{folder}"
        silver = f"{SILVER_PATH}/{folder}"
        try:
            df = spark.read.parquet(bronze)
        except Exception as e:
            print(f"  ⚠️  Cannot read {bronze}: {e}")
            continue
        out = fn(df)
        out.write.mode("overwrite").partitionBy("ingestion_date").parquet(silver)
        print(f"  ✅ {src}: {df.count()} bronze → {out.count()} silver → {silver}")
    print("\n🎉 Silver complete.")


def run_stream(spark):
    print("\n🌊 STREAM mode. Ctrl+C to stop.\n")
    queries = []
    for src, (fn, folder) in SOURCE_CONFIG.items():
        bronze = f"{BRONZE_PATH}/{folder}"
        silver = f"{SILVER_PATH}/{folder}"
        ckpt   = f"{SILVER_PATH}/_checkpoints/{folder}"
        schema = spark.read.parquet(bronze).schema
        stream = fn(spark.readStream.schema(schema).parquet(bronze))
        q = (
            stream.writeStream.format("parquet")
            .option("path", silver)
            .option("checkpointLocation", ckpt)
            .partitionBy("ingestion_date")
            .trigger(processingTime="60 seconds")
            .start()
        )
        queries.append(q)
        print(f"  ▶ {src}")
    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        for q in queries: q.stop()
        print("✅ Stopped.")


if __name__ == "__main__":
    print("=" * 55)
    print("  SILVER TRANSFORM — ShopStream")
    print(f"  Mode: {MODE.upper()}")
    print("=" * 55)
    spark = get_spark("shopstream-silver")
    if MODE == "batch":
        run_batch(spark)
    else:
        run_stream(spark)
    spark.stop()
