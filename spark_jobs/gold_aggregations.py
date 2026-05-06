#!/usr/bin/env python3
"""
GOLD LAYER — Windowed aggregations → PostgreSQL

  hourly_sales          ← silver/purchases  (revenue per category per hour)
  hourly_search_trends  ← silver/clickstream (top keywords per hour)
  hourly_site_traffic   ← silver/clickstream (sessions, clicks, pageviews per hour)
  hourly_sentiment      ← silver/product_reviews + silver/social_buzz
  user_segments         ← silver/purchases + silver/product_reviews (daily snapshot)
"""

import argparse
import sys
from pathlib import Path

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.window import Window

sys.path.insert(0, str(Path(__file__).parent.parent))
from spark_jobs.spark_utils import PG_PROPS, PG_URL, SILVER_PATH, get_spark

parser = argparse.ArgumentParser()
parser.add_argument("--mode", choices=["batch", "stream"], default="batch")
args = parser.parse_args()
MODE = args.mode

WIN = "1 hour"


def write_pg(df: DataFrame, table: str, mode: str = "overwrite"):
    (
        df.write.format("jdbc")
        .option("url",      PG_URL)
        .option("dbtable",  table)
        .option("user",     PG_PROPS["user"])
        .option("password", PG_PROPS["password"])
        .option("driver",   PG_PROPS["driver"])
        .option("batchsize", "1000")
        .option("truncate",  "true")
        .mode(mode)
        .save()
    )


def agg_hourly_sales(df: DataFrame) -> DataFrame:
    return (
        df.filter(F.col("payment_status") == "success")
        .groupBy(F.window("event_ts", WIN).alias("w"), F.col("item_category").alias("category"))
        .agg(
            F.round(F.sum("item_subtotal"), 2).cast("decimal(16,2)").alias("gross_revenue"),
            F.countDistinct("order_id").cast("integer").alias("num_orders"),
            F.sum(F.when(F.col("is_return"), 1).otherwise(0)).cast("integer").alias("num_returns"),
        )
        .select(F.col("w.start").alias("window_start"), F.col("w.end").alias("window_end"),
                "category", "gross_revenue", "num_orders", "num_returns")
    )


def agg_hourly_search_trends(df: DataFrame) -> DataFrame:
    return (
        df.filter(F.col("is_search") & F.col("keyword").isNotNull())
        .groupBy(F.window("event_ts", WIN).alias("w"), F.lower("keyword").alias("keyword"))
        .agg(
            F.count("*").cast("integer").alias("search_count"),
            F.countDistinct("user_id").cast("integer").alias("unique_visitors"),
        )
        .select(F.col("w.start").alias("window_start"), F.col("w.end").alias("window_end"),
                "keyword", "search_count", "unique_visitors")
        .filter(F.col("search_count") > 1)
    )


def agg_hourly_site_traffic(df: DataFrame) -> DataFrame:
    return (
        df.filter(~F.col("is_bot"))
        .groupBy(F.window("event_ts", WIN).alias("w"))
        .agg(
            F.countDistinct("session_id").cast("integer").alias("num_sessions"),
            F.sum(F.when(F.col("event_type") == "view",    1).otherwise(0)).cast("integer").alias("num_clicks"),
            F.sum(F.when(F.col("event_type") == "scroll",  1).otherwise(0)).cast("integer").alias("num_pageviews"),
            F.countDistinct("user_id").cast("integer").alias("unique_visitors"),
            F.round(F.avg("dwell_ms"), 0).cast("long").alias("avg_session_ms"),
        )
        .select(F.col("w.start").alias("window_start"), F.col("w.end").alias("window_end"),
                "num_sessions", "num_clicks", "num_pageviews", "unique_visitors", "avg_session_ms")
    )


def agg_hourly_sentiment(reviews_df: DataFrame, social_df: DataFrame) -> DataFrame:
    def _one(df, src):
        return (
            df.groupBy(F.window("event_ts", WIN).alias("w"), F.lit(src).alias("data_source"))
            .agg(
                F.round(F.avg("sentiment_score"), 4).alias("mean_sentiment"),
                F.sum(F.when(F.col("sentiment_score") >= 0.05,  1).otherwise(0)).cast("integer").alias("positive_cnt"),
                F.sum(F.when((F.col("sentiment_score") > -0.05) & (F.col("sentiment_score") < 0.05), 1).otherwise(0)).cast("integer").alias("neutral_cnt"),
                F.sum(F.when(F.col("sentiment_score") <= -0.05, 1).otherwise(0)).cast("integer").alias("negative_cnt"),
                F.count("*").cast("integer").alias("total_cnt"),
            )
            .select(F.col("w.start").alias("window_start"), F.col("w.end").alias("window_end"),
                    "data_source", "mean_sentiment", "positive_cnt", "neutral_cnt", "negative_cnt", "total_cnt")
        )
    return _one(reviews_df, "reviews").union(_one(social_df, "social_buzz"))


def agg_user_segments(purchases_df: DataFrame, reviews_df: DataFrame) -> DataFrame:
    txn = (
        purchases_df.filter(F.col("payment_status") == "success")
        .groupBy("user_id")
        .agg(
            F.round(F.sum("final_amount"), 2).alias("revenue_30d"),
            F.countDistinct("order_id").cast("integer").alias("order_count_30d"),
            F.max("event_ts").alias("last_ts"),
            F.sum(F.when(F.col("is_return"), 1).otherwise(0)).cast("integer").alias("ret_count"),
        )
        .withColumn("last_purchase_date", F.to_date("last_ts")).drop("last_ts")
    )
    rev = (
        reviews_df.groupBy("user_id")
        .agg(F.round(F.avg("rating"), 2).alias("avg_review_score"))
    )
    combined = txn.join(rev, on="user_id", how="left")
    spend_w  = Window.orderBy(F.col("revenue_30d").desc())
    combined = combined.withColumn("_spend_rank", F.percent_rank().over(spend_w))

    max_spend  = combined.agg(F.max("revenue_30d")).collect()[0][0] or 1.0
    max_orders = combined.agg(F.max("order_count_30d")).collect()[0][0] or 1.0

    combined = (
        combined
        .withColumn("_s_score",  F.col("revenue_30d") / max_spend)
        .withColumn("_o_score",  F.col("order_count_30d") / max_orders)
        .withColumn("_r_score",  F.coalesce(F.col("avg_review_score"), F.lit(3.0)) / 5.0)
        .withColumn("churn_score",
            F.round(0.4*(1-F.col("_o_score")) + 0.3*(1-F.col("_s_score")) + 0.3*(1-F.col("_r_score")), 4))
        .withColumn("segment",
            F.when(F.col("order_count_30d") == 1,    "new")
             .when(F.col("churn_score") > 0.65,      "at_risk")
             .when(F.col("_spend_rank") <= 0.20,     "platinum")
             .otherwise("regular"))
        .select("user_id", "segment", "revenue_30d", "order_count_30d",
                "last_purchase_date", "avg_review_score", "churn_score")
    )
    now = F.current_timestamp()
    return combined.withColumn("window_start", now).withColumn("window_end", now)


def run_batch(spark):
    print("\n📦 BATCH mode.\n")

    def _read(folder):
        try:
            return spark.read.parquet(f"{SILVER_PATH}/{folder}")
        except Exception as e:
            print(f"  ⚠️  {folder}: {e}")
            return None

    clicks = _read("clickstream")
    orders = _read("purchases")
    revs   = _read("product_reviews")
    social = _read("social_buzz")

    jobs = [
        ("hourly_sales",         lambda: agg_hourly_sales(orders)),
        ("hourly_search_trends", lambda: agg_hourly_search_trends(clicks)),
        ("hourly_site_traffic",  lambda: agg_hourly_site_traffic(clicks)),
        ("hourly_sentiment",     lambda: agg_hourly_sentiment(revs, social)),
        ("user_segments",        lambda: agg_user_segments(orders, revs)),
    ]

    for table, fn in jobs:
        if any(df is None for df in [clicks, orders, revs, social]):
            print(f"  ⚠️  Skipping {table} — missing silver data.")
            continue
        try:
            df = fn()
            write_pg(df, table)
            print(f"  ✅ {table}: {df.count()} rows → PostgreSQL")
        except Exception as e:
            print(f"  ❌ {table}: {e}")

    print("\n🎉 Gold complete.")


def run_stream(spark):
    print("\n🌊 STREAM mode. Ctrl+C to stop.\n")

    def _stream(folder):
        path   = f"{SILVER_PATH}/{folder}"
        schema = spark.read.parquet(path).schema
        return spark.readStream.schema(schema).parquet(path)

    clicks_s = _stream("clickstream")
    orders_s = _stream("purchases")
    revs_s   = _stream("product_reviews")
    social_s = _stream("social_buzz")

    stream_jobs = [
        ("hourly_sales",        agg_hourly_sales(orders_s),                      f"{SILVER_PATH}/_ckpt/sales"),
        ("hourly_site_traffic", agg_hourly_site_traffic(clicks_s),               f"{SILVER_PATH}/_ckpt/traffic"),
        ("hourly_sentiment",    agg_hourly_sentiment(revs_s, social_s),           f"{SILVER_PATH}/_ckpt/sentiment"),
    ]

    queries = []
    for table, agg_df, ckpt in stream_jobs:
        def _fb(tname):
            def _fn(batch_df, batch_id):
                if batch_df.count() > 0:
                    write_pg(batch_df, tname, mode="append")
            return _fn
        q = (
            agg_df.writeStream
            .foreachBatch(_fb(table))
            .option("checkpointLocation", ckpt)
            .trigger(processingTime="60 seconds")
            .outputMode("update")
            .start()
        )
        queries.append(q)
        print(f"  ▶ {table}")

    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        for q in queries: q.stop()
        print("✅ Stopped.")


if __name__ == "__main__":
    print("=" * 55)
    print("  GOLD AGGREGATIONS — ShopStream")
    print(f"  Mode: {MODE.upper()}")
    print("=" * 55)
    spark = get_spark("shopstream-gold")
    if MODE == "batch":
        run_batch(spark)
    else:
        run_stream(spark)
    spark.stop()
