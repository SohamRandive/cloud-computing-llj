#!/usr/bin/env python3
"""
spark_jobs/gold_aggregations.py
─────────────────────────────────────────────────────
GOLD LAYER: Windowed aggregations from silver → PostgreSQL

What this job computes (all from silver layer):

  gold_revenue_hourly
    ← silver/transactions
    GROUP BY 1-hour window, item_category
    → total_revenue, total_orders, total_returns

  gold_search_trends_hourly
    ← silver/web_logs  (event_type == 'search')
    GROUP BY 1-hour window, search_term
    → search_count, unique_users

  gold_traffic_hourly
    ← silver/web_logs
    GROUP BY 1-hour window
    → sessions, clicks, pageviews, unique_users, avg_duration

  gold_sentiment_hourly
    ← silver/reviews + silver/social_media
    GROUP BY 1-hour window, source
    → avg_sentiment, positive/neutral/negative counts

  gold_customer_segments  (daily snapshot, not windowed)
    ← silver/transactions + silver/reviews
    GROUP BY customer_id
    → total_spent_30d, order_count, churn_risk_score, segment

Usage:
  python3 spark_jobs/gold_aggregations.py --mode batch    (default)
  python3 spark_jobs/gold_aggregations.py --mode stream
"""

import argparse
import sys
from pathlib import Path

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.window import Window

sys.path.insert(0, str(Path(__file__).parent.parent))
from spark_jobs.spark_utils import PG_PROPS, PG_URL, SILVER_PATH, get_spark

# ── CLI ───────────────────────────────────────────────
parser = argparse.ArgumentParser(description="Gold aggregation job")
parser.add_argument("--mode", choices=["batch", "stream"], default="batch")
args = parser.parse_args()
MODE = args.mode

# ── Window size ───────────────────────────────────────
WINDOW_DURATION = "1 hour"
SLIDE_DURATION  = "1 hour"   # tumbling (non-overlapping) window

# ─────────────────────────────────────────────────────
#  HELPER — write DataFrame to PostgreSQL table
# ─────────────────────────────────────────────────────

def write_to_postgres(df: DataFrame, table: str, mode: str = "overwrite"):
    """
    Write a DataFrame to PostgreSQL via JDBC.
    mode='overwrite' → TRUNCATE + INSERT (default — prevents duplicate rows
                       when pipeline re-runs on same Kafka data)
    mode='append'    → INSERT only (used for streaming micro-batches)

    NOTE: Spark JDBC overwrite on PostgreSQL does a DROP+CREATE by default.
    We use truncate=true to preserve the schema and indexes instead.
    """
    (
        df.write
        .format("jdbc")
        .option("url",       PG_URL)
        .option("dbtable",   table)
        .option("user",      PG_PROPS["user"])
        .option("password",  PG_PROPS["password"])
        .option("driver",    PG_PROPS["driver"])
        .option("batchsize", "1000")
        .option("truncate",  "true")   # TRUNCATE instead of DROP+CREATE
        .mode(mode)
        .save()
    )

# ─────────────────────────────────────────────────────
#  AGGREGATION FUNCTIONS
# ─────────────────────────────────────────────────────

def agg_revenue_hourly(transactions_df: DataFrame) -> DataFrame:
    """
    Revenue aggregation per product category per hour.

    Window function: tumbling 1-hour on event_timestamp.
    F.window() returns a struct with {start, end} fields.
    """
    return (
        transactions_df
        .filter(F.col("payment_status") == "success")
        .groupBy(
            F.window(F.col("event_timestamp"), WINDOW_DURATION).alias("w"),
            F.col("item_category").alias("product_category"),
        )
        .agg(
            F.round(F.sum("item_subtotal"), 2).alias("total_revenue"),
            # count distinct orders (not line items)
            F.countDistinct("order_id").alias("total_orders"),
            F.sum(F.when(F.col("is_return"), 1).otherwise(0))
             .cast("integer").alias("total_returns"),
        )
        .select(
            F.col("w.start").alias("window_start"),
            F.col("w.end").alias("window_end"),
            "product_category",
            "total_revenue",
            "total_orders",
            "total_returns",
        )
    )


def agg_search_trends_hourly(web_logs_df: DataFrame) -> DataFrame:
    """
    Top search terms per hour.
    Only rows where event_type == 'search' AND search_term not null.
    """
    return (
        web_logs_df
        .filter(
            (F.col("event_type") == "search") &
            F.col("search_term").isNotNull()
        )
        .groupBy(
            F.window(F.col("event_timestamp"), WINDOW_DURATION).alias("w"),
            F.lower(F.col("search_term")).alias("search_term"),
        )
        .agg(
            F.count("*").cast("integer").alias("search_count"),
            F.countDistinct("customer_id").cast("integer").alias("unique_users"),
        )
        .select(
            F.col("w.start").alias("window_start"),
            F.col("w.end").alias("window_end"),
            "search_term",
            "search_count",
            "unique_users",
        )
        # only keep terms searched more than once (filter noise)
        .filter(F.col("search_count") > 1)
    )


def agg_traffic_hourly(web_logs_df: DataFrame) -> DataFrame:
    """
    Overall traffic metrics per hour.
    Aggregates all event types together.
    """
    return (
        web_logs_df
        .filter(~F.col("is_bot"))    # exclude bot traffic
        .groupBy(
            F.window(F.col("event_timestamp"), WINDOW_DURATION).alias("w"),
        )
        .agg(
            F.countDistinct("session_id").cast("integer").alias("total_sessions"),
            F.sum(F.when(F.col("event_type") == "click", 1).otherwise(0))
             .cast("integer").alias("total_clicks"),
            F.sum(F.when(F.col("event_type") == "pageview", 1).otherwise(0))
             .cast("integer").alias("total_pageviews"),
            F.countDistinct("customer_id").cast("integer").alias("unique_users"),
            F.round(F.avg("duration_ms"), 0).cast("long").alias("avg_session_dur_ms"),
        )
        .select(
            F.col("w.start").alias("window_start"),
            F.col("w.end").alias("window_end"),
            "total_sessions",
            "total_clicks",
            "total_pageviews",
            "unique_users",
            "avg_session_dur_ms",
        )
    )


def agg_sentiment_hourly(reviews_df: DataFrame,
                          social_df: DataFrame) -> DataFrame:
    """
    Average sentiment per hour per source (reviews vs social).

    VADER compound score thresholds:
      positive: >= 0.05
      negative: <= -0.05
      neutral:  otherwise
    """
    def _agg_one(df: DataFrame, source_name: str) -> DataFrame:
        return (
            df
            .groupBy(
                F.window(F.col("event_timestamp"), WINDOW_DURATION).alias("w"),
                F.lit(source_name).alias("source"),
            )
            .agg(
                F.round(F.avg("sentiment_score"), 4).alias("avg_sentiment"),
                F.sum(F.when(F.col("sentiment_score") >= 0.05, 1).otherwise(0))
                 .cast("integer").alias("positive_count"),
                F.sum(F.when(
                    (F.col("sentiment_score") > -0.05) &
                    (F.col("sentiment_score") < 0.05), 1).otherwise(0))
                 .cast("integer").alias("neutral_count"),
                F.sum(F.when(F.col("sentiment_score") <= -0.05, 1).otherwise(0))
                 .cast("integer").alias("negative_count"),
                F.count("*").cast("integer").alias("total_count"),
            )
            .select(
                F.col("w.start").alias("window_start"),
                F.col("w.end").alias("window_end"),
                "source", "avg_sentiment",
                "positive_count", "neutral_count",
                "negative_count", "total_count",
            )
        )

    return _agg_one(reviews_df, "reviews").union(
           _agg_one(social_df,  "social"))


def agg_customer_segments(transactions_df: DataFrame,
                           reviews_df: DataFrame) -> DataFrame:
    """
    Daily customer segment snapshot.

    Churn risk heuristic (rule-based, not ML):
      score = 0.4 * (1 - order_freq_score)    ← recency/frequency
            + 0.3 * (1 - spend_score)          ← monetary
            + 0.3 * (1 - avg_rating / 5)       ← satisfaction
    Normalised to [0, 1]. Higher = more at risk.

    Segment labels:
      vip      : top 20% by spend
      regular  : middle 60%
      at_risk  : churn_risk_score > 0.65
      new      : order_count_30d == 1
    """
    # 30-day transaction summary per customer
    txn_agg = (
        transactions_df
        .filter(F.col("payment_status") == "success")
        .groupBy("customer_id")
        .agg(
            F.round(F.sum("final_amount"), 2).alias("total_spent_30d"),
            F.countDistinct("order_id").cast("integer").alias("order_count_30d"),
            F.max("event_timestamp").alias("last_order_ts"),
            F.first("payment_method").alias("preferred_payment"),
            F.sum(F.when(F.col("is_return"), 1).otherwise(0))
             .cast("integer").alias("return_count"),
        )
        .withColumn("last_order_date", F.to_date(F.col("last_order_ts")))
        .drop("last_order_ts")
    )

    # average review rating per customer
    review_agg = (
        reviews_df
        .groupBy("customer_id")
        .agg(
            F.round(F.avg("rating"), 2).alias("avg_review_rating"),
        )
    )

    # join
    combined = txn_agg.join(review_agg, on="customer_id", how="left")

    # spend percentile for segmentation using window rank
    spend_window = Window.orderBy(F.col("total_spent_30d").desc())
    combined = combined.withColumn("spend_rank", F.percent_rank().over(spend_window))

    # churn risk score computation
    max_spend = combined.agg(F.max("total_spent_30d")).collect()[0][0] or 1.0
    max_orders = combined.agg(F.max("order_count_30d")).collect()[0][0] or 1.0

    combined = (
        combined
        .withColumn("spend_score",
                    F.col("total_spent_30d") / max_spend)
        .withColumn("order_freq_score",
                    F.col("order_count_30d") / max_orders)
        .withColumn("satisfaction_score",
                    F.coalesce(F.col("avg_review_rating"), F.lit(3.0)) / 5.0)
        .withColumn("churn_risk_score",
            F.round(
                0.4 * (1 - F.col("order_freq_score")) +
                0.3 * (1 - F.col("spend_score")) +
                0.3 * (1 - F.col("satisfaction_score")),
                4
            )
        )
        .withColumn("segment",
            F.when(F.col("order_count_30d") == 1, "new")
             .when(F.col("churn_risk_score") > 0.65, "at_risk")
             .when(F.col("spend_rank") <= 0.20, "vip")
             .otherwise("regular")
        )
        .select(
            "customer_id", "segment",
            "total_spent_30d", "order_count_30d",
            "last_order_date", "avg_review_rating",
            F.col("churn_risk_score"),
        )
    )
    return combined


# ─────────────────────────────────────────────────────
#  BATCH MODE
# ─────────────────────────────────────────────────────

def run_batch(spark):
    print("\n📦 Running in BATCH mode.\n")

    # ── read silver sources ───────────────────────────
    def read_silver(folder):
        path = f"{SILVER_PATH}/{folder}"
        try:
            return spark.read.parquet(path)
        except Exception as e:
            print(f"  ⚠️  Cannot read {path}: {e}")
            print(f"      Run silver_transform.py first.")
            return None

    web_logs_df     = read_silver("web_logs")
    transactions_df = read_silver("transactions")
    reviews_df      = read_silver("reviews")
    social_df       = read_silver("social_media")

    if not all([web_logs_df, transactions_df, reviews_df, social_df]):
        print("❌ Missing silver data. Aborting.")
        return

    # ── compute and write each gold table ─────────────
    jobs = [
        ("gold_revenue_hourly",
         lambda: agg_revenue_hourly(transactions_df)),
        ("gold_search_trends_hourly",
         lambda: agg_search_trends_hourly(web_logs_df)),
        ("gold_traffic_hourly",
         lambda: agg_traffic_hourly(web_logs_df)),
        ("gold_sentiment_hourly",
         lambda: agg_sentiment_hourly(reviews_df, social_df)),
        ("gold_customer_segments",
         lambda: agg_customer_segments(transactions_df, reviews_df)),
    ]

    for table_name, compute_fn in jobs:
        print(f"  ⏳ Computing {table_name} ...")
        try:
            result_df = compute_fn()
            count = result_df.count()

            # customer_segments is a full snapshot → overwrite
            write_mode = "overwrite"  # always overwrite — batch re-reads full Kafka history
            write_to_postgres(result_df, table_name, mode=write_mode)
            print(f"  ✅ {table_name}: {count} rows → PostgreSQL ({write_mode})")
        except Exception as e:
            print(f"  ❌ {table_name} failed: {e}")

    print("\n🎉 Gold batch complete.")


def run_stream(spark):
    """
    Streaming gold aggregations use foreachBatch to write
    micro-batch results to PostgreSQL. This is the standard
    pattern for writing aggregations to a sink that doesn't
    have native Structured Streaming support.
    """
    print("\n🌊 Running in STREAM mode. Ctrl+C to stop.\n")

    def read_silver_stream(folder):
        path = f"{SILVER_PATH}/{folder}"
        schema = spark.read.parquet(path).schema
        return spark.readStream.schema(schema).parquet(path)

    web_logs_stream     = read_silver_stream("web_logs")
    transactions_stream = read_silver_stream("transactions")
    reviews_stream      = read_silver_stream("reviews")
    social_stream       = read_silver_stream("social_media")

    queries = []

    stream_jobs = [
        ("gold_revenue_hourly",
         agg_revenue_hourly(transactions_stream),
         f"{SILVER_PATH}/_checkpoints/gold_revenue"),

        ("gold_traffic_hourly",
         agg_traffic_hourly(web_logs_stream),
         f"{SILVER_PATH}/_checkpoints/gold_traffic"),

        ("gold_sentiment_hourly",
         agg_sentiment_hourly(reviews_stream, social_stream),
         f"{SILVER_PATH}/_checkpoints/gold_sentiment"),
    ]

    for table_name, agg_df, checkpoint in stream_jobs:
        def make_foreach_batch(tname):
            def foreach_batch(batch_df, batch_id):
                if batch_df.count() > 0:
                    write_to_postgres(batch_df, tname, mode="append")
                    print(f"  ✔ batch {batch_id}: wrote to {tname}")
            return foreach_batch

        query = (
            agg_df
            .writeStream
            .foreachBatch(make_foreach_batch(table_name))
            .option("checkpointLocation", checkpoint)
            .trigger(processingTime="60 seconds")
            .outputMode("update")
            .start()
        )
        queries.append(query)
        print(f"  ▶ Stream started: {table_name}")

    print(f"\n  {len(queries)} streams running.")
    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        for q in queries:
            q.stop()
        print("✅ All streams stopped.")


if __name__ == "__main__":
    print("=" * 55)
    print("  GOLD AGGREGATIONS JOB")
    print(f"  Mode: {MODE.upper()}")
    print(f"  Input:  {SILVER_PATH}")
    print(f"  Output: PostgreSQL llj_gold")
    print("=" * 55)

    spark = get_spark("llj-gold-aggregations")

    if MODE == "batch":
        run_batch(spark)
    else:
        run_stream(spark)

    spark.stop()
    print("\nSpark session closed.")
