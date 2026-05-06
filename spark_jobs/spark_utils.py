# spark_jobs/spark_utils.py
# ─────────────────────────────────────────────────────
#  Shared SparkSession factory + MinIO/S3 config.
#  All jobs import get_spark() from here.
# ─────────────────────────────────────────────────────

import logging
import os
import warnings
from pathlib import Path

from pyspark.sql import SparkSession

# ── Suppress Python-level warnings ───────────────────
warnings.filterwarnings("ignore")
logging.getLogger("py4j").setLevel(logging.ERROR)
logging.getLogger("pyspark").setLevel(logging.ERROR)

# ── Path to log4j2 config ─────────────────────────────
LOG4J_CONFIG = str(Path(__file__).parent / "log4j2.properties")

# ── MinIO connection (S3-compatible) ──────────────────
MINIO_ENDPOINT   = "http://localhost:9000"
MINIO_ACCESS_KEY = "llj_minio_admin"
MINIO_SECRET_KEY = "llj_minio_pass"

# ── Kafka broker ──────────────────────────────────────
KAFKA_BROKER = "localhost:9092"

# ── MinIO bucket paths ────────────────────────────────
BRONZE_PATH = "s3a://bronze"
SILVER_PATH = "s3a://silver"
GOLD_PATH   = "s3a://gold"

# ── PostgreSQL ────────────────────────────────────────
PG_URL  = "jdbc:postgresql://localhost:5432/llj_gold"
PG_PROPS = {
    "user":     "llj_user",
    "password": "llj_pg_pass",
    "driver":   "org.postgresql.Driver",
}

def get_spark(app_name: str, shuffle_partitions: int = 4) -> SparkSession:
    """
    Build a local SparkSession with:
      - hadoop-aws for S3A (MinIO) access
      - kafka connector for structured streaming
      - postgresql JDBC driver
      - all warnings suppressed
      - conservative memory settings for 16GB laptop
    """
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[2]")

        # ── Packages ──────────────────────────────────
        .config(
            "spark.jars.packages",
            ",".join([
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3",
                "org.apache.hadoop:hadoop-aws:3.3.4",
                "com.amazonaws:aws-java-sdk-bundle:1.12.262",
                "org.postgresql:postgresql:42.7.3",
            ])
        )

        # ── Suppress all Spark/Hadoop logs ────────────
        .config("spark.driver.extraJavaOptions",
                f"-Dlog4j.configurationFile=file:{LOG4J_CONFIG} "
                f"-Dlog4j2.configurationFile=file:{LOG4J_CONFIG} "
                f"-Dlog4j2.formatMsgNoLookups=true "
                f"-Dhadoop.home.dir=/tmp "
                f"-Djava.util.logging.manager=org.apache.logging.log4j.jul.LogManager")
        .config("spark.executor.extraJavaOptions",
                f"-Dlog4j.configurationFile=file:{LOG4J_CONFIG} "
                f"-Dlog4j2.configurationFile=file:{LOG4J_CONFIG}")

        # suppress specific noisy warnings
        .config("spark.sql.adaptive.enabled",               "true")
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")

        # ── Memory ────────────────────────────────────
        .config("spark.driver.memory",           "2g")
        .config("spark.sql.shuffle.partitions",  str(shuffle_partitions))
        .config("spark.default.parallelism",     str(shuffle_partitions))

        # ── S3A / MinIO ───────────────────────────────
        .config("spark.hadoop.fs.s3a.endpoint",           MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key",         MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key",         MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access",  "true")
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.hadoop.fs.s3a.connection.maximum", "10")
        .config("spark.hadoop.fs.s3a.attempts.maximum",   "3")
        # suppress S3A metrics warning
        .config("spark.hadoop.fs.s3a.metrics.enabled",    "false")

        # ── Misc ──────────────────────────────────────
        .config("spark.ui.port",                 "4040")
        .config("spark.ui.showConsoleProgress",  "true")

        .getOrCreate()
    )

    # suppress all log levels at runtime
    spark.sparkContext.setLogLevel("ERROR")

    # suppress AdminClientConfig warnings from Kafka connector
    log4j = spark.sparkContext._jvm.org.apache.log4j
    log4j.Logger.getLogger("org.apache.kafka").setLevel(log4j.Level.ERROR)
    log4j.Logger.getLogger("org.apache.hadoop").setLevel(log4j.Level.ERROR)
    log4j.Logger.getLogger("org.apache.spark").setLevel(log4j.Level.ERROR)
    log4j.Logger.getLogger("com.amazonaws").setLevel(log4j.Level.ERROR)

    return spark
