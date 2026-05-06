# spark_jobs/spark_utils.py — Shared SparkSession factory for ShopStream

import logging
import warnings
from pathlib import Path

from pyspark.sql import SparkSession

warnings.filterwarnings("ignore")
logging.getLogger("py4j").setLevel(logging.ERROR)
logging.getLogger("pyspark").setLevel(logging.ERROR)

LOG4J_CONFIG = str(Path(__file__).parent / "log4j2.properties")

MINIO_ENDPOINT   = "http://localhost:9000"
MINIO_ACCESS_KEY = "shopstream_minio_admin"
MINIO_SECRET_KEY = "shopstream_minio_pass"

KAFKA_BROKER = "localhost:9092"

BRONZE_PATH = "s3a://bronze"
SILVER_PATH = "s3a://silver"
GOLD_PATH   = "s3a://gold"

PG_URL   = "jdbc:postgresql://localhost:5432/shopstream_gold"
PG_PROPS = {
    "user":     "shopstream_user",
    "password": "shopstream_pg_pass",
    "driver":   "org.postgresql.Driver",
}


def get_spark(app_name: str, shuffle_partitions: int = 4) -> SparkSession:
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[2]")
        .config(
            "spark.jars.packages",
            ",".join([
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3",
                "org.apache.hadoop:hadoop-aws:3.3.4",
                "com.amazonaws:aws-java-sdk-bundle:1.12.262",
                "org.postgresql:postgresql:42.7.3",
            ])
        )
        .config("spark.driver.extraJavaOptions",
                f"-Dlog4j.configurationFile=file:{LOG4J_CONFIG} "
                f"-Dlog4j2.configurationFile=file:{LOG4J_CONFIG} "
                f"-Dlog4j2.formatMsgNoLookups=true "
                f"-Dhadoop.home.dir=/tmp "
                f"-Djava.util.logging.manager=org.apache.logging.log4j.jul.LogManager")
        .config("spark.executor.extraJavaOptions",
                f"-Dlog4j.configurationFile=file:{LOG4J_CONFIG} "
                f"-Dlog4j2.configurationFile=file:{LOG4J_CONFIG}")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
        .config("spark.driver.memory",          "2g")
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.default.parallelism",    str(shuffle_partitions))
        .config("spark.hadoop.fs.s3a.endpoint",           MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key",         MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key",         MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access",  "true")
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.hadoop.fs.s3a.connection.maximum", "10")
        .config("spark.hadoop.fs.s3a.metrics.enabled",    "false")
        .config("spark.ui.port",                "4040")
        .config("spark.ui.showConsoleProgress", "true")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")
    log4j = spark.sparkContext._jvm.org.apache.log4j
    for pkg in ["org.apache.kafka", "org.apache.hadoop",
                "org.apache.spark", "com.amazonaws"]:
        log4j.Logger.getLogger(pkg).setLevel(log4j.Level.ERROR)

    return spark
