#!/usr/bin/env python3
"""ShopStream smoke test — verifies Kafka, MongoDB, PostgreSQL, MinIO are healthy."""

import sys
from datetime import datetime, timezone

GREEN  = "\033[92m"
RED    = "\033[91m"
YELLOW = "\033[93m"
RESET  = "\033[0m"

results = {}


def ok(msg):
    print(f"  {GREEN}✅ {msg}{RESET}")


def fail(msg):
    print(f"  {RED}❌ {msg}{RESET}")


def section(title):
    print(f"\n{YELLOW}{'─'*50}")
    print(f"  {title}")
    print(f"{'─'*50}{RESET}")


# ── 1. KAFKA ──────────────────────────────────────────
section("1 / 4  Kafka")
try:
    from kafka import KafkaAdminClient
    admin = KafkaAdminClient(bootstrap_servers="localhost:9092", request_timeout_ms=5000)
    topics = admin.list_topics()
    ok(f"Kafka reachable. Topics: {sorted(topics)}")
    expected = {"clickstream", "purchases", "product-reviews", "social-buzz"}
    missing  = expected - set(topics)
    if missing:
        fail(f"Missing topics: {missing}")
    else:
        ok("All 4 topics present")
    admin.close()
    kafka_ok = True
except Exception as e:
    fail(f"Kafka error: {e}")
    kafka_ok = False

# ── 2. MONGODB ────────────────────────────────────────
section("2 / 4  MongoDB")
try:
    from pymongo import MongoClient
    client = MongoClient("mongodb://admin:shopstream_mongo_pass@localhost:27017/",
                         serverSelectionTimeoutMS=5000)
    client.admin.command("ping")
    ok("MongoDB ping successful")
    db   = client["shopstream_profiles"]
    cols = db.list_collection_names()
    ok(f"Collections in shopstream_profiles: {cols}")
    db.user_profiles.update_one(
        {"user_id": "smoke-test-001"},
        {"$set": {"user_id": "smoke-test-001", "updated_at": datetime.now(timezone.utc), "smoke": True}},
        upsert=True,
    )
    doc = db.user_profiles.find_one({"user_id": "smoke-test-001"})
    ok(f"Upsert + read OK — _id: {doc['_id']}")
    db.user_profiles.delete_one({"user_id": "smoke-test-001"})
    mongo_ok = True
except Exception as e:
    fail(f"MongoDB error: {e}")
    mongo_ok = False

# ── 3. POSTGRESQL ─────────────────────────────────────
section("3 / 4  PostgreSQL")
try:
    import psycopg2
    conn = psycopg2.connect(
        host="localhost", port=5432,
        user="shopstream_user", password="shopstream_pg_pass",
        dbname="shopstream_gold", connect_timeout=5,
    )
    cur = conn.cursor()
    cur.execute("SELECT version();")
    ok(f"Connected: {cur.fetchone()[0][:50]}...")
    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public' ORDER BY table_name;")
    tables = [r[0] for r in cur.fetchall()]
    ok(f"Gold tables: {tables}")
    cur.close(); conn.close()
    pg_ok = True
except Exception as e:
    fail(f"PostgreSQL error: {e}")
    pg_ok = False

# ── 4. MINIO ──────────────────────────────────────────
section("4 / 4  MinIO")
try:
    import boto3
    from botocore.client import Config
    s3 = boto3.client("s3",
                      endpoint_url="http://localhost:9000",
                      aws_access_key_id="shopstream_minio_admin",
                      aws_secret_access_key="shopstream_minio_pass",
                      config=Config(signature_version="s3v4"),
                      region_name="us-east-1")
    buckets = [b["Name"] for b in s3.list_buckets()["Buckets"]]
    ok(f"MinIO buckets: {buckets}")
    minio_ok = True
except Exception as e:
    fail(f"MinIO error: {e}")
    minio_ok = False

# ── Summary ───────────────────────────────────────────
print(f"\n{YELLOW}{'═'*50}")
print("  SMOKE TEST SUMMARY")
print(f"{'═'*50}{RESET}")
all_pass = True
for svc, status in [("Kafka", kafka_ok), ("MongoDB", mongo_ok),
                    ("PostgreSQL", pg_ok), ("MinIO", minio_ok)]:
    icon = f"{GREEN}✅" if status else f"{RED}❌"
    print(f"  {icon}  {svc}{RESET}")
    if not status:
        all_pass = False

print()
if all_pass:
    print(f"{GREEN}  All services healthy. Ready to run `make generate`!{RESET}")
else:
    print(f"{RED}  Some services failed. Run `make up` and wait 30s.{RESET}")
    sys.exit(1)
