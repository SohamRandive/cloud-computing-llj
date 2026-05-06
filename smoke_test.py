#!/usr/bin/env python3
import json
import time
import sys
from datetime import datetime, timezone

# ─── colour helpers ───────────────────────────────────
GREEN  = "\033[92m"
RED    = "\033[91m"
YELLOW = "\033[93m"
RESET  = "\033[0m"
ok   = lambda msg: print(f"  {GREEN}✅ {msg}{RESET}")
fail = lambda msg: print(f"  {RED}❌ {msg}{RESET}")
info = lambda msg: print(f"  {YELLOW}ℹ️  {msg}{RESET}")

def section(title):
    print(f"\n{'─'*50}")
    print(f"  {title}")
    print(f"{'─'*50}")

# ─────────────────────────────────────────────────────
#  1. KAFKA
# ─────────────────────────────────────────────────────
section("1 / 5  Kafka")
try:
    from kafka import KafkaProducer, KafkaConsumer
    from kafka.admin import KafkaAdminClient

    BROKER = "localhost:9092"
    TOPICS = ["web-logs", "transactions", "reviews", "social-media"]

    admin = KafkaAdminClient(bootstrap_servers=BROKER, client_id="smoke-test")
    existing = admin.list_topics()
    ok(f"Connected to broker: {BROKER}")

    missing = [t for t in TOPICS if t not in existing]
    if missing:
        fail(f"Missing topics: {missing}  → run 'make up' and wait for kafka-setup to finish")
        sys.exit(1)
    else:
        ok(f"All 4 topics exist: {TOPICS}")

    # ── Produce ──────────────────────────────────────
    producer = KafkaProducer(
        bootstrap_servers=BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
    )
    test_payload = {
        "smoke_test": True,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    for topic in TOPICS:
        future = producer.send(topic, value=test_payload)
        record_metadata = future.get(timeout=10)
        ok(f"Produced to '{topic}' → partition {record_metadata.partition}, offset {record_metadata.offset}")
    producer.flush()

    # ── Consume (read from beginning, grab first smoke-test message) ──
    for topic in TOPICS:
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=BROKER,
            auto_offset_reset="earliest",
            consumer_timeout_ms=5000,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            group_id=None,
        )
        found = False
        for msg in consumer:
            if msg.value.get("smoke_test"):
                ok(f"Consumed from '{topic}': partition={msg.partition} offset={msg.offset}")
                found = True
                break
        if not found:
            fail(f"No smoke_test message found in '{topic}'")
        consumer.close()

    kafka_ok = True
except Exception as e:
    fail(f"Kafka error: {e}")
    kafka_ok = False

# ─────────────────────────────────────────────────────
#  2. MONGODB
# ─────────────────────────────────────────────────────
section("2 / 5  MongoDB")
try:
    from pymongo import MongoClient

    client = MongoClient(
        "mongodb://admin:llj_mongo_pass@localhost:27017/",
        serverSelectionTimeoutMS=5000,
    )
    client.admin.command("ping")
    ok("MongoDB ping successful")

    db = client["llj_cvs"]
    collections = db.list_collection_names()
    ok(f"Collections in llj_cvs: {collections}")

    # Insert + retrieve test document
    db.customer_profiles.update_one(
        {"customer_id": "smoke-test-001"},
        {"$set": {
            "customer_id": "smoke-test-001",
            "updated_at": datetime.now(timezone.utc),
            "smoke_test": True,
        }},
        upsert=True,
    )
    doc = db.customer_profiles.find_one({"customer_id": "smoke-test-001"})
    ok(f"Upsert + read back OK — _id: {doc['_id']}")
    # Cleanup
    db.customer_profiles.delete_one({"customer_id": "smoke-test-001"})
    mongo_ok = True
except Exception as e:
    fail(f"MongoDB error: {e}")
    mongo_ok = False

# ─────────────────────────────────────────────────────
#  3. POSTGRESQL
# ─────────────────────────────────────────────────────
section("3 / 5  PostgreSQL")
try:
    import psycopg2

    conn = psycopg2.connect(
        host="localhost",
        port=5432,
        user="llj_user",
        password="llj_pg_pass",
        dbname="llj_gold",
        connect_timeout=5,
    )
    cur = conn.cursor()
    cur.execute("SELECT version();")
    version = cur.fetchone()[0]
    ok(f"Connected: {version[:50]}...")

    cur.execute("""
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = 'public'
        ORDER BY table_name;
    """)
    tables = [r[0] for r in cur.fetchall()]
    ok(f"Gold tables: {tables}")
    cur.close()
    conn.close()
    pg_ok = True
except Exception as e:
    fail(f"PostgreSQL error: {e}")
    pg_ok = False

# ─────────────────────────────────────────────────────
#  4. MINIO
# ─────────────────────────────────────────────────────
section("4 / 5  MinIO")
try:
    import boto3
    from botocore.client import Config

    s3 = boto3.client(
        "s3",
        endpoint_url="http://localhost:9000",
        aws_access_key_id="llj_minio_admin",
        aws_secret_access_key="llj_minio_pass",
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )
    response = s3.list_buckets()
    buckets = [b["Name"] for b in response["Buckets"]]
    ok(f"MinIO buckets: {buckets}")

    expected = {"bronze", "silver", "gold"}
    missing_buckets = expected - set(buckets)
    if missing_buckets:
        fail(f"Missing buckets: {missing_buckets} — wait for minio-setup container to finish")
    else:
        ok("All 3 buckets present: bronze / silver / gold")

    # Test object write/read
    s3.put_object(Bucket="bronze", Key="smoke-test/ping.json",
                  Body=json.dumps({"ping": True}).encode())
    obj = s3.get_object(Bucket="bronze", Key="smoke-test/ping.json")
    content = json.loads(obj["Body"].read())
    ok(f"Object write+read OK: {content}")
    s3.delete_object(Bucket="bronze", Key="smoke-test/ping.json")
    minio_ok = True
except Exception as e:
    fail(f"MinIO error: {e}")
    minio_ok = False

# ─────────────────────────────────────────────────────
#  SUMMARY
# ─────────────────────────────────────────────────────
section("SUMMARY")
results = {
    "Kafka":      kafka_ok,
    "MongoDB":    mongo_ok,
    "PostgreSQL": pg_ok,
    "MinIO":      minio_ok,
}
all_ok = True
for svc, status in results.items():
    if status:
        ok(svc)
    else:
        fail(svc)
        all_ok = False

print()
if all_ok:
    print(f"{GREEN}🎉 All systems go. Ready for Phase 2 (data generators).{RESET}")
else:
    print(f"{RED}⚠️  Some services failed. Check 'make status' and 'make logs svc=<name>'.{RESET}")
print()
