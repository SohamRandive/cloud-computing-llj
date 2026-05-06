# Cloud Computing LLJ — Soham Randive (RA2512052010049)

A production-grade data engineering pipeline that simulates a retail company collecting **~2.8TB/day** of multi-modal data. Built with Apache Kafka, PySpark, MongoDB, PostgreSQL, and MinIO — fully containerised with Docker Compose and visualised through a live-updating Streamlit dashboard.

---

## 📌 Problem Statement

Modern e-commerce companies face four core data challenges:

| Problem | Impact |
|---|---|
| Slow reporting | Business decisions made on stale, day-old data |
| No single customer view | Customer data siloed across web, transactions, reviews, social |
| Data engineer / scientist struggle | No unified pipeline — every team builds their own ETL |
| Security concerns | PII scattered across unmanaged raw stores |

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────┐
│                   DATA GENERATORS                       │
│  Web Logs · Transactions · Reviews · Social Media       │
│              (Python + Faker + VADER)                   │
└──────────────────────┬──────────────────────────────────┘
                       │ Kafka Topics (KRaft, no Zookeeper)
                       ▼
┌─────────────────────────────────────────────────────────┐
│                  APACHE KAFKA 4.1.1                     │
│   web-logs (3p) · transactions (3p) · reviews (2p)      │
│                  social-media (2p)                      │
└──────────────────────┬──────────────────────────────────┘
                       │ PySpark Structured Streaming / Batch
                       ▼
┌─────────────────────────────────────────────────────────┐
│              MEDALLION ARCHITECTURE (MinIO S3A)         │
│                                                         │
│  🥉 Bronze  →  Raw Parquet (schema enforced)            │
│  🥈 Silver  →  Cleaned, deduplicated, PII masked        │
│  🥇 Gold    →  Windowed aggregations → PostgreSQL       │
└──────────┬──────────────────────────┬───────────────────┘
           │                          │
           ▼                          ▼
┌─────────────────┐        ┌──────────────────────┐
│    MongoDB      │        │      PostgreSQL       │
│  Single Customer│        │   Gold Layer / DW     │
│  View (SCV)     │        │  5 aggregation tables │
└────────┬────────┘        └──────────┬───────────┘
         │                            │
         └──────────┬─────────────────┘
                    ▼
         ┌─────────────────┐
         │    Streamlit    │
         │  Live Dashboard │
         │  (auto-refresh) │
         └─────────────────┘
```

---

## 🛠️ Tech Stack

| Layer | Technology | Purpose |
|---|---|---|
| Data Generation | Python 3.10, Faker, VADER | Synthetic realistic data |
| Message Broker | Apache Kafka 4.1.1 (KRaft) | Real-time event streaming |
| Stream Processing | PySpark 3.5.3 | Bronze → Silver → Gold |
| Data Lake | MinIO (S3-compatible) | Parquet storage |
| Document Store | MongoDB 7.0 | Single Customer View |
| Data Warehouse | PostgreSQL 16 | Aggregated gold tables |
| Dashboard | Streamlit + streamlit-autorefresh | Live analytics UI |
| Orchestration | Docker Compose | Full stack management |

---

## 📊 Data Sources

| Source | Format | Simulated Volume | Kafka Topic | Partitions |
|---|---|---|---|---|
| Web Logs | Unstructured JSON | ~10 events/sec | `web-logs` | 3 |
| Transactions | Structured JSON | ~3 orders/sec | `transactions` | 3 |
| Customer Reviews | Semi-structured JSON | ~2 reviews/sec | `reviews` | 2 |
| Social Media | Unstructured JSON | ~2 posts/sec | `social-media` | 2 |

---

## 🗂️ Project Structure

```
cloud-computing-llj/
│
├── config/
│   ├── settings.py          # Shared config: customer pool, products, Kafka settings
│   ├── mongo-init.js        # MongoDB schema + indexes (auto-runs on first start)
│   └── postgres-init.sql    # PostgreSQL gold layer tables + indexes
│
├── generators/
│   ├── web_logs_producer.py      # Simulates clicks, searches, pageviews
│   ├── transactions_producer.py  # Simulates orders, payments, returns
│   ├── reviews_producer.py       # Simulates reviews with VADER sentiment
│   ├── social_media_producer.py  # Simulates FB/Twitter/Instagram posts
│   └── run_all.py                # Launches all 4 producers in parallel
│
├── spark_jobs/
│   ├── spark_utils.py        # SparkSession factory, MinIO/PG config
│   ├── log4j2.properties     # Suppresses Spark/Hadoop warning noise
│   ├── bronze_ingestion.py   # Kafka → MinIO (raw Parquet, schema enforced)
│   ├── silver_transform.py   # Bronze → Silver (clean, dedup, enrich)
│   └── gold_aggregations.py  # Silver → Gold (windowed aggs → PostgreSQL)
│
├── mongodb/
│   └── customer_view_builder.py  # Builds Single Customer View in MongoDB
│
├── dashboard/
│   └── app.py               # Streamlit 5-page live analytics dashboard
│
├── smoke_test.py            # Phase 1 verification: Kafka + MongoDB + PG + MinIO
├── docker-compose.yml       # Full stack definition (no Spark in Docker)
├── Makefile                 # All commands as make targets
└── requirements.txt         # Python dependencies
```

---

## ⚡ Quick Start

### Prerequisites

| Tool | Version | Install |
|---|---|---|
| Docker | 29.x+ | [docs.docker.com](https://docs.docker.com/get-docker/) |
| Python | 3.10+ | System or pyenv |
| Java (JDK) | 17 | `sudo apt install default-jdk` |

### 1. Clone and set up

```bash
git clone https://github.com/<your-username>/cloud-computing-llj.git
cd cloud-computing-llj

# Create virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### 2. Configure environment

```bash
# Copy the example env file and edit credentials if needed
cp .env.example .env
```

> ⚠️ The `.env` file contains database credentials and is excluded from Git. Never commit it.

### 3. Start the Docker stack

```bash
make up
# Wait ~30 seconds for all services to become healthy
make status
```

### 4. Verify all services

```bash
python3 smoke_test.py
# Expected: ✅ Kafka  ✅ MongoDB  ✅ PostgreSQL  ✅ MinIO
```

### 5. Generate data and run pipeline

```bash
# Generate synthetic data (run for ~90 seconds, then Ctrl+C)
make generate

# Run full pipeline: Kafka → Bronze → Silver → Gold → MongoDB
make pipeline
make scv
```

### 6. Launch dashboard

```bash
make dashboard
# Open http://localhost:8501
```

---

## 🎯 Makefile Commands

### Infrastructure

```bash
make up              # Start full Docker stack (detached)
make down            # Stop containers (keep volumes)
make status          # Check health of all containers
make clean           # Stop + remove all volumes (destructive)
make logs svc=kafka  # Tail logs for a specific service
make topics          # List Kafka topics
make peek topic=transactions n=5  # Peek at Kafka messages
```

### Data Generation

```bash
make generate        # Run all 4 producers simultaneously
make gen-weblogs     # Web logs only
make gen-transactions
make gen-reviews
make gen-social
```

### Pipeline

```bash
make pipeline        # Full run: bronze → silver → gold
make bronze          # Kafka → MinIO bronze (batch)
make silver          # Bronze → MinIO silver (batch)
make gold            # Silver → PostgreSQL gold (batch)

# Stream modes
make bronze-stream
make silver-stream
```

### Single Customer View

```bash
make scv             # Build all customer profiles (batch, 100/chunk)
make scv-stream      # Stream mode (one customer at a time, visible in dashboard)
make scv-test        # Test with first 50 customers
```

### Dashboard

```bash
make dashboard       # Launch Streamlit on http://localhost:8501
```

### Live Demo Loop

```bash
# Continuously generates data and re-runs pipeline every cycle
while true; do
  make generate & sleep 60; kill %1
  make pipeline && make scv
  sleep 10
done
```

---

## 🖥️ Dashboard Pages

| Page | Data Source | What it shows |
|---|---|---|
| 📈 Overview | PostgreSQL + MongoDB | Revenue by category, customer segments, traffic KPIs |
| 🔍 Search Trends | PostgreSQL | Top searched terms with count and unique users |
| 😊 Sentiment | PostgreSQL + MongoDB | VADER compound scores for reviews and social posts |
| 👤 Customer 360 | MongoDB | Full unified profile for any `CUST-XXXXX` |
| 🚨 Churn Risk | PostgreSQL + MongoDB | At-risk customers ranked by heuristic churn score |

The dashboard auto-refreshes every N seconds (configurable 3–30s via sidebar toggle) using a JavaScript timer — no page reload required.

---

## 🗄️ Data Schema

### Kafka Message Schemas

**web-logs**
```json
{
  "session_id": "uuid",
  "customer_id": "CUST-00001",
  "event_type": "click | search | pageview | add_to_cart | remove_from_cart",
  "url": "/products",
  "referrer": "google",
  "device": "mobile | desktop | tablet",
  "search_term": "shoes",
  "product_id": "PROD-0042",
  "duration_ms": 4521,
  "ip_address": "192.168.1.xxx",
  "timestamp": "2026-05-04T16:00:00Z"
}
```

**transactions**
```json
{
  "order_id": "ORD-A1B2C3D4",
  "customer_id": "CUST-00001",
  "status": "placed | confirmed | shipped | delivered | returned | cancelled",
  "items": [{"product_id": "PROD-0042", "qty": 2, "unit_price": 499.0, "subtotal": 998.0}],
  "total_amount": 998.0,
  "discount_pct": 10,
  "final_amount": 898.2,
  "payment_method": "upi",
  "payment_status": "success | failed | pending",
  "is_return": false,
  "timestamp": "2026-05-04T16:00:00Z"
}
```

**reviews**
```json
{
  "review_id": "REV-A1B2C3D4",
  "customer_id": "CUST-00001",
  "product_id": "PROD-0042",
  "rating": 4,
  "review_text": "Great product, fast delivery!",
  "sentiment_label": "positive",
  "sentiment_score": 0.8271,
  "verified_purchase": true,
  "timestamp": "2026-05-04T16:00:00Z"
}
```

### PostgreSQL Gold Tables

| Table | Description | Key Columns |
|---|---|---|
| `gold_revenue_hourly` | Revenue per category per hour | `window_start`, `product_category`, `total_revenue` |
| `gold_search_trends_hourly` | Top search terms per hour | `search_term`, `search_count`, `unique_users` |
| `gold_traffic_hourly` | Traffic metrics per hour | `total_sessions`, `total_clicks`, `unique_users` |
| `gold_sentiment_hourly` | Sentiment scores per source/hour | `source`, `avg_sentiment`, `positive_count` |
| `gold_customer_segments` | Daily customer segmentation | `segment`, `churn_risk_score`, `total_spent_30d` |

### MongoDB Single Customer View

```json
{
  "customer_id": "CUST-00001",
  "updated_at": "ISODate",
  "segment": {
    "label": "vip | regular | at_risk | new",
    "churn_risk_score": 0.42,
    "total_spent_30d": 15420.50
  },
  "transactions": { "total_orders": 12, "total_spent": 45200.0, "return_rate": 0.08 },
  "reviews": { "avg_rating": 4.2, "avg_sentiment_score": 0.61 },
  "web_behaviour": { "total_sessions": 47, "total_searches": 23, "top_device": "mobile" },
  "social": { "post_count": 3, "avg_sentiment": 0.72, "platforms": ["twitter", "facebook"] }
}
```

---

## 🌐 Service UIs

| Service | URL | Credentials |
|---|---|---|
| Streamlit Dashboard | http://localhost:8501 | — |
| Kafka UI | http://localhost:8080 | — |
| MinIO Console | http://localhost:9001 | See `.env` |
| Mongo Express | http://localhost:8081 | — |
| pgAdmin | http://localhost:5050 | admin@llj.local / admin |

---

## 🧮 Churn Risk Model

The churn risk score is a **rule-based heuristic** (not ML) computed in the gold aggregation job:

```
churn_risk_score = 0.4 × (1 − order_frequency_score)
                 + 0.3 × (1 − spend_score)
                 + 0.3 × (1 − satisfaction_score)
```

Where:
- `order_frequency_score = order_count / max_orders` (normalised 0–1)
- `spend_score = total_spent / max_spend` (normalised 0–1)
- `satisfaction_score = avg_rating / 5`

Customer segments:
- **New** — only 1 order ever
- **VIP** — top 20% by spend
- **At Risk** — churn score > 0.65
- **Regular** — everyone else

---

## 📦 Requirements

```
pyspark==3.5.3
kafka-python
pymongo
psycopg2-binary
boto3
faker
vaderSentiment
streamlit
streamlit-autorefresh
```

Install with:
```bash
pip install -r requirements.txt
```

---

## 🚀 Production Equivalent

This project is architecturally equivalent to a cloud deployment:

| Local (this project) | Cloud equivalent |
|---|---|
| Apache Kafka (Docker) | AWS MSK / Confluent Cloud |
| MinIO (Docker) | AWS S3 / GCP GCS |
| PySpark (local mode) | AWS EMR / Databricks |
| PostgreSQL (Docker) | AWS RDS / Redshift |
| MongoDB (Docker) | MongoDB Atlas |
| Streamlit (local) | Streamlit Cloud / AWS ECS |

----

## 👨‍💻 About

**Cloud Computing LLJ** is a cloud computing project demonstrating real-time data engineering patterns including streaming ingestion, medallion architecture, unified customer profiling, and live analytics.

**Author:** Soham Randive — RA2512052010049

---

## 📄 License

MIT License — free to use, modify, and distribute.
