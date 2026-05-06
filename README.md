# ShopStream — Real-Time E-Commerce Data Platform
### Cloud Computing LLJ | Soham Randive (RA2512052010049)

---

## Problem Statement

An online retail company named **ShopStream** sells products worldwide and generates massive volumes of data every day:

| Data Source | Type | Volume |
|---|---|---|
| User Clickstream | Browsing events (views, searches, cart adds) | ~2 TB/day |
| Purchase Records | Orders, payments, returns | ~500 GB/day |
| Product Reviews | Ratings, text, sentiment | ~200 GB/day |
| Social Buzz | Facebook, Instagram, Twitter posts | ~100 GB/day |

**Core business problems this platform solves:**

1. Reporting is slow — decisions are made on day-old data
2. No unified user view — data is siloed across 4 different sources
3. Data teams struggle — no shared pipeline, every team builds its own ETL
4. Security risk — PII scattered across raw unmanaged storage

---

## System Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                     DATA PRODUCERS                           │
│  Clickstream · Purchases · Product Reviews · Social Buzz     │
│                (Python · Faker · VADER NLP)                  │
└─────────────────────────┬────────────────────────────────────┘
                          │  4 Kafka Topics (KRaft Mode)
                          ▼
┌──────────────────────────────────────────────────────────────┐
│                   APACHE KAFKA 4.1.1                         │
│  clickstream (3p) · purchases (3p)                           │
│  product-reviews (2p) · social-buzz (2p)                     │
└─────────────────────────┬────────────────────────────────────┘
                          │  PySpark Structured Streaming / Batch
                          ▼
┌──────────────────────────────────────────────────────────────┐
│               3-TIER MEDALLION ARCHITECTURE                  │
│                    (MinIO / S3-compatible)                    │
│                                                              │
│   RAW LAYER    →  Parquet files, schema-validated            │
│   CLEAN LAYER  →  Deduped, enriched, PII-masked              │
│   METRICS LAYER→  Windowed aggregations → PostgreSQL         │
└─────────────────┬────────────────────────┬───────────────────┘
                  │                        │
                  ▼                        ▼
     ┌────────────────────┐    ┌─────────────────────┐
     │      MongoDB       │    │     PostgreSQL 16    │
     │  Unified User      │    │   Analytics Tables   │
     │  Profiles (UUP)    │    │   (5 gold tables)    │
     └──────────┬─────────┘    └──────────┬──────────┘
                │                         │
                └────────────┬────────────┘
                             ▼
                  ┌──────────────────┐
                  │   Streamlit      │
                  │  Live Dashboard  │
                  │  (auto-refresh)  │
                  └──────────────────┘
```

---

## Tech Stack

| Layer | Technology | Role |
|---|---|---|
| Data Simulation | Python 3.10, Faker, VADER | Generate realistic synthetic data |
| Message Queue | Apache Kafka 4.1.1 (KRaft) | Real-time event streaming, no Zookeeper |
| Batch / Stream Processing | PySpark 3.5.3 | Raw → Clean → Metrics transformation |
| Object Storage | MinIO (S3-compatible) | Parquet data lake storage |
| Document Database | MongoDB 7.0 | Unified User Profiles |
| Relational Database | PostgreSQL 16 | Aggregated analytics tables |
| Visualisation | Streamlit + streamlit-autorefresh | Live 5-page analytics dashboard |
| Infrastructure | Docker Compose | One-command full-stack deployment |

---

## Data Streams

| Stream | Kafka Topic | Rate | Partitions |
|---|---|---|---|
| User clickstream events | `clickstream` | ~10 events/sec | 3 |
| Purchase & order records | `purchases` | ~3 orders/sec | 3 |
| Product reviews | `product-reviews` | ~2 reviews/sec | 2 |
| Social media mentions | `social-buzz` | ~2 posts/sec | 2 |

---

## Project Structure

```
cloud-computing-llj/
│
├── config/
│   ├── settings.py          # User pool, product catalogue, Kafka config
│   ├── mongo-init.js        # MongoDB collections + indexes
│   └── postgres-init.sql    # Analytics table definitions
│
├── generators/
│   ├── clickstream_producer.py   # Browsing events (view, search, cart)
│   ├── purchases_producer.py     # Orders, payments, returns
│   ├── reviews_producer.py       # Product reviews with VADER sentiment
│   ├── social_buzz_producer.py   # Social media posts and hashtags
│   └── launch_producers.py       # Runs all 4 producers in parallel
│
├── spark_jobs/
│   ├── spark_config.py       # SparkSession factory + MinIO/PostgreSQL config
│   ├── log4j2.properties     # Suppresses verbose Spark/Hadoop logs
│   ├── raw_ingestion.py      # Kafka → MinIO raw Parquet (schema enforced)
│   ├── data_cleaner.py       # Raw → Clean (dedup, enrich, PII masking)
│   └── metrics_aggregator.py # Clean → Metrics (windowed aggs → PostgreSQL)
│
├── mongodb/
│   └── user_profile_builder.py  # Builds Unified User Profiles in MongoDB
│
├── dashboard/
│   └── app.py                # Streamlit 5-page live analytics dashboard
│
├── verify_stack.py           # Service health check (Kafka, MongoDB, PG, MinIO)
├── docker-compose.yml        # Full infrastructure definition
├── Makefile                  # All workflow commands
└── requirements.txt          # Python dependencies
```

---

## Getting Started

### Prerequisites

| Tool | Version |
|---|---|
| Docker | 24.x or higher |
| Python | 3.10+ |
| Java JDK | 17 |

### Step 1 — Clone the repository

```bash
git clone https://github.com/SohamRandive/cloud-computing-llj.git
cd cloud-computing-llj
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

### Step 2 — Set up environment

```bash
cp .env.example .env
# Edit .env to set your credentials (or use the defaults for local dev)
```

### Step 3 — Start the infrastructure

```bash
make up
# Wait ~30 seconds, then:
make status
```

### Step 4 — Verify all services are healthy

```bash
python3 verify_stack.py
# Expected output:
# ✅ Kafka   ✅ MongoDB   ✅ PostgreSQL   ✅ MinIO
```

### Step 5 — Run the data pipeline

```bash
# Start all 4 data producers (~90 seconds, then Ctrl+C)
make generate

# Run full pipeline: Raw → Clean → Metrics → MongoDB
make pipeline
make profile
```

### Step 6 — Open the dashboard

```bash
make dashboard
# Visit http://localhost:8501
```

---

## Available Commands

### Infrastructure
```bash
make up                          # Start all Docker services
make down                        # Stop containers (data preserved)
make clean                       # Stop + wipe all volumes
make status                      # Check container health
make logs svc=shopstream-kafka   # Stream logs for a service
make topics                      # List Kafka topics
make peek topic=purchases n=5    # Preview Kafka messages
```

### Data Generation
```bash
make generate        # Launch all 4 producers together
make gen-clicks      # Clickstream only
make gen-purchases   # Purchases only
make gen-reviews     # Reviews only
make gen-social      # Social buzz only
```

### Pipeline
```bash
make pipeline        # Full run: raw → clean → metrics
make bronze          # Kafka → MinIO raw layer
make silver          # Raw → Clean layer
make gold            # Clean → PostgreSQL metrics
make bronze-stream   # Continuous raw ingestion
make silver-stream   # Continuous clean processing
```

### User Profiles
```bash
make profile         # Build all profiles in batch (100/chunk)
make profile-stream  # Stream mode — one user at a time
make profile-test    # Quick test with first 50 users
```

### Dashboard
```bash
make dashboard       # Open at http://localhost:8501
```

---

## Dashboard Pages

| Page | Source | Description |
|---|---|---|
| 📊 Overview | PostgreSQL + MongoDB | Gross revenue by category, user segments, session metrics |
| 🔍 Keyword Trends | PostgreSQL | Top searched keywords with volume and unique visitor counts |
| 😊 Sentiment | PostgreSQL + MongoDB | VADER sentiment breakdown for reviews and social posts |
| 👤 User Profile | MongoDB | Full unified profile for any user ID (`USR-XXXXXX`) |
| ⚠️ Churn Risk | PostgreSQL + MongoDB | At-risk users ranked by churn score with spend analysis |

Auto-refresh interval is adjustable from 3–30 seconds via the sidebar toggle.

---

## Message Schemas

**clickstream**
```json
{
  "session_id": "uuid",
  "user_id": "USR-000042",
  "event_type": "view | search | scroll | add_to_wishlist | add_to_cart",
  "page": "/shop",
  "platform": "mobile_app | desktop_web | tablet_web",
  "traffic_src": "organic | paid_search | email | social",
  "keyword": "running shoes",
  "sku": "SKU-00123",
  "dwell_ms": 8400,
  "timestamp": "2026-05-06T10:00:00Z"
}
```

**purchases**
```json
{
  "order_id": "ORD-X8K2M5PQ",
  "user_id": "USR-000042",
  "status": "placed | confirmed | dispatched | delivered | returned | cancelled",
  "items": [{"sku": "SKU-00123", "title": "Running Shoes", "qty": 1, "unit_price": 2499.0}],
  "total_amount": 2499.0,
  "discount_pct": 10,
  "final_amount": 2249.1,
  "payment_method": "upi | card | wallet | buy_now_pay_later | cod",
  "payment_status": "success | failed | pending",
  "is_return": false,
  "timestamp": "2026-05-06T10:00:00Z"
}
```

**product-reviews**
```json
{
  "review_id": "RVW-A1B2C3D4",
  "user_id": "USR-000042",
  "sku": "SKU-00123",
  "rating": 5,
  "review_text": "Absolutely worth every rupee. Top quality and fast shipping.",
  "verified_purchase": true,
  "sentiment_label": "positive",
  "sentiment_score": 0.8762,
  "timestamp": "2026-05-06T10:00:00Z"
}
```

---

## Analytics Tables (PostgreSQL)

| Table | Description | Key Columns |
|---|---|---|
| `hourly_sales` | Revenue per product category per hour | `category`, `gross_revenue`, `num_orders` |
| `hourly_search_trends` | Top keywords searched per hour | `keyword`, `search_count`, `unique_visitors` |
| `hourly_site_traffic` | Session and click metrics per hour | `num_sessions`, `num_clicks`, `unique_visitors` |
| `hourly_sentiment` | Sentiment scores by source per hour | `data_source`, `mean_sentiment`, `positive_cnt` |
| `user_segments` | Daily user segmentation snapshot | `segment`, `churn_score`, `revenue_30d` |

---

## Unified User Profile (MongoDB)

```json
{
  "user_id": "USR-000042",
  "updated_at": "ISODate",
  "segment": {
    "label": "platinum | gold | silver | regular | at_risk | new",
    "churn_score": 0.38,
    "revenue_30d": 12800.50,
    "order_count_30d": 7
  },
  "purchases": {
    "total_orders": 24, "total_spent": 58400.0,
    "return_rate": 0.04, "preferred_payment": "upi"
  },
  "reviews": { "avg_rating": 4.5, "avg_sentiment": 0.72, "total_reviews": 11 },
  "browsing": { "total_sessions": 63, "total_searches": 29, "top_platform": "mobile_app" },
  "social": { "post_count": 5, "avg_sentiment": 0.68, "viral_posts": 1 }
}
```

---

## Churn Risk Model

Rule-based heuristic computed in `metrics_aggregator.py`:

```
churn_score = 0.4 × (1 − order_frequency_score)
            + 0.3 × (1 − spend_score)
            + 0.3 × (1 − satisfaction_score)
```

User segments:
- **New** — only 1 order placed
- **Platinum** — top 20% by 30-day revenue
- **At Risk** — churn score > 0.65
- **Regular** — all others

---

## Service URLs

| Service | URL | Login |
|---|---|---|
| ShopStream Dashboard | http://localhost:8501 | — |
| Kafka UI | http://localhost:8080 | — |
| Mongo Express | http://localhost:8081 | — |
| MinIO Console | http://localhost:9001 | See `.env` |
| pgAdmin | http://localhost:5050 | admin@shopstream.local / admin |

---

## Cloud Equivalents

| Local Setup | Cloud Equivalent |
|---|---|
| Apache Kafka (Docker) | AWS MSK / Confluent Cloud |
| MinIO (Docker) | AWS S3 / Google Cloud Storage |
| PySpark (local mode) | AWS EMR / Databricks |
| PostgreSQL (Docker) | AWS RDS / Amazon Redshift |
| MongoDB (Docker) | MongoDB Atlas |
| Streamlit (local) | Streamlit Community Cloud / AWS ECS |

---

## Author

**Soham Randive** — RA2512052010049
Cloud Computing LLJ | M.Tech Data Science
