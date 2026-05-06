# ─────────────────────────────────────────────────────
#  CLOUD COMPUTING LLJ — Makefile
#  Usage: make <target>
# ─────────────────────────────────────────────────────

.PHONY: up down restart logs status topics clean nuke generate gen-weblogs gen-transactions gen-reviews gen-social bronze silver gold pipeline bronze-stream silver-stream gold-stream scv scv-stream scv-test dashboard

# ── Bring full stack up (detached) ───────────────────
up:
	docker compose --env-file .env up -d
	@echo ""
	@echo "🚀 Stack starting. UIs will be available at:"
	@echo "   Kafka UI      → http://localhost:8080"
	@echo "   Spark Master  → http://localhost:9090"
	@echo "   Mongo Express → http://localhost:8081"
	@echo "   MinIO Console → http://localhost:9001"
	@echo "   pgAdmin       → http://localhost:5050"
	@echo ""
	@echo "Run 'make status' to check health."

# ── Stop all containers (keep volumes) ───────────────
down:
	docker compose --env-file .env down

# ── Restart a specific service ────────────────────────
# Usage: make restart svc=kafka
restart:
	docker compose --env-file .env restart $(svc)

# ── Tail logs for a specific service ─────────────────
# Usage: make logs svc=kafka
logs:
	docker compose --env-file .env logs -f $(svc)

# ── Check health of all containers ───────────────────
status:
	docker compose --env-file .env ps

# ── List Kafka topics ─────────────────────────────────
topics:
	docker exec llj-kafka /opt/kafka/bin/kafka-topics.sh \
		--bootstrap-server kafka:29092 --list

# ── Describe a specific Kafka topic ──────────────────
# Usage: make describe-topic topic=transactions
describe-topic:
	docker exec llj-kafka /opt/kafka/bin/kafka-topics.sh \
		--bootstrap-server kafka:29092 --describe --topic $(topic)

# ── Peek at latest N messages from a topic ───────────
# Usage: make peek topic=transactions n=5
peek:
	docker exec llj-kafka /opt/kafka/bin/kafka-console-consumer.sh \
		--bootstrap-server kafka:29092 \
		--topic $(topic) \
		--from-beginning \
		--max-messages $(n)

# ── Stop containers AND remove volumes (DESTRUCTIVE) ──
clean:
	docker compose --env-file .env down -v
	@echo "⚠️  All volumes removed. Data is gone."

# ── Nuclear option: remove everything including images ─
nuke:
	docker compose --env-file .env down -v --rmi all
	@echo "💥 Everything removed."

# ── Phase 2: Data Generators ──────────────────────────
# Run all 4 producers together (Ctrl+C to stop all)
generate:
	python3 generators/run_all.py

# Run individual producers
gen-weblogs:
	python3 generators/web_logs_producer.py

gen-transactions:
	python3 generators/transactions_producer.py

gen-reviews:
	python3 generators/reviews_producer.py

gen-social:
	python3 generators/social_media_producer.py

# ── Phase 3: Spark Jobs ───────────────────────────────
bronze:
	python3 spark_jobs/bronze_ingestion.py --mode batch

silver:
	python3 spark_jobs/silver_transform.py --mode batch

gold:
	python3 spark_jobs/gold_aggregations.py --mode batch

# Run full pipeline in sequence
pipeline:
	python3 spark_jobs/bronze_ingestion.py --mode batch && \
	python3 spark_jobs/silver_transform.py --mode batch && \
	python3 spark_jobs/gold_aggregations.py --mode batch

# Stream modes
bronze-stream:
	python3 spark_jobs/bronze_ingestion.py --mode stream

silver-stream:
	python3 spark_jobs/silver_transform.py --mode stream

gold-stream:
	python3 spark_jobs/gold_aggregations.py --mode stream

# ── Phase 4: Single Customer View ────────────────────
# Batch mode (default) — 100 customers per chunk
scv:
	python3 mongodb/customer_view_builder.py --mode batch --batch-size 100

# Stream mode — one customer at a time
scv-stream:
	python3 mongodb/customer_view_builder.py --mode stream

# Test with first 50 customers
scv-test:
	python3 mongodb/customer_view_builder.py --mode batch --batch-size 10 --limit 50

# ── Phase 5: Dashboard ────────────────────────────────
dashboard:
	streamlit run dashboard/app.py --server.port 8501
