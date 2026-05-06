# ─────────────────────────────────────────────────────
#  CLOUD COMPUTING LLJ — Makefile
# ─────────────────────────────────────────────────────

.PHONY: up down restart logs status topics clean nuke \
        generate gen-clicks gen-purchases gen-reviews gen-social \
        bronze silver gold pipeline bronze-stream silver-stream \
        profile profile-stream profile-test dashboard

up:
	docker compose --env-file .env up -d
	@echo ""
	@echo "Stack starting. UIs available at:"
	@echo "  Kafka UI      → http://localhost:8080"
	@echo "  Mongo Express → http://localhost:8081"
	@echo "  MinIO Console → http://localhost:9001"
	@echo "  pgAdmin       → http://localhost:5050"
	@echo ""
	@echo "Run 'make status' to check health."

down:
	docker compose --env-file .env down

restart:
	docker compose --env-file .env restart $(svc)

logs:
	docker compose --env-file .env logs -f $(svc)

status:
	docker compose --env-file .env ps

topics:
	docker exec shopstream-kafka /opt/kafka/bin/kafka-topics.sh \
		--bootstrap-server kafka:29092 --list

describe-topic:
	docker exec shopstream-kafka /opt/kafka/bin/kafka-topics.sh \
		--bootstrap-server kafka:29092 --describe --topic $(topic)

peek:
	docker exec shopstream-kafka /opt/kafka/bin/kafka-console-consumer.sh \
		--bootstrap-server kafka:29092 \
		--topic $(topic) --from-beginning --max-messages $(n)

clean:
	docker compose --env-file .env down -v
	@echo "All volumes removed."

nuke:
	docker compose --env-file .env down -v --rmi all

generate:
	python3 generators/run_all.py

gen-clicks:
	python3 generators/clickstream_producer.py

gen-purchases:
	python3 generators/purchases_producer.py

gen-reviews:
	python3 generators/reviews_producer.py

gen-social:
	python3 generators/social_buzz_producer.py

bronze:
	python3 spark_jobs/bronze_ingestion.py --mode batch

silver:
	python3 spark_jobs/silver_transform.py --mode batch

gold:
	python3 spark_jobs/gold_aggregations.py --mode batch

pipeline:
	python3 spark_jobs/bronze_ingestion.py --mode batch && \
	python3 spark_jobs/silver_transform.py --mode batch && \
	python3 spark_jobs/gold_aggregations.py --mode batch

bronze-stream:
	python3 spark_jobs/bronze_ingestion.py --mode stream

silver-stream:
	python3 spark_jobs/silver_transform.py --mode stream

profile:
	python3 mongodb/customer_view_builder.py --mode batch --batch-size 100

profile-stream:
	python3 mongodb/customer_view_builder.py --mode stream

profile-test:
	python3 mongodb/customer_view_builder.py --mode batch --batch-size 10 --limit 50

dashboard:
	streamlit run dashboard/app.py --server.port 8501
