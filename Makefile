.PHONY: help setup start stop clean generate-data ingest-bronze run-pipeline

help:
	@echo "Lakehouse Architecture - Makefile Commands"
	@echo ""
	@echo "Setup:"
	@echo "  make setup          - Set up Python environment and install dependencies"
	@echo "  make start          - Start all Docker services"
	@echo "  make stop           - Stop all Docker services"
	@echo ""
	@echo "Data:"
	@echo "  make generate-data  - Generate synthetic e-commerce data"
	@echo "  make ingest-bronze  - Run Bronze layer ingestion"
	@echo "  make run-pipeline   - Run complete pipeline (Bronze → Silver → Gold)"
	@echo ""
	@echo "Maintenance:"
	@echo "  make clean          - Clean up generated files and Docker volumes"
	@echo "  make logs           - View Docker logs"

setup:
	python3 -m venv venv
	. venv/bin/activate && pip install --upgrade pip && pip install -r requirements.txt

start:
	docker-compose up -d
	@echo "Services started. Access:"
	@echo "  - MinIO Console: http://localhost:9001"
	@echo "  - Jupyter: http://localhost:8888"
	@echo "  - Airflow: http://localhost:8080"

stop:
	docker-compose down

clean:
	docker-compose down -v
	rm -rf data/raw/*.csv
	rm -rf __pycache__ */__pycache__ */*/__pycache__
	find . -type d -name "*.egg-info" -exec rm -r {} +
	find . -type d -name ".pytest_cache" -exec rm -r {} +

generate-data:
	python scripts/data_generator.py

ingest-bronze:
	python scripts/bronze/ingest_raw_data.py

process-silver:
	python scripts/silver/process_silver.py

run-pipeline:
	python scripts/run_pipeline.py

test:
	pytest tests/ -v

test-coverage:
	pytest tests/ --cov=scripts --cov-report=html

logs:
	docker-compose logs -f

