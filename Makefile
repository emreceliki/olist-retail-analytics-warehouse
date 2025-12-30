.PHONY: help up down restart logs status init-schema ingest bronze silver gold quality all clean beeline hdfs-ls

# Default target
help:
	@echo "Olist Retail Analytics Warehouse - Available Commands"
	@echo "=============================================="
	@echo ""
	@echo "Infrastructure:"
	@echo "  make up          - Start all containers"
	@echo "  make down        - Stop all containers"
	@echo "  make restart     - Restart all containers"
	@echo "  make logs        - View container logs"
	@echo "  make status      - Show container status"
	@echo "  make init-schema - Initialize Hive metastore schema (first run only)"
	@echo ""
	@echo "Data Pipeline:"
	@echo "  make ingest      - Ingest data from ~/datasets/olist to HDFS"
	@echo "  make bronze      - Create Bronze layer tables"
	@echo "  make silver      - Create Silver layer tables"
	@echo "  make gold        - Create Gold layer (analytics marts)"
	@echo "  make quality     - Run data quality checks"
	@echo "  make all         - Run complete pipeline (ingest → bronze → silver → gold → quality)"
	@echo ""
	@echo "Utilities:"
	@echo "  make beeline     - Open Beeline CLI"
	@echo "  make hdfs-ls     - List HDFS bronze directory"
	@echo "  make clean       - Stop containers and remove volumes"


# Infrastructure commands
up:
	docker-compose up -d
	@echo "Waiting for services to be ready..."
	@sleep 30
	@docker-compose ps

down:
	docker-compose down

restart:
	docker-compose restart

logs:
	docker-compose logs -f

status:
	docker-compose ps

init-schema:
	@echo "Initializing Hive metastore schema (first run only)..."
	docker exec hive-server /opt/hive/bin/schematool -dbType postgres -initSchema
	@echo "Restarting Hive services..."
	docker-compose restart hive-metastore hive-server
	@echo "Waiting for services to restart..."
	@sleep 60
	@echo "Schema initialization complete!"


# Data Pipeline commands
DATASET_PATH ?= ~/datasets/olist

ingest:
	@echo "Ingesting data from $(DATASET_PATH) to HDFS..."
	./scripts/01_ingest_hdfs.sh $(DATASET_PATH)

bronze:
	@echo "Creating Bronze layer tables..."
	docker exec -it hive-server beeline -u jdbc:hive2://localhost:10000 -f /scripts/02_create_bronze.hql

silver:
	@echo "Creating Silver layer tables..."
	docker exec -it hive-server beeline -u jdbc:hive2://localhost:10000 -f /scripts/03_create_silver.hql

gold:
	@echo "Creating Gold layer (Analytics Marts)..."
	docker exec -it hive-server beeline -u jdbc:hive2://localhost:10000 -f /scripts/04_create_gold.hql

quality:
	@echo "Running data quality checks..."
	docker exec -it hive-server beeline -u jdbc:hive2://localhost:10000 -f /scripts/05_quality_checks.hql

all: ingest bronze silver gold quality
	@echo "----------------------------------------------"
	@echo "Pipeline complete!"
	@echo "----------------------------------------------"


# Utility commands
beeline:
	docker exec -it hive-server beeline -u jdbc:hive2://localhost:10000

hdfs-ls:
	docker exec -it namenode hdfs dfs -ls -R /data/bronze/olist/

clean:
	docker-compose down -v
	@echo "All containers and volumes removed."
