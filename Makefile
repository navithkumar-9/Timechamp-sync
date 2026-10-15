.PHONY: help build up down logs restart ps test clean ch-shell

help:
	@echo "Timechamp Production Stack Management Commands:"
	@echo "  make build         Build all Docker images"
	@echo "  make up            Start all services in background (dev mode)"
	@echo "  make up-prod       Start all services using production config"
	@echo "  make down          Stop and remove all containers"
	@echo "  make logs          Tail logs for all services"
	@echo "  make ps            Check running status of containers"
	@echo "  make ch-shell      Open ClickHouse client shell"
	@echo "  make redis-streams Inspect Redis Stream events"
	@echo "  make redis-cli     Open Redis CLI shell"
	@echo "  make test          Run test suite"
	@echo "  make clean         Clean python cache and temporary files"

build:
	docker compose build

up:
	docker compose up -d

up-prod:
	docker compose -f docker-compose.yml -f docker-compose.prod.yml up -d

down:
	docker compose down

logs:
	docker compose logs -f

ps:
	docker compose ps

ch-shell:
	docker compose exec clickhouse clickhouse-client --user default --password clickhouse_secret --database timechamp_analytics

redis-streams:
	docker compose exec redis redis-cli xrevrange timechamp:events:stream + - COUNT 10

redis-cli:
	docker compose exec redis redis-cli


test:
	docker compose exec backend pytest backend/tests

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
