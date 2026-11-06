.DEFAULT_GOAL := help
COMPOSE ?= docker compose
UV ?= uv
ARGS ?=

.PHONY: help hooks env up down logs ps config sync run migrate test lint check
help:
	@printf '%s\n' 'make hooks    Enable Git hooks from .githooks' 'make up       Build and start consumer + PostgreSQL' 'make down     Stop services and preserve data' 'make logs     Follow consumer logs' 'make sync     Install local dependencies' 'make run      Run locally (requires Kafka and migrated PostgreSQL)' 'make migrate  Apply migrations locally' 'make check    Run Ruff, formatting check, and tests' 'make config   Validate Compose'
hooks:
	git config --local core.hooksPath .githooks

env:
	@test -f .env || cp .env.example .env
up: env
	$(COMPOSE) up -d --build

down:
	$(COMPOSE) down
logs:
	$(COMPOSE) logs -f consumer
ps:
	$(COMPOSE) ps
config: env
	$(COMPOSE) config --quiet
sync:
	$(UV) sync --locked
run: env
	$(UV) run --locked uvicorn app.main:app --host 0.0.0.0 --port 8081 --workers 1
migrate: env
	$(UV) run --locked alembic upgrade head
test:
	$(UV) run --locked pytest $(ARGS)
lint:
	$(UV) run --locked ruff check .
	$(UV) run --locked ruff format --check .
check: lint test
