# Temporal x MongoDB PRA — local dev orchestration.
# Run `make` (or `make help`) to list targets.

SHELL := /bin/bash
COMPOSE := docker compose --env-file .env -f infra/docker-compose.yml
PY := uv run python
LOGDIR := .local

# Optional args:
#   make seed FILE=./doc.md KEY=docs/doc.md
#   make query Q="what does the cookbook say?"
#   make backfill MODEL=voyage-3-large
#   make seed-docs
FILE ?= seed/awesome-temporal.md
KEY ?=
Q ?= what does Temporal own in this architecture?
MODEL ?= voyage-3-large
REPO_DIR ?=
PREFIX ?= temporalio-documentation-md-only
DRY_RUN ?=
REPO_URL ?= https://github.com/temporalio/documentation.git
REPO_REF ?= main
CHECKOUT_DIR ?= .local/imports/temporal-documentation
DELAY_MS ?= 250

.DEFAULT_GOAL := help

.PHONY: help
help: ## Show this help
	@echo "Temporal x MongoDB PRA — local dev"
	@echo
	@grep -E '^[a-zA-Z0-9_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'
	@echo
	@echo "One-shot:   make start   (infra + temporal + worker + trigger-api + agent-api)"
	@echo "Then:       make index (once) ; make seed ; make agent-ui"
	@echo "Teardown:   make stop"

# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------

.PHONY: install
install: ## Install Python deps with uv
	uv sync

.env: ## Create .env from the example if missing
	@test -f .env || (cp .env.example .env && echo "created .env — fill in MONGODB_URI, VOYAGE_API_KEY, OPENAI_API_KEY")

.PHONY: check-env
check-env: .env
	@grep -q '^MONGODB_URI=mongodb' .env || echo "WARN: MONGODB_URI not set in .env"
	@grep -qE '^VOYAGE_API_KEY=.+' .env && ! grep -q '^VOYAGE_API_KEY=<' .env || echo "WARN: VOYAGE_API_KEY not set in .env"

.PHONY: setup
setup: check-env install ## Setup Python deps and UI (npm install)
	@echo "setting up UI dependencies..."
	@cd agent/ui && npm install
	@echo "setup complete. Run 'make start' to start all services."

# ---------------------------------------------------------------------------
# Infra (MinIO)
# ---------------------------------------------------------------------------

# --wait only covers long-running services; the *-setup containers are one-shot (exit 0),
# which `docker compose up --wait` would otherwise treat as a failure.
.PHONY: infra-up
infra-up: .env ## Start MinIO (default local ingress: MinIO webhook -> trigger_api /ingest-event)
	@$(COMPOSE) up -d --wait minio
	@$(COMPOSE) up -d minio-setup

.PHONY: infra-down
infra-down: ## Stop infra containers (keep volumes)
	$(COMPOSE) down

.PHONY: infra-clean
infra-clean: ## Stop infra and wipe volumes (MinIO data)
	$(COMPOSE) down -v

.PHONY: infra-logs
infra-logs: ## Tail infra container logs
	$(COMPOSE) logs -f

.PHONY: index
index: check-env ## Create Atlas Vector Search index on the active collection
	$(PY) -m infra.create_atlas_index

# ---------------------------------------------------------------------------
# Long-running processes (foreground) — run each in its own terminal
# ---------------------------------------------------------------------------

.PHONY: temporal
temporal: ## Run the Temporal dev server (foreground; Web UI :8233)
	temporal server start-dev

.PHONY: worker
worker: check-env ## Run the Temporal worker (foreground)
	$(PY) -m pipeline.worker

.PHONY: trigger-api
trigger-api: check-env ## Run the trigger HTTP endpoint (MinIO webhook -> /ingest-event; manual /ingest-trigger)
	$(PY) -m pipeline.trigger_api

.PHONY: agent-api
agent-api: check-env ## Run the deep-agent FastAPI backend
	$(PY) -m agent.api

.PHONY: agent-ui
agent-ui: ## Run the React (Vite) deep-agent UI
	cd agent/ui && npm install && npm run dev

# ---------------------------------------------------------------------------
# One-command start / stop
# ---------------------------------------------------------------------------

.PHONY: start
start: install .env infra-up ## Start everything in the background (infra + temporal + worker + trigger-api + agent-api + agent-ui)
	@mkdir -p $(LOGDIR)
	@if bash -c 'exec 3<>/dev/tcp/127.0.0.1/7233' 2>/dev/null; then \
		echo "temporal: already running on :7233 — reusing it"; \
	else \
		echo "temporal: starting dev server (logs -> $(LOGDIR)/temporal.log)"; \
		nohup temporal server start-dev > $(LOGDIR)/temporal.log 2>&1 & echo $$! > $(LOGDIR)/temporal.pid; \
		until bash -c 'exec 3<>/dev/tcp/127.0.0.1/7233' 2>/dev/null; do sleep 0.5; done; \
	fi
	@$(MAKE) -s _bg NAME=worker CMD="$(PY) -u -m pipeline.worker"
	@$(MAKE) -s _bg NAME=trigger-api CMD="$(PY) -u -m pipeline.trigger_api"
	@$(MAKE) -s _bg NAME=agent-api CMD="$(PY) -u -m agent.api"
	@if [ ! -d agent/ui/node_modules ]; then \
		echo "agent-ui: installing npm dependencies"; \
		npm --prefix agent/ui install; \
	fi
	@$(MAKE) -s _bg NAME=agent-ui CMD="npm --prefix agent/ui run dev -- --host 0.0.0.0"
	@sleep 2
	@echo
	@echo "started. Temporal UI: http://localhost:8233 | Agent UI: http://localhost:5173 | MinIO: http://localhost:9001 | Trigger API: http://localhost:8088"
	@echo "next: 'make index' (once) ; 'make seed'"
	@echo "logs: 'make app-logs'   stop: 'make stop'"

# Internal: background a process with a pidfile + unbuffered logs.
.PHONY: _bg
_bg:
	@echo "$(NAME): starting (logs -> $(LOGDIR)/$(NAME).log)"
	@PYTHONUNBUFFERED=1 nohup $(CMD) > $(LOGDIR)/$(NAME).log 2>&1 & echo $$! > $(LOGDIR)/$(NAME).pid

.PHONY: stop
stop: stop-app ## Stop background app processes, Temporal, and infra
	@-if [ -f $(LOGDIR)/temporal.pid ]; then \
		kill $$(cat $(LOGDIR)/temporal.pid) 2>/dev/null && echo "stopped temporal" || true; \
		rm -f $(LOGDIR)/temporal.pid; \
	fi
	@$(COMPOSE) down

.PHONY: stop-app
stop-app: ## Stop worker + trigger-api + agent-api + agent-ui (leaves Temporal + infra up)
	@-for pat in pipeline.worker pipeline.trigger_api agent.api "agent/ui.*vite"; do \
		pkill -f "$$pat" 2>/dev/null && echo "stopped $$pat" || true; \
	done
	@-for p in worker trigger-api agent-api agent-ui; do \
		if [ -f $(LOGDIR)/$$p.pid ]; then kill $$(cat $(LOGDIR)/$$p.pid) 2>/dev/null || true; rm -f $(LOGDIR)/$$p.pid; fi; \
	done

.PHONY: restart-app
restart-app: stop-app ## Restart app processes (e.g. after editing .env) — leaves infra + Temporal up
	@mkdir -p $(LOGDIR)
	@sleep 1
	@$(MAKE) -s _bg NAME=worker CMD="$(PY) -u -m pipeline.worker"
	@$(MAKE) -s _bg NAME=trigger-api CMD="$(PY) -u -m pipeline.trigger_api"
	@$(MAKE) -s _bg NAME=agent-api CMD="$(PY) -u -m agent.api"
	@if [ ! -d agent/ui/node_modules ]; then \
		echo "agent-ui: installing npm dependencies"; \
		npm --prefix agent/ui install; \
	fi
	@$(MAKE) -s _bg NAME=agent-ui CMD="npm --prefix agent/ui run dev -- --host 0.0.0.0"
	@sleep 2
	@echo "restarted app processes with current .env"

.PHONY: app-logs
app-logs: ## Tail worker + trigger-api + agent-api + agent-ui + temporal logs
	@tail -n +1 -f $(LOGDIR)/worker.log $(LOGDIR)/trigger-api.log $(LOGDIR)/agent-api.log $(LOGDIR)/agent-ui.log $(LOGDIR)/temporal.log 2>/dev/null

# ---------------------------------------------------------------------------
# Drive the pipeline
# ---------------------------------------------------------------------------

.PHONY: seed
seed: check-env ## Upload a file to MinIO to trigger ingestion (defaults to seed/awesome-temporal.md)
	$(PY) -m pipeline.seed $(if $(FILE),--file $(FILE)) $(if $(KEY),--key $(KEY))

.PHONY: seed-docs
seed-docs: check-env ## Clone/update Temporal docs repo, upload only .md/.mdx files, 250ms delay by default
	$(PY) -m pipeline.seed_repo $(if $(REPO_DIR),$(REPO_DIR),) $(if $(REPO_URL),--repo-url $(REPO_URL)) $(if $(CHECKOUT_DIR),--checkout-dir $(CHECKOUT_DIR)) --ref $(REPO_REF) --prefix $(PREFIX) --delay-ms $(DELAY_MS) $(if $(DRY_RUN),--dry-run)

.PHONY: query
query: check-env ## Vector-search the active collection (Q="your question")
	$(PY) -m infra.query_atlas "$(Q)"

.PHONY: backfill
backfill: check-env ## Re-embed into knowledge_v2 with a new model (MODEL=voyage-3-large)
	$(PY) -m pipeline.trigger_backfill --model $(MODEL)

.PHONY: cutover
cutover: check-env ## Flip the active collection/index to the backfilled set (TO=knowledge_v2)
	$(PY) -m pipeline.cutover $(if $(TO),--to $(TO))
