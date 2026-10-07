.PHONY: help install dev-install setup-hooks format lint type-check security test test-api test-cli test-cov clean run ci-check openapi e2e-dev e2e-prod e2e-auth e2e-both e2e-run-limits e2e-ttl-delta e2e-enqueue-local e2e-enqueue-worker e2e-enqueue-both

help:
	@echo "Available commands:"
	@echo "  make install       - Install production dependencies"
	@echo "  make dev-install   - Install all dependencies + git hooks"
	@echo "  make setup-hooks   - Reinstall git hooks (if needed)"
	@echo "  make format        - Format code with ruff"
	@echo "  make lint          - Lint code with ruff"
	@echo "  make type-check    - Run ty type checking"
	@echo "  make security      - Run security checks with bandit"
	@echo "  make test          - Run all tests"
	@echo "  make test-api      - Run aegra-api tests only"
	@echo "  make test-cli      - Run aegra-cli tests only"
	@echo "  make test-cov      - Run tests with coverage"
	@echo "  make openapi       - Regenerate docs/openapi.json from code"
	@echo "  make ci-check      - Run all CI checks locally"
	@echo "  make e2e-dev       - Run E2E tests in dev mode (no Redis)"
	@echo "  make e2e-prod      - Run E2E tests in prod mode (Redis workers)"
	@echo "  make e2e-auth      - Run auth E2E tests (JWT mock auth enabled)"
	@echo "  make e2e-both      - Run E2E tests in both modes"
	@echo "  make e2e-run-limits - Run per-org run-limit E2E tests (add REDIS=1 for worker mode)"
	@echo "  make e2e-ttl-delta - Run thread-TTL keep_latest / DeltaChannel guard E2E tests"
	@echo "  make e2e-enqueue-both - Run enqueue FIFO E2E tests in local and worker modes"
	@echo "  make clean         - Clean cache files"
	@echo "  make run           - Run the server"

install:
	uv sync --all-packages --no-dev

dev-install:
	uv sync --all-packages
	@uv run pre-commit install
	@uv run pre-commit install --hook-type commit-msg
	@echo ""
	@echo "Done! Dependencies installed and git hooks set up."

setup-hooks:
	uv run pre-commit install
	uv run pre-commit install --hook-type commit-msg
	@echo ""
	@echo "Git hooks reinstalled!"

format:
	uv run ruff format .
	uv run ruff check --fix .

lint:
	uv run ruff check .

type-check:
	uv run ty check libs/aegra-api/src/ libs/aegra-cli/src/

security:
	uv run bandit -c pyproject.toml -r libs/aegra-api/src/ libs/aegra-cli/src/

test: test-api test-cli

test-api:
	uv run --package aegra-api pytest libs/aegra-api/tests/

test-cli:
	uv run --package aegra-cli pytest libs/aegra-cli/tests/

test-cov:
	uv run --package aegra-api pytest libs/aegra-api/tests/ --cov=libs/aegra-api/src --cov-report=html --cov-report=term
	uv run --package aegra-cli pytest libs/aegra-cli/tests/ --cov=libs/aegra-cli/src --cov-report=term

openapi:
	uv run --package aegra-api python scripts/export_openapi.py

ci-check: format lint
	-uv run ty check libs/aegra-api/src/ libs/aegra-cli/src/
	-uv run bandit -c pyproject.toml -r libs/aegra-api/src/ libs/aegra-cli/src/
	$(MAKE) test
	@echo ""
	@echo "All CI checks completed! (ty and bandit are non-blocking)"

E2E_IGNORE := --ignore=libs/aegra-api/tests/e2e/manual_auth_tests --ignore=libs/aegra-api/tests/e2e/multi_instance

e2e-dev:
	@echo "Starting dev mode (LocalExecutor, no Redis)..."
	@docker compose -f docker-compose.yml -f docker-compose.dev.yml up -d
	@echo "Waiting for server..."; \
	ready=0; \
	for i in $$(seq 1 30); do \
		if curl -s http://localhost:2026/health > /dev/null 2>&1; then ready=1; break; fi; \
		sleep 2; \
	done; \
	if [ "$$ready" = "0" ]; then echo "Server failed to start within 60s"; docker compose -f docker-compose.yml -f docker-compose.dev.yml down; exit 1; fi
	@rc=0; \
	uv run --package aegra-api pytest libs/aegra-api/tests/e2e/ -m "not prod_only" $(E2E_IGNORE) -v --tb=short || rc=$$?; \
	docker compose -f docker-compose.yml -f docker-compose.dev.yml down; \
	exit $$rc

e2e-prod:
	@echo "Starting prod mode (WorkerExecutor + Redis)..."
	@docker compose up -d
	@echo "Waiting for server..."; \
	ready=0; \
	for i in $$(seq 1 30); do \
		if curl -s http://localhost:2026/health > /dev/null 2>&1; then ready=1; break; fi; \
		sleep 2; \
	done; \
	if [ "$$ready" = "0" ]; then echo "Server failed to start within 60s"; docker compose down; exit 1; fi
	@rc=0; \
	uv run --package aegra-api pytest libs/aegra-api/tests/e2e/ $(E2E_IGNORE) -v --tb=short || rc=$$?; \
	docker compose down; \
	exit $$rc

e2e-auth:
	@echo "Starting auth mode (JWT mock auth, LocalExecutor)..."
	@docker compose -f docker-compose.yml -f docker-compose.dev.yml -f docker-compose.auth.yml up -d postgres aegra
	@echo "Waiting for server..."; \
	ready=0; \
	for i in $$(seq 1 45); do \
		if curl -s http://localhost:2026/health > /dev/null 2>&1; then ready=1; break; fi; \
		sleep 2; \
	done; \
	if [ "$$ready" = "0" ]; then \
		echo "Server failed to start within 90s"; \
		docker compose -f docker-compose.yml -f docker-compose.dev.yml -f docker-compose.auth.yml logs --tail=80; \
		docker compose -f docker-compose.yml -f docker-compose.dev.yml -f docker-compose.auth.yml down; \
		exit 1; \
	fi
	@rc=0; \
	uv run --package aegra-api pytest libs/aegra-api/tests/e2e/manual_auth_tests/ -v --tb=short -m auth_only -o addopts="--strict-markers -ra --color=yes" || rc=$$?; \
	docker compose -f docker-compose.yml -f docker-compose.dev.yml -f docker-compose.auth.yml down; \
	exit $$rc

e2e-both: e2e-dev e2e-prod

# Per-org run limits need a bespoke server: a low ceiling plus a graph slow
# enough to observe queueing. Runs uvicorn directly against the compose
# Postgres — no image rebuild, and it exercises both executor modes.
# Usage: make e2e-run-limits            (dev mode, LocalExecutor)
#        make e2e-run-limits REDIS=1    (prod mode, WorkerExecutor)
RUN_LIMITS_HARNESS := libs/aegra-api/tests/e2e/harness/run_limits
RUN_LIMITS_PORT := 2029
RUN_LIMITS_CEILING := 2

TTL_DELTA_HARNESS := libs/aegra-api/tests/e2e/harness/thread_ttl
TTL_DELTA_PORT := 2030

ENQUEUE_HARNESS := libs/aegra-api/tests/e2e/harness/enqueue
ENQUEUE_TEST := libs/aegra-api/tests/e2e/test_runs/test_multitask_enqueue.py
ENQUEUE_LOCAL_PORT := 2031
ENQUEUE_WORKER_PORT := 2032

e2e-run-limits:
	@docker compose up -d postgres $(if $(REDIS),redis,)
	@echo "Waiting for Postgres..."; \
	for i in $$(seq 1 30); do \
		docker compose exec -T postgres pg_isready -U user -d aegra > /dev/null 2>&1 && break; \
		sleep 2; \
	done
	@set -e; \
	export DATABASE_URL=postgresql://user:password@localhost:5434/aegra; \
	export AEGRA_CONFIG=$(PWD)/$(RUN_LIMITS_HARNESS)/aegra.json; \
	export SERVER_URL=http://127.0.0.1:$(RUN_LIMITS_PORT); \
	export AUTH_TYPE=noop BACKFILL_THREAD_STATE_ON_STARTUP=false; \
	export ORG_RUN_LIMIT_MODE=enforce ORG_MAX_CONCURRENT_RUNS=$(RUN_LIMITS_CEILING); \
	export ORG_RUN_PROMOTER_INTERVAL_SECONDS=1 SLEEP_GRAPH_SECONDS=6; \
	export AEGRA_E2E_ORG_RUN_LIMIT=$(RUN_LIMITS_CEILING); \
	if [ -n "$(REDIS)" ]; then \
		export REDIS_BROKER_ENABLED=true WORKER_COUNT=2 N_JOBS_PER_WORKER=10; \
		export REDIS_URL=redis://localhost:$$(docker compose port redis 6379 | cut -d: -f2)/3; \
	else \
		export REDIS_BROKER_ENABLED=false; \
	fi; \
	uv run --package aegra-api uvicorn aegra_api.main:app \
		--host 127.0.0.1 --port $(RUN_LIMITS_PORT) > /tmp/aegra-run-limits.log 2>&1 & \
	echo $$! > /tmp/aegra-run-limits.pid; \
	for i in $$(seq 1 30); do \
		curl -sf http://127.0.0.1:$(RUN_LIMITS_PORT)/health > /dev/null 2>&1 && break; \
		sleep 2; \
	done; \
	rc=0; \
	SERVER_URL=http://127.0.0.1:$(RUN_LIMITS_PORT) AEGRA_E2E_ORG_RUN_LIMIT=$(RUN_LIMITS_CEILING) \
		uv run --package aegra-api pytest libs/aegra-api/tests/e2e/test_runs/test_org_run_limits.py -v --tb=short || rc=$$?; \
	kill $$(cat /tmp/aegra-run-limits.pid) 2>/dev/null || true; \
	rm -f /tmp/aegra-run-limits.pid; \
	exit $$rc

e2e-ttl-delta:
	@docker compose up -d postgres
	@echo "Waiting for Postgres..."; \
	for i in $$(seq 1 30); do \
		docker compose exec -T postgres pg_isready -U user -d aegra > /dev/null 2>&1 && break; \
		sleep 2; \
	done
	@set -e; \
	export DATABASE_URL=postgresql://user:password@localhost:5434/aegra; \
	export AEGRA_E2E_DSN=postgresql://user:password@localhost:5434/aegra; \
	export AEGRA_CONFIG=$(PWD)/$(TTL_DELTA_HARNESS)/aegra.json; \
	export SERVER_URL=http://127.0.0.1:$(TTL_DELTA_PORT); \
	export AUTH_TYPE=noop BACKFILL_THREAD_STATE_ON_STARTUP=false; \
	export REDIS_BROKER_ENABLED=false; \
	uv run --package aegra-api uvicorn aegra_api.main:app \
		--host 127.0.0.1 --port $(TTL_DELTA_PORT) > /tmp/aegra-ttl-delta.log 2>&1 & \
	echo $$! > /tmp/aegra-ttl-delta.pid; \
	for i in $$(seq 1 30); do \
		curl -sf http://127.0.0.1:$(TTL_DELTA_PORT)/health > /dev/null 2>&1 && break; \
		sleep 2; \
	done; \
	rc=0; \
	SERVER_URL=http://127.0.0.1:$(TTL_DELTA_PORT) \
	AEGRA_E2E_DSN=postgresql://user:password@localhost:5434/aegra \
		uv run --package aegra-api pytest \
		libs/aegra-api/tests/e2e/test_threads/test_thread_ttl_delta_guard.py -v --tb=short || rc=$$?; \
	kill $$(cat /tmp/aegra-ttl-delta.pid) 2>/dev/null || true; \
	rm -f /tmp/aegra-ttl-delta.pid; \
	exit $$rc

e2e-enqueue-local:
	@docker compose up -d postgres
	@set -e; \
	export DATABASE_URL=postgresql://user:password@localhost:5434/aegra; \
	export AEGRA_CONFIG=$(PWD)/$(ENQUEUE_HARNESS)/aegra.json; \
	export SERVER_URL=http://127.0.0.1:$(ENQUEUE_LOCAL_PORT); \
	export AUTH_TYPE=noop BACKFILL_THREAD_STATE_ON_STARTUP=false; \
	export REDIS_BROKER_ENABLED=false ORG_RUN_LIMIT_MODE=enforce; \
	export ORG_MAX_CONCURRENT_RUNS=2 ORG_RUN_PROMOTER_INTERVAL_SECONDS=0.2; \
	cleanup() { kill $$server_pid 2>/dev/null || true; }; \
	trap cleanup EXIT INT TERM; \
	.venv/bin/python -m uvicorn aegra_api.main:app \
		--host 127.0.0.1 --port $(ENQUEUE_LOCAL_PORT) \
		> /tmp/aegra-enqueue-local.log 2>&1 & server_pid=$$!; \
	for i in $$(seq 1 45); do \
		curl -sf http://127.0.0.1:$(ENQUEUE_LOCAL_PORT)/health > /dev/null 2>&1 && break; \
		sleep 2; \
	done; \
	curl -sf http://127.0.0.1:$(ENQUEUE_LOCAL_PORT)/health > /dev/null; \
	AEGRA_E2E_ENQUEUE=1 SERVER_URL=http://127.0.0.1:$(ENQUEUE_LOCAL_PORT) \
		.venv/bin/pytest $(ENQUEUE_TEST) -v --tb=short

e2e-enqueue-worker:
	@docker compose up -d postgres redis
	@set -e; \
	export DATABASE_URL=postgresql://user:password@localhost:5434/aegra; \
	export AEGRA_CONFIG=$(PWD)/$(ENQUEUE_HARNESS)/aegra.json; \
	export SERVER_URL=http://127.0.0.1:$(ENQUEUE_WORKER_PORT); \
	export AUTH_TYPE=noop BACKFILL_THREAD_STATE_ON_STARTUP=false; \
	export REDIS_BROKER_ENABLED=true WORKER_COUNT=2 N_JOBS_PER_WORKER=10; \
	export REDIS_URL=redis://localhost:$$(docker compose port redis 6379 | cut -d: -f2)/4; \
	export ORG_RUN_LIMIT_MODE=enforce ORG_MAX_CONCURRENT_RUNS=2; \
	export ORG_RUN_PROMOTER_INTERVAL_SECONDS=0.2; \
	cleanup() { kill $$server_pid 2>/dev/null || true; }; \
	trap cleanup EXIT INT TERM; \
	.venv/bin/python -m uvicorn aegra_api.main:app \
		--host 127.0.0.1 --port $(ENQUEUE_WORKER_PORT) \
		> /tmp/aegra-enqueue-worker.log 2>&1 & server_pid=$$!; \
	for i in $$(seq 1 45); do \
		curl -sf http://127.0.0.1:$(ENQUEUE_WORKER_PORT)/health > /dev/null 2>&1 && break; \
		sleep 2; \
	done; \
	curl -sf http://127.0.0.1:$(ENQUEUE_WORKER_PORT)/health > /dev/null; \
	AEGRA_E2E_ENQUEUE=1 SERVER_URL=http://127.0.0.1:$(ENQUEUE_WORKER_PORT) \
		.venv/bin/pytest $(ENQUEUE_TEST) -v --tb=short

e2e-enqueue-both: e2e-enqueue-local e2e-enqueue-worker

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true
	rm -rf .pytest_cache .ty_cache .ruff_cache htmlcov 2>/dev/null || true

run:
	uv run --package aegra-api uvicorn aegra_api.main:app --reload
