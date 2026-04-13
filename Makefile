.PHONY: up down logs status clean smoke-test reset-checkpoint replay heimdall heimdall-run heimdall-watch

up:
	@# Inject a Jetstream cursor 1h in the past so fresh starts replay
	@# enough backlog to exercise the pipeline under sustained load. On
	@# restart (checkpoint already in the spark-checkpoints volume), the
	@# checkpoint wins and this env cursor is ignored — see the resolution
	@# order in spark/sources/jetstream_source.py.
	JETSTREAM_CURSOR=$$(( ($$(date +%s) - 3600) * 1000000 )) \
		docker compose up -d --build
	@echo "\n=== Waiting for init to complete ==="
	@docker wait init >/dev/null 2>&1 || true
	@docker compose logs init
	@echo "\n=== Service Status ==="
	@docker compose ps -a

down:
	docker compose down --remove-orphans

logs:
	docker compose logs -f

status:
	docker compose ps -a

clean:
	docker compose down -v --remove-orphans
	@echo "Volumes and containers removed."

reset-checkpoint:
	@echo "Usage: make reset-checkpoint LAYER=<layer>"
	@echo "  e.g. make reset-checkpoint LAYER=staging"
	@test -n "$(LAYER)" && ./scripts/reset-checkpoint.sh $(LAYER) || true

replay:
	@echo "Usage: make replay TIME=<timestamp>"
	@echo "  e.g. make replay TIME=-1h"
	@test -n "$(TIME)" && ./scripts/replay.sh $(TIME) || true

smoke-test:
	./scripts/smoke-test.sh --wait

heimdall:
	@.venv/bin/python -m scripts.heimdall tui

heimdall-run:
	@.venv/bin/python -m scripts.heimdall run

heimdall-watch:
	@.venv/bin/python -m scripts.heimdall watch
