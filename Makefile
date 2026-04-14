.PHONY: up down logs status clean smoke-test reset-checkpoint heimdall heimdall-run heimdall-watch

up:
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

smoke-test:
	./scripts/smoke-test.sh --wait

heimdall:
	@.venv/bin/python -m scripts.heimdall tui

heimdall-run:
	@.venv/bin/python -m scripts.heimdall run

heimdall-watch:
	@.venv/bin/python -m scripts.heimdall watch
