.PHONY: up down logs status clean monitor monitor-loop maintain reset-checkpoint replay

up:
	docker compose up -d --build
	@echo "\n=== Waiting for services ==="
	@docker compose logs -f init 2>/dev/null || true
	@echo "\n=== Service Status ==="
	@docker compose ps -a
	@echo "\n=== Starting monitor ==="
	@./scripts/monitor.sh --loop &

down:
	docker compose down -v --remove-orphans

logs:
	docker compose logs -f

status:
	docker compose ps -a

clean:
	docker compose down -v --remove-orphans
	@echo "Volumes and containers removed."

monitor:
	./scripts/monitor.sh

monitor-loop:
	./scripts/monitor.sh --loop

maintain:
	./scripts/maintain.sh

reset-checkpoint:
	@echo "Usage: make reset-checkpoint SVC=<service>"
	@echo "  e.g. make reset-checkpoint SVC=spark-staging"
	@test -n "$(SVC)" && ./scripts/reset-checkpoint.sh $(SVC) || true

replay:
	@echo "Usage: make replay TIME=<timestamp>"
	@echo "  e.g. make replay TIME=-1h"
	@test -n "$(TIME)" && ./scripts/replay.sh $(TIME) || true
