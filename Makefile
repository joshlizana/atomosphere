.PHONY: up down logs status clean monitor monitor-loop

up:
	docker compose up -d --build
	@echo "\n=== Waiting for services ==="
	@docker compose logs -f init 2>/dev/null || true
	@echo "\n=== Service Status ==="
	@docker compose ps -a

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
