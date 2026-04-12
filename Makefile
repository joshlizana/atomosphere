.PHONY: up down logs status clean monitor health maintain reset-checkpoint replay smoke-test sizing-start sizing-fetch sizing-report

QUERY_API_URL ?= http://localhost:8000
REPORTS_DIR := reports
SIZING_DOTFILE := $(REPORTS_DIR)/.last-job.json
TIMEOUT ?= 300

up:
	docker compose up -d --build
	@echo "\n=== Waiting for init to complete ==="
	@docker wait init >/dev/null 2>&1 || true
	@docker compose logs init
	@echo "\n=== Service Status ==="
	@docker compose ps -a
	@echo "\n=== Starting monitor daemon (Ctrl-C to stop) ==="
	@exec ./scripts/monitor.sh

down:
	docker compose down --remove-orphans

logs:
	docker compose logs -f

status:
	docker compose ps -a

clean:
	docker compose down -v --remove-orphans
	@docker volume ls -q --filter name=atmosphere_ | xargs -r docker volume rm || true
	@echo "Volumes and containers removed."

monitor:
	./scripts/monitor.sh

health:
	./scripts/monitor.sh --once

maintain:
	@echo "Triggering compaction at $(QUERY_API_URL)/api/maintenance/run ..."
	@response=$$(curl -fsS -X POST $(QUERY_API_URL)/api/maintenance/run -H 'Content-Type: application/json' -d '{}') || { \
		echo "ERROR: failed to POST maintenance run"; exit 1; }; \
	job_id=$$(echo "$$response" | jq -r '.job_id'); \
	if [ -z "$$job_id" ] || [ "$$job_id" = "null" ]; then \
		echo "ERROR: no job_id in response: $$response"; exit 1; \
	fi; \
	echo "Job started: $$job_id"; \
	deadline=$$(( $$(date +%s) + $(TIMEOUT) )); \
	poll=0; \
	start_ts=$$(date +%s); \
	while :; do \
		poll=$$(( poll + 1 )); \
		now_ts=$$(date +%s); \
		elapsed=$$(( now_ts - start_ts )); \
		body=$$(curl -fsS $(QUERY_API_URL)/api/maintenance/run/$$job_id) || { \
			echo "[poll $$poll +$${elapsed}s] ERROR: GET failed"; exit 1; }; \
		status=$$(echo "$$body" | jq -r '.status'); \
		stage=$$(echo "$$body" | jq -r '.progress.stage // "-"'); \
		done_n=$$(echo "$$body" | jq -r '.progress.done // 0'); \
		total_n=$$(echo "$$body" | jq -r '.progress.total // 0'); \
		printf "[poll %2d +%3ss] status=%-7s stage=%-10s progress=%s/%s\n" \
			"$$poll" "$$elapsed" "$$status" "$$stage" "$$done_n" "$$total_n"; \
		if [ "$$status" = "done" ]; then break; fi; \
		if [ "$$status" = "error" ]; then \
			echo "ERROR: job failed: $$(echo "$$body" | jq -r '.error')"; exit 1; \
		fi; \
		if [ $$now_ts -ge $$deadline ]; then \
			echo "ERROR: timeout after $(TIMEOUT)s (last status=$$status)"; exit 1; \
		fi; \
		sleep 5; \
	done; \
	echo ""; \
	echo "$$body" | jq '.result | {targeted_tables, skipped_rate_limited, results: [.results[] | {table, status, elapsed_seconds, files_before, files_after}]}'

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

# --- Sizing analysis ---------------------------------------------------------
# sizing-start  : POST /api/analysis/sizing, write {job_id,status,started_at}
#                 to reports/.last-job.json
# sizing-fetch  : poll GET /api/analysis/sizing/<id> until done (or TIMEOUT),
#                 save pretty JSON to reports/sizing-<UTC timestamp>.json,
#                 update dotfile with status=done + saved_to path
# sizing-report : sizing-start then sizing-fetch (sequential prereqs)

sizing-start:
	@mkdir -p $(REPORTS_DIR)
	@echo "Starting sizing job at $(QUERY_API_URL)/api/analysis/sizing ..."
	@response=$$(curl -fsS -X POST $(QUERY_API_URL)/api/analysis/sizing) || { \
		echo "ERROR: failed to POST sizing job"; exit 1; }; \
	job_id=$$(echo "$$response" | jq -r '.job_id'); \
	if [ -z "$$job_id" ] || [ "$$job_id" = "null" ]; then \
		echo "ERROR: no job_id in response: $$response"; exit 1; \
	fi; \
	started_at=$$(date -u +%Y-%m-%dT%H:%M:%SZ); \
	jq -n --arg id "$$job_id" --arg ts "$$started_at" \
		'{job_id:$$id, status:"running", started_at:$$ts}' > $(SIZING_DOTFILE); \
	echo "Job started: $$job_id"; \
	echo "Dotfile: $(SIZING_DOTFILE)"

sizing-fetch:
	@if [ ! -f $(SIZING_DOTFILE) ]; then \
		echo "ERROR: $(SIZING_DOTFILE) not found. Run 'make sizing-start' first."; \
		exit 1; \
	fi
	@job_id=$$(jq -r '.job_id' $(SIZING_DOTFILE)); \
	if [ -z "$$job_id" ] || [ "$$job_id" = "null" ]; then \
		echo "ERROR: no job_id in $(SIZING_DOTFILE)"; exit 1; \
	fi; \
	echo "Polling job $$job_id (timeout $(TIMEOUT)s) ..."; \
	deadline=$$(( $$(date +%s) + $(TIMEOUT) )); \
	poll=0; \
	start_ts=$$(date +%s); \
	while :; do \
		poll=$$(( poll + 1 )); \
		now_ts=$$(date +%s); \
		elapsed=$$(( now_ts - start_ts )); \
		now_iso=$$(date -u +%H:%M:%SZ); \
		body=$$(curl -fsS $(QUERY_API_URL)/api/analysis/sizing/$$job_id) || { \
			echo "[poll $$poll @ $$now_iso +$${elapsed}s] ERROR: GET failed"; exit 1; }; \
		status=$$(echo "$$body" | jq -r '.status'); \
		stage=$$(echo "$$body" | jq -r '.progress.stage // "-"'); \
		done_n=$$(echo "$$body" | jq -r '.progress.done // 0'); \
		total_n=$$(echo "$$body" | jq -r '.progress.total // 0'); \
		printf "[poll %2d @ %s +%3ss] status=%-7s stage=%-12s progress=%s/%s\n" \
			"$$poll" "$$now_iso" "$$elapsed" "$$status" "$$stage" "$$done_n" "$$total_n"; \
		if [ "$$status" = "done" ]; then break; fi; \
		if [ "$$status" = "error" ]; then \
			echo "ERROR: job failed: $$(echo "$$body" | jq -r '.error')"; exit 1; \
		fi; \
		if [ $$now_ts -ge $$deadline ]; then \
			echo "ERROR: timeout after $(TIMEOUT)s (last status=$$status)"; \
			exit 1; \
		fi; \
		sleep 5; \
	done; \
	ts=$$(date -u +%Y%m%d-%H%M%S); \
	out=$(REPORTS_DIR)/sizing-$$ts.json; \
	echo "$$body" | jq '.result' > $$out; \
	jq --arg path "$$out" --arg ts "$$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
		'. + {status:"done", saved_to:$$path, fetched_at:$$ts}' \
		$(SIZING_DOTFILE) > $(SIZING_DOTFILE).tmp && \
		mv $(SIZING_DOTFILE).tmp $(SIZING_DOTFILE); \
	echo "Saved $$out"

sizing-report: sizing-start sizing-fetch
