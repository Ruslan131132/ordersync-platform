.PHONY: up down logs test lint tidy run-api run-worker seed bench-load fmt

export PATH := $(shell brew --prefix 2>/dev/null)/bin:$(PATH)

up:
	docker compose up --build -d

up-multi:
	docker compose --profile multi up --build -d

down:
	docker compose down -v

logs:
	docker compose logs -f --tail=200

tidy:
	go mod tidy

fmt:
	go fmt ./...

test:
	go test ./... -count=1

test-integration:
	go test ./... -count=1 -tags=integration

lint:
	@command -v golangci-lint >/dev/null 2>&1 || { echo "install golangci-lint: https://golangci-lint.run"; exit 1; }
	golangci-lint run ./...

run-api:
	go run ./cmd/api

run-worker:
	go run ./cmd/worker

seed:
	curl -sS -X POST http://localhost:8080/v1/demo/seed \
	  -H 'Content-Type: application/json' \
	  -H 'X-API-Key: dev-local-key' \
	  -d '{"count":50}' | jq .

bench-load:
	@echo "Seeding 200 orders and triggering sync..."
	curl -sS -X POST http://localhost:8080/v1/sync/trigger \
	  -H 'Content-Type: application/json' \
	  -H 'X-API-Key: dev-local-key' \
	  -d '{"seed":200}' | jq .
	@echo "Watch: curl -s localhost:8080/metrics | grep ordersync_"
	@echo "Prometheus UI: http://localhost:9090"
