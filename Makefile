GOLANGCI_LINT_VERSION ?= v2.14.0
GOLANGCI_LINT ?= go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)
API_KEY ?= dev-local-key
ORDERS ?= 2000
WORKERS ?= 2

.PHONY: up up-multi down logs ps test test-integration lint fmt tidy build run-api run-worker seed bench loadgen

up:
	docker compose up --build -d

up-multi:
	docker compose up --build -d --scale worker=$(WORKERS)

down:
	docker compose down -v

logs:
	docker compose logs -f --tail=200 api worker

ps:
	docker compose ps

build:
	go build ./...

test:
	go test -race -count=1 ./...

test-integration:
	go test -race -count=1 -tags=integration ./internal/integration/...

lint:
	$(GOLANGCI_LINT) run ./...

fmt:
	$(GOLANGCI_LINT) fmt ./...

tidy:
	go mod tidy

run-api:
	go run ./cmd/api

run-worker:
	go run ./cmd/worker

seed:
	curl -sS -X POST http://localhost:8080/v1/demo/seed \
	  -H 'Content-Type: application/json' \
	  -H 'X-API-Key: $(API_KEY)' \
	  -d '{"count":50}'

loadgen:
	go run ./cmd/loadgen -orders $(ORDERS) -api-key $(API_KEY)

# Micro-benchmarks for the hot pure functions (batch sizing, token bucket).
bench:
	go test -run=^$$ -bench=. -benchmem ./internal/syncer/ ./internal/limiter/
