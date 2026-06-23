.PHONY: dev dev-full wait-infra up down logs run run-consumer run-replay build test test-e2e migrate lint tidy swag proto

# =============================================================================
# Smart Dev Experience
# =============================================================================

## dev: Infra + API local (con espera real)
dev:
	@bash scripts/dev-up.sh
	@echo ""
	@echo "🚀 Starting ingest-api locally..."
	@ELASTICSEARCH_ADDRS=http://localhost:9200 \
	 POSTGRES_DSN=postgres://events:events@localhost:5434/events?sslmode=disable \
	 KAFKA_BROKERS=localhost:9094 \
	 go run ./cmd/ingest-api

## dev-consumer: Infra + consumer local
dev-consumer:
	@bash scripts/dev-up.sh
	@echo ""
	@echo "🚀 Starting consumer locally..."
	@ELASTICSEARCH_ADDRS=http://localhost:9200 \
	 KAFKA_BROKERS=localhost:9094 \
	 go run ./cmd/consumer-service

## dev-full: Todo en Docker (modo producción-like)
dev-full:
	docker compose up --build

## wait-infra: Solo levantar infra + esperar
wait-infra:
	@bash scripts/dev-up.sh

# =============================================================================
# Docker stack (infra + servicios)
# =============================================================================

## up: Full stack en Docker
up:
	docker compose up --build -d

## down: Apagar todo
down:
	docker compose down

## logs: Ver logs
logs:
	docker compose logs -f

# =============================================================================
# Local run (manual, sin helpers)
# =============================================================================

run:
	go run ./cmd/ingest-api

run-consumer:
	go run ./cmd/consumer-service

run-replay:
	go run ./cmd/replay-service

# =============================================================================
# Build & tooling
# =============================================================================

build:
	go build -o bin/ingest-api       ./cmd/ingest-api
	go build -o bin/consumer-service ./cmd/consumer-service
	go build -o bin/replay-service   ./cmd/replay-service
	go build -o bin/migrate          ./cmd/migrate

test:
	go test ./...

test-e2e:
	@bash scripts/smoke-test.sh

migrate:
	go run ./cmd/migrate

lint:
	golangci-lint run ./...

tidy:
	go mod tidy

# =============================================================================
# Codegen
# =============================================================================

swag:
	swag init --generalInfo cmd/ingest-api/main.go --output docs --parseDependency --parseInternal

proto:
	mkdir -p gen/proto
	protoc \
		--go_out=gen/proto \
		--go_opt=paths=source_relative \
		--go-grpc_out=gen/proto \
		--go-grpc_opt=paths=source_relative \
		-I proto \
		proto/events.proto

# =============================================================================
# Aliases
# =============================================================================

docker-up:   up
docker-down: down
docker-logs: logs