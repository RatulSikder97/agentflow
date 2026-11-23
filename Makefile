GO        ?= go
PG_URL    ?= postgres://agentflow:agentflow@localhost:55432/agentflow?sslmode=disable
REDIS_URL ?= redis://localhost:56379/0

.PHONY: all build test race lint bench integration services services-down docker examples clean

all: lint race

build:
	$(GO) build -o bin/ ./cmd/...

test:
	$(GO) test ./...

race:
	$(GO) test -race ./...

lint:
	golangci-lint run ./...

bench:
	$(GO) test -run '^$$' -bench . -benchmem ./engine/ ./workflow/

## Start throwaway Postgres + Redis for integration tests.
services:
	docker run -d --rm --name agentflow-test-pg -e POSTGRES_USER=agentflow -e POSTGRES_PASSWORD=agentflow \
		-e POSTGRES_DB=agentflow -p 55432:5432 postgres:17-alpine
	docker run -d --rm --name agentflow-test-redis -p 56379:6379 redis:7-alpine

services-down:
	-docker rm -f agentflow-test-pg agentflow-test-redis

integration:
	AGENTFLOW_TEST_DATABASE_URL="$(PG_URL)" AGENTFLOW_TEST_REDIS_URL="$(REDIS_URL)" \
		$(GO) test -race -tags integration -count=1 ./...

examples:
	$(GO) run ./cmd/afctl run -f examples/research-summarize/workflow.yaml \
		-fixtures examples/fake-llm.yaml -input '{"topic":"small language models"}'
	$(GO) run ./cmd/afctl run -f examples/support-triage/workflow.yaml \
		-fixtures examples/fake-llm.yaml \
		-input '{"ticket_id":"T-1042","subject":"Double charge","body":"I was charged twice, please refund one payment."}'

docker:
	docker build -t agentflow:local .

clean:
	rm -rf bin
