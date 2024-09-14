.PHONY: build test test-race integration-test lint proto migrate docker-up docker-down load-test benchmark
build:
	go build ./cmd/...
test:
	go test ./...
test-race:
	go test -race ./...
integration-test:
	go test -race -tags=integration -count=1 ./tests/...
lint:
	go vet ./...
	golangci-lint run
proto:
	protoc --go_out=. --go_opt=module=jobplatform --go-grpc_out=. --go-grpc_opt=module=jobplatform proto/worker/v1/worker.proto
migrate:
	docker compose run --rm migrate
docker-up:
	docker compose up --build -d
docker-down:
	docker compose down
load-test:
	docker run --rm --network job-platform_default -v $(CURDIR)/load-tests:/scripts grafana/k6:1.3.0 run /scripts/create.js
benchmark:
	go test -run=^$$ -bench=. -benchmem ./internal/...
