.PHONY: build test fmt vet run bench cluster-up cluster-down failover

build:
	go build -o bin/kvnode ./cmd/kvnode
	go build -o bin/kvbench ./cmd/kvbench

test:
	go test -race ./...

fmt:
	gofmt -w $$(find . -name '*.go' -not -path './vendor/*')

vet:
	go vet ./...

run:
	go run ./cmd/kvnode

bench:
	go run ./cmd/kvbench -target http://127.0.0.1:8081 -concurrency 50 -duration 10s

cluster-up:
	docker compose up --detach --build

cluster-down:
	docker compose down

failover:
	./scripts/failover-demo.sh
