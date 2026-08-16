GOCACHE ?= /tmp/distributed-kv-go-cache

.PHONY: build test fmt vet verify run bench cluster-up cluster-down failover failover-load

build:
	GOCACHE=$(GOCACHE) go build -o bin/kvnode ./cmd/kvnode
	GOCACHE=$(GOCACHE) go build -o bin/kvbench ./cmd/kvbench

test:
	GOCACHE=$(GOCACHE) go test -race ./...

fmt:
	gofmt -w $$(find . -name '*.go' -not -path './vendor/*')

vet:
	GOCACHE=$(GOCACHE) go vet ./...

verify: test vet
	GOCACHE=$(GOCACHE) go build -o /tmp/distributed-kv-node ./cmd/kvnode
	GOCACHE=$(GOCACHE) go build -o /tmp/distributed-kv-bench ./cmd/kvbench
	test -z "$$(gofmt -l cmd internal)"
	docker compose config --quiet
	git diff --check

run:
	go run ./cmd/kvnode

bench:
	./scripts/benchmark.sh

cluster-up:
	docker compose up --detach --build

cluster-down:
	docker compose down

failover:
	./scripts/failover-demo.sh

failover-load:
	./scripts/failover-load.sh
