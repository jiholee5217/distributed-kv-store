.PHONY: build test fmt vet run

build:
	go build -o bin/kvnode ./cmd/kvnode

test:
	go test -race ./...

fmt:
	gofmt -w $$(find . -name '*.go' -not -path './vendor/*')

vet:
	go vet ./...

run:
	go run ./cmd/kvnode
