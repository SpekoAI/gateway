.PHONY: build test race vet check

build:
	mkdir -p bin
	go build -trimpath -o bin/speko-gateway ./cmd/speko-gateway

test:
	go test ./...

race:
	go test -race ./...

vet:
	go vet ./...

check: vet test race
