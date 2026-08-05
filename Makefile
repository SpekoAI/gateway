.PHONY: build test test-python race vet check check-python

build:
	mkdir -p bin
	go build -trimpath -o bin/speko-gateway ./cmd/speko-gateway

test:
	go test ./...

test-python:
	cd integrations/python && uv run --locked --extra dev pytest

check-python:
	cd integrations/python && uv run --locked --extra dev ruff check .
	cd integrations/python && uv run --locked --extra dev pytest

race:
	go test -race ./...

vet:
	go vet ./...

check: vet test race check-python
