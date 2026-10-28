.PHONY: run test race vet build load-test

run:
	go run ./cmd/queueforge

test:
	go test ./...

race:
	go test -race ./...

vet:
	go vet ./...

build:
	go build -o bin/queueforge ./cmd/queueforge

load-test:
	sh scripts/load-test.sh
