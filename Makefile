VERSION := $(shell git describe --tags --always --dirty)

all: build

version:
	@echo $(VERSION)

build: version
	go test -cover ./...
	go build -v ./example/sample.go

run: build
	./sample