VERSION := $(shell git describe --tags --always --dirty)

all: build

version:
	@echo $(VERSION)

clean:
	go clean -i ./...

vet:
	go vet ./...

test: vet
	go test -race -cover ./...

build: test
	go build -v -o sample ./examples/first/

vuln:
	go run golang.org/x/vuln/cmd/govulncheck@latest ./...

update:
	go get -u ./...

run: build
	./sample
