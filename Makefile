BIN_FOLDER := bin
VERSION=$(shell git describe --tags --dirty)

SINKER_LDFLAGS += -s -v -w
SINKER_LDFLAGS += -X "main.version=$(VERSION)"
SINKER_LDFLAGS += -X "main.date=$(shell date --iso-8601=s)"
SINKER_LDFLAGS += -X "main.commit=$(shell git rev-parse HEAD)"
SINKER_LDFLAGS += -X "main.builtBy=$(shell echo `whoami`@`hostname`)"
SINKER_LDFLAGS += -checklinkname=0

GOBUILD := CGO_ENABLED=1 go build $(BUILD_FLAG)

.PHONY: vendor
vendor:
	$(V)go mod tidy
	$(V)go mod vendor
	$(V)git add vendor go.mod go.sum

.PHONY: build
build: vendor
	$(GOBUILD) -ldflags '$(SINKER_LDFLAGS)' -o ${BIN_FOLDER}/ ./...

.PHONY: debug
debug: vendor
	$(GOBUILD) -ldflags '$(SINKER_LDFLAGS)' -gcflags "all=-N -l" -o ${BIN_FOLDER}/ ./...

.PHONY: benchtest
benchtest: vendor
	go test -bench=. ./...

.PHONY: systest
systest: build
	bash go.test.sh
	bash go.metrictest.sh

.PHONY: gotest
gotest: vendor
	go test -v ./... -coverprofile=coverage.out -covermode count
	go tool cover -func coverage.out

.PHONY: lint
lint:
	golangci-lint run -D errcheck,govet,gosimple

.PHONY: run
run: vendor
	go run cmd/clickhouse_sinker/main.go --local-cfg-file docker/test_dynamic_schema.hjson

.PHONY: generate
generate:
	buf generate
	go generate -x ./...
