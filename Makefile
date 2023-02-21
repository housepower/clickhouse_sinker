SINKER_LDFLAGS += -X "main.version=$(shell git describe --tags --dirty)"
SINKER_LDFLAGS += -X "main.date=$(shell date --iso-8601=s)"
SINKER_LDFLAGS += -X "main.commit=$(shell git rev-parse HEAD)"
SINKER_LDFLAGS += -X "main.builtBy=$(shell echo `whoami`@`hostname`)"

GO        := CGO_ENABLED=0 go
GOBUILD   := $(GO) build $(BUILD_FLAG)

pre:
	go mod tidy
build: pre
	$(GOBUILD) -ldflags '$(SINKER_LDFLAGS)' -o . ./...
debug: pre
	$(GOBUILD) -ldflags '$(SINKER_LDFLAGS)' -gcflags "all=-N -l" -o . ./...
unittest: pre
	go test -v ./...
benchtest: pre
	go test -bench=. ./...
systest: build
	bash go.test.sh
	bash go.metrictest.sh
lint:
	golangci-lint run --timeout=3m
run: pre
	go run cmd/clickhouse_sinker/main.go --local-cfg-file docker/test_dynamic_schema.hjson
