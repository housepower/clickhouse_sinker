BIN_FOLDER := bin

SINKER_LDFLAGS += -s -v -w
SINKER_LDFLAGS += -X "main.version=$(shell git describe --tags --dirty)"
SINKER_LDFLAGS += -X "main.date=$(shell date --iso-8601=s)"
SINKER_LDFLAGS += -X "main.commit=$(shell git rev-parse HEAD)"
SINKER_LDFLAGS += -X "main.builtBy=$(shell echo `whoami`@`hostname`)"

GOBUILD := CGO_ENABLED=1 go build $(BUILD_FLAG)

.PHONY: vendor
vendor:
	$(V)go mod tidy -compat=1.19
	$(V)go mod vendor
	$(V)git add vendor go.mod go.sum

build: vendor
	$(GOBUILD) -ldflags '$(SINKER_LDFLAGS)' -o ${BIN_FOLDER}/clickhouse_sinker cmd/clickhouse_sinker/main.go
	$(GOBUILD) -ldflags '$(SINKER_LDFLAGS)' -o ${BIN_FOLDER}/nacos_publish_config cmd/nacos_publish_config/main.go
	$(GOBUILD) -ldflags '$(SINKER_LDFLAGS)' -o ${BIN_FOLDER}/kafka_gen_log cmd/kafka_gen_log/main.go
	$(GOBUILD) -ldflags '$(SINKER_LDFLAGS)' -o ${BIN_FOLDER}/kafka_gen_metric cmd/kafka_gen_metric/main.go
	$(GOBUILD) -ldflags '$(SINKER_LDFLAGS)' -o ${BIN_FOLDER}/kafka_gen_prom cmd/kafka_gen_prom/main.go

debug: vendor
	$(GOBUILD) -ldflags '$(SINKER_LDFLAGS)' -gcflags "all=-N -l" -o ${BIN_FOLDER}/clickhouse_sinker cmd/clickhouse_sinker/main.go
	$(GOBUILD) -ldflags '$(SINKER_LDFLAGS)' -gcflags "all=-N -l" -o ${BIN_FOLDER}/nacos_publish_config cmd/nacos_publish_config/main.go
	$(GOBUILD) -ldflags '$(SINKER_LDFLAGS)' -gcflags "all=-N -l" -o ${BIN_FOLDER}/kafka_gen_log cmd/kafka_gen_log/main.go
	$(GOBUILD) -ldflags '$(SINKER_LDFLAGS)' -gcflags "all=-N -l" -o ${BIN_FOLDER}/kafka_gen_metric cmd/kafka_gen_metric/main.go
	$(GOBUILD) -ldflags '$(SINKER_LDFLAGS)' -gcflags "all=-N -l" -o ${BIN_FOLDER}/kafka_gen_prom cmd/kafka_gen_prom/main.go

unittest: vendor
	go test -v ./...

benchtest: vendor
	go test -bench=. ./...

systest: build
	bash go.test.sh

lint:
	golangci-lint run --timeout=3m

run: vendor
	go run cmd/clickhouse_sinker/main.go --local-cfg-file docker/test_dynamic_schema.json

generate:
	buf generate
	go generate -x ./...
