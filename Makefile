SINKER_LDFLAGS += -X "main.version=$(shell git describe --tags --dirty)"
SINKER_LDFLAGS += -X "main.date=$(shell date --iso-8601=s)"
SINKER_LDFLAGS += -X "main.commit=$(shell git rev-parse HEAD)"
SINKER_LDFLAGS += -X "main.builtBy=$(shell echo `whoami`@`hostname`)"

GO        := CGO_ENABLED=0 go
GOBUILD   := $(GO) build $(BUILD_FLAG)

pre:
	go mod tidy
build: pre
	$(GOBUILD) -ldflags '$(SINKER_LDFLAGS)' -o clickhouse_sinker cmd/clickhouse_sinker/main.go
	$(GOBUILD) -ldflags '$(SINKER_LDFLAGS)' -o nacos_publish_config cmd/nacos_publish_config/main.go
	$(GOBUILD) -ldflags '$(SINKER_LDFLAGS)' -o kafka_gen_log cmd/kafka_gen_log/main.go
	$(GOBUILD) -ldflags '$(SINKER_LDFLAGS)' -o kafka_gen_metric cmd/kafka_gen_metric/main.go
debug: pre
	$(GOBUILD) -ldflags '$(SINKER_LDFLAGS)' -gcflags "all=-N -l" -o clickhouse_sinker cmd/clickhouse_sinker/main.go
	$(GOBUILD) -ldflags '$(SINKER_LDFLAGS)' -gcflags "all=-N -l" -o nacos_publish_config cmd/nacos_publish_config/main.go
	$(GOBUILD) -ldflags '$(SINKER_LDFLAGS)' -gcflags "all=-N -l" -o kafka_gen_log cmd/kafka_gen_log/main.go
	$(GOBUILD) -ldflags '$(SINKER_LDFLAGS)' -gcflags "all=-N -l" -o kafka_gen_metric cmd/kafka_gen_metric/main.go
unittest: pre
	go test -v ./...
benchtest: pre
	go test -bench=. ./...
systest: build
	bash go.test.sh
lint:
	golangci-lint run --issues-exit-code=0 --disable=nakedret,exhaustivestruct,wrapcheck,paralleltest,rowserrcheck,cyclop,scopelint,nilerr,interfacer,funlen,golint,revive,tagliatelle
run: pre
	go run cmd/clickhouse_sinker/main.go --local-cfg-file docker/test_dynamic_schema.json
