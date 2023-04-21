VERSION=$(shell git describe --tags --dirty)
SINKER_LDFLAGS += -X "main.version=$(VERSION)"
SINKER_LDFLAGS += -X "main.date=$(shell date --iso-8601=s)"
SINKER_LDFLAGS += -X "main.commit=$(shell git rev-parse HEAD)"
SINKER_LDFLAGS += -X "main.builtBy=$(shell echo `whoami`@`hostname`)"
DEFAULT_CFG_PATH = /etc/clickhouse_sinker.hjson
IMG_TAGGED = hub.eoitek.net/storage/clickhouse_sinker:${VERSION}
IMG_LATEST = hub.eoitek.net/storage/clickhouse_sinker:latest
export GOPROXY=https://goproxy.cn,direct

GO        := CGO_ENABLED=0 go
GOBUILD   := $(GO) build $(BUILD_FLAG)


.PHONY: pre
pre:
	go mod tidy

.PHONY: build
build: pre
	$(GOBUILD) -ldflags '$(SINKER_LDFLAGS)' -o . ./...

.PHONY: debug
debug: pre
	$(GOBUILD) -ldflags '$(SINKER_LDFLAGS)' -gcflags "all=-N -l" -o . ./...

.PHONY: unittest
unittest: pre
	go test -v ./...

.PHONY: benchtest
benchtest: pre
	go test -bench=. ./...

.PHONY: systest
systest: build
	bash go.test.sh
	bash go.metrictest.sh

.PHONY: coverage
coverage:
	go test ./... -coverprofile=coverage.txt -covermode count
	go tool cover -func coverage.txt

.PHONY: lint
lint:
	golangci-lint run -D errcheck,govet,gosimple

.PHONY: run
run: pre
	go run cmd/clickhouse_sinker/main.go --local-cfg-file docker/test_dynamic_schema.hjson

.PHONY: release
release:
	goreleaser release --skip-publish --clean

.PHONY: docker-build
docker-build: release
	docker build . -t clickhouse_sinker:${VERSION} -f Dockerfile_goreleaser
	docker tag clickhouse_sinker:${VERSION} ${IMG_TAGGED}
	docker tag clickhouse_sinker:${VERSION} ${IMG_LATEST}
	docker rmi clickhouse_sinker:${VERSION}

.PHONY: docker-push
docker-push:
	docker push ${IMG_TAGGED}
	docker push ${IMG_LATEST}

.PHONY: docker-run
docker-run:
	docker run -d -v ${DEFAULT_CFG_PATH}:${DEFAULT_CFG_PATH} ${IMG_LATEST}
