PKG := github.com/housepower/clickhouse_sinker
EDITION ?= housepower

SINKER_LDFLAGS += -X "$(PKG)/config.SinkerReleaseVersion=$(git describe --tags --dirty)"
SINKER_LDFLAGS += -X "$(PKG)/config.SinkerBuildTS=$(shell date -u '+%Y-%m-%d %I:%M:%S')"
SINKER_LDFLAGS += -X "$(PKG)/config.SinkerGitHash=$(shell git rev-parse HEAD)"
SINKER_LDFLAGS += -X "$(PKG)/config.SinkerGitBranch=$(shell git rev-parse --abbrev-ref HEAD)"
SINKER_LDFLAGS += -X "$(PKG)/config.SinkerEdition=$(EDITION)"

GO        := CGO_ENABLED=0 go
GOBUILD   := $(GO) build $(BUILD_FLAG)

pre:
	go mod tidy
build: pre
	$(GOBUILD) -ldflags '$(SINKER_LDFLAGS)' -o dist/clickhouse_sinker cmd/clickhouse_sinker/main.go
	$(GOBUILD) -ldflags '$(SINKER_LDFLAGS)' -o dist/nacos_publish_config cmd/nacos_publish_config/main.go
debug: pre
	$(GOBUILD) -ldflags '$(SINKER_LDFLAGS)' -gcflags "all=-N -l" -o dist/clickhouse_sinker cmd/clickhouse_sinker/main.go
	$(GOBUILD) -ldflags '$(SINKER_LDFLAGS)' -gcflags "all=-N -l" -o dist/nacos_publish_config cmd/nacos_publish_config/main.go
unittest: pre
	go test -v ./...
benchtest: pre
	go test -bench=. ./...
systest: build
	bash go.test.sh
lint:
	golangci-lint run --issues-exit-code=0 --disable=nakedret,exhaustivestruct,wrapcheck,paralleltest
run: pre
	go run cmd/clickhouse_sinker/main.go --local-cfg-dir conf/

docker-run:
	docker run --net=host -e "CONFIG=`cat conf/config.json`" -e "TASK=`cat conf/tasks/logstash_sample.json`" --rm -it `docker build -q .`