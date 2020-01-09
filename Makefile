pre:
	export GOPATH=/tmp
	export GO111MODULE=auto
	go mod tidy
build: pre
	go build -gcflags "-N -l" -o dist/clickhouse_sinker bin/main.go

unittest: pre
	go test ./...

systest: build
	bash go.test.sh

