build:
	export GO111MODULE=auto
	go mod tidy
	go build -gcflags "-N -l" -o dist/clickhouse_sinker bin/main.go

unittest:
	go test ./...

systest:
	bash go.test.sh

