pre:
	go mod vendor
build: pre
	go build -gcflags "-N -l" -o dist/clickhouse_sinker bin/main.go
unittest: pre
	go test -v ./...
benchtest: pre
	go test -bench=. ./...
systest: build
	bash go.test.sh
lint:
	golangci-lint run
run: pre
	go run bin/main.go -conf conf/