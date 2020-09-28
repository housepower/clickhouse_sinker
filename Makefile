pre:
	go mod tidy
build: pre
	go build -o dist/clickhouse_sinker bin/main.go
debug: pre
	go build -gcflags "all=-N -l" -o dist/clickhouse_sinker bin/main.go
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

docker-run:
	docker run --net=host -e "CONFIG=`cat conf/config.json`" -e "TASK=`cat conf/tasks/logstash_sample.json`" --rm -it `docker build -q .`