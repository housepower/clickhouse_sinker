# clickhouse_sinker

clickhouse_sinker is a sinker program that consumes kafka message and import them to [ClickHouse](https://clickhouse.yandex/).

## Features

* Easy to use and deploy, you don't need write any hard code, just care about the configuration file
* Support multiple sinker tasks, each runs on parallel.
* Support multiply kafka and ClickHouse clusters.
* Bulk insert (by config `bufferSize` and `flushInterval`).
* Uses Native ClickHouse client-server TCP protocol, with higher performance than HTTP.

## Install 

* Install Golang

* Go Get

```
go get -u github.com/housepower/clickhouse_sinker
go get -u github.com/housepower/clickhouse_sinker/...
```

## Run

```
cd $GOPATH/src/github.com/housepower/clickhouse_sinker
go install github.com/kardianos/govendor
govendor sync
go build -o sinker bin/main.go

## modify the config files, then run it
./sinker -conf conf
```

## Support parsers

* [x] Json

## Supported data types

* [x] UInt8, UInt16, UInt32, UInt64, Int8, Int16, Int32, Int64
* [x] Float32, Float64
* [x] String
* [x] FixedString
* [ ] DateTime


## Configuration

See config [example](./conf/config.json)

