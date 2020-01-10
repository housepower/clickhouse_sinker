# clickhouse_sinker
[![Build Status](https://travis-ci.com/housepower/clickhouse_sinker.svg?branch=master)](https://travis-ci.com/housepower/clickhouse_sinker)

clickhouse_sinker is a sinker program that consumes kafka message and import them to [ClickHouse](https://clickhouse.yandex/).

## Features

* Easy to use and deploy, you don't need write any hard code, just care about the configuration file
* Custom parser support.
* Support multiple sinker tasks, each runs on parallel.
* Support multiply kafka and ClickHouse clusters.
* Bulk insert (by config `bufferSize` and `flushInterval`).
* Loop write (when some node crashes, it will retry write the data to the other healthy node)
* Uses Native ClickHouse client-server TCP protocol, with higher performance than HTTP.

## Install && Run

### By binary files (suggested)

Download the binary files from [release](https://github.com/housepower/clickhouse_sinker/releases), choose the executable binary file according to your env, modify the `conf` files, then run ` ./clickhouse_sinker -conf conf  `

### By source

* Install Golang

* Go Get

```
go get -u github.com/housepower/clickhouse_sinker
```

* Build && Run
```
make build
## modify the config files, set the configuration directory, then run it
./dist/clickhouse_sinker -conf conf
```

## Examples

* look at the [integration test](https://github.com/housepower/clickhouse_sinker/blob/master/go.test.sh).
* there is a simple [tutorial in Chinese](https://note.youdao.com/ynoteshare1/index.html?id=c4b4a84a08e2312da6c6d733a5074c7a&type=note) which created by user @taiyang.

## Support parsers

* [x] Json
* [x] Csv

## Supported data types

* [x] UInt8, UInt16, UInt32, UInt64, Int8, Int16, Int32, Int64
* [x] Float32, Float64
* [x] String
* [x] FixedString
* [x] DateTime(UInt32), Date(UInt16)
* [x] Array(UInt8, UInt16, UInt32, UInt64, Int8, Int16, Int32, Int64)
* [x] Array(Float32, Float64)
* [x] Array(String)
* [x] Array(FixedString)
* [x] Array(DateTime(UInt32), Date(UInt16))
* [x] Nullable

## Configuration

See config [example](./conf/config.json)

## Custom metric parser

* You just need to implement the parser interface on your own

```
type Parser interface {
	Parse(bs []byte) model.Metric
}
```
See [json parser](./parser/json.go)
