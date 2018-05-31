# ch_sinker

ch_sinker is a sinker program that consumes kafka message and import to [ClickHouse](https://clickhouse.yandex/).

## Features

* Easy to use and deploy, you don't need write any hard code, just care about the configuration file
* Support multiple sinker tasks, each runs on parallel.
* Bulk insert (by config `bufferSize` and `flushInterval`).
* Uses native ClickHouse client-server protocol, with higher performance than HTTP.


## Install 

```
	go get -u github.com/houseflys/ch_sinker
```


## Run

```
	cd $GOPATH/src/github.com/houseflys/ch_sinker
	go build -o ch_sinker bin/main.go
	./ch_sinker -conf conf
```


## Configuration

See config [example](./conf/config.json)


## Supported data types

* [x] UInt8, UInt16, UInt32, UInt64, Int8, Int16, Int32, Int64
* [x] Float32, Float64
* [x] String
* [x] FixedString
* [ ] DateTime
* [ ] Tuple
* [ ] Nested
* [ ] Array(T)
* [ ] Enum
* [ ] UUID