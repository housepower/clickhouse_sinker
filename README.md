# clickhouse_sinker

[![Build Status](https://travis-ci.com/housepower/clickhouse_sinker.svg?branch=master)](https://travis-ci.com/housepower/clickhouse_sinker)
[![Go Report Card](https://goreportcard.com/badge/github.com/housepower/clickhouse_sinker)](https://goreportcard.com/report/github.com/housepower/clickhouse_sinker)

clickhouse_sinker is a sinker program that transfer kafka message into [ClickHouse](https://clickhouse.yandex/).

## Features

- Uses Native ClickHouse client-server TCP protocol, with higher performance than HTTP.
- Easy to use and deploy, you don't need write any hard code, just care about the configuration file
- Support multiple parsers: csv, fastjson, gjson.
- Support multiple Kafka client: sarama, kafka-go.
- Custom parser support.
- Support multiple sinker tasks, each runs on parallel.
- Support multiply kafka and ClickHouse clusters.
- Bulk insert (by config `bufferSize` and `flushInterval`).
- Parse messages concurrently (by config `concurrentParsers`).
- Write batches concurrently.
- Every batch is sharded to a determined clickhouse node. Exit if loop write failed.
- Custom sharding policy (by config `shardingKey` and `shardingPolicy`).
- At least once delivery guarantee, [more info](https://github.com/housepower/clickhouse_sinker/issues/76) about it.

## Install && Run

### By binary files (suggested)

Download the binary files from [release](https://github.com/housepower/clickhouse_sinker/releases), choose the executable binary file according to your env, modify the `conf` files, then run `./clickhouse_sinker -conf conf `

### By source

- Install Golang

- Go Get

```
go get -u github.com/housepower/clickhouse_sinker/...
```

- Build && Run

```
make build
## modify the config files, set the configuration directory, then run it
./dist/clickhouse_sinker -conf conf
```

## Examples

- look at the [integration test](https://github.com/housepower/clickhouse_sinker/blob/master/go.test.sh).
- there is a simple [tutorial in Chinese](https://note.youdao.com/ynoteshare1/index.html?id=c4b4a84a08e2312da6c6d733a5074c7a&type=note) which created by user @taiyang-li.

## Supported data types

- [x] UInt8, UInt16, UInt32, UInt64, Int8, Int16, Int32, Int64
- [x] Float32, Float64
- [x] String
- [x] FixedString
- [x] Date, DateTime, DateTime64 (Custom Layout parser)
- [x] Array(UInt8, UInt16, UInt32, UInt64, Int8, Int16, Int32, Int64)
- [x] Array(Float32, Float64)
- [x] Array(String)
- [x] Array(FixedString)
- [x] Nullable
- [x] [ElasticDateTime](https://www.elastic.co/guide/en/elasticsearch/reference/current/date.html) => Int64 (2019-12-16T12:10:30Z => 1576498230)

## Configuration

See config [example](./conf/config.json)

### Authentication with Kafka

clickhouse_sinker support following three authentication mechanism:

* [x] No authentication

An example clickhouse_sinker config:

```
    "kfk1": {
      "brokers": "127.0.0.1:9092",
      "Version": "0.10.2.1"
    }
```

* [x] SASL/PLAIN

An example clickhouse_sinker config:
```
    "kfk2": {
      "brokers": "127.0.0.1:9093",
      "sasl": {
        "enable": true,
        "password": "username",
        "username": "password"
      "Version": "0.10.2.1"
    }
```

* [x] SASL/GSSAPI(Kerberos)

You need to configuring a Kerberos Client on the host on which the clickhouse_sinker run. Refers to [Kafka Security](https://kafka.apache.org/documentation/#security).

1. Install the krb5-libs package on all of the client machine.
```
$ sudo yum install -y krb5-libs
```
2. Supply a valid /etc/krb5.conf file for each client. Usually this can be the same krb5.conf file used by the Kerberos Distribution Center (KDC).


An example clickhouse_sinker config:

```
    "kfk3": {
      "brokers": "127.0.0.1:9094",
      "sasl": {
        "enable": true,
        "gssapi": {
          "authtype": 2,
          "keytabpath": "/home/keytab/zhangtao.keytab",
          "kerberosconfigpath": "/etc/krb5.conf",
          "servicename": "kafka",
          "username": "zhangtao/localhost",
          "realm": "ALANWANG.COM"
        }
      },
      "Version": "0.10.2.1"
    }
```

FYI. The same config looks like the following in Java code:
```
security.protocol：SASL_PLAINTEXT
sasl.kerberos.service.name：kafka
sasl.mechanism：GSSAPI
sasl.jaas.config：com.sun.security.auth.module.Krb5LoginModule required useKeyTab=true storeKey=true debug=true keyTab=\"/home/keytab/zhangtao.keytab\" principal=\"zhangtao/localhost@ALANWANG.COM\";
```

### Prometheus Metrics

All metrics are in `statistics.go`.

* [x] Push to promethues

Add a piece like the following to config.json:

```
  "statistics": {
    "enable": true,
    "pushGatewayAddrs": ["http://node3:33445/cks/prom/write"]
  },
```

Or set env `METRICS_PUSH_GATEWAY_ADDR` to a list of urls joined with comma.
The env have precedence over the config file.

* [x] Pull with prometheus

The command line parameter `--http-addr` expects a `ip:port`. Metrics are exposed at `http://ip:port/metrics`.


## Custom metric parser

- You just need to implement the parser interface on your own

```
type Parser interface {
	Parse(bs []byte) model.Metric
}
```

See [json parser](./parser/json.go)

# Debugging

```bash
echo '{"date": "2019-07-11T12:10:30Z", "level": "info", "message": "msg4"}' | kafkacat -b 127.0.0.1:9093 -P -t logstash

clickhouse-client -q 'select * from default.logstash'
2019-12-16	info	msg4
2019-07-11	info	msg4
2015-05-11	info	msg4
2019-07-11	info	msg4

```
