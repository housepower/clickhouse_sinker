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
- Parse messages concurrently.
- Write batches concurrently.
- Every batch is sharded to a determined clickhouse node. Exit if loop write failed.
- Custom sharding policy (by config `shardingKey` and `shardingPolicy`).
- At least once delivery guarantee.
- Dynamic config management with Nacos.

## Install && Run

### By binary files (suggested)

Download the binary files from [release](https://github.com/housepower/clickhouse_sinker/releases), choose the executable binary file according to your env, modify the `conf` files, then run `./clickhouse_sinker --local-cfg-dir conf`

### By container image

Download the binary files from [release](https://quay.io/repository/housepower/clickhouse_sinker), modify the `conf` files, then run `docker run --volume conf:/etc/clickhouse_sinker quay.io/housepower/clickhouse_sinker`

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
./dist/clickhouse_sinker --local-cfg-dir conf
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

### Dynamic Config Management with Nacos

Controled by:

- CLI parameters: `nacos-register-enable, nacos-addr, nacos-namespace-id, nacos-group, nacos-username, nacos-password`
- env variables: `NACOS_REGISTER_ENABLE, NACOS_ADDR, NACOS_NAMESPACE_ID, NACOS_GROUP, NACOS_USERNAME, NACOS_PASSWORD`

### Dynamic Config Management with Consul

Currently sinker is able to register with Consul, but not able to get config from Consul.
Controled by:

- CLI parameters: `consul-register-enable, consul-addr, consul-deregister-critical-services-after`
- env variables: `CONSUL_REGISTER_ENABLE, CONSUL_ADDR, CONSUL_DEREGISTER_CRITICAL_SERVICES_AFTER`

### Dynamic Config Management with Local Files
TODO. Currently sinker is able to parse local config files at startup, but not able to detect file changes.

## Prometheus Metrics

All metrics are defined in `statistics.go`. You can create Grafana dashboard for clickhouse_sinker by importing the template `clickhouse_sinker-dashboard.json`.

* [x] Pull with prometheus

Metrics are exposed at `http://ip:port/metrics`. IP is the outbound IP of this machine. Port is from CLI `--http-port` or env `HTTP_PORT`.

Sinker registers with Nacos if CLI `--consul-register-enable` or env `CONSUL_REGISTER_ENABLE` is present. However Prometheus is [unable](https://github.com/alibaba/nacos/issues/1032) to obtain dynamic service list from nacos server.

* [x] Push to promethues

If CLI `--push-gateway-addrs` or env `PUSH_GATEWAY_ADDRS` (a list of comma-separated urls) is present, metrics are pushed to the given one of given URLs regualarly.


## Custom message parser

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
