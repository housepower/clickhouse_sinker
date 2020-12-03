# clickhouse_sinker

[![Build Status](https://travis-ci.com/housepower/clickhouse_sinker.svg?branch=master)](https://travis-ci.com/housepower/clickhouse_sinker)
[![Go Report Card](https://goreportcard.com/badge/github.com/housepower/clickhouse_sinker)](https://goreportcard.com/report/github.com/housepower/clickhouse_sinker)

clickhouse_sinker is a sinker program that transfer kafka message into [ClickHouse](https://clickhouse.yandex/).

Refers to [design](./design.md) for how it works.

## Features

- Uses native ClickHouse client-server TCP protocol, with higher performance than HTTP.
- Easy to use and deploy, you don't need write any hard code, just care about the configuration file
- Support multiple parsers: fastjson(recommended), gjson, csv.
- Support multiple Kafka client: kafka-go(recommended), sarama.
- Support multiple Kafka security mechanisms: SSL, SASL/PLAIN, SASL/SCRAM, SASL/GSSAPI and combinations of them.
- Support multiple sinker tasks, each runs on parallel.
- Support multiple kafka and ClickHouse clusters.
- Bulk insert (by config `bufferSize` and `flushInterval`).
- Parse messages concurrently.
- Write batches concurrently.
- Every batch is routed to a determined clickhouse shard. Exit if loop write fail.
- Custom sharding policy (by config `shardingKey` and `shardingPolicy`).
- Tolerate replica single-point-failure.
- At least once delivery guarantee.
- Dynamic config management with Nacos.

## Supported data types

- [x] UInt8, UInt16, UInt32, UInt64, Int8, Int16, Int32, Int64
- [x] Float32, Float64
- [x] String
- [x] FixedString
- [x] Date, DateTime, DateTime64 (custom layout parser)
- [x] Array(UInt8, UInt16, UInt32, UInt64, Int8, Int16, Int32, Int64)
- [x] Array(Float32, Float64)
- [x] Array(String)
- [x] Array(FixedString)
- [x] Nullable
- [x] [ElasticDateTime](https://www.elastic.co/guide/en/elasticsearch/reference/current/date.html) => Int64 (2019-12-16T12:10:30Z => 1576498230)

## Install && Run

### By binary files (recommended)

Download the binary files from [release](https://github.com/housepower/clickhouse_sinker/releases), choose the executable binary file according to your env, modify the `conf` files, then run `./clickhouse_sinker --local-cfg-dir conf`

### By container image

Modify the `conf` files, then run `docker run --volume conf:/etc/clickhouse_sinker quay.io/housepower/clickhouse_sinker`

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

## Configuration

Refers to how [integration test](./go.test.sh) use the [example config](./conf/config.json).
Also refers to [code](./config/config.go) for all config items.

### Kafka Encryption

clickhouse_sinker supports following encryption mechanisms:

- No encryption

An example kafka config:

```
    "kfk1": {
      "brokers": "192.168.31.64:9092",
      "@version": "Required if you use sarama. It's the the Kafka server version.",
      "version": "2.2.1"
    }
```

- Encryption using SSL

An example kafka config:
```
    "kfk2": {
      "brokers": "192.168.31.64:9093",
      "version": "2.2.1",
      "tls": {
        "enable": true,
        "@caCertFiles": "Required. It's the CA certificate with which Kafka brokers certs be signed. This cert is added to kafka.client.truststore.jks which kafka-console-consumer.sh uses",
        "caCertFiles": "/etc/security/ca-cert",
        "@insecureSkipVerify": "Whether disable broker FQDN verification. Set it to `true` if kafka-console-consumer.sh uses `ssl.endpoint.identification.algorithm=`.",
        "insecureSkipVerify": true
      }
    }
```

FYI. `kafka-console-consumer.sh` works as the following setup:

```
$ cat config/SSL_NOAUTH_client.properties 
security.protocol=SSL
ssl.truststore.location=/etc/security/kafka.client.truststore.jks 
ssl.truststore.password=123456
ssl.endpoint.identification.algorithm=

$ bin/kafka-console-consumer.sh --bootstrap-server 192.168.31.64:9094 --topic sunshine --group test-consumer-group --from-beginning --consumer.config config/SSL_NOAUTH_client.properties
```

Please follow [`Kafka SSL setup`](https://kafka.apache.org/documentation/#security_ssl). Use `-keyalg RSA` when you create the broker keystore, otherwise there will be no cipher suites in common between the keystore and those Golang supports. See [this](https://github.com/Shopify/sarama/issues/643#issuecomment-216839760) for reference.

### Kafka Authentication

clickhouse_sinker support following following authentication mechanisms:

- No authentication

An example kafka config:

```
    "kfk1": {
      "brokers": "192.168.31.64:9092",
      "@version": "Required if you use sarama. It's the the Kafka server version.",
      "version": "2.2.1"
    }
```

- SASL/PLAIN

An example kafka config:
```
    "kfk3": {
      "brokers": "192.168.31.64:9094",
      "version": "2.2.1",
      "sasl": {
        "enable": true,
        "mechanism": "PLAIN",
        "username": "alice",
        "password": "alice-secret"
      }
    }
```

FYI. Java clients work with the following setup:

```
$ cat config/PLAINTEXT_PLAIN_client.properties
security.protocol=SASL_PLAINTEXT
sasl.kerberos.service.name=kafka
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="alice" password="alice-secret";

$ bin/kafka-console-producer.sh --broker-list 192.168.31.64:9094 --topic sunshine --producer.config config/PLAINTEXT_PLAIN_client.properties

$ bin/kafka-console-consumer.sh --bootstrap-server 192.168.31.64:9094 --topic sunshine --group test-consumer-group --from-beginning --consumer.config config/PLAINTEXT_PLAIN_client.properties
```

- SASL/SCRAM

An example kafka config:
```
    "kfk4": {
      "brokers": "192.168.31.64:9094",
      "version": "2.2.1",
      "sasl": {
        "enable": true,
        "@mechanism": "SCRAM-SHA-256 or SCRAM-SHA-512",
        "mechanism": "SCRAM-SHA-256",
        "username": "alice",
        "password": "alice-secret"
      }
    }
```

FYI. Java clients work with the following setup:

```
$ cat config/PLAINTEXT_SCRAM_client.properties
security.protocol=SASL_PLAINTEXT
sasl.kerberos.service.name=kafka
sasl.mechanism=SCRAM-SHA-256
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="alice" password="alice-secret";

$ bin/kafka-console-producer.sh --broker-list 192.168.31.64:9094 --topic sunshine --producer.config config/PLAINTEXT_SCRAM_client.properties

$ bin/kafka-console-consumer.sh --bootstrap-server 192.168.31.64:9094 --topic sunshine --group test-consumer-group --from-beginning --consumer.config config/PLAINTEXT_SCRAM_client.properties
```

- SASL/GSSAPI(Kerberos)

An example kafka config:
```
    "kfk5": {
      "brokers": "192.168.31.64:9094",
      "version": "2.2.1",
      "sasl": {
        "enable": true,
        "mechanism": "GSSAPI",
        "gssapi": {
          "@authtype": "1 - Username and password, 2 - Keytab",
          "authtype": 2,
          "keytabpath": "/etc/security/mmmtest.keytab",
          "kerberosconfigpath": "/etc/krb5.conf",
          "servicename": "kafka",
          "@username": "`principal` consists of `username` `@` `realm`",
          "username": "mmm",
          "realm": "ALANWANG.COM"
        }
      }
    }
```

FYI. Java clients work with the following setup:

```
$ cat config/PLAINTEXT_GSSAPI_client.properties
security.protocol=SASL_PLAINTEXT
sasl.kerberos.service.name=kafka
sasl.mechanism=GSSAPI
sasl.jaas.config=com.sun.security.auth.module.Krb5LoginModule required useKeyTab=true storeKey=true debug=true keyTab="/etc/security/mmmtest.keytab" principal="mmm@ALANWANG.COM";

$ bin/kafka-console-producer.sh --broker-list 192.168.31.64:9094 --topic sunshine --producer.config config/PLAINTEXT_GSSAPI_client.properties

$ bin/kafka-console-consumer.sh --bootstrap-server 192.168.31.64:9094 --topic sunshine --group test-consumer-group --from-beginning --consumer.config config/PLAINTEXT_GSSAPI_client.properties
```

Kerberos setup is complex. Please ensure [`kafka-console-consumer.sh`](https://docs.cloudera.com/runtime/7.2.1/kafka-managing/topics/kafka-manage-cli-consumer.html) Kerberos keytab authentication work STRICTLY FOLLOW [this article](https://stackoverflow.com/questions/48744660/kafka-console-consumer-with-kerberos-authentication/49140414#49140414), then test `clickhouse_sinker` Kerberos authentication on the SAME machine which `kafka-console-consumer.sh` runs. I tested sarama Kerberos authentication against Kafka [2.2.1](https://archive.apache.org/dist/kafka/2.2.1/kafka_2.11-2.2.1.tgz). Not sure other Kafka versions work.

### Sharding Policy

Every message is routed to a determined ClickHouse shard.

By default, the shard number is caculated by `(kafka_offset/roundup(batch_size))%clickhouse_shards`, where `roundup()` round upward an unsigned integer to the the nearest 2^n.

This above expression can be customized with `shardingKey` and `shardingPolicy`. `shardingKey` value is a column name. `shardingPolicy` value could be:

- `stripe,<size>`. This requires `shardingKey` be a numeric-like (bool, int, float, date etc.) column. The expression is `(uint64(shardingKey)/stripe_size)%clickhouse_shards`.
- `hash`. This requires `shardingKey` be a string-like column. The hash function used internally is [xxHash64](https://github.com/cespare/xxhash). The expression is `xxhash64(string(shardingKey))%clickhouse_shards`.

## Configuration Management

### Nacos

Sinker is able to register with Nacos, get and apply config changes dynamically without restart the whole process.
Controled by:

- CLI parameters: `nacos-register-enable, nacos-addr, nacos-namespace-id, nacos-group, nacos-username, nacos-password`
- env variables: `NACOS_REGISTER_ENABLE, NACOS_ADDR, NACOS_NAMESPACE_ID, NACOS_GROUP, NACOS_USERNAME, NACOS_PASSWORD`

### Consul

Currently sinker is able to register with Consul, but unable to get config.
Controled by:

- CLI parameters: `consul-register-enable, consul-addr, consul-deregister-critical-services-after`
- env variables: `CONSUL_REGISTER_ENABLE, CONSUL_ADDR, CONSUL_DEREGISTER_CRITICAL_SERVICES_AFTER`

### Local Files

Currently sinker is able to parse local config files at startup, but unable to detect file changes.

## Prometheus Metrics

All metrics are defined in `statistics.go`. You can create Grafana dashboard for clickhouse_sinker by importing the template `clickhouse_sinker-dashboard.json`.

- Pull with prometheus

Metrics are exposed at `http://ip:port/metrics`. IP is the outbound IP of this machine. Port is from CLI `--http-port` or env `HTTP_PORT`.

Sinker registers with Nacos if CLI `--consul-register-enable` or env `CONSUL_REGISTER_ENABLE` is present. However Prometheus is [unable](https://github.com/alibaba/nacos/issues/1032) to obtain dynamic service list from nacos server.

- Push to promethues

If CLI `--metric-push-gateway-addrs` or env `METRIC_PUSH_GATEWAY_ADDRS` (a list of comma-separated urls) is present, metrics are pushed to one of given URLs regualarly.


## Extending

There are several abstract interfaces which you can implement to support more message format, message queue and config management mechanism.

```
type Parser interface {
	Parse(bs []byte) model.Metric
}

type Inputer interface {
	Init(cfg *config.Config, taskName string, putFn func(msg model.InputMessage)) error
	Run(ctx context.Context)
	Stop() error
	CommitMessages(ctx context.Context, message *model.InputMessage) error
}

// RemoteConfManager can be implemented by many backends: Nacos, Consul, etcd, ZooKeeper...
type RemoteConfManager interface {
	Init(properties map[string]interface{}) error
	// Register this instance, and keep-alive via heartbeat.
	Register(ip string, port int) error
	Deregister(ip string, port int) error
	// GetInstances fetchs healthy instances.
	// Mature service-discovery solutions(Nacos, Consul etc.) have client side cache
	// so that frequent invoking of GetInstances() and GetGlobalConfig() don't harm.
	GetInstances() (instances []Instance, err error)
	// GetConfig fetchs the config. The manager shall not reference the returned Config object after call.
	GetConfig() (conf *Config, err error)
	// PublishConfig publishs the config. The manager shall not reference the passed Config object after call.
	PublishConfig(conf *Config) (err error)
}

```

## Why not [`Kafka Engine`](https://clickhouse.tech/docs/en/engines/table-engines/integrations/kafka/) built in ClickHouse?

- My experience indicates `Kafka Engine` is complicated, buggy and hard to debug.
- `Kafka Engine` runs inside the db process, lowers the database stability. On the other side, [Vertica](https://www.vertica.com/)'s official kafka importer is separated with the database server.
- `Kafka Engine` doesn't support custom sharding policy.
- Neither `Kafka Engine` nor clickhouse_sinker support exactly-once.


## Kafka Compatibility

Kafka broker [exposes versions of various APIs it supports since 0.10.0.0](https://kafka.apache.org/protocol#api_versions).

### Kafka-go

- Kafka-go [negotiate it's protocol Version](https://github.com/segmentio/kafka-go/blob/c66d8ca149e7f1a7905b47a60962745ceb08a6a9/conn.go#L209).
- Kafka-go [doesn't support Kerberos authentication](https://github.com/segmentio/kafka-go/issues/237).

### Sarama

- Sarama guarantees compatibility [with Kafka 2.4 through 2.6](https://github.com/Shopify/sarama/blob/master/README.md#compatibility-and-api-stability).
- Sarama [has tied it's protocol usage to the Version field in Config](https://github.com/Shopify/sarama/issues/1732).

### Conclusion

- Neither Kafka-go nor sarama is mature as Java clients. You need to try both if clickhouse_sinker fails to connect with Kafka.
- Our experience is sarama can't work well with new kafka server if set its `Config.Version` to "0.11.0.0". So we suggest `KafkaConfig.Version` in clickhouse_sinker config matchs the Kafka server.
