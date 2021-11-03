# clickhouse_sinker

[![Build Status](https://travis-ci.com/housepower/clickhouse_sinker.svg?branch=master)](https://travis-ci.com/housepower/clickhouse_sinker)
[![Go Report Card](https://goreportcard.com/badge/github.com/housepower/clickhouse_sinker)](https://goreportcard.com/report/github.com/housepower/clickhouse_sinker)

clickhouse_sinker is a sinker program that transfer kafka message into [ClickHouse](https://clickhouse.yandex/).

Refers to [design](./design.md) for how it works.

## Features

- Uses native ClickHouse client-server TCP protocol, with higher performance than HTTP.
- Easy to use and deploy, you don't need write any hard code, just care about the configuration file
- Support multiple parsers: fastjson(recommended), gjson, csv.
- Support multiple Kafka client: sarama(recommended), kafka-go.
- Support multiple Kafka security mechanisms: SSL, SASL/PLAIN, SASL/SCRAM, SASL/GSSAPI and combinations of them.
- Bulk insert (by config `bufferSize` and `flushInterval`).
- Parse messages concurrently.
- Write batches concurrently.
- Every batch is routed to a determined clickhouse shard. Exit if loop write fail.
- Custom sharding policy (by config `shardingKey` and `shardingPolicy`).
- Tolerate replica single-point-failure.
- At-least-once delivery guarantee.
- Config management with local file or Nacos.
- One clickhouse_sinker instance assign tasks to all instances in balance of message lag (by config `nacos-service-name`).

## Supported data types

- [x] UInt8, UInt16, UInt32, UInt64, Int8, Int16, Int32, Int64
- [x] Float32, Float64
- [x] Decimal, Decimal32, Decimal64, Decimal128, Decimal256
- [x] String, FixedString, LowCardinality(String)
- [x] Date, DateTime, DateTime64. Assuming that all values of a field of kafka message has the same layout, and layouts of each field are unrelated. Automatically detect the layout from [these date layouts](https://github.com/housepower/clickhouse_sinker/blob/master/parser/parser.go) till the first successful detection and reuse that layout forever.
- [x] UUID
- [x] Enum
- [x] Array(T), where T is one of above basic types
- [x] Nullable(T), where T is one of above basic types
- [x] [ElasticDateTime](https://www.elastic.co/guide/en/elasticsearch/reference/current/date.html) => Int64 (2019-12-16T12:10:30Z => 1576498230)

Note:

- A message is ignored if it's invalid json, or CSV value doesn't match with the format. This is counted by `ParseMsgsErrorTotal`.
- If a message field type is imcompatible with the type `T` declared in ClickHouse, or field value is invalid to parse, the default value of `T` (see the following table) is filled.
- If a message field type is compatible with the type `T` declared in ClickHouse, but field value is overflow, the nearer border of `T` is filled.

| ClickHouse data type | default value | compatible Json data type           | valid range                           |
|:--------------------:|:-------------:|:-----------------------------------:|:-------------------------------------:|
| Int8, Int16, ...     | 0             | Bool, Number                        | Int8 [-128,127], ...                  |
| Float32, Float64     | 0.0           | Number                              | Float32 [-MaxFloat32,MaxFloat32], ... |
| Decimal, ...         | 0.0           | Number                              | [decimal-value-ranges](https://clickhouse.tech/docs/en/sql-reference/data-types/decimal/#decimal-value-ranges) |
| String, ...          | ""            | Bool, Number, String, Object, Array | N/A                                   |
| Date, DateTime, ...  | EPOCH         | Number, String                      | [EPOCH,MaxUint32_seconds_since_epoch) |
| Nullable(T)          | NULL          | (The same as T)                     | (The same as T)                       |
| Array(T)             | []            | (The same as T)                     | (The same as T)                       |

## Benchmark

### clickhouse_sinker

- ClickHouse cluster: 3 shards, 2 physical hosts in each shard. Each host contains 48 cpu, 256 GB RAM, 12TB HDD RAID5.
- ZooKeeper cluster: on three hosts of ClickHouse cluster.
- Kafka cluster: 2 nodes on three hosts of ClickHouse cluster. Share the same zookeeper cluster wich ClickHouse.
- Kafka topic apache_access_log1: partition 1, replicator factor: 1
- Kafka topic apache_access_log2: partition 2, replicator factor: 1
- Kafka topic apache_access_log4: partition 4, replicator factor: 1
- Generate json messages via kafka_gen_log(https://github.com/housepower/clickhouse_sinker/blob/master/cmd/kafka_gen_log). Messages avg lenght is 754 bytes.

| config     | thoughput(rows/s) | writer total cost   | clickhouse cost per node |
|-----------------------------|-------------------|---------------|---------------|
| 1 kafka partition, 1 sinker | 142 K             | 11.0 cpu, 8 GB | 0.3 cpu |
| 2 kafka partition, 1 sinker | 159 K             | 14.0 cpu, 14 GB | 0.7 cpu |
| 4 kafka partition, 1 sinker | 25~127 K          | 2~22 cpu, 16 GB | 1 cpu |
| 2 kafka partition, 2 sinker | 275 K             | 22 cpu, 8 GB | 1.3 cpu |
| 4 kafka partition, 2 sinker | 301 K             | 25 cpu, 18 GB | 1.5 cpu |

### Flink pipeline

Here's the Flink pipeline which moves date from kafka to ClickHouse. The cpu hotspot of the Flink pipeline is JSON decode, and Row.setField.

Kafka Source -> JSON decode -> DateTime formart conversion -> Interger type conversion -> JDBCSinkJob

| config     | thoughput(rows/s) | writer total cost   | clickhouse cost per node |
|-----------------------------|-------------------|---------------|---------------|
| 1 kafka partition, pipeline Parallelism: 20 | 44.7 K             | 13.8 cpu, 20 GB | 1.1 cpu |

### Conclusion

- clickhouse_sinker is 3x fast as the Flink pipeline, and cost much less connection and cpu overhead on clickhouse-server.
- clickhouse_sinker retry other replicas on writing failures.
- clickhouse_sinker get table schema from ClickHouse. The pipeline need manual config of all fields.
- clickhouse_sinker detect DateTime format. The pipeline need dedicated steps to do format and type conversion.

## Configuration

Refers to how [integration test](https://github.com/housepower/clickhouse_sinker/blob/master/go.test.sh) use the example config. Also refers to [code](https://github.com/housepower/clickhouse_sinker/blob/master/config/config.go) for all config items.

### Kafka Encryption

clickhouse_sinker supports following encryption mechanisms:

- No encryption

An example kafka config:

```json
  "kafka": {
    "brokers": "192.168.31.64:9092",
    "@version": "Required if you use sarama. It's the the Kafka server version.",
    "version": "2.5.0"
  }
```

- Encryption using SSL

An example kafka config:

```json
  "kafka": {
    "brokers": "192.168.31.64:9093",
    "version": "2.5.0",
    "tls": {
      "enable": true,
      "@trustStoreLocation": "ssl.truststore.location which kafka-console-consumer.sh uses",
      "trustStoreLocation": "/etc/security/kafka.client.truststore.jks",
      "@trustStorePassword": "ssl.truststore.password which kafka-console-consumer.sh uses",
      "trustStorePassword": "123456",
      "@endpIdentAlgo": "ssl.endpoint.identification.algorithm which kafka-console-consumer.sh uses",
      "endpIdentAlgo": ""
    }
  }
```

Or if you have extracted certificates from JKS, use the following config:

```json
  "kafka": {
    "brokers": "192.168.31.64:9093",
    "version": "2.5.0",
    "tls": {
      "enable": true,
      "@caCertFiles": "Required. It's the CA certificate with which Kafka brokers certs be signed. This cert is added to kafka.client.truststore.jks which kafka-console-consumer.sh uses",
      "caCertFiles": "/etc/security/ca-cert",
      "@endpIdentAlgo": "ssl.endpoint.identification.algorithm which kafka-console-consumer.sh uses",
      "endpIdentAlgo": ""
    }
  }
```

FYI. `kafka-console-consumer.sh` works as the following setup:

```bash
$ cat config/client_SSL_NOAUTH.properties
security.protocol=SSL
ssl.truststore.location=/etc/security/kafka.client.truststore.jks
ssl.truststore.password=123456
ssl.endpoint.identification.algorithm=

$ bin/kafka-console-consumer.sh --bootstrap-server 192.168.31.64:9094 --topic sunshine --group test-consumer-group --from-beginning --consumer.config config/client_SSL_NOAUTH.properties
```

Please follow [`Kafka SSL setup`](https://kafka.apache.org/documentation/#security_ssl). Use `-keyalg RSA` when you create the broker keystore, otherwise there will be no cipher suites in common between the keystore and those Golang supports. See [this](https://github.com/Shopify/sarama/issues/643#issuecomment-216839760) for reference.

### Kafka Authentication

clickhouse_sinker support following following authentication mechanisms:

- No authentication

An example kafka config:

```json
  "kafka": {
    "brokers": "192.168.31.64:9092",
    "@version": "Required if you use sarama. It's the the Kafka server version.",
    "version": "2.5.0"
  }
```

- SASL/PLAIN

An example kafka config:

```json
  "kafka": {
    "brokers": "192.168.31.64:9094",
    "version": "2.5.0",
    "sasl": {
      "enable": true,
      "mechanism": "PLAIN",
      "username": "alice",
      "password": "alice-secret"
    }
  }
```

FYI. Java clients work with the following setup:

```bash
$ cat config/client_PLAINTEXT_PLAIN.properties
security.protocol=SASL_PLAINTEXT
sasl.kerberos.service.name=kafka
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="alice" password="alice-secret";

$ bin/kafka-console-producer.sh --broker-list 192.168.31.64:9094 --topic sunshine --producer.config config/client_PLAINTEXT_PLAIN.properties

$ bin/kafka-console-consumer.sh --bootstrap-server 192.168.31.64:9094 --topic sunshine --group test-consumer-group --from-beginning --consumer.config config/client_PLAINTEXT_PLAIN.properties
```

- SASL/SCRAM

An example kafka config:

```json
  "kafka": {
    "brokers": "192.168.31.64:9094",
    "version": "2.5.0",
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

```bash
$ cat config/client_PLAINTEXT_SCRAM.properties
security.protocol=SASL_PLAINTEXT
sasl.kerberos.service.name=kafka
sasl.mechanism=SCRAM-SHA-256
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="alice" password="alice-secret";

$ bin/kafka-console-producer.sh --broker-list 192.168.31.64:9094 --topic sunshine --producer.config config/client_PLAINTEXT_SCRAM.properties

$ bin/kafka-console-consumer.sh --bootstrap-server 192.168.31.64:9094 --topic sunshine --group test-consumer-group --from-beginning --consumer.config config/client_PLAINTEXT_SCRAM.properties
```

- SASL/GSSAPI(Kerberos)

An example kafka config:

```json
  "kafka": {
    "brokers": "192.168.31.64:9094",
    "version": "2.5.0",
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

```bash
$ cat config/client_PLAINTEXT_GSSAPI.properties
security.protocol=SASL_PLAINTEXT
sasl.kerberos.service.name=kafka
sasl.mechanism=GSSAPI
sasl.jaas.config=com.sun.security.auth.module.Krb5LoginModule required useKeyTab=true storeKey=true debug=true keyTab="/etc/security/mmmtest.keytab" principal="mmm@ALANWANG.COM";

$ bin/kafka-console-producer.sh --broker-list 192.168.31.64:9094 --topic sunshine --producer.config config/client_PLAINTEXT_GSSAPI.properties

$ bin/kafka-console-consumer.sh --bootstrap-server 192.168.31.64:9094 --topic sunshine --group test-consumer-group --from-beginning --consumer.config config/client_PLAINTEXT_GSSAPI.properties
```

Kerberos setup is complex. Please ensure [`kafka-console-consumer.sh`](https://docs.cloudera.com/runtime/7.2.1/kafka-managing/topics/kafka-manage-cli-consumer.html) Kerberos keytab authentication work STRICTLY FOLLOW [this article](https://stackoverflow.com/questions/48744660/kafka-console-consumer-with-kerberos-authentication/49140414#49140414), then test `clickhouse_sinker` Kerberos authentication on the SAME machine which `kafka-console-consumer.sh` runs. I tested sarama Kerberos authentication against Kafka [2.2.1](https://archive.apache.org/dist/kafka/2.2.1/kafka_2.11-2.2.1.tgz). Not sure other Kafka versions work.

### Sharding Policy

Every message is routed to a determined ClickHouse shard.

By default, the shard number is caculated by `(kafka_offset/roundup(buffer_size))%clickhouse_shards`, where `roundup()` round upward an unsigned integer to the the nearest 2^n.

This above expression can be customized with `shardingKey` and `shardingPolicy`. `shardingKey` value is a column name. `shardingPolicy` value could be:

- `stripe,<size>`. This requires `shardingKey` be a numeric-like (bool, int, float, date etc.) column. The expression is `(uint64(shardingKey)/stripe_size)%clickhouse_shards`.
- `hash`. This requires `shardingKey` be a string-like column. The hash function used internally is [xxHash64](https://github.com/cespare/xxhash). The expression is `xxhash64(string(shardingKey))%clickhouse_shards`.

## Configuration Management

The precedence of config items:

- CLI parameters > env variables
- Nacos > Local Config File

### Nacos

Sinker is able to register with Nacos, get and apply config changes dynamically without restart the whole process.
Controled by:

- CLI parameters: `nacos-addr, nacos-username, nacos-password, nacos-namespace-id, nacos-group, nacos-dataid`
- env variables: `NACOS_ADDR, NACOS_USERNAME, NACOS_PASSWORD, NACOS_NAMESPACE_ID, NACOS_GROUP, NACOS_DATAID`

### Local Config File

Currently sinker is able to parse local config file at startup, but unable to detect file changes.
Controled by:

- CLI parameters: `local-cfg-file`
- env variables: `LOCAL_CFG_FILE`

## Prometheus Metrics

All metrics are defined in `statistics.go`. You can create Grafana dashboard for clickhouse_sinker by importing the template `clickhouse_sinker-dashboard.json`.

- Pull with prometheus

Metrics are exposed at `http://ip:port/metrics`. IP is the outbound IP of this machine. Port is from CLI `--http-port` or env `HTTP_PORT`.

Sinker registers with Nacos if CLI `--consul-cfg-enable` or env `CONSUL_REGISTER_ENABLE` is present. However Prometheus is [unable](https://github.com/alibaba/nacos/issues/1032) to obtain dynamic service list from nacos server.

- Push to promethues

If CLI `--metric-push-gateway-addrs` or env `METRIC_PUSH_GATEWAY_ADDRS` (a list of comma-separated urls) is present, metrics are pushed to one of given URLs regualarly.

## Extending

There are several abstract interfaces which you can implement to support more message format, message queue and config management mechanism.

```go
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

Kafka release history is at [here](https://kafka.apache.org/downloads). Kafka broker [exposes versions of various APIs it supports since 0.10.0.0](https://kafka.apache.org/protocol#api_versions).

### Kafka-go

- Kafka-go [negotiate it's protocol Version](https://github.com/segmentio/kafka-go/blob/c66d8ca149e7f1a7905b47a60962745ceb08a6a9/conn.go#L209).
- Kafka-go [doesn't support Kerberos authentication](https://github.com/segmentio/kafka-go/issues/237).

### Sarama

- Sarama guarantees compatibility [with Kafka 2.6 through 2.8](https://github.com/Shopify/sarama/blob/master/README.md#compatibility-and-api-stability).
- Sarama [has tied it's protocol usage to the Version field in Config](https://github.com/Shopify/sarama/issues/1732).
- Sarama consumer API provides generation cleanup callback. This ensures `exactly once` when consumer-group rebalance occur.

### Conclusion

- Sarama is better than kafka-go, though neither is as mature as the officaial Kafka Java client. You need to try both if clickhouse_sinker fails to connect with Kafka.
- Our experience is sarama can't work well with new kafka server if set its `Config.Version` to "0.11.0.0". So we suggest `KafkaConfig.Version` in clickhouse_sinker config matchs the Kafka server.
