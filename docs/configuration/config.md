# Config Items

> Here we use json with comments for documentation

```json
{
  // ClickHouse config
  "clickhouse": {
    // cluster the ClickHouse node belongs
    "cluster": "test",
    // hosts for connection, it's Array(Array(String))
    // we can put hosts with same shard into the inner array
    // it helps data deduplication for ReplicateMergeTree when driver error occurs
    "hosts": [
      [
        "192.168.101.106",
        "192.168.101.108"
      ],
      [
        "192.168.102.114",
        "192.168.101.110"
      ],
      [
        "192.168.102.115"
      ]
    ],
    "port": 9000,
    "username": "default",
    "password": "",
    // database name
    "db": "default",
    // Whether enable TLS encryption with clickhouse-server
    "secure": false,
    // Whether skip verify clickhouse-server cert if secure=true.
    "insecureSkipVerify": false,
    // retryTimes when error occurs in inserting datas
    "retryTimes": 0,
    // max open connections with each clickhouse node. default to 1.
    "maxOpenConns": 1
  },

  // Kafka config
  "kafka": {
    "brokers": "127.0.0.1:9093",
    
    // jave client style security authentication
    "security":{
        "security.protocol": "SASL_PLAINTEXT",
        "sasl.kerberos.service.name": "kafka",
        "sasl.mechanism":"GSSAPI",
        "sasl.jaas.config":"com.sun.security.auth.module.Krb5LoginModule required useKeyTab=true storeKey=true debug=true keyTab=\"/etc/security/mmmtest.keytab\" principal=\"mmm@ALANWANG.COM\";"
    },

    // SSL
    "tls": {
      "enable": false,
      // Required. It's the CA certificate with which Kafka brokers certs be signed.
      "caCertFiles": "/etc/security/ca-cert",
      // Required if Kafka brokers require client authentication.
      "clientCertFile": "",
      // Required if and only if ClientCertFile is present.
      "clientKeyFile": ""
    },

    // SASL
    "sasl": {
      "enable": false,
      // Mechanism is the name of the enabled SASL mechanism.
      // Possible values: PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, GSSAPI (defaults to PLAIN)
      "mechanism": "PLAIN",
      // Username is the authentication identity (authcid) to present for
      // SASL/PLAIN or SASL/SCRAM authentication
      "username": "",
      // Password for SASL/PLAIN or SASL/SCRAM authentication
      "password": "",
      "gssapi": {
        // authtype - 1. KRB5_USER_AUTH, 2. KRB5_KEYTAB_AUTH
        "authtype": 0,
        "keytabpath": "",
        "kerberosconfigpath": "",
        "servicename": "",
        "username": "",
        "password": "",
        "realm": "",
        "disablepafxfast": false
      }
    },

    // kafka version, if you use sarama, the version must be specified
    "version": "2.5.0"
  },

  "task": {
    "name": "test_dynamic_schema",
    // kafka client, possible values: sarama, kafka-go. (defaults to sarama)
    "kafkaClient": "sarama",
    // kafka topic
    "topic": "topic",
    // kafka consume from earliest or latest
    "earliest": true,
    // kafka consumer group
    "consumerGroup": "group",

    // message parser
    "parser": "json",

    // clickhouse table name
    "tableName": "daily",

    // columns of the table
    "dims": [
      {
        // column name
        "name": "timestamp",
        // column type
        "type": "DateTime"
      },
      {
        "name": "name",
        "type": "String"
      },
      {
        "name": "value",
        "type": "Float32",
        // json field name. This must be specified if it doesn't match with the column name.
        "sourcename": "val"
      }
    ],

    // if it's specified, clickhouse_sinker will detect table schema instead of using the fixed schema given by "dims".
    "autoSchema" : true,
    // these columns will be excluded from the detected table schema. This takes effect only if "autoSchema" is true.
    "excludeColumns": []

    // (experiment feature) detect new fields and their type, and add columns to the ClickHouse table accordingly. This feature requires parser be "fastjson" or "gjson". New fields' type will be one of: Int64, Float64, String.
    "dynamicSchema": {
      // whether enable this feature, default to false
      "enable": true,
      // the upper limit of dynamic columns number, <=0 means math.MaxInt16. protecting dirty data attack
      "maxDims": 1024
    },

    // shardingKey is the column name to which sharding against
    "shardingKey": "",
    // shardingPolicy is `stripe,<interval>`(requires ShardingKey be numerical) or `hash`(requires ShardingKey be string)
    "shardingPolicy": "",

    // interval of flushing the batch. Default to 5, max to 600.
    "flushInterval": 5,
    // batch size to insert into clickhouse. sinker will round upward it to the the nearest 2^n. Default to 262114, max to 1048576.
    "bufferSize": 262114,

    // In the absence of time zone information, interprets the time as in the given location. Default to "Local" (aka /etc/localtime of the machine on which sinker runs)
    "timezone": ""
  },

  // log level, possible value: "debug", "info", "warn", "error", "dpanic", "panic", "fatal". Default to "info".
  "logLevel": "debug"
}
```
