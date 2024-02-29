# Config Items

> Here we use json with comments for documentation, config file in hjson format is also supported

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
    "maxOpenConns": 1,
    // native or http, if configured secure and http both, means support https. default to native.
    "protocol": "native"
  },

  // Kafka config
  "kafka": {
    "brokers": "127.0.0.1:9093",

    "properties":{
        // This corresponds to Kafka's heartbeat.interval.ms.
        "heartbeat.interval.ms": 3000,
        // This option corresponds to Kafka's session.timeout.ms setting and must be within the broker's group.min.session.timeout.ms and group.max.session.timeout.ms.
        "session.timeout.ms": 120000,
        // This corresponds to Kafka's rebalance.timeout.ms.
        "rebalance.timeout.ms": 120000,
        // This option is roughly equivalent to request.timeout.ms, but grants additional time to requests that have timeout fields.
        "request.timeout.ms": 60000
    }
  
    // jave client style security authentication
    "security":{
        "security.protocol": "SASL_PLAINTEXT",
        "sasl.kerberos.service.name": "kafka",
        "sasl.mechanism":"GSSAPI",
        "sasl.jaas.config":"com.sun.security.auth.module.Krb5LoginModule required useKeyTab=true storeKey=true debug=true keyTab=\"/etc/security/mmmtest.keytab\" principal=\"mmm@ALANWANG.COM\";"
    },
    // whether reset domain realm. if this option is true, domain realm will replaced by "hadoop.{toLower(GSSAPI.Realm)}:{port}", this feature is worked when clickhouse_sinker connect to HUAWEI MRS kerberos kafka.
    "resetSaslRealm": false,

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
  },

  "task": {
    "name": "test_dynamic_schema",
    // kafka topic
    "topic": "topic",
    // kafka consume from earliest or latest
    "earliest": true,
    // kafka consumer group
    "consumerGroup": "group",

    // message parser
    "parser": "json",

    // clickhouse table name
    // override the clickhouse.db with "db.tableName" format, eg "default.tbl1"
    "tableName": "prom_metric",

    // name of the timeseries table, by default it is tableName with a "_series" suffix
    "seriesTableName": "prom_metric_myseries",

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
    "excludeColumns": [],

    // (experiment feature) detect new fields and their type, and add columns to the ClickHouse table accordingly. This feature requires parser be "fastjson" or "gjson". New fields' type will be one of: Int64, Float64, String.
    // A column is added for new key K if all following conditions are true:
    // - K isn't in ExcludeColumns
    // - number of existing columns doesn't reach MaxDims-1
    // - WhiteList is empty, or K matchs WhiteList
    // - BlackList is empty, or K doesn't match BlackList
    "dynamicSchema": {
      // whether enable this feature, default to false
      "enable": true,
      // the upper limit of dynamic columns number, <=0 means math.MaxInt16. protecting dirty data attack
      "maxDims": 1024,
      // the regexp of white list. syntax reference: https://github.com/google/re2/wiki/Syntax
      "whiteList": "^[0-9A-Za-z_]+$",
      // the regexp of black list
      "blackList": "@"
    },

    // additional fields to be appended to each input message, should be a valid json string
    // e.g. fields: "{\"Enable\":true,\"MaxDims\":0,\"Earliest\":false,\"Parser\":\"fastjson\"}"
    "fields": "",

    // PrometheusSchema expects each message is a Prometheus metric(timestamp, value, metric name and a list of labels).
    "prometheusSchema": true,
    // the regexp of labels black list, fields match promLabelsBlackList are not considered as part of labels column in series table
    // Requires PrometheusSchema be true.
    "promLabelsBlackList": "",

    // shardingKey is the column name to which sharding against
    "shardingKey": "",
    // shardingStripe take effect if the sharding key is numerical
    "shardingStripe": 0,

    // interval of flushing the batch. Default to 5, max to 600.
    "flushInterval": 5,
    // Approximate batch size to insert into clickhouse per shard, also control the kafka max.partition.fetch.bytes.
    // Sinker will round upward it to the the nearest 2^n. Default to 262114, max to 1048576.
    "bufferSize": 262114,

    // In the absence of time zone information, interprets the time as in the given location. Default to "Local" (aka /etc/localtime of the machine on which sinker runs)
    "timeZone": "",
    // Time unit when interprete a number as time. Default to 1.0.
    // Java's timestamp is milliseconds since epoch. Change timeUnit to 0.001 at this case.
    "timeUnit": 1.0
  },

  // log level, possible value: "debug", "info", "warn", "error", "dpanic", "panic", "fatal". Default to "info".
  "logLevel": "debug",
  // The Series table may contain hundreds of columns, and writing this table every time a datapoint is persisted can result in significant
  // performance overhead. This should be unnecessary since the lables from the same timeseries usually do not change(mid could be an exception).
  // Therefore, it would be reasonable to keep the map between "sid" and "mid" in cache to avoid frequent write operations. To optimize the memory
  // utilization, only active series from the last "activeSeriesRange" seconds will be cached, and the map in the cache will be updated every
  // "reloadSeriesMapInterval" seconds. By default, series from the last 24 hours will be cached, and the cache will be updated every hour.
	"reloadSeriesMapInterval": 3600,
	"activeSeriesRange": 86400
}
```
