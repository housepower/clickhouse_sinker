# basic configs
> Here we use json with comments for documentation


```
{
  // clickhouse configs, it's map[string]ClickHouse for multiple clickhouse
  "clickhouse": {
    // key for clickhouse config
    "ch1": {
      "db": "default",  // database name

      // hosts for connection, it's Array(Array(String))
      // we can put hosts with same shard into the inner array
      // it helps data deduplication for ReplicateMergeTree when driver error occurs
      "hosts": [
        [
          "127.0.0.1"
        ]
      ],
      "password": "",
      // retryTimes when error occurs in inserting datas
      "retryTimes": 0,
      "port": 9000,
      "username": "default"
    }
  },

  // kafka configs
  "kafka": {
    "kfk1": {
      "brokers": "127.0.0.1:9093",

      // somethings about sasl
      "sasl": {
        "enable": false,
        "password": "",
        "username": "",
        "gssapi": {
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
      "version": "2.2.1"
    }
  },

  "common": {
    // batch size to insert into clickhouse
    "bufferSize": 90000,
    // min batch size to insert into clickhouse
    "minBufferSize": 1,

    // msg bytes per message
    "msgSizeHint": 1000,

    // interval flush the batch
    "flushInterval": 5,

    // log level
    "logLevel": "debug"
  }
}
```