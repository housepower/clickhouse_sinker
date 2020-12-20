# Run

## Requirements

Note: we shall enable we have `clickhouse-server` and `kafka` envs, before running clickhouse_sinker.

## Configs

> There are two ways to pass the local config, multiple files and single file.

- For multiple files:

  `./clickhouse_sinker --local-cfg-dir conf`

  `conf` is the configuration directorys, and it'll read `conf/config.json` as main config and all tasks files in `conf/tasks/*.json` as task configs

- For single file:

  `./clickhouse_sinker --local-cfg-file config_single.json`.

> Read more detail descriptions of config in [here](docs/config.md)

## Example

* Let's use single file to test `clickhouse_sinker`

  - let's touch config.json
  `touch config.json`

  - put sample config into the config file

  ```
  {
    "clickhouse": {
      "default": {
        "db": "default",
        "hosts": [
          [
            "127.0.0.1"
          ]
        ],
        "port": 9000
      }
    },
    "kafka": {
      "default": {
        "brokers": "127.0.0.1:9092",
        "version": "2.2.1"
      }
    },
    "common": {
      "bufferSize": 90000,
      "minBufferSize": 1,
      "msgSizeHint": 1000,
      "flushInterval": 5,
      "logLevel": "debug"
    },
    "tasks": {
        "logstash": {
              "name" : "logstash",
              "kafkaClient": "kafka-go",
              "kafka": "default",
              "topic": "logstash",
              "consumerGroup" : "logstash_sinker",
              "parser" : "json",
              "clickhouse" : "default",
              "tableName" : "logstash",

              "autoSchema" : true,
              "@desc_of_exclude_columns" : "this columns will be excluded by insert SQL ",
              "excludeColumns" : ["day"]
        }
    }
  }
  ```

* Create a simple table in Clickhouse

  > It's not the duty for clickhouse_sinker to auto create table, so we should maually do that.

  ```
  CREATE TABLE logstash
  (
      `time` DateTime,
      `day` Date DEFAULT toDate(time),
      `request_uri` String,
      `age` UInt8
  )
  ENGINE = Memory

  Ok.

  0 rows in set. Elapsed: 0.014 sec.

  ```

* Enable topic is created in kafka

  > I use [kaf](https://github.com/birdayz/kaf) tool to create topics.

  ```
  kaf topic create logstash -p 1 -r 1
  ✅ Created topic!
        Topic Name:            logstash
        Partitions:            1
        Replication Factor:    1
        Cleanup Policy:        delete
  ```


* Run clickhouse_sinker

  ```
  ./clickhouse_sinker --local-cfg-file config.json
  ```


* Send messages to the topic

  ```
  echo '{"time" : "2020-12-18T03:38:39.000Z", "age" : 33 }' | kaf -b '127.0.0.1:9092' produce logstash
  echo '{"time" : "2020-12-18T03:38:39.000Z", "age" : 33 }' | kaf -b '127.0.0.1:9092' produce logstash
  echo '{"time" : "2020-12-18T03:38:39.000Z", "age" : 33 }' | kaf -b '127.0.0.1:9092' produce logstash
  ```

  - Check the data in clickhouse
  ```
  SELECT *
  FROM logstash

  ┌────────────────time─┬────────day─┬─request_uri─┬─age─┐
  │ 2020-12-18 11:38:39 │ 2020-12-18 │             │  33 │
  │ 2020-12-18 11:38:39 │ 2020-12-18 │             │  33 │
  │ 2020-12-18 11:38:39 │ 2020-12-18 │             │  33 │
  └─────────────────────┴────────────┴─────────────┴─────┘

  3 rows in set. Elapsed: 0.016 sec.

  ```
