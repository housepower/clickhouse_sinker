# Run

## Requirements

Note: Ensure `clickhouse-server` and `kafka` work before running clickhouse_sinker.

## Configs

There are two ways to get config: a local single config, or a config center (Nacos, etcd or Apollo).

- For local file:

  `clickhouse_sinker --local-cfg-file docker/test_auto_schema.hjson`

- For Nacos:

  `clickhouse_sinker --nacos-addr 127.0.0.1:8848 --nacos-username nacos --nacos-password nacos --nacos-dataid test_auto_schema`

- For etcd (the key must exist, e.g. `etcdctl put /clickhouse_sinker/prod "$(cat conf.hjson)"`):

  `clickhouse_sinker --etcd-endpoints 10.0.0.1:2379,10.0.0.2:2379 --etcd-key /clickhouse_sinker/prod`

  TLS: `--etcd-ca-file` alone for one-way TLS, plus `--etcd-cert-file` and `--etcd-key-file` for mutual TLS.

- For Apollo, put the whole config into a txt or json namespace:

  `clickhouse_sinker --apollo-meta http://apollo-config:8080 --apollo-app-id sinker --apollo-namespace clickhouse_sinker.txt`

  ClickHouse cluster discovery writes `clickhouse.hosts` back to the config center. With Apollo this needs the Portal
  Open API: `--apollo-portal`, `--apollo-token`, `--apollo-env` and `--apollo-operator`, all four or none. The token's
  third-party app needs modify and release permission on the namespace, the operator must be an existing Portal user,
  and write-back cannot work when the namespace requires different users to modify and release. Write-back is skipped,
  with a warning, while the namespace has unreleased edits.

Only one of `--nacos-dataid`, `--etcd-key` and `--apollo-namespace` may be set.

Every option can also be given as an environment variable: upper case, `-` replaced by `_`, prefixed with `SINKER_`,
e.g. `SINKER_ETCD_KEY`. Options that existed before the prefix also accept the unprefixed name, e.g. `NACOS_ADDR`.

Write-back stores the whole config as indented JSON (valid hjson too), so comments and formatting in the config center are lost.

Nacos enforces the compare-and-swap (`casMd5`) used by write-back only when it runs with an external MySQL datasource.
Standalone Nacos with the embedded Derby storage ignores `casMd5`, so there write-back is best-effort (read the latest
config, then write; the window is milliseconds), like Apollo.

Read more detail descriptions of config in [here](../configuration/config.html)

## Example

Let's follow up a piece of the systest script.

* Prepare

  - let's checkout `clickhouse_sinker`

  ```bash
  git clone https://github.com/housepower/clickhouse_sinker.git
  cd clickhouse_sinker
  ```

  - let's start standalone clickhouse-server and kafka in container:

  ```bash
  docker compose up -d
  ```
* Connect to the ClickHouse server

  ```bash
  docker exec -it clickhouse_sinker-clickhouse-1 clickhouse client
  ```

* Create a simple table in Clickhouse

  > It's not the duty for clickhouse_sinker to auto create table, so we should do that manually.

  ```sql
  CREATE TABLE IF NOT EXISTS test_auto_schema
  (
      `day` Date DEFAULT toDate(time),
      `time` DateTime,
      `name` String,
      `value` Float64
  )
  ENGINE = MergeTree
  PARTITION BY day
  ORDER BY (time, name);
  ```

* Create a topic in kafka

  > I use [kaf](https://github.com/birdayz/kaf) tool to create topics.

  ```bash
  kaf topic create topic1 -p 1 -r 1
  ```

  Output:
  ```bash
  ✅ Created topic!
        Topic Name:            topic1
        Partitions:            1
        Replication Factor:    1
        Cleanup Policy:        delete
  ```


* Run clickhouse_sinker

  ```bash
  ./bin/clickhouse_sinker --local-cfg-file docker/test_auto_schema.hjson
  ```


* Send messages to the topic

  ```bash
  echo '{"time" : "2020-12-18T03:38:39.000Z", "name" : "name1", "value" : 1}' | kaf -b '127.0.0.1:9092' produce topic1
  echo '{"time" : "2020-12-18T03:38:39.000Z", "name" : "name2", "value" : 2}' | kaf -b '127.0.0.1:9092' produce topic1
  echo '{"time" : "2020-12-18T03:38:39.000Z", "name" : "name3", "value" : 3}' | kaf -b '127.0.0.1:9092' produce topic1
  ```

* Check the data in clickhouse

  ```sql
  SELECT count() FROM test_auto_schema;
  ```
  Output:
  ```bash
  3 rows in set. Elapsed: 0.016 sec.
  ```

## Run as a daemon

On systemd managed Linux OSs such as RHEL, Debian and their variants, it's doable to run `clickhouse_sinker` as a system service to achieve auto-restart, coredump management etc.

### Create `/etc/systemd/system/sinker_metric.service`

```
[Unit]
Description=ck-sink-metric
Requires=network-online.target
After=network-online.target

[Service]
Type=simple
User=eoi
LimitCORE=infinity
Environment="GOTRACEBACK=crash"
ExecStart=/data02/app/sinker/sinker/clickhouse_sinker --local-cfg-file=/data02/app/sinker/sinker/ck-sink-metric.json --log-paths=/data02/app/sinker/sinker/logs/sinker_metric.log
Restart=on-failure
RestartSec=3s
StartLimitInterval=0

[Install]
WantedBy=multi-user.target
```

Note:

- Change pathes in `ExecStart` as necessary.
- `User=eoi` means to run service as non-root for security reason.
- `LimitCORE=infinity` for service is equivalent to `ulimit -c unlimited` for non-service.
- env `GOTRACEBACK=crash` is required for Go applications to dump core. Refers to `https://pkg.go.dev/runtime`.

### Modify `/etc/sysctl.conf`

```kernel.core_pattern = |/usr/lib/systemd/systemd-coredump %p %u %g %s %t```

Run `sysctl -p`.

### Modify `etc/systemd/coredump.conf`

```
[Coredump]
ProcessSizeMax=50G
ExternalSizeMax=50G
```

### Manage `clickhouse-sinker` service

- To start, `systemctl start sinker_metric`
- To stop, `systemctl stop sinker_metric`
- To view status, `systemctl status sinker_metric`

### Manage coredumps with `coredumpctl`

Coredumps are stored under `/var/lib/systemd/coredump`.
Refers to core(5), systemd.exec(5), systemd-coredump(8), coredump.conf(5).
