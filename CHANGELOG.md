# Changelog

#### Version 3.0.8 (2023-08-xx)

Improvements:

- "too many simultaneous queries" from ClickHouse will no longer be a fatal error, instead sinker will keep retrying untill complete the write operation
- Output the offset range per batch

New Features:

- Introduced a task level configuration property "field", which accepts a valid json string and allows additional
fields to be appened to each input message.

#### Version 3.0.7 (2023-07-19)

Improvements:

- when run sinker in cluster mode, caculate lag failure of certain task should not block the entire rebalance work. Instead, the current task should not be assigned to any running sinker instance.
- sinker will not crash when handling incompatible map type.

#### Version 3.0.6 (2023-05-19)

Improvements:

- When idenfying new columns, also update the schema of tables that are part of a logical cluster
- Allow writing non-numeric type metric

#### Version 3.0.5 (2023-05-12)

New Features:

- Introduced two new configuration properties to Sinker:
  - "ReloadSeriesMapInterval", allows you to manage how frequently the cached seriesMap is reloaded.
  - "ActiveSeriesRange", allows you to manage the amount of cached information.
    Refer to the "config.md" file for more info.
- Add a new http endpoint "/debug/vars" for viewing contents of seriesMap

Deprecations:

- DialTimeout option is deprecated.

#### Version 3.0.4 (2023-04-18)

Improvements:

- Automatically end sinker if it remains inactive in last 10 mins

#### Version 3.0.3 (2023-03-30)

Improvements:

- Have writingpool per shard to avoid ErrAcquireConnTimeout
- Do not create kafka client everytime when caculate lags
- Support configuring PlainloginModule in kafka.security section

Bug Fixes:

- Avoid program running into stuck when facing fatalpanic

#### Version 3.0.2 (2023-03-13)

Improvements:

- update sinker grafana dashboard
- combine nacos log into sinker log
- update dmseries map when applying new config, reload the records from series table every single day
- avoid recreating dist tables, alter the table schema instead
- update clickhouse_sinker_consume_lags metric every 10 secs

#### Version 3.0.1 (2023-03-03)

Bug Fixes:

- Fix nacos publish config error "BUG: got different config"
- Fix changing "TimeUnit" config property does not trigger config reload
- Fix illegal "TimeZone" value result in sinker crash
- Fix wrong parsing result of Decimal type [909](https://github.com/ClickHouse/clickhouse-go/pull/909)

Improvements:

- Metrics from GoCollector and ProcessCollector are now being pushed to metric-push-gateway-addrs
- Terminate program immediately when receiving one more exit signal
- Limit the fetch size and poll size based on the BufferSize config property

#### Version 3.0.0 (2023-02-07)

New Features:

- Add support of ingesting multi-value metrics, the metric table will be expanded accordingly
- Allow specifying the series table name
- Allow customization of DatabaseName in task level

Improvements:

- Group the tasks by consumerGroup property to reduce number of kafka client, see design.md for details

Deprecation:

- Kafka-go and Sarama are no longer internal options for sinker
- 

#### Version 2.6.9 (2023-02-07)

Improvements:

- Ignore SIGHUP signal, so that fire up sinker with nohup could work correctly
- Stop retrying when facing offsets commit error, leave it to the future commitment to sync the offsets
- Offsets commit error should not result in a process abort

#### Version 2.6.8 (2022-12-10)

New Features:

- Add clickhouse Map type support
- Small updates to allow TLS connections for AWS MSK, etc.
  ([169](https://github.com/housepower/clickhouse_sinker/pull/169))

Bug Fixes:

- Fix ClickHouse.Init goroutine leak

#### Version 2.6.7 (2022-12-07)

Improvements:

- Add new sinker metrics to show the wrSeriesQuota status
- Always allow writing new series to avoid data mismatch between series and metrics table

#### Version 2.6.6 (2022-12-05)

Bug Fixes:

- reset wrSeries timely to avoid failure of writing metric data to clickhouse

#### Version 2.6.5 (2022-11-30)

Bug Fixes:

- Fix the 'segmentation violation' in ch-go package
- Fix the create table error 'table already exists' when trying to create a distribution table

#### Previous releases

See https://github.com/housepower/clickhouse_sinker/releases
