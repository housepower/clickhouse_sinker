# Architecture

## Sharding

clickhouse_sinker guarantee:

- at-least-once
- Duplicated messages (per topic-partition-offset) are routed to the same ClickHouse shard.

So if you setup ClickHouse properly(ReplacingMergeTree ORDER BY (__kafak_topic, __kafka_partition, __kafka_offset)), you could get exactly-once semantic.

It's hard for clickhouse_sinker to guarantee exactly-once semantic without ReplacingMergeTree. Kafka consumer group load-balance cause duplicated messages if one consumer crash suddenly.

## Workflow

Internally, clickhouse_sinker groups tasks that with identical "consumerGroup" property set together for the purpose of reducing the number of Kafka client, so that Kafka server is able to handle more requests concurrently. And consequently, it's decided to commit Kafka offset only after messages in a whole fetch got written to clickhouse completely.

The flow is like this:

- Group tasks with identical "consumerGroup" property together, fetch messages for the group of tasks with a single goroutine.
- Route fetched messages to the individual tasks for further parsing. By default, the mapping between messages and tasks is controlled by "topic" and "tableName" property. But for messages with Kafka header "__table_name" specified, the mapping between "__table_name" and "tableName" will override the default behavior.
- Parse messages and calculate the dest shard:
-- For tasks with "shardingkey" property specified, if the sharding key is numerical(integer, float, time, etc.), the dest shard is determined by `(shardingKey/shardingStripe)%clickhouse_shards`, if not, it is determined by `xxHash64(shardingKey)%clickhouse_shards`.
-- Otherwise, the dest shard for each message is determined by `(kafka_offset/roundup(buffer_size))%clickhouse_shards`.
- Generate batches for all shard slots that are in the same Group, when total cached message count in the Group reached `sum(batchSize)*80%` boundary or flush timer fire.
- Write batches to ClickHouse in a global goroutine pool(pool size is a fixed number based on the number of tasks and Clickhouse shards).
- Commit offset back to Kafka


## Task scheduling

The clickhouse-server configuration item `max_concurrent_queries`(default 100) is the maximum number of simultaneously processed queries related to MergeTree table. If the number of concurrent INSERT is close to `max_concurrent_queries`, the user queries(`SELECT`) could fail due to the limit.

If the clickhouse-server is big, ingesting data to >=100 MergeTree tables via clickhouse_sinker bring pressure to the Clickhouse cluster. On the other side, large number of clickhouse_sinker instances requires lots of CPU/MEM resources.

The solution is, clickhouse_sinker instances coordinate with each other to assign tasks among themselves.

The task scheduling procedure:

- Some platform(Kubernetes, Yarn, etc.) start several clickhouse_sinker instances and may start/stop instances dynamically. Every clickhouse_sinker instance registers with Nacos as a single service(CLI option `--nacos-service-name`).
- Someone publish(add/delete/modify) a list of tasks(with empty assignment) to Nacos.
- The first clickhouse_sinker(per instance's ip+port) instance(named scheduler) is responsible to generate and publish task assignments regularly. The task list and assignment consist of the whole config. The task list change, service change, and task lag change will trigger another assignment. The scheduler ensures Each clickhouse_innker instance's total lag be balanced.
- Each clickhouse_sinker reloads the config regularly. This may start/stop tasks. clickhouse_sinker stops tasks gracefully so that there's no message lost/duplication during task transferring.
