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

Every clickhouse_sinker instance runs all tasks in its config. To scale out, start more instances with the same config: instances consuming the same task share its consumer group, and Kafka balances the topic partitions among them. A heavy task can therefore be spread over as many instances as its topic has partitions.

clickhouse_sinker used to register instances with Nacos (`--nacos-service-name`) and let one instance assign each task to exactly one instance by message lag. That has been removed:

- A task could only run on one instance, so a task too heavy for a single instance had no way to scale out.
- Every re-assignment (config change, instance up/down, lag timer) restarted consumers, which triggered Kafka consumer group rebalances on top of Kafka's own balancing.

`--nacos-service-name` and the config fields `assignment`, `kafka.assignInterval`, `kafka.calcLagInterval` and `kafka.rebalanceByLags` are still accepted but ignored, with a warning at startup. Nacos is still supported as a config source (`--nacos-dataid`).
