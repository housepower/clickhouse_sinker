# Architecture

## Sharding

clickhouse_sinker guarantee:

- at-least-once
- Duplicated messages (per topic-partition-offset) are routed to the same ClickHouse shard.

So if you setup ClickHouse properly(ReplacingMergeTree ORDER BY (__kafak_topic, __kafka_partition, __kafka_offset)), you could get exactly-once semantic.

It's hard for clickhouse_sinker to guarantee exactly-once semantic without ReplacingMergeTree. Kafka consumer group load-balance cause duplicated messages if one consumer crash suddenly.

### Sharding with kafka message offset stripe (default)

The flow is:

- Fetch message via Franz, Sarama, or kafka-go, which starts internally a goroutine for each partition.
- Parse messages in a global goroutine pool(pool size is customizable), fill the result into a ring according to the message's partition and offset.
- Generate a batch when messages in a ring reach a batchSize boundary or flush timer fire. For each message, the dest shard is determined by `(kafka_offset/roundup(buffer_size))%clickhouse_shards`.
- Write batch to ClickHouse in a global goroutine pool(pool size is fixed according to the number of tasks and Clickhouse shards).

### Sharding with custom key

The flow is:

- Fetch message via kafka-go or samara, which starts internally a goroutine for each partition.
- Parse messages in a global goroutine pool(pool size is customizable), fill the result into a ring according to the message's partition and offset.
- Shard messages in a ring when messages reach a batchSize boundary or flush timer fire. For each message, if the sharding key is numerical(integer, float, time, etc.), the dest shard is determined by `(shardingKey/shardingStripe)%clickhouse_shards`, otherwise it is determined by `xxHash64(shardingKey)%clickhouse_shards`.
- Generate batches for all shard slots if messages in one shard slot reach batchSize boundary or flush timer fire. Those batches form a `BatchGroup`. The `before` relationship could be impossible if messages of a partition are distributed to multiple batches. So those batches need to be committed after ALL of them have been written to Clickhouse.
- Write batch to ClickHouse in a global goroutine pool(pool size is fixed according to the number of tasks and Clickhouse shards).


## Task scheduling

The clickhouse-server configuration item `max_concurrent_queries`(default 100) is the maximum number of simultaneously processed queries related to MergeTree table. If the number of concurrent INSERT is close to `max_concurrent_queries`, the user queries(`SELECT`) could fail due to the limit.

If the clickhouse-server is big, ingesting data to >=100 MergeTree tables via clickhouse_sinker bring pressure to the Clickhouse cluster. On the other side, large number of clickhouse_sinker instances requires lots of CPU/MEM resources.

The solution is, clickhouse_sinker instances coordinate with each other to assign tasks among themselves.

The task scheduling procedure:

- Some platform(Kubernetes, Yarn, etc.) start several clickhouse_sinker instances and may start/stop instances dynamically. Every clickhouse_sinker instance registers with Nacos as a single service(CLI option `--nacos-service-name`).
- Someone publish(add/delete/modify) a list of tasks(with empty assignment) to Nacos.
- The first clickhouse_sinker(per instance's ip+port) instance(named scheduler) is responsible to generate and publish task assignments regularly. The task list and assignment consist of the whole config. The task list change, service change, and task lag change will trigger another assignment. The scheduler ensures Each clickhouse_innker instance's total lag be balanced.
- Each clickhouse_sinker reloads the config regularly. This may start/stop tasks. clickhouse_sinker stops tasks gracefully so that there's no message lost/duplication during task transferring.
