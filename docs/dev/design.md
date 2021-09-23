# Architecture

## Sharding
### Sharding with kafka message offset stripe (default)

clickhouse_sinker guarantee:

- at-least-once
- Duplicated messages (per topic-partition-offset) are routed to the same ClickHouse shard.

So if you setup ClickHouse properly(ReplacingMergeTree ORDER BY (__kafak_topic, __kafka_partition, __kafka_offset)), you could get exactly-once semantic.

It's hard for clickhouse_sinker to guarantee exactly-once semantic without ReplacingMergeTree. Kafka consumer group load-balance cause duplicated messages if one consumer quit suddenly.

The flow is:

- Fetch message via kafka-go or samara, which starts internally an goroutine for each partition.
- Parse messages in a global goroutine pool(pool size is customizable), fill the result to a ring according to the message's partition and offset.
- Generate a batch if messages in a ring reach a batchSize bondary, or flush timer fire. This ensures offset/batchSize be same for all messages inside a batch.
- Write batchs to ClickHouse in a global goroutine pool(pool size is fixed according to number of task and clickhouse shards). Batch is routed according to `(kafka_offset/roundup(buffer_size))%clickhouse_shards`.

### Sharding with custom key and policy

clickhouse_sinker guarantee:

- at-least-once
- Every message is routed to the determined (per `shardingKey` and `shardingPolicy`) ClickHouse shard.

`shardingKey` value is a column name. `shardingPolicy` value is `stripe,<size>` or `hash`.
The hash function used internally is xxHash64.

The flow is:

- Fetch message via kafka-go or samara, which starts internally an goroutine for each partition.
- Parse messages in a global goroutine pool(pool size is customizable), fill the result to a ring according to the message's partition and offset.
- Shard messages in a ring if reach a batchSize bondary, or flush timer fire. There's one-to-one relationship between shard slots and ClickHouse shards.
- Generate batches for all shard slots if messages in one shard slot reach batchSize, or flush timer fire. Those batches form a `BatchGroup`. The `before` relationship could be impossilbe if messages of a partition are distributed to multiple batches. So those batches need to be committed after ALL of them have been written to clickhouse.
- Write batchs to ClickHouse in a global goroutine pool(pool size is fixed according to number of task and clickhouse shards).


## Task scheduling

The clickhouse-server configuration item `max_concurrent_queries`(default 100) is the maximum number of simultaneously processed queries related to MergeTree table. If the number of concurrent INSERT is close to `max_concurrent_queries`, the user queries(`SELECT`) could fail due to the limit.

If the clickhouse-server is big, ingesting data to >=100 MergeTree tabls via clickhouse_sinker bring pressure to the clickhouse cluster. On the other side, large number of clickhouse_sinker instances requires lot of CPU/MEM resources.

The solution is, clickhouse_sinker instances coordinate with each other to assign tasks among themselves.

The task scheduling procedure:

- Some platform(Kubernetes, Yarn and etc.) start several clickhouse_sinker instances and may start/stop  instances dynamically. Every clickhouse_sinker instance register with Nacos as a single service(CLI option `--nacos-service-name`).
- Someone publish(add/delete/modify) a list of tasks(with empty assignment) to Nacos.
- The first clickhouse_sinker(per instance's ip+port) instance(named scheduler) is responsible to generate and publish task assignment regularly. The task list and assignment consist of the whole config. The task list change, service change and task lag change will trigger another assignment. The scheduler ensure Each clickhouse_innker instance's total lag be balanced.
- Each clickhouse_sinker reload the config regularly. This may start/stop tasks. clickhouse_sinker stop tasks gracefully so that there's no message lost/duplication during task transfering.
