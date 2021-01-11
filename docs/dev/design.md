# Architecture

## Sharding with kafka message offset stripe (default)

clickhouse_sinker guarantee:

- at-least-once
- Duplicated messages (per topic-partition-offset) are routed to the same ClickHouse shard.

So if you setup ClickHouse properly(ReplacingMergeTree ORDER BY (__kafak_topic, __kafka_partition, __kafka_offset)), you could get exactly-once semantic.

It's hard for clickhouse_sinker to guarantee exactly-once semantic without ReplacingMergeTree. Kafka consumer group load-balance cause duplicated messages if one consumer quit suddenly.

The flow is:

- Fetch message via kafka-go or samara, which starts internally an goroutine for each partition.
- Parse messages in a global goroutine pool(pool size is customizable), fill the result to a ring according to the message's partition and offset.
- Generate a batch if messages in a ring reach a batchSize bondary, or flush timer fire. This ensures offset/batchSize be same for all messages inside a batch.
- Write batchs to ClickHouse in a global goroutine pool(pool size is fixed according to number of task and clickhouse shards). Batch is routed according to `(kafka_offset/roundup(batch_size))%clickhouse_shards`.

## Sharding with custom key and policy

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

