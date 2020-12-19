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


# Remote Config Management

All clickhouse_sinker instances share(fetched from Nacos, Consul, ZK etc.) Config structure:

```
type Config struct {
	Kafka      map[string]*KafkaConfig
	Clickhouse map[string]*ClickHouseConfig
	Tasks []*TaskConfig
	Common struct {
		...
		Replicas          int //on how many sinker instances a task runs
	}
	Assigns map[string][]string //map instance_name to a list of task_name
}
type TaskConfig struct {
	...
	Replicas         int    //on how many sinker instances this task runs
}

```

Each instance can run multiple tasks.
Each task can be assigned to multiple instances. Each task declares how many instances it needs.

## The coordinator(outside this project)

- The coordinator provides API and/or webui to add/delete/modify tasks.
- The coordinator watches (do `service discovery`) instance startup/disappear events, and assign tasks to instances (do `publish config`). Refers to `cmd/nacos_publish_config/main.go` to assign tasks (from local config) via consistent-hash to instances(from CLI).

## The schedule platform(outside this project)
The schedule platform start some clickhouse_sinker instances and start another one if a instance fail.

## clickhouse_sinker

- Every clickhouse_sinker instance register itself to a service manager (Nacos, Consul, ZK etc.).
- clickhouse_sinker watches (do `get config`) cofig, compare its assignment with the current one, apply changes.
- clickhouse_sinker shall try to recover a task in endless loop if the task fail. This ensures the tasks run on an instance match the coordinator assigned.