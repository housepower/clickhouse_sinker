package rcm

import (
	"strings"

	"github.com/Shopify/sarama"
	"github.com/housepower/clickhouse_sinker/config"
	"github.com/housepower/clickhouse_sinker/input"
	"github.com/housepower/clickhouse_sinker/util"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

// GetTaskLags inspired by https://github.com/cloudhut/kminion/blob/1ffd02ba94a5edc26d4f11e57191ed3479d8a111/prometheus/collect_consumer_group_lags.go
func GetTaskLags(cfg *config.Config) (taskLags map[string]int64, err error) {
	var adminClient sarama.ClusterAdmin
	var client sarama.Client
	var sarCfg *sarama.Config
	taskLags = make(map[string]int64) // taskName -> totalLags
	if sarCfg, err = input.GetSaramaConfig(&cfg.Kafka); err != nil {
		return
	}
	if adminClient, err = sarama.NewClusterAdmin(strings.Split(cfg.Kafka.Brokers, ","), sarCfg); err != nil {
		return
	}
	if client, err = sarama.NewClient(strings.Split(cfg.Kafka.Brokers, ","), sarCfg); err != nil {
		return
	}
	defer func() {
		if err2 := client.Close(); err2 != nil {
			util.Logger.Error("failed to close Kafka client", zap.Error(err2))
		}
		if err2 := adminClient.Close(); err2 != nil {
			util.Logger.Error("failed to close Kafka admin client", zap.Error(err2))
		}
	}()

	// Get topics' partition id list
	var topics []string
	topicPartitions := make(map[string]int) //topic -> number of partitions
	for _, taskCfg := range cfg.Tasks {
		topicPartitions[taskCfg.Topic] = 0
	}
	for topic := range topicPartitions {
		topics = append(topics, topic)
	}
	var topicsMeta []*sarama.TopicMetadata
	if topicsMeta, err = adminClient.DescribeTopics(topics); err != nil {
		return
	}
	for i, topicMeta := range topicsMeta {
		if topicMeta != nil {
			topicPartitions[topics[i]] = len(topicMeta.Partitions)
		}
	}

	// Get partitions' oldest and newest offset
	topicOldestOffsets := make(map[string][]int64) //topic -> list of partitions' oldestOffset
	topicNewestOffsets := make(map[string][]int64) //topic -> list of partitions' newestOffset
	for _, topic := range topics {
		if partitions, ok := topicPartitions[topic]; ok {
			var oldestOffsets, newestOffsets []int64
			var oldestOffset, newestOffset int64
			for partition := 0; partition < partitions; partition++ {
				oldestOffset, err = client.GetOffset(topic, int32(partition), sarama.OffsetOldest)
				if err != nil {
					err = errors.Wrapf(err, "failed to get topic/partition offsets for %q partition %q", topic, partition)
					return
				}
				newestOffset, err = client.GetOffset(topic, int32(partition), sarama.OffsetNewest)
				if err != nil {
					err = errors.Wrapf(err, "failed to get topic/partition offsets for %q partition %q", topic, partition)
					return
				}
				oldestOffsets = append(oldestOffsets, oldestOffset)
				newestOffsets = append(newestOffsets, newestOffset)
			}
			topicOldestOffsets[topic] = oldestOffsets
			topicNewestOffsets[topic] = newestOffsets
		}
	}

	// Get consumer groups' offset
	for _, taskCfg := range cfg.Tasks {
		topic := taskCfg.Topic
		var totalLags int64
		oldestOffsets := topicOldestOffsets[topic]
		newestOffsets := topicNewestOffsets[topic]
		if partitions, ok := topicPartitions[topic]; ok {
			pidList := make([]int32, partitions)
			for partition := 0; partition < partitions; partition++ {
				pidList[partition] = int32(partition)
			}
			var rep *sarama.OffsetFetchResponse
			if rep, err = adminClient.ListConsumerGroupOffsets(taskCfg.ConsumerGroup, map[string][]int32{topic: pidList}); err != nil {
				for partition := 0; partition < partitions; partition++ {
					totalLags += newestOffsets[partition] - oldestOffsets[partition] + 1
				}
			} else {
				for partition := 0; partition < partitions; partition++ {
					block := rep.GetBlock(topic, int32(partition))
					lag := newestOffsets[partition] - block.Offset - 1
					if lag > 0 {
						totalLags += lag
					}
				}
			}
			taskLags[taskCfg.Name] = totalLags
		}
	}
	return
}
