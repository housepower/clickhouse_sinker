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

type StateLag struct {
	State string
	Lag   int64
}

// GetTaskStateAndLags get state and lag of all tasks.
func GetTaskStateAndLags(cfg *config.Config) (stateLags map[string]StateLag, err error) {
	var client sarama.Client
	var adminClient sarama.ClusterAdmin
	var closer func()
	if client, adminClient, closer, err = newClient(cfg); err != nil {
		return
	}
	defer closer()

	stateLags = make(map[string]StateLag)
	for _, taskCfg := range cfg.Tasks {
		var totalLags int64
		if totalLags, err = getLag(client, adminClient, taskCfg.Topic, taskCfg.ConsumerGroup); err != nil {
			return
		}
		stateLags[taskCfg.Name] = StateLag{Lag: totalLags}
	}

	// Get consumer groups' state
	groups := make([]string, len(cfg.Tasks))
	for i, taskCfg := range cfg.Tasks {
		groups[i] = taskCfg.ConsumerGroup
	}
	var gd []*sarama.GroupDescription
	if gd, err = adminClient.DescribeConsumerGroups(groups); err != nil {
		err = errors.Wrapf(err, "failed to describe consumer groups")
		return
	}
	for i, taskCfg := range cfg.Tasks {
		if stateLag, ok := stateLags[taskCfg.Name]; ok {
			stateLag.State = gd[i].State
			stateLags[taskCfg.Name] = stateLag
		}
	}
	return
}

// GetTaskStateAndLag get state and lag of a task.
func GetTaskStateAndLag(cfg *config.Config, taskName string) (stateLag StateLag, err error) {
	var client sarama.Client
	var adminClient sarama.ClusterAdmin
	var closer func()
	if client, adminClient, closer, err = newClient(cfg); err != nil {
		return
	}
	defer closer()

	var taskCfg *config.TaskConfig
	for _, tskCfg := range cfg.Tasks {
		if tskCfg.Name == taskName {
			taskCfg = tskCfg
			break
		}
	}
	if taskCfg == nil {
		err = errors.Errorf("task %q doesn't exist", taskName)
		return
	}
	if stateLag.Lag, err = getLag(client, adminClient, taskCfg.Topic, taskCfg.ConsumerGroup); err != nil {
		return
	}

	// Get consumer group' state
	var gd []*sarama.GroupDescription
	if gd, err = adminClient.DescribeConsumerGroups([]string{taskCfg.ConsumerGroup}); err != nil {
		err = errors.Wrapf(err, "failed to describe consumer groups")
		return
	}
	stateLag.State = gd[0].State
	return
}

func newClient(cfg *config.Config) (client sarama.Client, adminClient sarama.ClusterAdmin, closer func(), err error) {
	var sarCfg *sarama.Config
	if sarCfg, err = input.GetSaramaConfig(&cfg.Kafka); err != nil {
		return
	}
	if adminClient, err = sarama.NewClusterAdmin(strings.Split(cfg.Kafka.Brokers, ","), sarCfg); err != nil {
		err = errors.Wrapf(err, "sarama.NewClusterAdmin failed")
		return
	}
	if client, err = sarama.NewClient(strings.Split(cfg.Kafka.Brokers, ","), sarCfg); err != nil {
		err = errors.Wrapf(err, "sarama.NewClient failed")
		return
	}
	closer = func() {
		if err2 := client.Close(); err2 != nil {
			util.Logger.Error("failed to close Kafka client", zap.Error(err2))
		}
		if err2 := adminClient.Close(); err2 != nil {
			util.Logger.Error("failed to close Kafka admin client", zap.Error(err2))
		}
	}
	return
}

// getLag is inspired by https://github.com/cloudhut/kminion/blob/1ffd02ba94a5edc26d4f11e57191ed3479d8a111/prometheus/collect_consumer_group_lags.go
func getLag(client sarama.Client, adminClient sarama.ClusterAdmin, topic, group string) (totalLags int64, err error) {
	// Get number of partitions
	var partitions int
	var topicsMeta []*sarama.TopicMetadata
	if topicsMeta, err = adminClient.DescribeTopics([]string{topic}); err != nil {
		err = errors.Wrapf(err, "failed to describe topic %q", topic)
		return
	}
	partitions = len(topicsMeta[0].Partitions)

	// Get partitions' oldest and newest offset
	var oldestOffsets, newestOffsets []int64
	var oldestOffset, newestOffset int64
	for partition := 0; partition < partitions; partition++ {
		oldestOffset, err = client.GetOffset(topic, int32(partition), sarama.OffsetOldest)
		if err != nil {
			err = errors.Wrapf(err, "failed to get oldest offset for topic %q partition %q", topic, partition)
			return
		}
		newestOffset, err = client.GetOffset(topic, int32(partition), sarama.OffsetNewest)
		if err != nil {
			err = errors.Wrapf(err, "failed to get newest offset for topic %q partition %q", topic, partition)
			return
		}
		oldestOffsets = append(oldestOffsets, oldestOffset)
		newestOffsets = append(newestOffsets, newestOffset)
	}

	// Get consumer group' offset, calculate lag
	pidList := make([]int32, partitions)
	for partition := 0; partition < partitions; partition++ {
		pidList[partition] = int32(partition)
	}
	var rep *sarama.OffsetFetchResponse
	if rep, err = adminClient.ListConsumerGroupOffsets(group, map[string][]int32{topic: pidList}); err != nil {
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
	return
}
