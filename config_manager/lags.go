package rcm

import (
	"context"
	"reflect"

	"github.com/housepower/clickhouse_sinker/config"
	"github.com/housepower/clickhouse_sinker/input"
	"github.com/housepower/clickhouse_sinker/statistics"
	"github.com/thanos-io/thanos/pkg/errors"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

var (
	theCl       *kgo.Client
	theAdm      *kadm.Client
	kafkaConfig *config.KafkaConfig
)

type StateLag struct {
	State string
	Lag   int64
}

// GetTaskStateAndLags get state and lag of all tasks.
func GetTaskStateAndLags(cfg *config.Config) (stateLags map[string]StateLag, err error) {
	kconf := cfg.Kafka
	if !reflect.DeepEqual(&kconf, kafkaConfig) {
		cleanupKafkaClient()
		if err = newClient(cfg.Kafka); err != nil {
			return
		}
		kafkaConfig = &kconf
	}

	stateLags = make(map[string]StateLag, len(cfg.Tasks))
	for _, taskCfg := range cfg.Tasks {
		var state string
		var totalLags int64
		if state, totalLags, err = getStateAndLag(theAdm, taskCfg.Topic, taskCfg.ConsumerGroup); err != nil {
			return
		}
		stateLags[taskCfg.Name] = StateLag{State: state, Lag: totalLags}
		statistics.ConsumeLags.WithLabelValues(taskCfg.ConsumerGroup, taskCfg.Topic, taskCfg.Name).Set(float64(totalLags))
	}
	return
}

func cleanupKafkaClient() {
	if theCl != nil {
		theCl.Close()
		theCl = nil
	}
	if theAdm != nil {
		theAdm.Close()
		theAdm = nil
	}
}

func newClient(cfg config.KafkaConfig) (err error) {
	var opts []kgo.Opt
	if opts, err = input.GetFranzConfig(&cfg); err != nil {
		return
	}
	// franz.config.go 379 - invalid autocommit options specified when a group was not specified
	if theCl, err = kgo.NewClient(opts...); err != nil {
		err = errors.Wrapf(err, "")
		return
	}
	theAdm = kadm.NewClient(theCl)
	return
}

// getStateAndLag is inspired by https://github.com/cloudhut/kminion/blob/1ffd02ba94a5edc26d4f11e57191ed3479d8a111/prometheus/collect_consumer_group_lags.go
func getStateAndLag(adm *kadm.Client, topic, group string) (state string, totalLags int64, err error) {
	ctx := context.Background()
	var ok bool
	var descGroups kadm.DescribedGroups
	var descGroup kadm.DescribedGroup
	if descGroups, err = adm.DescribeGroups(ctx, group); err != nil {
		err = errors.Wrapf(err, "")
		return
	}
	if descGroup, ok = descGroups[group]; ok {
		state = descGroup.State
	} else {
		state = "NA"
	}
	var commit kadm.OffsetResponses
	if commit, err = adm.FetchOffsets(ctx, group); err != nil {
		err = errors.Wrapf(err, "")
		return
	}
	var offsets kadm.ListedOffsets
	if offsets, err = adm.ListEndOffsets(ctx, topic); err != nil {
		err = errors.Wrapf(err, "")
		return
	}
	grpLag := kadm.CalculateGroupLag(descGroup, commit, offsets)
	if topLag, ok := grpLag[topic]; ok {
		for _, grpMemberLag := range topLag {
			if grpMemberLag.Lag >= 0 {
				totalLags += grpMemberLag.Lag
			}
		}
	}
	return
}
