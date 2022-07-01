package rcm

import (
	"context"

	"github.com/housepower/clickhouse_sinker/config"
	"github.com/housepower/clickhouse_sinker/input"
	"github.com/thanos-io/thanos/pkg/errors"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

type StateLag struct {
	State string
	Lag   int64
}

// GetTaskStateAndLags get state and lag of all tasks.
func GetTaskStateAndLags(cfg *config.Config) (stateLags map[string]StateLag, err error) {
	var cl *kgo.Client
	var adm *kadm.Client
	if cl, adm, err = newClient(cfg); err != nil {
		return
	}
	defer adm.Close()
	defer cl.Close()

	stateLags = make(map[string]StateLag)
	for _, taskCfg := range cfg.Tasks {
		var state string
		var totalLags int64
		if state, totalLags, err = getStateAndLag(adm, taskCfg.Topic, taskCfg.ConsumerGroup); err != nil {
			return
		}
		stateLags[taskCfg.Name] = StateLag{State: state, Lag: totalLags}
	}
	return
}

// GetTaskStateAndLag get state and lag of a task.
func GetTaskStateAndLag(cfg *config.Config, taskName string) (stateLag StateLag, err error) {
	var cl *kgo.Client
	var adm *kadm.Client
	if cl, adm, err = newClient(cfg); err != nil {
		return
	}
	defer adm.Close()
	defer cl.Close()

	var taskCfg *config.TaskConfig
	for _, tskCfg := range cfg.Tasks {
		if tskCfg.Name == taskName {
			taskCfg = tskCfg
			break
		}
	}
	if taskCfg == nil {
		err = errors.Newf("task %q doesn't exist", taskName)
		return
	}
	if stateLag.State, stateLag.Lag, err = getStateAndLag(adm, taskCfg.Topic, taskCfg.ConsumerGroup); err != nil {
		return
	}
	return
}

func newClient(cfg *config.Config) (cl *kgo.Client, adm *kadm.Client, err error) {
	var opts []kgo.Opt
	if opts, err = input.GetFranzConfig(&cfg.Kafka); err != nil {
		return
	}
	if cl, err = kgo.NewClient(opts...); err != nil {
		err = errors.Wrapf(err, "")
		return
	}
	adm = kadm.NewClient(cl)
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
