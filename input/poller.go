package input

import (
	"sync"
	"time"

	"github.com/housepower/clickhouse_sinker/util"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
)

var consumerPoller sync.Map

type Poller struct {
	consumerName string
	client       *kgo.Client
	active       time.Time
}

func NewConsumerPoller(consumerId, consumerName string, client *kgo.Client) {
	util.Logger.Info("new consumer poller", zap.String("consumerId", consumerId))
	consumerPoller.Store(consumerId, Poller{
		consumerName: consumerName,
		client:       client,
		active:       time.Now()})
}

func OnConsumerPoll(consumerId string) {
	if v, ok := consumerPoller.Load(consumerId); ok {
		poller := v.(Poller)
		poller.active = time.Now()
		util.Logger.Debug("consumer poller active", zap.String("consumerId", consumerId), zap.String("consumerName", poller.consumerName), zap.Time("active", poller.active))
		consumerPoller.Store(consumerId, poller)
	} else {
		util.Logger.Warn("consumer poller not found", zap.String("consumerId", consumerId))
	}
}

func Leave(consumerId string) {
	consumerPoller.Delete(consumerId)
}

// Walk removes every consumer that has not polled within maxPollInterval ms
// and returns their consumer group names; the caller restarts them.
func Walk(maxPollInterval int) []string {
	var expired []string
	util.Logger.Debug("consumer poller walk started")
	consumerPoller.Range(func(k, v any) bool {
		poller := v.(Poller)
		util.Logger.Debug("consumer poller walked", zap.String("consumerId", k.(string)),
			zap.String("consumerName", poller.consumerName),
			zap.Time("active", poller.active))
		if time.Since(poller.active) > time.Duration(maxPollInterval)*time.Millisecond {
			util.Logger.Warn("consumer group expired", zap.String("consumerId", k.(string)), zap.String("consumerName", poller.consumerName))
			expired = append(expired, poller.consumerName)
			//poller.client.LeaveGroup()
			Leave(k.(string))
		}
		return true
	})
	return expired
}
