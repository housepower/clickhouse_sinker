/*Copyright [2019] housepower

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package statistics

import (
	"context"
	"math/rand"
	"time"

	"github.com/housepower/clickhouse_sinker/util"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/push"
	"github.com/prometheus/common/expfmt"
	"go.uber.org/zap"
)

var (
	prefix = "clickhouse_sinker_"

	// ConsumeMsgsTotal = ParseMsgsErrorTotal + RingMsgsOffTooSmallErrorTotal + FlushMsgsTotal + FlushMsgsErrorTotal
	ConsumeMsgsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: prefix + "consume_msgs_total",
			Help: "total num of consumed msgs",
		},
		[]string{"task"},
	)
	ConsumeMsgsErrorTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: prefix + "consumer_msgs_error_total",
			Help: "total num of consume errors",
		},
		[]string{"task"},
	)
	ParseMsgsErrorTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: prefix + "parse_msgs_error_total",
			Help: "total num of msgs with parse failure",
		},
		[]string{"task"},
	)
	RingMsgsOffTooSmallErrorTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: prefix + "ring_msgs_offset_too_small_error_total",
			Help: "total num of msgs with too small offset to put into ring",
		},
		[]string{"task"},
	)
	RingMsgsOffTooLargeErrorTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: prefix + "ring_msgs_offset_too_large_error_total",
			Help: "total num of msgs with too large offset to put into ring",
		},
		[]string{"task"},
	)
	RingNormalBatchsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: prefix + "ring_normal_batchs_total",
			Help: "total num of normal batches generated",
		},
		[]string{"task"},
	)
	RingForceBatchsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: prefix + "ring_force_batchs_total",
			Help: "total num of force batches generated",
		},
		[]string{"task"},
	)
	RingForceBatchAllTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: prefix + "ring_force_batch_all_total",
			Help: "total num of force batch_all generated",
		},
		[]string{"task"},
	)
	RingForceBatchAllGapTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: prefix + "ring_force_batch_all_gap_total",
			Help: "total num of force batch_all generated with some offset gap",
		},
		[]string{"task"},
	)
	FlushMsgsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: prefix + "flush_msgs_total",
			Help: "total num of flushed msgs",
		},
		[]string{"task"},
	)
	FlushMsgsErrorTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: prefix + "flush_msgs_error_total",
			Help: "total num of msgs failed to flush to ck",
		},
		[]string{"task"},
	)
	ConsumeOffsets = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: prefix + "consume_offsets",
			Help: "last committed offset for each topic partition pair",
		},
		[]string{"task", "topic", "partition"},
	)
	ClickhouseReconnectTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: prefix + "clickhouse_reconnect_total",
			Help: "total num of ClickHouse reconnects",
		},
		[]string{"task"},
	)
	RingMsgs = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: prefix + "ring_msgs",
			Help: "num of msgs in ring",
		},
		[]string{"task"},
	)
	ShardMsgs = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: prefix + "shard_msgs",
			Help: "num of msgs in shard",
		},
		[]string{"task"},
	)
	ParsingPoolBacklog = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: prefix + "parsing_pool_backlog",
			Help: "GlobalParsingPool backlog",
		},
		[]string{"task"},
	)
	WritingPoolBacklog = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: prefix + "writing_pool_backlog",
			Help: "GlobalWritingPool backlog",
		},
		[]string{"task"},
	)
)

func init() {
	prometheus.MustRegister(ConsumeMsgsTotal)
	prometheus.MustRegister(ConsumeMsgsErrorTotal)
	prometheus.MustRegister(ParseMsgsErrorTotal)
	prometheus.MustRegister(RingMsgsOffTooSmallErrorTotal)
	prometheus.MustRegister(RingMsgsOffTooLargeErrorTotal)
	prometheus.MustRegister(RingNormalBatchsTotal)
	prometheus.MustRegister(RingForceBatchsTotal)
	prometheus.MustRegister(RingForceBatchAllTotal)
	prometheus.MustRegister(RingForceBatchAllGapTotal)
	prometheus.MustRegister(FlushMsgsTotal)
	prometheus.MustRegister(FlushMsgsErrorTotal)
	prometheus.MustRegister(ConsumeOffsets)
	prometheus.MustRegister(ClickhouseReconnectTotal)
	prometheus.MustRegister(RingMsgs)
	prometheus.MustRegister(ShardMsgs)
	prometheus.MustRegister(ParsingPoolBacklog)
	prometheus.MustRegister(WritingPoolBacklog)
	prometheus.MustRegister(collectors.NewBuildInfoCollector())
}

// Pusher is the service to push the metrics to pushgateway
type Pusher struct {
	pgwAddrs     []string
	pushInterval int
	pusher       *push.Pusher
	inUseAddr    int
	instance     string
	ctx          context.Context
	cancel       context.CancelFunc
}

func NewPusher(addrs []string, interval int, selfAddr string) *Pusher {
	return &Pusher{
		pgwAddrs:     addrs,
		pushInterval: interval,
		inUseAddr:    -1,
		instance:     selfAddr,
	}
}

var (
	errPgwEmpty = errors.New("invalid configuration for pusher")
)

func (p *Pusher) Init() error {
	if len(p.pgwAddrs) == 0 || p.pushInterval <= 0 {
		return errPgwEmpty
	}
	p.reconnect()
	return nil
}

func (p *Pusher) Run(ctx context.Context) {
	p.ctx, p.cancel = context.WithCancel(ctx)
	ticker := time.NewTicker(time.Second * time.Duration(p.pushInterval))
FOR:
	for {
		select {
		case <-ticker.C:
			if err := p.pusher.Push(); err != nil {
				err = errors.Wrapf(err, "")
				util.Logger.Error("pushing metrics failed", zap.Error(err))
				p.reconnect()
			}
		case <-p.ctx.Done():
			util.Logger.Info("Pusher.Run quit due to context has been canceled")
			break FOR
		}
	}
}

func (p *Pusher) Stop() {
	// https://stackoverflow.com/questions/63540280/how-to-set-a-retention-time-for-pushgateway-for-metrics-to-expire
	// https://github.com/prometheus/pushgateway/issues/19
	if err := p.pusher.Delete(); err != nil {
		err = errors.Wrapf(err, "")
		util.Logger.Error("failed to delete metric group", zap.String("pushgateway", p.pgwAddrs[p.inUseAddr]),
			zap.String("job", "clickhouse_sinker"), zap.String("instance", p.instance), zap.Error(err))
	}
	p.cancel()
	util.Logger.Info("stopped metric pusher")
}

func (p *Pusher) reconnect() {
	var nextAddr int
	if p.inUseAddr == -1 {
		nextAddr = rand.Intn(len(p.pgwAddrs))
	} else {
		nextAddr = (p.inUseAddr + 1) % len(p.pgwAddrs)
	}
	p.pusher = push.New(p.pgwAddrs[nextAddr], "clickhouse_sinker").
		Collector(ConsumeMsgsTotal).
		Collector(ConsumeMsgsErrorTotal).
		Collector(ParseMsgsErrorTotal).
		Collector(RingMsgsOffTooSmallErrorTotal).
		Collector(RingMsgsOffTooLargeErrorTotal).
		Collector(RingNormalBatchsTotal).
		Collector(RingForceBatchsTotal).
		Collector(RingForceBatchAllTotal).
		Collector(RingForceBatchAllGapTotal).
		Collector(FlushMsgsTotal).
		Collector(FlushMsgsErrorTotal).
		Collector(ConsumeOffsets).
		Collector(ClickhouseReconnectTotal).
		Collector(RingMsgs).
		Collector(ShardMsgs).
		Collector(ParsingPoolBacklog).
		Collector(WritingPoolBacklog).
		Grouping("instance", p.instance).Format(expfmt.FmtText)
	p.inUseAddr = nextAddr
}
