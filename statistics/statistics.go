/*
Copyright [2019] housepower

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
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/push"
	"github.com/prometheus/common/expfmt"
	"github.com/thanos-io/thanos/pkg/errors"
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
	ParseMsgsErrorTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: prefix + "parse_msgs_error_total",
			Help: "total num of msgs with parse failure",
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
		[]string{"consumer", "topic", "partition"},
	)
	ConsumeLags = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: prefix + "consume_lags",
			Help: "message lags for each task, work with cluster of sinker",
		},
		[]string{"consumer", "topic", "task"},
	)
	ShardMsgs = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: prefix + "shard_msgs",
			Help: "num of msgs in shard",
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
	WritingDurations = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    prefix + "writing_durations",
			Help:    "writing durations",
			Buckets: []float64{1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 64.0, 128.0},
		},
		[]string{"task", "table"},
	)
	WriteSeriesAllowNew = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: prefix + "write_series_allow_new",
			Help: "num of allowed new series",
		},
		[]string{"task"},
	)
	WriteSeriesAllowChanged = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: prefix + "write_series_allow_changed",
			Help: "num of allowed changed series",
		},
		[]string{"task"},
	)
	WriteSeriesDropQuota = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: prefix + "write_series_drop_quota",
			Help: "num of disallowed write_series due to quota",
		},
		[]string{"task"},
	)
	WriteSeriesDropUnchanged = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: prefix + "write_series_drop_unchanged",
			Help: "num of disallowed write_series due to unchanged",
		},
		[]string{"task"},
	)
	// WriteSeriesSucceed = WriteSeriesAllowNew + WriteSeriesAllowChanged
	WriteSeriesSucceed = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: prefix + "write_series_succeed",
			Help: "num of series handled by writeSeries",
		},
		[]string{"task"},
	)
)

func init() {
	prometheus.MustRegister(ConsumeMsgsTotal)
	prometheus.MustRegister(ParseMsgsErrorTotal)
	prometheus.MustRegister(FlushMsgsTotal)
	prometheus.MustRegister(FlushMsgsErrorTotal)
	prometheus.MustRegister(ConsumeOffsets)
	prometheus.MustRegister(ConsumeLags)
	prometheus.MustRegister(ShardMsgs)
	prometheus.MustRegister(WritingPoolBacklog)
	prometheus.MustRegister(WritingDurations)
	prometheus.MustRegister(WriteSeriesAllowNew)
	prometheus.MustRegister(WriteSeriesAllowChanged)
	prometheus.MustRegister(WriteSeriesDropQuota)
	prometheus.MustRegister(WriteSeriesDropUnchanged)
	prometheus.MustRegister(WriteSeriesSucceed)
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
	stopped      chan struct{}
}

func NewPusher(addrs []string, interval int, selfAddr string) *Pusher {
	return &Pusher{
		pgwAddrs:     addrs,
		pushInterval: interval,
		inUseAddr:    -1,
		instance:     selfAddr,
		stopped:      make(chan struct{}),
	}
}

var (
	errPgwEmpty = errors.Newf("invalid configuration for pusher")
)

func (p *Pusher) Init() error {
	if len(p.pgwAddrs) == 0 || p.pushInterval <= 0 {
		return errPgwEmpty
	}
	p.reconnect()
	p.ctx, p.cancel = context.WithCancel(context.Background())
	return nil
}

func (p *Pusher) Run() {
	ticker := time.NewTicker(time.Second * time.Duration(p.pushInterval))
	defer ticker.Stop()
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
	p.stopped <- struct{}{}
}

func (p *Pusher) Stop() {
	p.cancel()
	<-p.stopped
	// https://stackoverflow.com/questions/63540280/how-to-set-a-retention-time-for-pushgateway-for-metrics-to-expire
	// https://github.com/prometheus/pushgateway/issues/19
	if err := p.pusher.Delete(); err != nil {
		err = errors.Wrapf(err, "")
		util.Logger.Error("failed to delete metric group", zap.String("pushgateway", p.pgwAddrs[p.inUseAddr]),
			zap.String("job", "clickhouse_sinker"), zap.String("instance", p.instance), zap.Error(err))
	}
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
		Collector(ParseMsgsErrorTotal).
		Collector(FlushMsgsTotal).
		Collector(FlushMsgsErrorTotal).
		Collector(ConsumeOffsets).
		Collector(ConsumeLags).
		Collector(ShardMsgs).
		Collector(WritingPoolBacklog).
		Collector(WritingDurations).
		Collector(WriteSeriesAllowNew).
		Collector(WriteSeriesAllowChanged).
		Collector(WriteSeriesDropQuota).
		Collector(WriteSeriesDropUnchanged).
		Collector(WriteSeriesSucceed).
		Grouping("instance", p.instance).Format(expfmt.FmtText)
	p.inUseAddr = nextAddr
}
