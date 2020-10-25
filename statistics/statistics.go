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
	"math/rand"
	"net"
	"time"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/push"
	"github.com/prometheus/common/expfmt"
	"github.com/sundy-li/go_commons/log"
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
	ParseMsgsBacklog = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: prefix + "parse_msgs_backlog",
			Help: "num of msgs pending to parse",
		},
		[]string{"task"},
	)
	FlushBatchBacklog = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: prefix + "flush_batch_backlog",
			Help: "num of batches pending to flush",
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
	prometheus.MustRegister(ParseMsgsBacklog)
	prometheus.MustRegister(FlushBatchBacklog)
	prometheus.MustRegister(prometheus.NewBuildInfoCollector())
}

// Pusher is the service to push the metrics to pushgateway
type Pusher struct {
	pgwAddrs     []string
	pushInterval int
	pusher       *push.Pusher
	inUseAddr    int
	instance     string
	stopped      chan struct{}
}

func NewPusher(addrs []string, interval int) *Pusher {
	return &Pusher{
		pgwAddrs:     addrs,
		pushInterval: interval,
		inUseAddr:    -1,
		stopped:      make(chan struct{}),
	}
}

var (
	errPgwEmpty = errors.New("invalid configuration for pusher")
)

func (p *Pusher) Init() error {
	if len(p.pgwAddrs) == 0 || p.pushInterval <= 0 {
		return errPgwEmpty
	}
	p.instance = p.getInstance()
	p.reconnect()
	return nil
}

func (p *Pusher) Run() {
	ticker := time.NewTicker(time.Second * time.Duration(p.pushInterval))
FOR:
	for {
		select {
		case <-ticker.C:
			err := p.pusher.Push()
			if err != nil {
				err = errors.Wrapf(err, "")
				log.Infof("pushing metrics failed. %v", err)
				p.reconnect()
			}
		case <-p.stopped:
			ticker.Stop()
			break FOR
		}
	}
}

func (p *Pusher) Stop() {
	close(p.stopped)
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
		Collector(ParseMsgsBacklog).
		Collector(FlushBatchBacklog).
		Grouping("instance", p.instance).Format(expfmt.FmtText)
	p.inUseAddr = nextAddr
}

func (p *Pusher) getInstance() string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return "unknown"
	}

	for _, addr := range addrs {
		if ipnet, ok := addr.(*net.IPNet); ok {
			if p.IsExternalIP(ipnet.IP) {
				return ipnet.IP.String()
			}
		}
	}
	return "unknown"
}

func (p *Pusher) IsExternalIP(ip net.IP) bool {
	if ip.IsLoopback() || ip.IsLinkLocalMulticast() || ip.IsLinkLocalUnicast() {
		return false
	}

	if ip4 := ip.To4(); ip4 != nil {
		switch {
		case ip4[0] == 10:
			return false
		case ip4[0] == 172 && ip4[1] >= 16 && ip4[1] <= 31:
			return false
		case ip4[0] == 192 && ip4[1] == 168:
			return false
		default:
			return true
		}
	}
	return false
}
