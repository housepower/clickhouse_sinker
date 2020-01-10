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
	"errors"
	"math/rand"
	"net"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/push"
)

var (
	prefix = "clickhouse_sinker_"

	tasks = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: prefix + "tasks",
			Help: "number of tasks by name",
		},
		[]string{"name"},
	)
	rebalanceTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: prefix + "rebalance_total",
			Help: "rebalance total times in each task",
		},
		[]string{"task"},
	)
	toppars = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: prefix + "toppars",
			Help: "topic and partition pairs processed",
		},
		[]string{"task", "topic", "partition"},
	)
	consumeMsgsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: prefix + "consume_msgs_total",
			Help: "total num of consume msgs",
		},
		[]string{"task", "topic", "partition"},
	)
	parseInMsgsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: prefix + "parse_in_msgs_total",
			Help: "input msg total nums before parse",
		},
		[]string{"task"},
	)
	parseOutMsgsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: prefix + "parse_out_msgs_total",
			Help: "output msg total nums after parse",
		},
		[]string{"task"},
	)
	parseTimespan = prometheus.NewSummaryVec(
		prometheus.SummaryOpts{
			Name:       prefix + "parse_timespan",
			Help:       "cost time of each parse unit:second",
			Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
		},
		[]string{"task"},
	)
	flushMsgsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: prefix + "flush_msgs_total",
			Help: "msg total nums flushed",
		},
		[]string{"task"},
	)
	flushTimespan = prometheus.NewSummaryVec(
		prometheus.SummaryOpts{
			Name:       prefix + "flush_timespan",
			Help:       "cost time of each flush unit:second",
			Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
		},
		[]string{"task"},
	)

	flushErrorTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: prefix + "flush_error_total",
			Help: "flush to ck error total count",
		},
		[]string{"task"},
	)

	offsets = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: prefix + "offsets",
			Help: "last commited offset for each topic partition pair",
		},
		[]string{"task", "topic", "partition"},
	)
	parseErrorTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: prefix + "parse_error_total",
			Help: "parse error total nums",
		},
		[]string{"task"},
	)
)

func init() {
	prometheus.MustRegister(tasks)
	prometheus.MustRegister(rebalanceTotal)
	prometheus.MustRegister(toppars)
	prometheus.MustRegister(consumeMsgsTotal)
	prometheus.MustRegister(parseInMsgsTotal)
	prometheus.MustRegister(parseOutMsgsTotal)
	prometheus.MustRegister(parseTimespan)
	prometheus.MustRegister(flushMsgsTotal)
	prometheus.MustRegister(flushTimespan)
	prometheus.MustRegister(offsets)
	prometheus.MustRegister(parseErrorTotal)
	prometheus.MustRegister(flushErrorTotal)
}

func UpdateTasks(name string) {
	tasks.WithLabelValues(name).Set(1)
}

func UpdateRebalanceTotal(name string, delta int) {
	rebalanceTotal.WithLabelValues(name).Add(float64(delta))
}

func UpdateToppars(task, topic string, partition int32, val int) {
	toppars.WithLabelValues(task, topic, strconv.FormatInt(int64(partition), 10)).Set(float64(val))
}

func UpdateConsumeMsgsTotal(task, topic string, partition int32, delta int) {
	consumeMsgsTotal.WithLabelValues(task, topic, strconv.FormatInt(int64(partition), 10)).Add(float64(delta))
}

func UpdateParseInMsgsTotal(task string, delta int) {
	parseInMsgsTotal.WithLabelValues(task).Add(float64(delta))
}

func UpdateParseOutMsgsTotal(task string, delta int) {
	parseOutMsgsTotal.WithLabelValues(task).Add(float64(delta))
}

func UpdateParseTimespan(task string, start time.Time) {
	parseTimespan.WithLabelValues(task).Observe(time.Now().Sub(start).Seconds())
}

func UpdateFlushMsgsTotal(task string, delta int) {
	flushMsgsTotal.WithLabelValues(task).Add(float64(delta))
}

func UpdateFlushErrorsTotal(task string, delta int) {
	flushErrorTotal.WithLabelValues(task).Add(float64(delta))
}

func UpdateFlushTimespan(task string, start time.Time) {
	flushTimespan.WithLabelValues(task).Observe(time.Now().Sub(start).Seconds())
}

func UpdateOffsets(task, topic string, partition int32, offset int64) {
	offsets.WithLabelValues(task, topic, strconv.FormatInt(int64(partition), 10)).Set(float64(offset))
}

func UpdateParseErrorTotal(task string, delta int) {
	parseErrorTotal.WithLabelValues(task).Add(float64(delta))
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

func (p *Pusher) Init() error {
	if len(p.pgwAddrs) == 0 || p.pushInterval <= 0 {
		return errors.New("invalid configuration for pusher")
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
		Collector(tasks).
		Collector(rebalanceTotal).
		Collector(toppars).
		Collector(consumeMsgsTotal).
		Collector(parseInMsgsTotal).
		Collector(parseOutMsgsTotal).
		Collector(parseTimespan).
		Collector(flushMsgsTotal).
		Collector(flushTimespan).
		Collector(offsets).
		Collector(parseErrorTotal).
		Collector(flushErrorTotal).
		Grouping("instance", p.instance)
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
		switch true {
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
