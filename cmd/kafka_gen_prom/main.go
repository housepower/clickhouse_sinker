package main

/*
https://github.com/ClickHouse/ClickHouse/issues/38878
performance of inserting to sparse wide table is bad

-- Prometheus metric solution 1 - one wide table, each row is a datapoint and its series lables
CREATE TABLE default.prom_extend ON CLUSTER abc (
    timestamp DateTime,
    value Float32,
    __name__ String,
    labels String
) ENGINE=ReplicatedMergeTree()
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (__name__, timestamp);

CREATE TABLE default.dist_prom_extend ON CLUSTER abc AS prom_extend ENGINE = Distributed(abc, default, prom_extend);

-- Prometheus metric solution 2 - seperated table for datapoints and series labels can join on series id
CREATE TABLE default.prom_metric ON CLUSTER abc (
    __series_id Int64,
    timestamp DateTime CODEC(DoubleDelta, LZ4),
    value Float32 CODEC(ZSTD(15))
) ENGINE=ReplicatedReplacingMergeTree()
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (__series_id, timestamp);

CREATE TABLE default.dist_prom_metric ON CLUSTER abc AS prom_metric ENGINE = Distributed(abc, default, prom_metric);

CREATE TABLE default.prom_metric_series ON CLUSTER abc (
    __series_id Int64,
    __mgmt_id Int64,
    labels String,
    __name__ String
) ENGINE=ReplicatedReplacingMergeTree()
ORDER BY (__name__, __series_id);

CREATE TABLE default.dist_prom_metric_series ON CLUSTER abc AS prom_metric_series ENGINE = Distributed(abc, default, prom_metric_series);

CREATE TABLE default.prom_metric_agg ON CLUSTER abc (
    __series_id Int64,
    timestamp DateTime CODEC(DoubleDelta, LZ4),
    max_value AggregateFunction(max, Float32),
    min_value AggregateFunction(min, Float32),
    avg_value AggregateFunction(avg, Float32)
) ENGINE=ReplicatedReplacingMergeTree()
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (__series_id, timestamp);

CREATE TABLE default.dist_prom_metric_agg ON CLUSTER abc AS prom_metric_agg ENGINE = Distributed(abc, default, prom_metric_agg);

SELECT __series_id,
    toStartOfDay(timestamp) AS timestamp,
    maxMerge(max_value) AS max_value,
    minMerge(min_value) AS min_value,
    avgMerge(avg_value) AS avg_value
FROM default.dist_prom_metric_agg
WHERE __series_id IN (-9223014754132113609, -9223015002162651005)
GROUP BY __series_id, timestamp
ORDER BY __series_id, timestamp;

-- Activate aggregation for future datapoints by creating a materialized view
CREATE MATERIALIZED VIEW default.prom_metric_mv ON CLUSTER abc
TO prom_metric_agg
AS SELECT __series_id,
    toStartOfHour(timestamp) AS timestamp,
    maxState(value) AS max_value,
    minState(value) AS min_value,
    avgState(value) AS avg_value
FROM prom_metric
GROUP BY __series_id, timestamp;

-- Deactivate aggregation by dropping the materialized view. You can revise and create it later as you will.
DROP TABLE default.prom_metric_mv ON CLUSTER abc SYNC;

*/

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/bytedance/sonic"
	"github.com/google/gops/agent"
	"github.com/thanos-io/thanos/pkg/errors"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/viru-tech/clickhouse_sinker/util"
	"go.uber.org/zap"
)

const (
	Alpha      = "abcdefghijklmnopqrstuvwxyz"
	NumMetrics = 1000
	NumKeys    = 5
	NumRunes   = 10
	LenVal     = 1 // 1000 * (10^1)^5 = 10^8 series
	NumAllKeys = 1000
)

var (
	KafkaBrokers string
	KafkaTopic   string
	gLines       int64
	gSize        int64
	metrics      []PromMetric
)

type Labels map[string]string

type Datapoint struct {
	Timestamp time.Time
	Value     float32
	Name      string `json:"__name__"`
	Labels
}

// I need every label be present at the top level.
func (dp Datapoint) MarshalJSON() ([]byte, error) {
	labels, err := sonic.MarshalString(dp.Labels)
	if err != nil {
		return nil, err
	}
	labels2 := labels[1 : len(labels)-1]
	msg := fmt.Sprintf(`{"timestamp":"%s", "value":%f,"__name__":"%s","labels":%s,%s}`, dp.Timestamp.Format(time.RFC3339), dp.Value, dp.Name, labels, labels2)
	return []byte(msg), nil
}

type PromMetric struct {
	Name      string
	LabelKeys []string
}

func randValue() (val string) {
	b := make([]byte, LenVal)
	for i := 0; i < LenVal; i++ {
		b[i] = Alpha[rand.Intn(NumRunes+1)]
	}
	val = string(b)
	return
}

func initMetrics() {
	metrics = make([]PromMetric, NumMetrics)
	for i := 0; i < NumMetrics; i++ {
		m := PromMetric{
			Name:      fmt.Sprintf("metric_%08d", i),
			LabelKeys: make([]string, NumKeys),
		}
		for j := 0; j < NumKeys; j++ {
			key := fmt.Sprintf("key_%06d", rand.Intn(NumAllKeys+1))
			m.LabelKeys[j] = key
		}
		metrics[i] = m
	}
}

func generate() {
	initMetrics()
	toRound := time.Now().Add(time.Duration(-30*24) * time.Hour)
	// refers to time.Time.Truncate
	rounded := time.Date(toRound.Year(), toRound.Month(), toRound.Day(), 0, 0, 0, 0, toRound.Location())

	wp := util.NewWorkerPool(10, 10000)
	opts := []kgo.Opt{
		kgo.SeedBrokers(strings.Split(KafkaBrokers, ",")...),
	}
	var err error
	var cl *kgo.Client
	if cl, err = kgo.NewClient(opts...); err != nil {
		util.Logger.Fatal("kgo.NewClient failed", zap.Error(err))
	}
	defer cl.Close()

	ctx := context.Background()
	produceCb := func(rec *kgo.Record, err error) {
		if err != nil {
			util.Logger.Fatal("kgo.Client.Produce failed", zap.Error(err))
		}
		atomic.AddInt64(&gLines, int64(1))
		atomic.AddInt64(&gSize, int64(len(rec.Value)))
	}

	for day := 0; ; day++ {
		tsDay := rounded.Add(time.Duration(24*day) * time.Hour)
		for step := 0; step < 24*60*60; step++ {
			timestamp := tsDay.Add(time.Duration(step) * time.Second)
			for i := 0; i < NumMetrics; i++ {
				dp := Datapoint{
					Timestamp: timestamp,
					Value:     rand.Float32(),
					Name:      metrics[i].Name,
					Labels:    make(Labels),
				}
				for _, key := range metrics[i].LabelKeys {
					dp.Labels[key] = randValue()
				}

				_ = wp.Submit(func() {
					var b []byte
					if b, err = dp.MarshalJSON(); err != nil {
						err = errors.Wrapf(err, "")
						util.Logger.Fatal("got error", zap.Error(err))
					}
					cl.Produce(ctx, &kgo.Record{
						Topic: KafkaTopic,
						Key:   []byte(dp.Name),
						Value: b,
					}, produceCb)
				})
			}
		}
	}
}

func main() {
	util.InitLogger([]string{"stdout"})
	flag.Usage = func() {
		usage := fmt.Sprintf(`Usage of %s
    %s kakfa_brokers topic
This util fill some fields with random content, serialize and send to kafka.
kakfa_brokers: for example, 192.168.110.8:9092,192.168.110.12:9092,192.168.110.16:9092
topic: for example, prom_extend`, os.Args[0], os.Args[0])
		util.Logger.Info(usage)
		os.Exit(0)
	}
	flag.Parse()
	args := flag.Args()
	if len(args) != 2 {
		flag.Usage()
	}
	KafkaBrokers = args[0]
	KafkaTopic = args[1]
	util.Logger.Info("CLI options",
		zap.String("KafkaBrokers", KafkaBrokers),
		zap.String("KafkaTopic", KafkaTopic),
		zap.Int("NumMetrics", NumMetrics),
		zap.Int("NumKeys", NumKeys),
		zap.Int("NumRunes", NumRunes),
		zap.Int("LenVal", LenVal),
		zap.Int("NumAllKeys", NumAllKeys),
	)

	if err := agent.Listen(agent.Options{}); err != nil {
		util.Logger.Fatal("got error", zap.Error(err))
	}

	var prevLines, prevSize int64
	ctx, _ := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM, syscall.SIGHUP)
	go generate()

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
LOOP:
	for {
		select {
		case <-ctx.Done():
			util.Logger.Info("quit due to context been canceled")
			break LOOP
		case <-ticker.C:
			var speedLine, speedSize int64
			if gLines != 0 {
				speedLine = (gLines - prevLines) / int64(10)
				speedSize = (gSize - prevSize) / int64(10)
			}
			prevLines = gLines
			prevSize = gSize
			util.Logger.Info("status", zap.Int64("lines", gLines), zap.Int64("bytes", gSize), zap.Int64("speed(lines/s)", speedLine), zap.Int64("speed(bytes/s)", speedSize))
		}
	}
}
