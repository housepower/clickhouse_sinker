package main

/*
CREATE TABLE sensor_dt_result_online ON CLUSTER abc (
	`@time` DateTime,
	`@ItemGUID` String,
	`@MetricName` LowCardinality(String),
	`@AlgName` LowCardinality(String),
	value Float64,
	upper Float64,
	lower Float64,
	yhat_upper Float64,
	yhat_lower Float64,
	yhat_flag Int32,
	total_anomaly Int64,
	anomaly Float32,
	abnormal_type Int16,
	abnormality Int16,
	container_id Int64,
	hard_upper Float64,
	hard_lower Float64,
	hard_anomaly Int64,
	shift_tag Int32,
	season_tag Int32,
	spike_tag Int32,
	is_missing Int32
) ENGINE=ReplicatedMergeTree('/clickhouse/tables/{cluster}/{database}/{table}/{shard}', '{replica}')
PARTITION BY toYYYYMMDD(`@time`)
ORDER BY (`@ItemGUID`, `@MetricName`, `@time`);

CREATE TABLE dist_sensor_dt_result_online ON CLUSTER abc AS sensor_dt_result_online ENGINE = Distributed(abc, default, sensor_dt_result_online);

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

	"github.com/google/gops/agent"
	"github.com/thanos-io/thanos/pkg/errors"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/viru-tech/clickhouse_sinker/util"
	"go.uber.org/zap"
)

const (
	BusinessNum = 10
	InstanceNum = 100
)

var (
	KafkaBrokers string
	KafkaTopic   string

	ListMetricName = []string{"CPU", "RAM", "IOPS"}
	ListArgName    = []string{
		"DecisionTrees",
		"NaiveBayesClassification",
		"OrdinaryLeastSquaresRegression",
		"LogisticRegression",
		"SupportVectorMachines",
		"EnsembleMethods",
		"ClusteringAlgorithms",
		"PrincipalComponentAnalysis",
		"SingularValueDecomposition",
		"IndependentComponentAnalysis"}

	gLines int64
	gSize  int64
)

type Metric struct {
	Time         time.Time `json:"@time"` //seconds since epoch
	ItemGUID     string    `json:"@item_guid"`
	MetricName   string    `json:"@metric_name"`
	AlgName      string    `json:"@alg_name"`
	Value        float64   `json:"value"`
	Upper        float64   `json:"upper"`
	Lower        float64   `json:"lower"`
	YhatUpper    float64   `json:"yhat_upper"`
	YhatLower    float64   `json:"yhat_lower"`
	YhatFlag     int32     `json:"yhat_flag"`
	TotalAnomaly int64     `json:"total_anomaly"`
	Anomaly      float64   `json:"anomaly"`
	AbnormalType int16     `json:"abnormal_type"`
	Abnormality  int16     `json:"abnormality"`
	ContainerID  int64     `json:"container_id"`
	HardUpper    float64   `json:"hard_upper"`
	HardLower    float64   `json:"hard_lower"`
	HardAnomaly  int64     `json:"hard_anomaly"`
	ShiftTag     int32     `json:"shift_tag"`
	SeasonTag    int32     `json:"season_tag"`
	SpikeTag     int32     `json:"spike_tag"`
	IsMissing    int32     `json:"is_missing"`
}

func randElement(list []string) string {
	off := rand.Intn(len(list))
	return list[off]
}

func generate() {
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
			for bus := 0; bus < BusinessNum; bus++ {
				for ins := 0; ins < InstanceNum; ins++ {
					metric := Metric{
						Time:         timestamp,
						ItemGUID:     fmt.Sprintf("bus%03d_ins%03d", bus, ins),
						MetricName:   randElement(ListMetricName),
						AlgName:      randElement(ListArgName),
						Value:        float64(rand.Intn(100)),
						Upper:        float64(100.0),
						Lower:        float64(60.0),
						YhatUpper:    float64(100.0),
						YhatLower:    float64(60.0),
						YhatFlag:     rand.Int31n(65535),
						TotalAnomaly: rand.Int63n(65535),
						Anomaly:      float64(rand.Intn(100)) / float64(100),
						AbnormalType: int16(rand.Intn(1000)),
						Abnormality:  int16(rand.Intn(1000)),
						ContainerID:  rand.Int63n(65535),
						HardUpper:    float64(100),
						HardLower:    float64(60),
						HardAnomaly:  int64(rand.Intn(65535)),
						ShiftTag:     int32(rand.Intn(65535)),
						SeasonTag:    int32(rand.Intn(65535)),
						SpikeTag:     int32(rand.Intn(65535)),
						IsMissing:    int32(rand.Intn(2)),
					}

					_ = wp.Submit(func() {
						var b []byte
						if b, err = util.JSONMarshal(&metric); err != nil {
							err = errors.Wrapf(err, "")
							util.Logger.Fatal("got error", zap.Error(err))
						}
						cl.Produce(ctx, &kgo.Record{
							Topic: KafkaTopic,
							Key:   []byte(metric.ItemGUID),
							Value: b,
						}, produceCb)
					})
				}
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
topic: for example, sensor_dt_result_online`, os.Args[0], os.Args[0])
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
		zap.Int("BusinessNum", BusinessNum),
		zap.Int("InstanceNum", InstanceNum))

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
