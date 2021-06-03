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
) ENGINE=ReplicatedReplacingMergeTree('/clickhouse/tables/{cluster}/{shard}/default/sensor_dt_result_online', '{replica}')
PARTITION BY toYYYYMMDD(`@time`)
ORDER BY (`@time`, `@ItemGUID`, `@MetricName`);

CREATE TABLE dist_sensor_dt_result_online ON CLUSTER abc AS sensor_dt_result_online ENGINE = Distributed(abc, default, sensor_dt_result_online);

*/

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"time"

	"github.com/pkg/errors"
	kafka "github.com/segmentio/kafka-go"
	log "github.com/sirupsen/logrus"
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

func generate() (err error) {
	toRound := time.Now().Add(time.Duration(-30*24) * time.Hour)
	// refers to time.Time.Truncate
	rounded := time.Date(toRound.Year(), toRound.Month(), toRound.Day(), 0, 0, 0, 0, toRound.Location())

	// make a writer that produces to topic-A, using the least-bytes distribution
	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  strings.Split(KafkaBrokers, ","),
		Topic:    KafkaTopic,
		Balancer: kafka.CRC32Balancer{},
		Async:    true,
		//Logger:   log.StandardLogger(),
	})
	defer w.Close()
	ctx := context.Background()
	var b []byte
	var lines, size int64
	var prevLines, prevSize int64
	var speedLine, speedSize int64
	var ts, prevTS int64

	for day := 0; ; day++ {
		tsDay := rounded.Add(time.Duration(24*day) * time.Hour)
		for step := 0; step < 24*60*60; step++ {
			timestamp := tsDay.Add(time.Duration(step) * time.Second)
			for bus := 0; bus < BusinessNum; bus++ {
				for ins := 0; ins < InstanceNum; ins++ {
					if lines%1000000 == 0 {
						ts = time.Now().Unix()
						if lines != 0 {
							speedLine = (lines - prevLines) / (ts - prevTS)
							speedSize = (size - prevSize) / (ts - prevTS)
						}
						prevLines = lines
						prevSize = size
						prevTS = ts
						log.Infof("generated %+v lines %+v Bytes, speedLine: %v lines/s, speedSize: %v B/s", lines, size, speedLine, speedSize)
					}
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
						IsMissing:    int32(rand.Intn(1)),
					}
					if b, err = json.Marshal(&metric); err != nil {
						err = errors.Wrapf(err, "got error")
						return
					}
					if err = w.WriteMessages(ctx, kafka.Message{Key: []byte(metric.ItemGUID), Value: b}); err != nil {
						err = errors.Wrapf(err, "got error")
						return
					}
					lines++
					size += int64(len(b))
				}
			}
		}
	}
}

func main() {
	flag.Usage = func() {
		usage := fmt.Sprintf(`Usage of %s
    %s kakfa_brokers topic
This util fill some fields with random content, serialize and send to kafka.
kakfa_brokers: for example, 192.168.102.114:9092,192.168.102.115:9092
topic: for example, apache_access_log`, os.Args[0], os.Args[0])
		log.Infof(usage)
	}
	flag.Parse()
	args := flag.Args()
	if len(args) != 2 {
		flag.Usage()
		log.Fatal("Invalid CLI arguments!")
	}
	KafkaBrokers = args[0]
	KafkaTopic = args[1]
	log.Infof("KafkaBrokers: %v\nKafkaTopic: %v\nBusinessNum: %v\nInstanceNum: %v\n", KafkaBrokers, KafkaTopic, BusinessNum, InstanceNum)
	if err := generate(); err != nil {
		log.Fatalf("got error %+v", err)
	}
}
