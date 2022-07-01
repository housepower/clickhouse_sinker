package main

/*
CREATE TABLE prom_extend ON CLUSTER abc (
	timestamp DateTime,
	value Float64,
	__name__ String
) ENGINE=ReplicatedMergeTree()
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (__name__, timestamp);

CREATE TABLE dist_prom_extend ON CLUSTER abc AS prom_extend ENGINE = Distributed(abc, default, prom_extend);

*/

import (
	"bytes"
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

	"github.com/Shopify/sarama"
	"github.com/google/gops/agent"
	"github.com/housepower/clickhouse_sinker/util"
	"github.com/thanos-io/thanos/pkg/errors"
	"go.uber.org/zap"
)

const (
	Alpha      = "abcdefghijklmnopqrstuvwxyz"
	NumMetrics = 1000000
	NumKeys    = 10
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
	Value     float64
	Name      string `json:"__name__"`
	Labels
}

// I need every label be present at the top level.
func (dp Datapoint) MarshalJSON() ([]byte, error) {
	var bbuf bytes.Buffer
	bbuf.WriteString(fmt.Sprintf(`{"timestamp":"%s", "value":%f,"__name__":"%s"`, dp.Timestamp.Format(time.RFC3339), dp.Value, dp.Name))
	for key, val := range dp.Labels {
		bbuf.WriteString(fmt.Sprintf(`,"%s":"%s"`, key, val))
	}
	bbuf.WriteByte('}')
	return bbuf.Bytes(), nil
}

type PromMetric struct {
	Name      string
	LabelKeys []string
}

func randValue() (val string) {
	mod := rand.Intn(2)
	var leng, maxN int
	if mod == 0 { //10^5=100000
		leng = 5
		maxN = 10
	} else { //3^10 = 59049
		leng = 10
		maxN = 3
	}
	b := make([]byte, leng)
	for i := 0; i < leng; i++ {
		b[i] = Alpha[rand.Intn(maxN+1)]
	}
	val = string(b)
	return
}

func initMetrics() {
	metrics = make([]PromMetric, NumMetrics)
	for i := 0; i < NumMetrics; i++ {
		m := PromMetric{
			Name:      fmt.Sprintf("metric-%08d", i),
			LabelKeys: make([]string, NumKeys),
		}
		for j := 0; j < NumKeys; j++ {
			key := fmt.Sprintf("key-%06d", rand.Intn(NumAllKeys+1))
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
	config := sarama.NewConfig()
	config.Version = sarama.V2_1_0_0
	w, err := sarama.NewAsyncProducer(strings.Split(KafkaBrokers, ","), config)
	if err != nil {
		util.Logger.Error("sarama.NewAsyncProducer failed", zap.Error(err))
	}
	defer w.Close()
	chInput := w.Input()

	for day := 0; ; day++ {
		tsDay := rounded.Add(time.Duration(24*day) * time.Hour)
		for step := 0; step < 24*60*60; step++ {
			timestamp := tsDay.Add(time.Duration(step) * time.Second)
			for i := 0; i < NumMetrics; i++ {
				dp := Datapoint{
					Timestamp: timestamp,
					Value:     rand.Float64(),
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
					chInput <- &sarama.ProducerMessage{
						Topic: KafkaTopic,
						Key:   sarama.StringEncoder(dp.Name),
						Value: sarama.ByteEncoder(b),
					}
					atomic.AddInt64(&gLines, int64(1))
					atomic.AddInt64(&gSize, int64(len(b)))
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
