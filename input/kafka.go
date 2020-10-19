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

package input

import (
	"context"
	"io"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/segmentio/kafka-go"
	"github.com/sundy-li/go_commons/log"

	"github.com/housepower/clickhouse_sinker/config"
	"github.com/housepower/clickhouse_sinker/model"
	"github.com/housepower/clickhouse_sinker/statistics"
)

// Kafka reader configuration
type Kafka struct {
	taskCfg *config.TaskConfig

	r       *kafka.Reader
	stopped chan struct{}
	putFn   func(msg model.InputMessage)
}

// NewKafka get instance of kafka reader
func NewKafka() *Kafka {
	return &Kafka{}
}

// Init Initialise the kafka instance with configuration
func (k *Kafka) Init(taskCfg *config.TaskConfig, putFn func(msg model.InputMessage)) error {
	k.taskCfg = taskCfg

	cfg := config.GetConfig()
	kfkCfg := cfg.Kafka[k.taskCfg.Kafka]
	k.stopped = make(chan struct{})
	offset := kafka.LastOffset
	if k.taskCfg.Earliest {
		offset = kafka.FirstOffset
	}

	k.r = kafka.NewReader(kafka.ReaderConfig{
		Brokers:        strings.Split(kfkCfg.Brokers, ","),
		GroupID:        k.taskCfg.ConsumerGroup,
		Topic:          k.taskCfg.Topic,
		StartOffset:    offset,
		MinBytes:       k.taskCfg.MinBufferSize * k.taskCfg.MsgSizeHint,
		MaxBytes:       k.taskCfg.BufferSize * k.taskCfg.MsgSizeHint,
		MaxWait:        time.Duration(k.taskCfg.FlushInterval) * time.Second,
		CommitInterval: time.Second, // flushes commits to Kafka every second
	})
	k.putFn = putFn
	return nil
}

// kafka main loop
func (k *Kafka) Run(ctx context.Context) {
LOOP:
	for {
		var err error
		var msg kafka.Message
		if msg, err = k.r.FetchMessage(ctx); err != nil {
			switch errors.Cause(err) {
			case context.Canceled:
				log.Infof("%s Kafka.Run quit due to context has been canceled", k.taskCfg.Name)
				break LOOP
			case io.EOF:
				log.Infof("%s Kafka.Run quit due to reader has been closed", k.taskCfg.Name)
				break LOOP
			default:
				statistics.ConsumeMsgsErrorTotal.WithLabelValues(k.taskCfg.Name).Inc()
				err = errors.Wrap(err, "")
				log.Errorf("%s Kafka.Run got error %+v", k.taskCfg.Name, err)
				continue
			}
		}
		k.putFn(model.InputMessage{
			Topic:     msg.Topic,
			Partition: msg.Partition,
			Key:       msg.Key,
			Value:     msg.Value,
			Offset:    msg.Offset,
			Timestamp: &msg.Time,
		})
	}
}

func (k *Kafka) CommitMessages(ctx context.Context, msg *model.InputMessage) error {
	return k.r.CommitMessages(ctx, kafka.Message{
		Topic:     msg.Topic,
		Partition: msg.Partition,
		Key:       msg.Key,
		Value:     msg.Value,
		Offset:    msg.Offset,
	})
}

// Stop kafka consumer and close all connections
func (k *Kafka) Stop() error {
	_ = k.r.Close()
	return nil
}

// Description of this kafka consumer, which topic it reads from
func (k *Kafka) Description() string {
	return "kafka consumer of topic " + k.taskCfg.Topic
}
