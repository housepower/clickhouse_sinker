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
	"crypto/tls"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/segmentio/kafka-go/sasl/scram"
	"github.com/thanos-io/thanos/pkg/errors"
	"go.uber.org/zap"

	"github.com/viru-tech/clickhouse_sinker/config"
	"github.com/viru-tech/clickhouse_sinker/model"
	"github.com/viru-tech/clickhouse_sinker/statistics"
	"github.com/viru-tech/clickhouse_sinker/util"
)

var _ Inputer = (*KafkaGo)(nil)

// KafkaGo implements input.Inputer
type KafkaGo struct {
	cfg       *config.Config
	taskCfg   *config.TaskConfig
	r         *kafka.Reader
	ctx       context.Context
	cancel    context.CancelFunc
	wgRun     sync.WaitGroup
	putFn     func(msg *model.InputMessage)
	cleanupFn func()
}

type KafkaGoLogger struct {
	logger *zap.Logger
}

func (kgl *KafkaGoLogger) Printf(template string, args ...interface{}) {
	kgl.logger.Sugar().Debugf(template, args)
}

// NewKafkaGo get instance of kafka reader
func NewKafkaGo() *KafkaGo {
	return &KafkaGo{}
}

// Init Initialise the kafka instance with configuration
func (k *KafkaGo) Init(cfg *config.Config, taskCfg *config.TaskConfig, putFn func(msg *model.InputMessage), cleanupFn func()) (err error) {
	k.cfg = cfg
	k.taskCfg = taskCfg
	kfkCfg := &cfg.Kafka
	k.ctx, k.cancel = context.WithCancel(context.Background())
	k.putFn = putFn
	k.cleanupFn = cleanupFn
	offset := kafka.LastOffset
	if k.taskCfg.Earliest {
		offset = kafka.FirstOffset
	}
	readerCfg := &kafka.ReaderConfig{
		Brokers:        strings.Split(kfkCfg.Brokers, ","),
		GroupID:        k.taskCfg.ConsumerGroup,
		Topic:          k.taskCfg.Topic,
		StartOffset:    offset,
		MinBytes:       1024 * 1024,                           // sarama.Config.Consumer.Fetch.Min
		MaxBytes:       100 * 1024 * 1024,                     // sarama.Config.MaxResponseSize
		MaxWait:        time.Duration(100) * time.Millisecond, // sarama.Config.Consumer.MaxWaitTime
		CommitInterval: time.Second,                           // sarama.Config.Consumer.Offsets.AutoCommit.Interval
		// PartitionWatchInterval is only used when GroupID is set and WatchPartitionChanges is set.
		PartitionWatchInterval: 600 * time.Second, // sarama.Config.Metadata.RefreshFrequency
		WatchPartitionChanges:  true,
		ErrorLogger:            &KafkaGoLogger{util.Logger},
	}
	var dialer *kafka.Dialer
	if kfkCfg.TLS.Enable {
		var tlsConfig *tls.Config
		if tlsConfig, err = util.NewTLSConfig(kfkCfg.TLS.CaCertFiles, kfkCfg.TLS.ClientCertFile, kfkCfg.TLS.ClientKeyFile, kfkCfg.TLS.EndpIdentAlgo == ""); err != nil {
			return
		}
		dialer = &kafka.Dialer{
			DualStack: true,
			TLS:       tlsConfig,
		}
	}
	if kfkCfg.Sasl.Enable {
		if dialer == nil {
			dialer = &kafka.Dialer{DualStack: true}
		}
		switch kfkCfg.Sasl.Mechanism {
		case "PLAIN":
			dialer.SASLMechanism = plain.Mechanism{
				Username: kfkCfg.Sasl.Username,
				Password: kfkCfg.Sasl.Password,
			}
		case "SCRAM-SHA-256":
			if dialer.SASLMechanism, err = scram.Mechanism(scram.SHA256, kfkCfg.Sasl.Username, kfkCfg.Sasl.Password); err != nil {
				err = errors.Wrapf(err, "")
				return
			}
		case "SCRAM-SHA-512":
			if dialer.SASLMechanism, err = scram.Mechanism(scram.SHA512, kfkCfg.Sasl.Username, kfkCfg.Sasl.Password); err != nil {
				err = errors.Wrapf(err, "")
				return
			}
		default:
			return errors.Newf("kafka-go doesn't support SASL/%s authentication", kfkCfg.Sasl.Mechanism)
		}
	}
	if dialer != nil {
		readerCfg.Dialer = dialer
	}
	k.r = kafka.NewReader(*readerCfg)
	return nil
}

// kafka main loop
func (k *KafkaGo) Run() {
	k.wgRun.Add(1)
	defer k.wgRun.Done()
LOOP_KAFKA_GO:
	for {
		var err error
		var msg kafka.Message
		if msg, err = k.r.FetchMessage(k.ctx); err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, io.EOF) {
				break LOOP_KAFKA_GO
			} else {
				statistics.ConsumeMsgsErrorTotal.WithLabelValues(k.taskCfg.Name).Inc()
				err = errors.Wrapf(err, "")
				util.Logger.Error("kafka.Reader.FetchMessage failed", zap.String("task", k.taskCfg.Name), zap.Error(err))
				continue
			}
		}
		k.putFn(&model.InputMessage{
			Topic:     msg.Topic,
			Partition: msg.Partition,
			Key:       msg.Key,
			Value:     msg.Value,
			Offset:    msg.Offset,
			Timestamp: &msg.Time,
		})
	}
	// Note: a closed kafka-go client cannot commit offsets.
	k.cleanupFn()
	k.r.Close()
	util.Logger.Info("KafkaGo.Run quit due to context has been canceled", zap.String("task", k.taskCfg.Name))
}

func (k *KafkaGo) CommitMessages(msg *model.InputMessage) (err error) {
	if err = k.r.CommitMessages(context.Background(), kafka.Message{
		Topic:     msg.Topic,
		Partition: msg.Partition,
		Offset:    msg.Offset,
	}); err != nil {
		err = errors.Wrapf(err, "")
		return
	}
	return
}

// Stop kafka consumer and close all connections
func (k *KafkaGo) Stop() error {
	k.cancel()
	k.wgRun.Wait()
	return nil
}

// Description of this kafka consumer, which topic it reads from
func (k *KafkaGo) Description() string {
	return "kafka consumer of topic " + k.taskCfg.Topic
}
