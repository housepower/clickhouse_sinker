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
	"time"

	"github.com/pkg/errors"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/segmentio/kafka-go/sasl/scram"
	"go.uber.org/zap"

	"github.com/housepower/clickhouse_sinker/config"
	"github.com/housepower/clickhouse_sinker/model"
	"github.com/housepower/clickhouse_sinker/statistics"
	"github.com/housepower/clickhouse_sinker/util"
)

var _ Inputer = (*KafkaGo)(nil)

// KafkaGo implements input.Inputer
type KafkaGo struct {
	cfg     *config.Config
	taskCfg *config.TaskConfig
	r       *kafka.Reader
	stopped chan struct{}
	putFn   func(msg model.InputMessage)
}

// NewKafkaGo get instance of kafka reader
func NewKafkaGo() *KafkaGo {
	return &KafkaGo{}
}

// Init Initialise the kafka instance with configuration
func (k *KafkaGo) Init(cfg *config.Config, taskCfg *config.TaskConfig, putFn func(msg model.InputMessage)) (err error) {
	k.cfg = cfg
	k.taskCfg = taskCfg
	kfkCfg := &cfg.Kafka
	k.stopped = make(chan struct{})
	k.putFn = putFn
	offset := kafka.LastOffset
	if k.taskCfg.Earliest {
		offset = kafka.FirstOffset
	}
	readerCfg := &kafka.ReaderConfig{
		Brokers:        strings.Split(kfkCfg.Brokers, ","),
		GroupID:        k.taskCfg.ConsumerGroup,
		Topic:          k.taskCfg.Topic,
		StartOffset:    offset,
		MinBytes:       1024 * 1024,                           // sarama.Consumer.Fetch.Min
		MaxBytes:       100 * 1024 * 1024,                     // sarama.MaxResponseSize
		MaxWait:        time.Duration(100) * time.Millisecond, // sarama.Consumer.MaxWaitTime
		CommitInterval: time.Second,                           // flushes commits to Kafka every second
	}
	if kfkCfg.TLS.CaCertFiles == "" && kfkCfg.TLS.TrustStoreLocation != "" {
		if kfkCfg.TLS.CaCertFiles, _, err = util.JksToPem(kfkCfg.TLS.TrustStoreLocation, kfkCfg.TLS.TrustStorePassword, false); err != nil {
			return
		}
	}
	if kfkCfg.TLS.ClientKeyFile == "" && kfkCfg.TLS.KeystoreLocation != "" {
		if kfkCfg.TLS.ClientCertFile, kfkCfg.TLS.ClientKeyFile, err = util.JksToPem(kfkCfg.TLS.KeystoreLocation, kfkCfg.TLS.KeystorePassword, false); err != nil {
			return
		}
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
			return errors.Errorf("kafka-go doesn't support SASL/%s authentication", kfkCfg.Sasl.Mechanism)
		}
	}
	if dialer != nil {
		readerCfg.Dialer = dialer
	}
	k.r = kafka.NewReader(*readerCfg)
	return nil
}

// kafka main loop
func (k *KafkaGo) Run(ctx context.Context) {
LOOP_KAFKA_GO:
	for {
		var err error
		var msg kafka.Message
		if msg, err = k.r.FetchMessage(ctx); err != nil {
			if errors.Is(err, context.Canceled) {
				util.Logger.Info("Kafka.Run quit due to context has been canceled", zap.String("task", k.taskCfg.Name))
				break LOOP_KAFKA_GO
			} else if errors.Is(err, io.EOF) {
				util.Logger.Info("Kafka.Run quit due to reader has been closed", zap.String("task", k.taskCfg.Name))
				break LOOP_KAFKA_GO
			} else {
				statistics.ConsumeMsgsErrorTotal.WithLabelValues(k.taskCfg.Name).Inc()
				err = errors.Wrap(err, "")
				util.Logger.Error("k.r.FetchMessage failed", zap.String("task", k.taskCfg.Name), zap.Error(err))
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

func (k *KafkaGo) CommitMessages(ctx context.Context, msg *model.InputMessage) (err error) {
	if err = k.r.CommitMessages(ctx, kafka.Message{
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
	if k.r != nil {
		k.r.Close()
	}
	return nil
}

// Description of this kafka consumer, which topic it reads from
func (k *KafkaGo) Description() string {
	return "kafka consumer of topic " + k.taskCfg.Topic
}
