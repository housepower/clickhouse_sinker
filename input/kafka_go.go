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
	log "github.com/sirupsen/logrus"

	"github.com/housepower/clickhouse_sinker/config"
	"github.com/housepower/clickhouse_sinker/model"
	"github.com/housepower/clickhouse_sinker/statistics"
	"github.com/housepower/clickhouse_sinker/util"
)

var _ Inputer = (*KafkaGo)(nil)

// KafkaGo implements input.Inputer
type KafkaGo struct {
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
func (k *KafkaGo) Init(cfg *config.Config, taskName string, putFn func(msg model.InputMessage)) (err error) {
	k.taskCfg = cfg.Tasks[taskName]
	kfkCfg := cfg.Kafka[k.taskCfg.Kafka]
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
		MinBytes:       k.taskCfg.MinBufferSize * k.taskCfg.MsgSizeHint,
		MaxBytes:       k.taskCfg.BufferSize * k.taskCfg.MsgSizeHint,
		MaxWait:        time.Duration(k.taskCfg.FlushInterval) * time.Second,
		CommitInterval: time.Second,          // flushes commits to Kafka every second
		ErrorLogger:    log.StandardLogger(), //kafka-go INFO log is too verbose
	}
	var dialer *kafka.Dialer
	if kfkCfg.TLS.Enable {
		var tlsConfig *tls.Config
		if tlsConfig, err = util.NewTLSConfig(kfkCfg.TLS.CaCertFiles, kfkCfg.TLS.ClientCertFile, kfkCfg.TLS.ClientKeyFile, kfkCfg.TLS.InsecureSkipVerify); err != nil {
			return
		}
		dialer = &kafka.Dialer{
			DualStack: true,
			TLS:       tlsConfig,
		}
	}
	if kfkCfg.Sasl.Enable {
		if kfkCfg.Sasl.Username != "" {
			if dialer == nil {
				dialer = &kafka.Dialer{DualStack: true}
			}
			dialer.SASLMechanism = plain.Mechanism{
				Username: kfkCfg.Sasl.Username,
				Password: kfkCfg.Sasl.Password,
			}
		} else {
			return errors.Errorf("kafka-go doesn't support SASL/GSSAPI(Kerberos)")
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
				log.Infof("%s: Kafka.Run quit due to context has been canceled", k.taskCfg.Name)
				break LOOP_KAFKA_GO
			} else if errors.Is(err, io.EOF) {
				log.Infof("%s: Kafka.Run quit due to reader has been closed", k.taskCfg.Name)
				break LOOP_KAFKA_GO
			} else {
				statistics.ConsumeMsgsErrorTotal.WithLabelValues(k.taskCfg.Name).Inc()
				err = errors.Wrap(err, "")
				log.Errorf("%s: Kafka.Run got error %+v", k.taskCfg.Name, err)
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
