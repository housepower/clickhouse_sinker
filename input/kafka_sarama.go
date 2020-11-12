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
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"

	"github.com/housepower/clickhouse_sinker/config"
	"github.com/housepower/clickhouse_sinker/model"
	"github.com/housepower/clickhouse_sinker/statistics"
)

var _ Inputer = (*KafkaSarama)(nil)

// KafkaSarama implements input.Inputer
type KafkaSarama struct {
	taskCfg *config.TaskConfig
	cg      sarama.ConsumerGroup
	sess    sarama.ConsumerGroupSession
	stopped chan struct{}
	putFn   func(msg model.InputMessage)
}

// NewKafkaSarama get instance of kafka reader
func NewKafkaSarama() *KafkaSarama {
	return &KafkaSarama{}
}

type MyConsumerGroupHandler struct {
	k *KafkaSarama //point back to which kafka this handler belongs to
}

func (h MyConsumerGroupHandler) Setup(sess sarama.ConsumerGroupSession) error {
	h.k.sess = sess
	return nil
}
func (h MyConsumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error {
	log.Infof("%s: consumer group %s cleanup", h.k.taskCfg.Name, h.k.taskCfg.ConsumerGroup)
	//TODO: Flush all rings helps to consuming duplicated messages?
	time.Sleep(1 * time.Second)
	return nil
}

func (h MyConsumerGroupHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		h.k.putFn(model.InputMessage{
			Topic:     msg.Topic,
			Partition: int(msg.Partition),
			Key:       msg.Key,
			Value:     msg.Value,
			Offset:    msg.Offset,
			Timestamp: &msg.Timestamp,
		})
	}
	return nil
}

// Init Initialise the kafka instance with configuration
func (k *KafkaSarama) Init(cfg *config.Config, taskName string, putFn func(msg model.InputMessage)) error {
	k.taskCfg = cfg.Tasks[taskName]
	kfkCfg := cfg.Kafka[k.taskCfg.Kafka]
	k.stopped = make(chan struct{})
	k.putFn = putFn
	config := sarama.NewConfig()
	if kfkCfg.Version != "" {
		version, err := sarama.ParseKafkaVersion(kfkCfg.Version)
		if err != nil {
			err = errors.Wrap(err, "")
			return err
		}
		config.Version = version
	}
	// sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)
	// check for authentication
	if kfkCfg.Sasl.Enable {
		config.Net.SASL.Enable = true
		if kfkCfg.Sasl.Username != "" {
			config.Net.SASL.Mechanism = sarama.SASLTypePlaintext
			config.Net.SASL.User = kfkCfg.Sasl.Username
			config.Net.SASL.Password = kfkCfg.Sasl.Password
		} else {
			config.Net.SASL.Mechanism = sarama.SASLTypeGSSAPI
			config.Net.SASL.GSSAPI = kfkCfg.Sasl.GSSAPI
		}
	}
	if k.taskCfg.Earliest {
		config.Consumer.Offsets.Initial = sarama.OffsetOldest
	}
	config.ChannelBufferSize = k.taskCfg.MinBufferSize
	cg, err := sarama.NewConsumerGroup(strings.Split(kfkCfg.Brokers, ","), k.taskCfg.ConsumerGroup, config)
	if err != nil {
		return err
	}
	k.cg = cg
	return nil
}

// kafka main loop
func (k *KafkaSarama) Run(ctx context.Context) {
LOOP_SARAMA:
	for {
		handler := MyConsumerGroupHandler{k}
		// `Consume` should be called inside an infinite loop, when a
		// server-side rebalance happens, the consumer session will need to be
		// recreated to get the new claims
		if err := k.cg.Consume(ctx, []string{k.taskCfg.Topic}, handler); err != nil {
			if errors.Is(err, context.Canceled) {
				log.Infof("%s: Kafka.Run quit due to context has been canceled", k.taskCfg.Name)
				break LOOP_SARAMA
			} else if errors.Is(err, sarama.ErrClosedConsumerGroup) {
				log.Infof("%s: Kafka.Run quit due to consumer group has been closed", k.taskCfg.Name)
				break LOOP_SARAMA
			} else {
				statistics.ConsumeMsgsErrorTotal.WithLabelValues(k.taskCfg.Name).Inc()
				err = errors.Wrap(err, "")
				log.Errorf("%s: Kafka.Run got error %+v", k.taskCfg.Name, err)
				continue
			}
		}
	}
}

func (k *KafkaSarama) CommitMessages(ctx context.Context, msg *model.InputMessage) error {
	k.sess.MarkOffset(msg.Topic, int32(msg.Partition), msg.Offset+1, "")
	return nil
}

// Stop kafka consumer and close all connections
func (k *KafkaSarama) Stop() error {
	k.cg.Close()
	return nil
}

// Description of this kafka consumer, which topic it reads from
func (k *KafkaSarama) Description() string {
	return "kafka consumer of topic " + k.taskCfg.Topic
}
