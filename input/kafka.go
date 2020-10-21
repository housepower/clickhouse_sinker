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

	"github.com/Shopify/sarama"
	"github.com/pkg/errors"
	"github.com/sundy-li/go_commons/log"

	"github.com/housepower/clickhouse_sinker/config"
	"github.com/housepower/clickhouse_sinker/model"
	"github.com/housepower/clickhouse_sinker/statistics"
)

// Kafka reader configuration
type Kafka struct {
	taskCfg *config.TaskConfig

	cg      sarama.ConsumerGroup
	sess    sarama.ConsumerGroupSession
	stopped chan struct{}
	putFn   func(msg model.InputMessage)
}

// NewKafka get instance of kafka reader
func NewKafka() *Kafka {
	return &Kafka{}
}

type MyConsumerGroupHandler struct {
	k *Kafka //point back to which kafka this handler belongs to
}

func (h MyConsumerGroupHandler) Setup(sess sarama.ConsumerGroupSession) error {
	h.k.sess = sess
	return nil
}
func (h MyConsumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error {
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
func (k *Kafka) Init(taskCfg *config.TaskConfig, putFn func(msg model.InputMessage)) error {
	k.taskCfg = taskCfg

	cfg := config.GetConfig()
	kfkCfg := cfg.Kafka[k.taskCfg.Kafka]
	k.stopped = make(chan struct{})
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
	k.putFn = putFn
	return nil
}

// kafka main loop
func (k *Kafka) Run(ctx context.Context) {
LOOP:
	for {
		handler := MyConsumerGroupHandler{k}
		// `Consume` should be called inside an infinite loop, when a
		// server-side rebalance happens, the consumer session will need to be
		// recreated to get the new claims
		if err := k.cg.Consume(ctx, []string{k.taskCfg.Topic}, handler); err != nil {
			switch errors.Cause(err) {
			case context.Canceled:
				log.Infof("%s Kafka.Run quit due to context has been canceled", k.taskCfg.Name)
				break LOOP
			case sarama.ErrClosedConsumerGroup:
				log.Infof("%s Kafka.Run quit due to consumer group has been closed", k.taskCfg.Name)
				break LOOP
			default:
				statistics.ConsumeMsgsErrorTotal.WithLabelValues(k.taskCfg.Name).Inc()
				err = errors.Wrap(err, "")
				log.Errorf("%s Kafka.Run got error %+v", k.taskCfg.Name, err)
				continue
			}
		}
	}
}

func (k *Kafka) CommitMessages(ctx context.Context, msg *model.InputMessage) error {
	k.sess.MarkOffset(msg.Topic, int32(msg.Partition), msg.Offset, "")
	return nil
}

// Stop kafka consumer and close all connections
func (k *Kafka) Stop() error {
	_ = k.cg.Close()
	return nil
}

// Description of this kafka consumer, which topic it reads from
func (k *Kafka) Description() string {
	return "kafka consumer of topic " + k.taskCfg.Topic
}
