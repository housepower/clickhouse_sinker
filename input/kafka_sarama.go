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
	"crypto/sha256"
	"crypto/sha512"
	"hash"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"github.com/pkg/errors"
	"github.com/xdg/scram"

	"github.com/housepower/clickhouse_sinker/config"
	"github.com/housepower/clickhouse_sinker/model"
	"github.com/housepower/clickhouse_sinker/statistics"
	"github.com/housepower/clickhouse_sinker/util"
)

var _ Inputer = (*KafkaSarama)(nil)

// KafkaSarama implements input.Inputer
type KafkaSarama struct {
	cfg     *config.Config
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
	util.Logger.Infof("%s: consumer group %s cleanup", h.k.cfg.Task.Name, h.k.cfg.Task.ConsumerGroup)
	time.Sleep(5 * time.Second)
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
func (k *KafkaSarama) Init(cfg *config.Config, taskName string, putFn func(msg model.InputMessage)) (err error) {
	k.cfg = cfg
	kfkCfg := &cfg.Kafka
	taskCfg := &cfg.Task
	k.stopped = make(chan struct{})
	k.putFn = putFn
	config := sarama.NewConfig()
	if config.Version, err = sarama.ParseKafkaVersion(kfkCfg.Version); err != nil {
		err = errors.Wrapf(err, "")
		return
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
	if kfkCfg.TLS.Enable {
		config.Net.TLS.Enable = true
		if config.Net.TLS.Config, err = util.NewTLSConfig(kfkCfg.TLS.CaCertFiles, kfkCfg.TLS.ClientCertFile, kfkCfg.TLS.ClientKeyFile, kfkCfg.TLS.EndpIdentAlgo == ""); err != nil {
			return
		}
	}
	// check for authentication
	if kfkCfg.Sasl.Enable {
		config.Net.SASL.Enable = true
		if config.Version.IsAtLeast(sarama.V1_0_0_0) {
			config.Net.SASL.Version = sarama.SASLHandshakeV1
		}
		config.Net.SASL.Mechanism = (sarama.SASLMechanism)(kfkCfg.Sasl.Mechanism)
		switch config.Net.SASL.Mechanism {
		case "SCRAM-SHA-256":
			config.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &XDGSCRAMClient{HashGeneratorFcn: SHA256} }
		case "SCRAM-SHA-512":
			config.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &XDGSCRAMClient{HashGeneratorFcn: SHA256} }
		default:
		}
		config.Net.SASL.User = kfkCfg.Sasl.Username
		config.Net.SASL.Password = kfkCfg.Sasl.Password
		config.Net.SASL.GSSAPI = kfkCfg.Sasl.GSSAPI
	}
	if taskCfg.Earliest {
		config.Consumer.Offsets.Initial = sarama.OffsetOldest
	}
	config.ChannelBufferSize = taskCfg.MinBufferSize
	cg, err := sarama.NewConsumerGroup(strings.Split(kfkCfg.Brokers, ","), taskCfg.ConsumerGroup, config)
	if err != nil {
		return err
	}
	k.cg = cg
	return nil
}

// kafka main loop
func (k *KafkaSarama) Run(ctx context.Context) {
	taskCfg := &k.cfg.Task
LOOP_SARAMA:
	for {
		handler := MyConsumerGroupHandler{k}
		// `Consume` should be called inside an infinite loop, when a
		// server-side rebalance happens, the consumer session will need to be
		// recreated to get the new claims
		if err := k.cg.Consume(ctx, []string{taskCfg.Topic}, handler); err != nil {
			if errors.Is(err, context.Canceled) {
				util.Logger.Infof("%s: Kafka.Run quit due to context has been canceled", taskCfg.Name)
				break LOOP_SARAMA
			} else if errors.Is(err, sarama.ErrClosedConsumerGroup) {
				util.Logger.Infof("%s: Kafka.Run quit due to consumer group has been closed", taskCfg.Name)
				break LOOP_SARAMA
			} else {
				statistics.ConsumeMsgsErrorTotal.WithLabelValues(taskCfg.Name).Inc()
				err = errors.Wrap(err, "")
				util.Logger.Errorf("%s: Kafka.Run got error %+v", taskCfg.Name, err)
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
	return "kafka consumer of topic " + k.cfg.Task.Topic
}

// Predefined SCRAMClientGeneratorFunc, copied from https://github.com/Shopify/sarama/blob/master/examples/sasl_scram_client/scram_client.go

var SHA256 scram.HashGeneratorFcn = func() hash.Hash { return sha256.New() }
var SHA512 scram.HashGeneratorFcn = func() hash.Hash { return sha512.New() }

type XDGSCRAMClient struct {
	*scram.Client
	*scram.ClientConversation
	scram.HashGeneratorFcn
}

func (x *XDGSCRAMClient) Begin(userName, password, authzID string) (err error) {
	x.Client, err = x.HashGeneratorFcn.NewClient(userName, password, authzID)
	if err != nil {
		return err
	}
	x.ClientConversation = x.Client.NewConversation()
	return nil
}

func (x *XDGSCRAMClient) Step(challenge string) (response string, err error) {
	response, err = x.ClientConversation.Step(challenge)
	return
}

func (x *XDGSCRAMClient) Done() bool {
	return x.ClientConversation.Done()
}
