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
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/pkg/errors"
	"github.com/xdg-go/scram"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/housepower/clickhouse_sinker/config"
	"github.com/housepower/clickhouse_sinker/model"
	"github.com/housepower/clickhouse_sinker/statistics"
	"github.com/housepower/clickhouse_sinker/util"
)

var _ Inputer = (*KafkaSarama)(nil)

// KafkaSarama implements input.Inputer
type KafkaSarama struct {
	cfg       *config.Config
	taskCfg   *config.TaskConfig
	cg        sarama.ConsumerGroup
	sess      sarama.ConsumerGroupSession
	ctx       context.Context
	cancel    context.CancelFunc
	wgRun     sync.WaitGroup
	putFn     func(msg *model.InputMessage)
	cleanupFn func()
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
	begin := time.Now()
	h.k.cleanupFn()
	util.Logger.Info("consumer group cleanup",
		zap.String("task", h.k.taskCfg.Name),
		zap.String("consumer group", h.k.taskCfg.ConsumerGroup),
		zap.Int32("generation id", h.k.sess.GenerationID()),
		zap.Duration("cost", time.Since(begin)))
	return nil
}

func (h MyConsumerGroupHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		h.k.putFn(&model.InputMessage{
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
func (k *KafkaSarama) Init(cfg *config.Config, taskCfg *config.TaskConfig, putFn func(msg *model.InputMessage), cleanupFn func()) (err error) {
	k.cfg = cfg
	k.taskCfg = taskCfg
	k.ctx, k.cancel = context.WithCancel(context.Background())
	k.putFn = putFn
	k.cleanupFn = cleanupFn
	kfkCfg := &cfg.Kafka
	sarCfg, err := GetSaramaConfig(&cfg.Kafka)
	if err != nil {
		return err
	}
	if taskCfg.Earliest {
		sarCfg.Consumer.Offsets.Initial = sarama.OffsetOldest
	}
	cg, err := sarama.NewConsumerGroup(strings.Split(kfkCfg.Brokers, ","), taskCfg.ConsumerGroup, sarCfg)
	if err != nil {
		return err
	}
	sarama.Logger, _ = zap.NewStdLogAt(util.Logger.With(zap.String("name", "sarama")), zapcore.DebugLevel)
	k.cg = cg
	return nil
}

func GetSaramaConfig(kfkCfg *config.KafkaConfig) (sarCfg *sarama.Config, err error) {
	sarCfg = sarama.NewConfig()
	if sarCfg.Version, err = sarama.ParseKafkaVersion(kfkCfg.Version); err != nil {
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
		sarCfg.Net.TLS.Enable = true
		if sarCfg.Net.TLS.Config, err = util.NewTLSConfig(kfkCfg.TLS.CaCertFiles, kfkCfg.TLS.ClientCertFile, kfkCfg.TLS.ClientKeyFile, kfkCfg.TLS.EndpIdentAlgo == ""); err != nil {
			return
		}
	}
	// check for authentication
	if kfkCfg.Sasl.Enable {
		sarCfg.Net.SASL.Enable = true
		if sarCfg.Version.IsAtLeast(sarama.V1_0_0_0) {
			sarCfg.Net.SASL.Version = sarama.SASLHandshakeV1
		}
		sarCfg.Net.SASL.Mechanism = (sarama.SASLMechanism)(kfkCfg.Sasl.Mechanism)
		switch sarCfg.Net.SASL.Mechanism {
		case "SCRAM-SHA-256":
			sarCfg.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &XDGSCRAMClient{HashGeneratorFcn: SHA256} }
		case "SCRAM-SHA-512":
			sarCfg.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &XDGSCRAMClient{HashGeneratorFcn: SHA256} }
		default:
		}
		sarCfg.Net.SASL.User = kfkCfg.Sasl.Username
		sarCfg.Net.SASL.Password = kfkCfg.Sasl.Password
		sarCfg.Net.SASL.GSSAPI = kfkCfg.Sasl.GSSAPI
	}
	sarCfg.ChannelBufferSize = 1024
	return
}

// kafka main loop
func (k *KafkaSarama) Run() {
	k.wgRun.Add(1)
	defer k.wgRun.Done()
	taskCfg := k.taskCfg
LOOP_SARAMA:
	for {
		handler := MyConsumerGroupHandler{k}
		// `Consume` should be called inside an infinite loop, when a
		// server-side rebalance happens, the consumer session will need to be
		// recreated to get the new claims
		if err := k.cg.Consume(k.ctx, []string{taskCfg.Topic}, handler); err != nil {
			if errors.Is(err, context.Canceled) {
				util.Logger.Info("KafkaSarama.Run quit due to context has been canceled", zap.String("task", k.taskCfg.Name))
				break LOOP_SARAMA
			} else if errors.Is(err, sarama.ErrClosedConsumerGroup) {
				util.Logger.Info("KafkaSarama.Run quit due to consumer group has been closed", zap.String("task", k.taskCfg.Name))
				break LOOP_SARAMA
			} else {
				statistics.ConsumeMsgsErrorTotal.WithLabelValues(taskCfg.Name).Inc()
				err = errors.Wrap(err, "")
				util.Logger.Error("sarama.ConsumerGroup.Consume failed", zap.String("task", k.taskCfg.Name), zap.Error(err))
				continue
			}
		}
	}
}

func (k *KafkaSarama) CommitMessages(msg *model.InputMessage) error {
	k.sess.MarkOffset(msg.Topic, int32(msg.Partition), msg.Offset+1, "")
	return nil
}

// Stop kafka consumer and close all connections
func (k *KafkaSarama) Stop() error {
	k.cancel()
	k.cg.Close()
	k.wgRun.Wait()
	return nil
}

// Description of this kafka consumer, which topic it reads from
func (k *KafkaSarama) Description() string {
	return "kafka consumer of topic " + k.taskCfg.Topic
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
