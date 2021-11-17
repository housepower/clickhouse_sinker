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
	"strings"
	"sync"
	"time"

	krb5client "github.com/jcmturner/gokrb5/v8/client"
	krb5config "github.com/jcmturner/gokrb5/v8/config"
	"github.com/jcmturner/gokrb5/v8/keytab"
	"github.com/pkg/errors"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/kerberos"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
	"github.com/twmb/franz-go/plugin/kzap"
	"go.uber.org/zap"

	"github.com/housepower/clickhouse_sinker/config"
	"github.com/housepower/clickhouse_sinker/model"
	"github.com/housepower/clickhouse_sinker/util"
)

const (
	TOK_ID_KRB_AP_REQ   = 256
	GSS_API_GENERIC_TAG = 0x60
	KRB5_USER_AUTH      = 1
	KRB5_KEYTAB_AUTH    = 2
	GSS_API_INITIAL     = 1
	GSS_API_VERIFY      = 2
	GSS_API_FINISH      = 3
)

var _ Inputer = (*KafkaFranz)(nil)

// KafkaFranz implements input.Inputer
// refers to examples/group_consuming/main.go
type KafkaFranz struct {
	cfg       *config.Config
	taskCfg   *config.TaskConfig
	cl        *kgo.Client
	ctx       context.Context
	cancel    context.CancelFunc
	wgRun     sync.WaitGroup
	putFn     func(msg *model.InputMessage)
	cleanupFn func()
}

// NewKafkaFranz get instance of kafka reader
func NewKafkaFranz() *KafkaFranz {
	return &KafkaFranz{}
}

// Init Initialise the kafka instance with configuration
func (k *KafkaFranz) Init(cfg *config.Config, taskCfg *config.TaskConfig, putFn func(msg *model.InputMessage), cleanupFn func()) (err error) {
	k.cfg = cfg
	k.taskCfg = taskCfg
	k.ctx, k.cancel = context.WithCancel(context.Background())
	k.putFn = putFn
	k.cleanupFn = cleanupFn
	kfkCfg := &cfg.Kafka

	opts := []kgo.Opt{
		kgo.SeedBrokers(strings.Split(kfkCfg.Brokers, ",")...),
		kgo.ConsumeTopics(taskCfg.Topic),
		kgo.ConsumerGroup(taskCfg.ConsumerGroup),
		kgo.DisableAutoCommit(),
		kgo.OnPartitionsRevoked(k.onPartitionRevoked),
		kgo.MaxConcurrentFetches(3),
		kgo.FetchMaxBytes(1 << 27),      //134 MB
		kgo.BrokerMaxReadBytes(1 << 27), //134 MB
		kgo.WithLogger(kzap.New(util.Logger)),
	}
	if !taskCfg.Earliest {
		opts = append(opts, kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()))
	}
	if kfkCfg.TLS.Enable {
		var tlsCfg *tls.Config
		if tlsCfg, err = util.NewTLSConfig(kfkCfg.TLS.CaCertFiles, kfkCfg.TLS.ClientCertFile, kfkCfg.TLS.ClientKeyFile, kfkCfg.TLS.EndpIdentAlgo == ""); err != nil {
			return
		}
		opts = append(opts, kgo.DialTLSConfig(tlsCfg))
	}
	if kfkCfg.Sasl.Enable {
		var mch sasl.Mechanism
		switch kfkCfg.Sasl.Mechanism {
		case "PLAIN":
			auth := plain.Auth{
				User: kfkCfg.Sasl.Username,
				Pass: kfkCfg.Sasl.Password,
			}
			mch = auth.AsMechanism()
		case "SCRAM-SHA-256", "SCRAM-SHA-512":
			auth := scram.Auth{
				User: kfkCfg.Sasl.Username,
				Pass: kfkCfg.Sasl.Password,
			}
			switch kfkCfg.Sasl.Mechanism {
			case "SCRAM-SHA-256":
				mch = auth.AsSha256Mechanism()
			case "SCRAM-SHA-512":
				mch = auth.AsSha512Mechanism()
			default:
			}
		case "GSSAPI":
			gssapiCfg := kfkCfg.Sasl.GSSAPI
			auth := kerberos.Auth{Service: gssapiCfg.ServiceName}
			// refers to https://github.com/Shopify/sarama/blob/main/kerberos_client.go
			var krbCfg *krb5config.Config
			var kt *keytab.Keytab
			if krbCfg, err = krb5config.Load(gssapiCfg.KerberosConfigPath); err != nil {
				err = errors.Wrap(err, "")
				return
			}
			if gssapiCfg.AuthType == KRB5_KEYTAB_AUTH {
				if kt, err = keytab.Load(gssapiCfg.KeyTabPath); err != nil {
					err = errors.Wrap(err, "")
					return
				}
				auth.Client = krb5client.NewWithKeytab(gssapiCfg.Username, gssapiCfg.Realm, kt, krbCfg, krb5client.DisablePAFXFAST(gssapiCfg.DisablePAFXFAST))
			} else {
				auth.Client = krb5client.NewWithPassword(gssapiCfg.Username,
					gssapiCfg.Realm, gssapiCfg.Password, krbCfg, krb5client.DisablePAFXFAST(gssapiCfg.DisablePAFXFAST))
			}
			mch = auth.AsMechanism()
		}
		if mch != nil {
			opts = append(opts, kgo.SASL(mch))
		}
	}

	if k.cl, err = kgo.NewClient(opts...); err != nil {
		err = errors.Wrap(err, "")
		return
	}
	return nil
}

// kafka main loop
func (k *KafkaFranz) Run() {
	k.wgRun.Add(1)
	defer k.wgRun.Done()
	for {
		fetches := k.cl.PollFetches(k.ctx)
		if fetches == nil || fetches.IsClientClosed() {
			break
		}
		var hasError bool
		fetches.EachError(func(_ string, _ int32, err error) {
			err = errors.Wrap(err, "")
			util.Logger.Error("kgo.Client.PollFetchs() failed", zap.Error(err))
			hasError = true
		})
		if hasError {
			continue
		}
		fetches.EachRecord(func(rec *kgo.Record) {
			msg := &model.InputMessage{
				Topic:     rec.Topic,
				Partition: int(rec.Partition),
				Key:       rec.Key,
				Value:     rec.Value,
				Offset:    rec.Offset,
				Timestamp: &rec.Timestamp,
			}
			k.putFn(msg)
		})
	}
	k.cl.Close() // will trigger k.onPartitionRevoked
	util.Logger.Info("KafkaFranz.Run quit due to context has been canceled", zap.String("task", k.taskCfg.Name))
}

func (k *KafkaFranz) CommitMessages(msg *model.InputMessage) error {
	// "LeaderEpoch: -1" will disable leader epoch validation
	k.cl.CommitRecords(context.Background(), &kgo.Record{Topic: msg.Topic, Partition: int32(msg.Partition), Offset: msg.Offset, LeaderEpoch: -1})
	return nil
}

// Stop kafka consumer and close all connections
func (k *KafkaFranz) Stop() error {
	k.cancel()
	k.wgRun.Wait()
	return nil
}

// Description of this kafka consumer, which topic it reads from
func (k *KafkaFranz) Description() string {
	return "kafka consumer of topic " + k.taskCfg.Topic
}

func (k *KafkaFranz) onPartitionRevoked(_ context.Context, _ *kgo.Client, _ map[string][]int32) {
	begin := time.Now()
	k.cleanupFn()
	util.Logger.Info("consumer group cleanup",
		zap.String("task", k.taskCfg.Name),
		zap.String("consumer group", k.taskCfg.ConsumerGroup),
		zap.Duration("cost", time.Since(begin)))
}
