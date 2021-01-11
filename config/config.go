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

package config

import (
	"encoding/json"
	"io/ioutil"
	"strings"
	"time"

	"github.com/housepower/clickhouse_sinker/util"

	"github.com/pkg/errors"
)

// RemoteConfManager can be implemented by many backends: Nacos, Consul, etcd, ZooKeeper...
type RemoteConfManager interface {
	Init(properties map[string]interface{}) error
	// GetConfig fetchs the config. The manager shall not reference the returned Config object after call.
	GetConfig() (conf *Config, err error)
	// PublishConfig publishs the config.
	PublishConfig(conf *Config) (err error)
}

// Config struct used for different configurations use
type Config struct {
	Kafka      KafkaConfig
	Clickhouse ClickHouseConfig
	Task       TaskConfig
	LogLevel   string
}

// KafkaConfig configuration parameters
type KafkaConfig struct {
	Brokers string
	Version string
	TLS     struct {
		Enable             bool
		CaCertFiles        string // Required. It's the CA certificate with which Kafka brokers certs be signed.
		ClientCertFile     string // Required if Kafka brokers require client authentication.
		ClientKeyFile      string // Required if and only if ClientCertFile is present.
		InsecureSkipVerify bool   // Whether disable broker FQDN verification.
	}
	//simplified sarama.Config.Net.SASL to only support SASL/PLAIN and SASL/GSSAPI(Kerberos)
	Sasl struct {
		// Whether or not to use SASL authentication when connecting to the broker
		// (defaults to false).
		Enable bool
		// Mechanism is the name of the enabled SASL mechanism.
		// Possible values: PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, GSSAPI (defaults to PLAIN)
		Mechanism string
		// Username is the authentication identity (authcid) to present for
		// SASL/PLAIN or SASL/SCRAM authentication
		Username string
		// Password for SASL/PLAIN or SASL/SCRAM authentication
		Password string
		GSSAPI   struct {
			AuthType           int //1. KRB5_USER_AUTH, 2. KRB5_KEYTAB_AUTH
			KeyTabPath         string
			KerberosConfigPath string
			ServiceName        string
			Username           string
			Password           string
			Realm              string
			DisablePAFXFAST    bool
		}
	}
}

// ClickHouseConfig configuration parameters
type ClickHouseConfig struct {
	DB    string
	Hosts [][]string
	Port  int

	Username   string
	Password   string
	DsnParams  string
	RetryTimes int //<=0 means retry infinitely
}

// Task configuration parameters
type TaskConfig struct {
	Name string

	KafkaClient   string
	Topic         string
	ConsumerGroup string

	// Earliest set to true to consume the message from oldest position
	Earliest bool
	Parser   string
	// the csv cloum title if Parser is csv
	CsvFormat []string
	Delimiter string

	TableName string

	// AutoSchema will auto fetch the schema from clickhouse
	AutoSchema     bool
	ExcludeColumns []string
	Dims           []struct {
		Name       string
		Type       string
		SourceName string
	} `json:"dims"`

	// ShardingKey is the column name to which sharding against
	ShardingKey string `json:"shardingKey,omitempty"`
	// ShardingPolicy is `stripe,<interval>`(requires ShardingKey be numerical) or `hash`(requires ShardingKey be string)
	ShardingPolicy string `json:"shardingPolicy,omitempty"`

	FlushInterval    int    `json:"flushInterval,omitempty"`
	BufferSize       int    `json:"bufferSize,omitempty"`
	MinBufferSize    int    `json:"minBufferSize,omitempty"`
	MsgSizeHint      int    `json:"msgSizeHint,omitempty"`
	LayoutDate       string `json:"layoutDate,omitempty"`
	LayoutDateTime   string `json:"layoutDateTime,omitempty"`
	LayoutDateTime64 string `json:"layoutDateTime64,omitempty"`
}

const (
	defaultFlushInterval    = 3
	defaultBufferSize       = 1 << 20 //1048576
	defaultMinBufferSize    = 1 << 13 //   8196
	defaultMsgSizeHint      = 1000
	defaultLayoutDate       = "2006-01-02"
	defaultLayoutDateTime   = time.RFC3339
	defaultLayoutDateTime64 = time.RFC3339
	defaultLogLevel         = "info"
)

func ParseLocalCfgFile(cfgPath string) (cfg *Config, err error) {
	cfg = &Config{}
	var b []byte
	b, err = ioutil.ReadFile(cfgPath)
	if err != nil {
		err = errors.Wrapf(err, "")
		return
	}
	if err = json.Unmarshal(b, cfg); err != nil {
		err = errors.Wrapf(err, "")
		return
	}
	return
}

// normallize and validate configuration
func (cfg *Config) Normallize() (err error) {
	if cfg.Kafka.Version == "" {
		cfg.Kafka.Version = "2.2.1"
	}
	if cfg.Kafka.Sasl.Enable {
		cfg.Kafka.Sasl.Mechanism = strings.ToUpper(cfg.Kafka.Sasl.Mechanism)
		switch cfg.Kafka.Sasl.Mechanism {
		case "PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512", "GSSAPI":
		default:
			err = errors.Errorf("kafka SASL mechanism %s is unsupported", cfg.Kafka.Sasl.Mechanism)
			return
		}
	}
	if cfg.Clickhouse.RetryTimes < 0 {
		cfg.Clickhouse.RetryTimes = 0
	}

	if cfg.Kafka.Sasl.Enable && cfg.Kafka.Sasl.Username == "" {
		//kafka-go doesn't support SASL/GSSAPI(Kerberos). https://github.com/segmentio/kafka-go/issues/539
		cfg.Task.KafkaClient = "sarama"
	} else if cfg.Task.KafkaClient == "" {
		cfg.Task.KafkaClient = "kafka-go"
	}
	if cfg.Task.Parser == "" {
		cfg.Task.Parser = "fastjson"
	}

	for i := range cfg.Task.Dims {
		if cfg.Task.Dims[i].SourceName == "" {
			cfg.Task.Dims[i].SourceName = util.GetSourceName(cfg.Task.Dims[i].Name)
		}
	}

	if cfg.Task.FlushInterval <= 0 {
		cfg.Task.FlushInterval = defaultFlushInterval
	}
	if cfg.Task.BufferSize <= 0 {
		cfg.Task.BufferSize = defaultBufferSize
	} else {
		cfg.Task.BufferSize = 1 << util.GetShift(cfg.Task.BufferSize)
	}
	if cfg.Task.MinBufferSize <= 0 {
		cfg.Task.MinBufferSize = defaultMinBufferSize
	} else {
		cfg.Task.MinBufferSize = 1 << util.GetShift(cfg.Task.MinBufferSize)
	}
	if cfg.Task.MsgSizeHint <= 0 {
		cfg.Task.MsgSizeHint = defaultMsgSizeHint
	}
	if cfg.Task.LayoutDate == "" {
		cfg.Task.LayoutDate = defaultLayoutDate
	}
	if cfg.Task.LayoutDateTime == "" {
		cfg.Task.LayoutDateTime = defaultLayoutDateTime
	}
	if cfg.Task.LayoutDateTime64 == "" {
		cfg.Task.LayoutDateTime64 = defaultLayoutDateTime64
	}
	switch strings.ToLower(cfg.LogLevel) {
	case "panic", "fatal", "error", "warn", "warning", "info", "debug", "trace":
	default:
		cfg.LogLevel = defaultLogLevel
	}
	return
}
