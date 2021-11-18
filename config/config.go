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
	"regexp"
	"strings"

	"github.com/housepower/clickhouse_sinker/util"

	"github.com/pkg/errors"
)

// Config struct used for different configurations use
type Config struct {
	Kafka      KafkaConfig
	Clickhouse ClickHouseConfig
	Task       *TaskConfig
	Tasks      []*TaskConfig
	Assignment Assignment
	LogLevel   string
}

// KafkaConfig configuration parameters
type KafkaConfig struct {
	Brokers  string
	Version  string
	Security map[string]string
	TLS      struct {
		Enable         bool
		CaCertFiles    string // Required. It's the CA cert.pem with which Kafka brokers certs be signed.
		ClientCertFile string // Required for client authentication. It's client cert.pem.
		ClientKeyFile  string // Required if and only if ClientCertFile is present. It's client key.pem.

		TrustStoreLocation string //JKS format of CA certificate, used to extract CA cert.pem.
		TrustStorePassword string
		KeystoreLocation   string //JKS format of client certificate and key, used to extrace client cert.pem and key.pem.
		KeystorePassword   string
		EndpIdentAlgo      string
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
	Cluster   string
	DB        string
	Hosts     [][]string
	Port      int
	Username  string
	Password  string
	DsnParams string

	// Whether enable TLS encryption with clickhouse-server
	Secure bool
	// Whether skip verify clickhouse-server cert
	InsecureSkipVerify bool

	RetryTimes   int //<=0 means retry infinitely
	MaxOpenConns int
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
	// DynamicSchema will add columns present in message to clickhouse. Requires AutoSchema be true.
	DynamicSchema struct {
		Enable  bool
		MaxDims int // the upper limit of dynamic columns number, <=0 means math.MaxInt16. protecting dirty data attack
		// A column is added for new key K if all following conditions are true:
		// - K isn't in ExcludeColumns
		// - number of existing columns doesn't reach MaxDims-1
		// - WhiteList is empty, or K matchs WhiteList
		// - BlackList is empty, or K doesn't match BlackList
		WhiteList string // the regexp of white list
		BlackList string // the regexp of black list
	}
	// PrometheusSchema expects each message is a Prometheus metric(timestamp, value, metric name and a list of labels).
	PrometheusSchema bool

	// ShardingKey is the column name to which sharding against
	ShardingKey string `json:"shardingKey,omitempty"`
	// ShardingPolicy is `stripe,<interval>`(requires ShardingKey be numerical) or `hash`(requires ShardingKey be string)
	ShardingPolicy string `json:"shardingPolicy,omitempty"`

	FlushInterval int     `json:"flushInterval,omitempty"`
	BufferSize    int     `json:"bufferSize,omitempty"`
	TimeZone      string  `json:"timeZone"`
	TimeUnit      float64 `json:"timeUnit"`
}

type Assignment struct {
	Version   int
	UpdatedAt int64               // timestamp when created
	UpdatedBy string              // leader instance
	Map       map[string][]string // map instance to a list of task_name
}

const (
	MaxBufferSize             = 1 << 20 //1048576
	defaultBufferSize         = 1 << 18 //262144
	maxFlushInterval          = 600
	defaultFlushInterval      = 5
	defaultTimeZone           = "Local"
	defaultLogLevel           = "info"
	defaultKerberosConfigPath = "/etc/krb5.conf"
	defaultMaxOpenConns       = 1
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
	if len(cfg.Clickhouse.Hosts) == 0 || cfg.Kafka.Brokers == "" {
		err = errors.Errorf("invalid configuration")
		return
	}
	if cfg.Kafka.Version == "" {
		// https://cwiki.apache.org/confluence/display/KAFKA/Compatibility+Matrix
		// KIP-35 - Retrieving protocol version introduced a mechanism for dynamically determining the functionality of a Kafka broker.
		// https://github.com/Shopify/sarama/issues/1732,
		// Historically Sarama has tied it's protocol usage to the Version field in Config.
		// https://kafka.apache.org/downloads
		// 2.0.0 is released at July 30, 2018.
		cfg.Kafka.Version = "2.0.0"
	}

	cfg.convertKfkSecurity()
	if cfg.Kafka.TLS.CaCertFiles == "" && cfg.Kafka.TLS.TrustStoreLocation != "" {
		if cfg.Kafka.TLS.CaCertFiles, _, err = util.JksToPem(cfg.Kafka.TLS.TrustStoreLocation, cfg.Kafka.TLS.TrustStorePassword, false); err != nil {
			return
		}
	}
	if cfg.Kafka.TLS.ClientKeyFile == "" && cfg.Kafka.TLS.KeystoreLocation != "" {
		if cfg.Kafka.TLS.ClientCertFile, cfg.Kafka.TLS.ClientKeyFile, err = util.JksToPem(cfg.Kafka.TLS.KeystoreLocation, cfg.Kafka.TLS.KeystorePassword, false); err != nil {
			return
		}
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
	if cfg.Clickhouse.MaxOpenConns <= 0 {
		cfg.Clickhouse.MaxOpenConns = defaultMaxOpenConns
	}

	if cfg.Task != nil {
		cfg.Tasks = append(cfg.Tasks, cfg.Task)
		cfg.Task = nil
	}
	for _, taskCfg := range cfg.Tasks {
		if err = cfg.normallizeTask(taskCfg); err != nil {
			return
		}
	}
	switch strings.ToLower(cfg.LogLevel) {
	case "debug", "info", "warn", "error", "dpanic", "panic", "fatal":
	default:
		cfg.LogLevel = defaultLogLevel
	}
	return
}

func (cfg *Config) normallizeTask(taskCfg *TaskConfig) (err error) {
	if taskCfg.KafkaClient == "" {
		// known limitations of kafka-go:
		// - The Reader API is too high-level. There's no generation cleanup callback which sarama provides.
		// - Doesn't support SASL/GSSAPI(Kerberos). https://github.com/segmentio/kafka-go/issues/539
		taskCfg.KafkaClient = "franz"
	}
	if taskCfg.Parser == "" || taskCfg.Parser == "json" {
		taskCfg.Parser = "fastjson"
	}

	for i := range taskCfg.Dims {
		if taskCfg.Dims[i].SourceName == "" {
			taskCfg.Dims[i].SourceName = util.GetSourceName(taskCfg.Dims[i].Name)
		}
	}

	if taskCfg.FlushInterval <= 0 {
		taskCfg.FlushInterval = defaultFlushInterval
	} else if taskCfg.FlushInterval > maxFlushInterval {
		taskCfg.FlushInterval = maxFlushInterval
	}
	if taskCfg.BufferSize <= 0 {
		taskCfg.BufferSize = defaultBufferSize
	} else if taskCfg.BufferSize > MaxBufferSize {
		taskCfg.BufferSize = MaxBufferSize
	} else {
		taskCfg.BufferSize = 1 << util.GetShift(taskCfg.BufferSize)
	}
	if taskCfg.TimeZone == "" {
		taskCfg.TimeZone = defaultTimeZone
	}
	if taskCfg.TimeUnit == 0.0 {
		taskCfg.TimeUnit = float64(1.0)
	}
	if taskCfg.PrometheusSchema {
		taskCfg.DynamicSchema.Enable = true
		taskCfg.AutoSchema = true
	}
	if taskCfg.DynamicSchema.Enable {
		if taskCfg.Parser != "fastjson" && taskCfg.Parser != "gjson" {
			err = errors.Errorf("Parser %s doesn't support DynamicSchema", taskCfg.Parser)
			return
		}
		if cfg.Clickhouse.Cluster == "" {
			var numHosts int
			for _, shard := range cfg.Clickhouse.Hosts {
				numHosts += len(shard)
			}
			if numHosts > 1 {
				err = errors.Errorf("Need to set cluster name when DynamicSchema is enabled and number of hosts is more than one")
				return
			}
		}
	}
	if taskCfg.DynamicSchema.WhiteList != "" {
		if _, err = regexp.Compile(taskCfg.DynamicSchema.WhiteList); err != nil {
			err = errors.Wrapf(err, "WhiteList %s is invalid regexp", taskCfg.DynamicSchema.WhiteList)
			return
		}
	}
	if taskCfg.DynamicSchema.BlackList != "" {
		if _, err = regexp.Compile(taskCfg.DynamicSchema.BlackList); err != nil {
			err = errors.Wrapf(err, "BlackList %s is invalid regexp", taskCfg.DynamicSchema.BlackList)
			return
		}
	}
	return
}

//convert java client style configuration into sinker
func (cfg *Config) convertKfkSecurity() {
	if protocol, ok := cfg.Kafka.Security["security.protocol"]; ok {
		if strings.Contains(protocol, "SASL") {
			cfg.Kafka.Sasl.Enable = true
		}
		if strings.Contains(protocol, "SSL") {
			cfg.Kafka.TLS.Enable = true
		}
	}

	if cfg.Kafka.TLS.Enable {
		if endpIdentAlgo, ok := cfg.Kafka.Security["ssl.endpoint.identification.algorithm"]; ok {
			cfg.Kafka.TLS.EndpIdentAlgo = endpIdentAlgo
		}
		if trustStoreLocation, ok := cfg.Kafka.Security["ssl.truststore.location"]; ok {
			cfg.Kafka.TLS.TrustStoreLocation = trustStoreLocation
		}
		if trustStorePassword, ok := cfg.Kafka.Security["ssl.truststore.password"]; ok {
			cfg.Kafka.TLS.TrustStorePassword = trustStorePassword
		}
		if keyStoreLocation, ok := cfg.Kafka.Security["ssl.keystore.location"]; ok {
			cfg.Kafka.TLS.KeystoreLocation = keyStoreLocation
		}
		if keyStorePassword, ok := cfg.Kafka.Security["ssl.keystore.password"]; ok {
			cfg.Kafka.TLS.KeystorePassword = keyStorePassword
		}
	}
	if cfg.Kafka.Sasl.Enable {
		if mechanism, ok := cfg.Kafka.Security["sasl.mechanism"]; ok {
			cfg.Kafka.Sasl.Mechanism = mechanism
		}
		if config, ok := cfg.Kafka.Security["sasl.jaas.config"]; ok {
			configMap := readConfig(config)
			if strings.Contains(cfg.Kafka.Sasl.Mechanism, "SCRAM") {
				// SCRAM-SHA-256 or SCRAM-SHA-512
				if username, ok := configMap["username"]; ok {
					cfg.Kafka.Sasl.Username = username
				}
				if password, ok := configMap["password"]; ok {
					cfg.Kafka.Sasl.Password = password
				}
			}
			if strings.Contains(cfg.Kafka.Sasl.Mechanism, "GSSAPI") {
				// GSSAPI
				if useKeyTab, ok := configMap["useKeyTab"]; ok {
					if useKeyTab == "true" {
						cfg.Kafka.Sasl.GSSAPI.AuthType = 2
					} else {
						cfg.Kafka.Sasl.GSSAPI.AuthType = 1
					}
				}
				if cfg.Kafka.Sasl.GSSAPI.AuthType == 1 {
					//Username and password
					if username, ok := configMap["username"]; ok {
						cfg.Kafka.Sasl.GSSAPI.Username = username
					}
					if password, ok := configMap["password"]; ok {
						cfg.Kafka.Sasl.GSSAPI.Password = password
					}
				} else {
					//Keytab
					if keyTab, ok := configMap["keyTab"]; ok {
						cfg.Kafka.Sasl.GSSAPI.KeyTabPath = keyTab
					}
					if principal, ok := configMap["principal"]; ok {
						username := strings.Split(principal, "@")[0]
						realm := strings.Split(principal, "@")[1]
						cfg.Kafka.Sasl.GSSAPI.Username = username
						cfg.Kafka.Sasl.GSSAPI.Realm = realm
					}
					if servicename, ok := cfg.Kafka.Security["sasl.kerberos.service.name"]; ok {
						cfg.Kafka.Sasl.GSSAPI.ServiceName = servicename
					}
					if cfg.Kafka.Sasl.GSSAPI.KerberosConfigPath == "" {
						cfg.Kafka.Sasl.GSSAPI.KerberosConfigPath = defaultKerberosConfigPath
					}
				}
			}
		}
	}
}

func (cfg *Config) IsAssigned(instance, task string) (assigned bool) {
	if taskNames, ok := cfg.Assignment.Map[instance]; ok {
		for _, taskName := range taskNames {
			if taskName == task {
				assigned = true
				return
			}
		}
	}
	return
}

func readConfig(config string) map[string]string {
	configMap := make(map[string]string)
	config = strings.TrimSuffix(config, ";")
	fields := strings.Split(config, " ")
	for _, field := range fields {
		if strings.Contains(field, "=") {
			key := strings.Split(field, "=")[0]
			value := strings.Split(field, "=")[1]
			value = strings.Trim(value, "\"")
			configMap[key] = value
		}
	}
	return configMap
}
