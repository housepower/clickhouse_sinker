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
	"context"
	"net"
	"os"
	"regexp"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/hjson/hjson-go/v4"
	"go.uber.org/zap"

	"github.com/housepower/clickhouse_sinker/util"

	"github.com/thanos-io/thanos/pkg/errors"
)

// Config struct used for different configurations use
type Config struct {
	Kafka                    KafkaConfig
	Clickhouse               ClickHouseConfig
	Task                     *TaskConfig
	Tasks                    []*TaskConfig
	Assignment               Assignment
	LogLevel                 string
	LogTrace                 bool
	RecordPoolSize           int64
	CleanupSeriesMapInterval int
	ActiveSeriesRange        int

	Groups map[string]*GroupConfig `json:"-"`
}

// KafkaConfig configuration parameters
type KafkaConfig struct {
	Brokers    string
	Properties struct {
		HeartbeatInterval      int `json:"heartbeat.interval.ms"`
		SessionTimeout         int `json:"session.timeout.ms"`
		RebalanceTimeout       int `json:"rebalance.timeout.ms"`
		RequestTimeoutOverhead int `json:"request.timeout.ms"`
		MaxPollInterval        int `json:"max.poll.interval.ms"`
	}
	ResetSaslRealm bool
	Security       map[string]string
	TLS            struct {
		Enable         bool
		CaCertFiles    string // CA cert.pem with which Kafka brokers certs be signed.  Leave empty for certificates trusted by the OS
		ClientCertFile string // Required for client authentication. It's client cert.pem.
		ClientKeyFile  string // Required if and only if ClientCertFile is present. It's client key.pem.

		TrustStoreLocation string // JKS format of CA certificate, used to extract CA cert.pem.
		TrustStorePassword string
		KeystoreLocation   string // JKS format of client certificate and key, used to extrace client cert.pem and key.pem.
		KeystorePassword   string
		EndpIdentAlgo      string
	}
	// simplified sarama.Config.Net.SASL to only support SASL/PLAIN and SASL/GSSAPI(Kerberos)
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
			AuthType           int // 1. KRB5_USER_AUTH, 2. KRB5_KEYTAB_AUTH
			KeyTabPath         string
			KerberosConfigPath string
			ServiceName        string
			Username           string
			Password           string
			Realm              string
			DisablePAFXFAST    bool
		}
	}
	AssignInterval  int
	CalcLagInterval int
	RebalanceByLags bool
}

// ClickHouseConfig configuration parameters
type ClickHouseConfig struct {
	Cluster  string
	DB       string
	Hosts    [][]string
	Port     int
	Username string
	Password string
	Protocol string //native, http

	// Whether enable TLS encryption with clickhouse-server
	Secure bool
	// Whether skip verify clickhouse-server cert
	InsecureSkipVerify bool
	RetryTimes         int // <=0 means retry infinitely
	MaxOpenConns       int
	ReadTimeout        int
	AsyncInsert        bool
	AsyncSettings      struct {
		// refers to https://clickhouse.com/docs/en/operations/settings/settings#async-insert
		AsyncInsertMaxDataSize    int `json:"async_insert_max_data_size,omitempty"`
		AsyncInsertMaxQueryNumber int `json:"async_insert_max_query_number,omitempty"` // 450
		AsyncInsertBusyTimeoutMs  int `json:"async_insert_busy_timeout_ms,omitempty"`  // 200
		WaitforAsyncInsert        int `json:"wait_for_async_insert,omitempty"`
		WaitforAsyncInsertTimeout int `json:"wait_for_async_insert_timeout,omitempty"`
		AsyncInsertThreads        int `json:"async_insert_threads,omitempty"` // 16
		AsyncInsertDeduplicate    int `json:"async_insert_deduplicate,omitempty"`
	}
	Ctx context.Context `json:"-"`
}

// TaskConfig parameters
type TaskConfig struct {
	Name string

	Topic         string
	ConsumerGroup string

	// Earliest set to true to consume the message from oldest position
	Earliest bool
	Parser   string
	// the csv cloum title if Parser is csv
	CsvFormat []string
	Delimiter string

	TableName       string
	SeriesTableName string

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
		Enable      bool
		NotNullable bool
		MaxDims     int // the upper limit of dynamic columns number, <=0 means math.MaxInt16. protecting dirty data attack
		// A column is added for new key K if all following conditions are true:
		// - K isn't in ExcludeColumns
		// - number of existing columns doesn't reach MaxDims-1
		// - WhiteList is empty, or K matchs WhiteList
		// - BlackList is empty, or K doesn't match BlackList
		WhiteList string // the regexp of white list
		BlackList string // the regexp of black list
	}
	// additional fields to be appended to each input message, should be a valid json string
	Fields string `json:"fields,omitempty"`
	// PrometheusSchema expects each message is a Prometheus metric(timestamp, value, metric name and a list of labels).
	PrometheusSchema bool
	// fields match PromLabelsBlackList are not considered as labels. Requires PrometheusSchema be true.
	PromLabelsBlackList string // the regexp of black list

	// ShardingKey is the column name to which sharding against
	ShardingKey string `json:"shardingKey,omitempty"`
	// ShardingStripe take effect if the sharding key is numerical
	ShardingStripe uint64 `json:"shardingStripe,omitempty"`

	FlushInterval int     `json:"flushInterval,omitempty"`
	BufferSize    int     `json:"bufferSize,omitempty"`
	TimeZone      string  `json:"timeZone"`
	TimeUnit      float64 `json:"timeUnit"`
}

type GroupConfig struct {
	Name          string
	Topics        []string
	Earliest      bool
	FlushInterval int
	BufferSize    int
	Configs       map[string]*TaskConfig
}

type Assignment struct {
	Version   int
	UpdatedAt int64               // timestamp when created
	UpdatedBy string              // leader instance
	Map       map[string][]string // map instance to a list of task_name
}

const (
	MaxBufferSize                   = 1 << 20 // 1048576
	defaultBufferSize               = 1 << 18 // 262144
	maxFlushInterval                = 600
	defaultFlushInterval            = 10
	defaultTimeZone                 = "Local"
	defaultLogLevel                 = "info"
	defaultKerberosConfigPath       = "/etc/krb5.conf"
	defaultMaxOpenConns             = 1
	defaultReadTimeout              = 3600
	defaultCleanupSeriesMapInterval = 10      // 10s
	defaultActiveSeriesRange        = 3600    // 1 hour
	defaultHeartbeatInterval        = 60000   // 1 min
	defaultSessionTimeout           = 300000  // 5 min
	defaultRebalanceTimeout         = 600000  // 10 min
	defaultRequestTimeoutOverhead   = 300000  // 5 min
	DefaultMaxPollInterval          = 3600000 // 1 hour
	defaultAssignInterval           = 5       // 5min
	defaultCalcLagInterval          = 10      // 10min
)

func ParseLocalCfgFile(cfgPath string) (cfg *Config, err error) {
	cfg = &Config{
		Groups: make(map[string]*GroupConfig),
	}
	var b []byte
	b, err = os.ReadFile(cfgPath)
	if err != nil {
		err = errors.Wrapf(err, "")
		return
	}
	if err = hjson.Unmarshal(b, cfg); err != nil {
		err = errors.Wrapf(err, "")
		return
	}
	return
}

// Normalize and validate configuration
func (cfg *Config) Normallize(constructGroup bool, httpAddr string, cred util.Credentials) (err error) {
	if cred.ClickhouseUsername != "" {
		cfg.Clickhouse.Username = cred.ClickhouseUsername
	}
	if cred.ClickhousePassword != "" {
		cfg.Clickhouse.Password = cred.ClickhousePassword
	}
	if cred.KafkaUsername != "" {
		cfg.Kafka.Sasl.Username = cred.KafkaUsername
	}
	if cred.KafkaPassword != "" {
		cfg.Kafka.Sasl.Password = cred.KafkaPassword
	}
	if cred.KafkaGSSAPIUsername != "" {
		cfg.Kafka.Sasl.GSSAPI.Username = cred.KafkaGSSAPIUsername
	}
	if cred.KafkaGSSAPIPassword != "" {
		cfg.Kafka.Sasl.GSSAPI.Password = cred.KafkaGSSAPIPassword
	}

	if len(cfg.Clickhouse.Hosts) == 0 || cfg.Kafka.Brokers == "" {
		err = errors.Newf("invalid configuration, Clickhouse or Kafka section is missing!")
		return
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
			err = errors.Newf("kafka SASL mechanism %s is unsupported", cfg.Kafka.Sasl.Mechanism)
			return
		}

		if cfg.Kafka.ResetSaslRealm {
			port := getKfkPort(cfg.Kafka.Brokers)
			os.Setenv("DOMAIN_REALM", net.JoinHostPort("hadoop."+strings.ToLower(cfg.Kafka.Sasl.GSSAPI.Realm), port))
		}
	}
	if cfg.Kafka.Properties.HeartbeatInterval == 0 {
		cfg.Kafka.Properties.HeartbeatInterval = defaultHeartbeatInterval
	}
	if cfg.Kafka.Properties.RebalanceTimeout == 0 {
		cfg.Kafka.Properties.RebalanceTimeout = defaultRebalanceTimeout
	}
	if cfg.Kafka.Properties.RequestTimeoutOverhead == 0 {
		cfg.Kafka.Properties.RequestTimeoutOverhead = defaultRequestTimeoutOverhead
	}
	if cfg.Kafka.Properties.SessionTimeout == 0 {
		cfg.Kafka.Properties.SessionTimeout = defaultSessionTimeout
	}
	if cfg.Kafka.Properties.MaxPollInterval == 0 {
		cfg.Kafka.Properties.MaxPollInterval = DefaultMaxPollInterval
	}
	if cfg.Kafka.AssignInterval == 0 {
		cfg.Kafka.AssignInterval = defaultAssignInterval
	}
	if cfg.Kafka.CalcLagInterval == 0 {
		cfg.Kafka.CalcLagInterval = defaultCalcLagInterval
	}

	if cfg.Clickhouse.RetryTimes < 0 {
		cfg.Clickhouse.RetryTimes = 0
	}
	if cfg.Clickhouse.MaxOpenConns <= 0 {
		cfg.Clickhouse.MaxOpenConns = defaultMaxOpenConns
	}
	if cfg.Clickhouse.ReadTimeout <= 0 {
		cfg.Clickhouse.ReadTimeout = defaultReadTimeout
	}

	if cfg.Clickhouse.Protocol == "" {
		cfg.Clickhouse.Protocol = clickhouse.Native.String()
	}

	if cfg.Clickhouse.Port == 0 {
		if cfg.Clickhouse.Protocol == clickhouse.HTTP.String() {
			cfg.Clickhouse.Port = 8123
		} else {
			cfg.Clickhouse.Port = 9000
		}
	}

	if cfg.Task != nil {
		cfg.Tasks = append(cfg.Tasks, cfg.Task)
		cfg.Task = nil
	}
	for _, taskCfg := range cfg.Tasks {
		if err = cfg.normallizeTask(taskCfg); err != nil {
			return
		}
		if constructGroup {
			if httpAddr != "" && !cfg.IsAssigned(httpAddr, taskCfg.Name) {
				continue
			}
			gCfg, ok := cfg.Groups[taskCfg.ConsumerGroup]
			if !ok {
				gCfg = &GroupConfig{
					Name:          taskCfg.ConsumerGroup,
					Earliest:      taskCfg.Earliest,
					Topics:        []string{taskCfg.Topic},
					FlushInterval: taskCfg.FlushInterval,
					BufferSize:    taskCfg.BufferSize,
					Configs:       make(map[string]*TaskConfig),
				}
				gCfg.Configs[taskCfg.Name] = taskCfg
				cfg.Groups[taskCfg.ConsumerGroup] = gCfg
			} else {
				if gCfg.Earliest != taskCfg.Earliest {
					util.Logger.Fatal("Tasks are sharing same consumer group, but with different Earliest property specified!",
						zap.String("task", gCfg.Name), zap.String("task", taskCfg.Name))
				} else if gCfg.FlushInterval != taskCfg.FlushInterval {
					util.Logger.Fatal("Tasks are sharing same consumer group, but with different FlushInterval property specified!",
						zap.String("task", gCfg.Name), zap.String("task", taskCfg.Name))
				}
				gCfg.Topics = append(gCfg.Topics, taskCfg.Topic)
				gCfg.BufferSize += taskCfg.BufferSize
				gCfg.Configs[taskCfg.Name] = taskCfg
			}
		}
	}
	if cfg.RecordPoolSize == 0 {
		cfg.RecordPoolSize = MaxBufferSize
	}
	switch strings.ToLower(cfg.LogLevel) {
	case "debug", "info", "warn", "error", "dpanic", "panic", "fatal":
	default:
		cfg.LogLevel = defaultLogLevel
	}
	if cfg.CleanupSeriesMapInterval <= 0 {
		cfg.CleanupSeriesMapInterval = defaultCleanupSeriesMapInterval
	}
	if cfg.ActiveSeriesRange <= 0 {
		cfg.ActiveSeriesRange = defaultActiveSeriesRange
	}

	if cfg.Clickhouse.Protocol == clickhouse.HTTP.String() {
		cfg.Clickhouse.AsyncInsert = false
	}

	ctx := context.Background()
	if cfg.Clickhouse.AsyncInsert {
		util.TrySetValue(&cfg.Clickhouse.AsyncSettings.AsyncInsertMaxDataSize, 1<<20)
		util.TrySetValue(&cfg.Clickhouse.AsyncSettings.AsyncInsertMaxQueryNumber, 450)
		util.TrySetValue(&cfg.Clickhouse.AsyncSettings.AsyncInsertBusyTimeoutMs, 200)
		util.TrySetValue(&cfg.Clickhouse.AsyncSettings.WaitforAsyncInsert, 1)
		util.TrySetValue(&cfg.Clickhouse.AsyncSettings.WaitforAsyncInsertTimeout, 120)
		util.TrySetValue(&cfg.Clickhouse.AsyncSettings.AsyncInsertThreads, 16)
		util.TrySetValue(&cfg.Clickhouse.AsyncSettings.AsyncInsertDeduplicate, 0)

		ctx = clickhouse.Context(context.Background(), clickhouse.WithSettings(clickhouse.Settings{
			"async_insert":                  1,
			"async_insert_max_data_size":    cfg.Clickhouse.AsyncSettings.AsyncInsertMaxDataSize,
			"async_insert_max_query_number": cfg.Clickhouse.AsyncSettings.AsyncInsertMaxQueryNumber,
			"async_insert_busy_timeout_ms":  cfg.Clickhouse.AsyncSettings.AsyncInsertBusyTimeoutMs,
			"wait_for_async_insert":         cfg.Clickhouse.AsyncSettings.WaitforAsyncInsert,
			"wait_for_async_insert_timeout": cfg.Clickhouse.AsyncSettings.WaitforAsyncInsertTimeout,
			"async_insert_threads":          cfg.Clickhouse.AsyncSettings.AsyncInsertThreads,
			"async_insert_deduplicate":      cfg.Clickhouse.AsyncSettings.AsyncInsertDeduplicate,
		}))
	}
	cfg.Clickhouse.Ctx = ctx

	return
}

func (cfg *Config) normallizeTask(taskCfg *TaskConfig) (err error) {
	if taskCfg.Parser == "" || taskCfg.Parser == "json" {
		taskCfg.Parser = "fastjson"
	}

	for i := range taskCfg.Dims {
		if taskCfg.Dims[i].SourceName == "" {
			taskCfg.Dims[i].SourceName = util.GetSourceName(taskCfg.Parser, taskCfg.Dims[i].Name)
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
	} else {
		taskCfg.PromLabelsBlackList = ""
	}
	if taskCfg.DynamicSchema.Enable {
		taskCfg.AutoSchema = true
		if taskCfg.Parser != "fastjson" && taskCfg.Parser != "gjson" {
			err = errors.Newf("Parser %s doesn't support DynamicSchema", taskCfg.Parser)
			return
		}
		if cfg.Clickhouse.Cluster == "" {
			var numHosts int
			for _, shard := range cfg.Clickhouse.Hosts {
				numHosts += len(shard)
			}
			if numHosts > 1 {
				err = errors.Newf("Need to set cluster name when DynamicSchema is enabled and number of hosts is more than one")
				return
			}
		}
	} else {
		taskCfg.DynamicSchema.WhiteList = ""
		taskCfg.DynamicSchema.BlackList = ""
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
	if taskCfg.PromLabelsBlackList != "" {
		if _, err = regexp.Compile(taskCfg.PromLabelsBlackList); err != nil {
			err = errors.Wrapf(err, "PromLabelsBlackList %s is invalid regexp", taskCfg.PromLabelsBlackList)
			return
		}
	}
	return
}

// convert java client style configuration into sinker
func (cfg *Config) convertKfkSecurity() {
	protocol := cfg.Kafka.Security["security.protocol"]
	if protocol == "" {
		return
	}

	if strings.Contains(protocol, "SSL") {
		util.TrySetValue(&cfg.Kafka.TLS.Enable, true)
		util.TrySetValue(&cfg.Kafka.TLS.EndpIdentAlgo, cfg.Kafka.Security["ssl.endpoint.identification.algorithm"])
		util.TrySetValue(&cfg.Kafka.TLS.TrustStoreLocation, cfg.Kafka.Security["ssl.truststore.location"])
		util.TrySetValue(&cfg.Kafka.TLS.TrustStorePassword, cfg.Kafka.Security["ssl.truststore.password"])
		util.TrySetValue(&cfg.Kafka.TLS.KeystoreLocation, cfg.Kafka.Security["ssl.keystore.location"])
		util.TrySetValue(&cfg.Kafka.TLS.KeystorePassword, cfg.Kafka.Security["ssl.keystore.password"])
	}

	if strings.Contains(protocol, "SASL") {
		util.TrySetValue(&cfg.Kafka.Sasl.Enable, true)
		util.TrySetValue(&cfg.Kafka.Sasl.Mechanism, cfg.Kafka.Security["sasl.mechanism"])
		if config, ok := cfg.Kafka.Security["sasl.jaas.config"]; ok {
			configMap := readConfig(config)
			if strings.Contains(cfg.Kafka.Sasl.Mechanism, "GSSAPI") {
				// GSSAPI
				if configMap["useKeyTab"] != "true" {
					//Username and password
					util.TrySetValue(&cfg.Kafka.Sasl.GSSAPI.AuthType, 1)
					util.TrySetValue(&cfg.Kafka.Sasl.GSSAPI.Username, configMap["username"])
					util.TrySetValue(&cfg.Kafka.Sasl.GSSAPI.Password, configMap["password"])
				} else {
					//Keytab
					util.TrySetValue(&cfg.Kafka.Sasl.GSSAPI.AuthType, 2)
					util.TrySetValue(&cfg.Kafka.Sasl.GSSAPI.KeyTabPath, configMap["keyTab"])
					if principal, ok := configMap["principal"]; ok {
						prins := strings.Split(principal, "@")
						util.TrySetValue(&cfg.Kafka.Sasl.GSSAPI.Username, prins[0])
						if len(prins) > 1 {
							util.TrySetValue(&cfg.Kafka.Sasl.GSSAPI.Realm, prins[1])
						}
					}
					util.TrySetValue(&cfg.Kafka.Sasl.GSSAPI.ServiceName, cfg.Kafka.Security["sasl.kerberos.service.name"])
					util.TrySetValue(&cfg.Kafka.Sasl.GSSAPI.KerberosConfigPath, defaultKerberosConfigPath)
				}
			} else {
				// PLAIN, SCRAM-SHA-256 or SCRAM-SHA-512
				util.TrySetValue(&cfg.Kafka.Sasl.Username, configMap["username"])
				util.TrySetValue(&cfg.Kafka.Sasl.Password, configMap["password"])
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

func getKfkPort(brokers string) string {
	hosts := strings.Split(brokers, ",")
	var port string
	for _, host := range hosts {
		_, p, err := net.SplitHostPort(host)
		if err != nil {
			port = p
			break
		}
	}
	return port
}
