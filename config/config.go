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
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/housepower/clickhouse_sinker/util"
	"github.com/pkg/errors"

	"github.com/sundy-li/go_commons/utils"
)

// RemoteConfManager can be implemented by many backends: Nacos, Consul, etcd, ZooKeeper...
type RemoteConfManager interface {
	Init(properties map[string]interface{}) error
	// Register this instance, and keep-alive via heartbeat.
	Register(ip string, port int) error
	Deregister(ip string, port int) error
	// GetInstances fetchs healthy instances.
	// Mature service-discovery solutions(Nacos, Consul etc.) have client side cache
	// so that frequent invoking of GetInstances() and GetGlobalConfig() don't harm.
	GetInstances() (instances []Instance, err error)
	// GetConfig fetchs the config. The manager shall not reference the returned Config object after call.
	GetConfig() (conf *Config, err error)
	// PublishConfig publishs the config. The manager shall not reference the passed Config object after call.
	PublishConfig(conf *Config) (err error)
}

type Instance struct {
	Addr   string
	Weight int
}

// Config struct used for different configurations use
type Config struct {
	Kafka      map[string]*KafkaConfig
	Clickhouse map[string]*ClickHouseConfig
	Tasks      map[string]*TaskConfig
	Common     struct {
		FlushInterval    int
		BufferSize       int
		MinBufferSize    int
		MsgSizeHint      int
		LayoutDate       string
		LayoutDateTime   string
		LayoutDateTime64 string
		LogLevel         string
		Replicas         int //on how many sinker instances a task runs
	}
	Assignment map[string][]string //map instance_name to a list of task_name
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
	Kafka         string
	Topic         string
	ConsumerGroup string

	// Earliest set to true to consume the message from oldest position
	Earliest bool
	Parser   string
	// the csv cloum title if Parser is csv
	CsvFormat []string
	Delimiter string

	Clickhouse string
	TableName  string

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
	Replicas         int    //on how many sinker instances this task runs
}

const (
	defaultFlushInterval    = 3
	defaultBufferSize       = 1 << 20 //1048576
	defaultMinBufferSize    = 1 << 13 //   8196
	defaultMsgSizeHint      = 1000
	defaultLayoutDate       = "2006-01-02"
	defaultLayoutDateTime   = time.RFC3339
	defaultLayoutDateTime64 = time.RFC3339
	defaultTaskReplicas     = 1
)

func ParseLocalCfgDir(cfgPath string) (cfg *Config, err error) {
	var f = "config.json"
	f = filepath.Join(cfgPath, f)
	var s string
	if s, err = utils.ExtendFile(f); err != nil {
		err = errors.Wrapf(err, "")
		return
	}
	cfg = &Config{}
	if err = json.Unmarshal([]byte(s), cfg); err != nil {
		err = errors.Wrapf(err, "")
		return
	}

	var files []os.FileInfo
	if files, err = ioutil.ReadDir(filepath.Join(cfgPath, "tasks")); err != nil {
		err = errors.Wrapf(err, "")
		return
	}
	cfg.Tasks = make(map[string]*TaskConfig)
	for _, f := range files {
		if strings.HasSuffix(f.Name(), ".json") {
			if s, err = utils.ExtendFile(filepath.Join(cfgPath, "tasks", f.Name())); err != nil {
				err = errors.Wrapf(err, "")
				return
			}
			taskConfig := &TaskConfig{}
			if err = json.Unmarshal([]byte(s), taskConfig); err != nil {
				err = errors.Wrapf(err, "")
				return
			}
			cfg.Tasks[taskConfig.Name] = taskConfig
		}
	}
	return
}

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

func (cfg *Config) AssignTasks(instances []Instance) {
	cfg.Assignment = make(map[string][]string)
	assignment := make(map[string]map[string]int)
	hr := util.NewHashRing(nil)
	for _, inst := range instances {
		hr.Add(inst.Addr, inst.Weight)
		cfg.Assignment[inst.Addr] = make([]string, 0)
		assignment[inst.Addr] = make(map[string]int)
	}
	for _, taskConfig := range cfg.Tasks {
		for i := 0; i < taskConfig.Replicas; i++ {
			instAddr := hr.Get(taskConfig.Name)
			assignment[instAddr][taskConfig.Name] = 1
		}
	}
	for _, inst := range instances {
		for taskName := range assignment[inst.Addr] {
			cfg.Assignment[inst.Addr] = append(cfg.Assignment[inst.Addr], taskName)
		}
		sort.Strings(cfg.Assignment[inst.Addr])
	}
}

// normallize and validate configuration
func (cfg *Config) Normallize() (err error) {
	if cfg.Common.FlushInterval <= 0 {
		cfg.Common.FlushInterval = defaultFlushInterval
	}
	if cfg.Common.BufferSize <= 0 {
		cfg.Common.BufferSize = defaultBufferSize
	} else {
		cfg.Common.BufferSize = 1 << util.GetShift(cfg.Common.BufferSize)
	}
	if cfg.Common.MinBufferSize <= 0 {
		cfg.Common.MinBufferSize = defaultMinBufferSize
	} else {
		cfg.Common.MinBufferSize = 1 << util.GetShift(cfg.Common.MinBufferSize)
	}
	if cfg.Common.MsgSizeHint <= 0 {
		cfg.Common.MsgSizeHint = defaultMsgSizeHint
	}
	if cfg.Common.LayoutDate == "" {
		cfg.Common.LayoutDate = defaultLayoutDate
	}
	if cfg.Common.LayoutDateTime == "" {
		cfg.Common.LayoutDateTime = defaultLayoutDateTime
	}
	if cfg.Common.LayoutDateTime64 == "" {
		cfg.Common.LayoutDateTime64 = defaultLayoutDateTime64
	}
	switch strings.ToLower(cfg.Common.LogLevel) {
	case "panic", "fatal", "error", "warn", "warning", "info", "debug", "trace":
	default:
		cfg.Common.LogLevel = "info"
	}
	if cfg.Common.Replicas <= 0 {
		cfg.Common.Replicas = defaultTaskReplicas
	}
	if err = cfg.normallizeTasks(); err != nil {
		return
	}
	for kfkName, kfkConfig := range cfg.Kafka {
		if kfkConfig.Version == "" {
			kfkConfig.Version = "2.2.1"
		}
		if kfkConfig.Sasl.Enable {
			kfkConfig.Sasl.Mechanism = strings.ToUpper(kfkConfig.Sasl.Mechanism)
			switch kfkConfig.Sasl.Mechanism {
			case "PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512", "GSSAPI":
			default:
				err = errors.Errorf("kafka %s mechanism %s is unsupported", kfkName, kfkConfig.Sasl.Mechanism)
				return
			}
		}
	}
	for _, chConfig := range cfg.Clickhouse {
		if chConfig.RetryTimes < 0 {
			chConfig.RetryTimes = 0
		}
	}
	for instAddr, taskNames := range cfg.Assignment {
		sort.Strings(taskNames)
		for _, taskName := range taskNames {
			if _, ok := cfg.Tasks[taskName]; !ok {
				err = errors.Errorf("instance %s assignment is invalid, task %s doesn't exit", instAddr, taskName)
				return
			}
		}
	}
	return
}

func (cfg *Config) normallizeTasks() (err error) {
	for taskName, taskConfig := range cfg.Tasks {
		if _, ok := cfg.Kafka[taskConfig.Kafka]; !ok {
			err = errors.Errorf("task %s config is invalid, kafka %s doesn't exist.", taskConfig.Name, taskConfig.Kafka)
			return
		}
		if _, ok := cfg.Clickhouse[taskConfig.Clickhouse]; !ok {
			err = errors.Errorf("task %s config is invalid, clickhouse %s doesn't exist.", taskConfig.Name, taskConfig.Clickhouse)
			return
		}
		if taskConfig.Name != taskName {
			taskConfig.Name = taskName
		}
		kfkCfg := cfg.Kafka[taskConfig.Kafka]
		if kfkCfg.Sasl.Enable && kfkCfg.Sasl.Username == "" {
			//kafka-go doesn't support SASL/GSSAPI(Kerberos). https://github.com/segmentio/kafka-go/issues/539
			taskConfig.KafkaClient = "sarama"
		} else if taskConfig.KafkaClient == "" {
			taskConfig.KafkaClient = "kafka-go"
		}
		if taskConfig.Parser == "" {
			taskConfig.Parser = "fastjson"
		}
		if taskConfig.FlushInterval <= 0 {
			taskConfig.FlushInterval = cfg.Common.FlushInterval
		}
		if taskConfig.BufferSize <= 0 {
			taskConfig.BufferSize = cfg.Common.BufferSize
		} else {
			taskConfig.BufferSize = 1 << util.GetShift(taskConfig.BufferSize)
		}
		if taskConfig.MinBufferSize <= 0 {
			taskConfig.MinBufferSize = cfg.Common.MinBufferSize
		} else {
			taskConfig.MinBufferSize = 1 << util.GetShift(taskConfig.BufferSize)
		}
		if taskConfig.MsgSizeHint <= 0 {
			taskConfig.MsgSizeHint = cfg.Common.MsgSizeHint
		}
		if taskConfig.LayoutDate == "" {
			taskConfig.LayoutDate = cfg.Common.LayoutDate
		}
		if taskConfig.LayoutDateTime == "" {
			taskConfig.LayoutDateTime = cfg.Common.LayoutDateTime
		}
		if taskConfig.LayoutDateTime64 == "" {
			taskConfig.LayoutDateTime64 = cfg.Common.LayoutDateTime64
		}
		if taskConfig.Replicas <= 0 {
			taskConfig.Replicas = cfg.Common.Replicas
		}
		for i := range taskConfig.Dims {
			if taskConfig.Dims[i].SourceName == "" {
				taskConfig.Dims[i].SourceName = util.GetSourceName(taskConfig.Dims[i].Name)
			}
		}
	}
	return
}
