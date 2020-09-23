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
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/k0kubun/pp"
	"github.com/sundy-li/go_commons/log"
	"github.com/sundy-li/go_commons/utils"
)

// Config struct used for different configurations use
type Config struct {
	Kafka      map[string]*KafkaConfig
	Clickhouse map[string]*ClickHouseConfig

	Tasks []*TaskConfig

	Statistics struct {
		Enable           bool
		PushGateWayAddrs []string
		PushInterval     int
	}

	Common struct {
		FlushInterval     int
		BufferSize        int
		MinBufferSize     int
		MsgSizeHint       int
		ConcurrentParsers int
		LayoutDate        string
		LayoutDateTime    string
		LayoutDateTime64  string
		LogLevel          string
	}
}

var (
	baseConfig     *Config
	configDir      string
	baseConfigOnce sync.Once
)

func SetConfigDir(dir string) {
	configDir = dir
}

func GetConfig() *Config {
	baseConfigOnce.Do(func() {
		if configDir == "" {
			panic("Need to call SetConfigDir before GetConfig")
		}
		baseConfig = initConfig(configDir)
	})
	return baseConfig
}

// InitConfig must run before the server start
func initConfig(dir string) *Config {
	confPath := ""
	if len(dir) > 0 {
		confPath = dir
	}
	var f = "config.json"
	f = filepath.Join(confPath, f)
	s, err := utils.ExtendFile(f)
	if err != nil {
		panic(err)
	}
	baseConfig = &Config{}
	err = json.Unmarshal([]byte(s), baseConfig)
	if err != nil {
		panic(err)
	}

	err = baseConfig.loadTasks(filepath.Join(confPath, "tasks"))
	if err != nil {
		panic(err)
	}
	baseConfig.normallize()

	log.SetLevelStr(baseConfig.Common.LogLevel)
	_, _ = pp.Println(baseConfig)
	return baseConfig
}

// loadTasks read the task definition from json configuration and load
func (config *Config) loadTasks(dir string) error {
	// Check if the configuration is correct
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return err
	}
	config.Tasks = make([]*TaskConfig, 0, len(files))
	for _, f := range files {
		if strings.HasSuffix(f.Name(), ".json") {
			s, err := utils.ExtendFile(filepath.Join(dir, f.Name()))
			if err != nil {
				return err
			}
			taskConfig := &TaskConfig{}
			err = json.Unmarshal([]byte(s), taskConfig)
			if err != nil {
				return err
			}
			config.Tasks = append(config.Tasks, taskConfig)
		}
	}
	return nil
}

// normallize configuration
func (config *Config) normallize() {
	if config.Common.FlushInterval <= 0 {
		config.Common.FlushInterval = defaultFlushInterval
	}
	if baseConfig.Common.BufferSize <= 0 {
		config.Common.BufferSize = defaultBufferSize
	}
	if baseConfig.Common.MinBufferSize <= 0 {
		config.Common.MinBufferSize = defaultMinBufferSize
	}
	if baseConfig.Common.MsgSizeHint <= 0 {
		config.Common.MsgSizeHint = defaultMsgSizeHint
	}
	if baseConfig.Common.ConcurrentParsers <= 0 {
		config.Common.ConcurrentParsers = defaultConcurrentParsers
	}
	if baseConfig.Common.LayoutDate == "" {
		baseConfig.Common.LayoutDate = defaultLayoutDate
	}
	if baseConfig.Common.LayoutDateTime == "" {
		baseConfig.Common.LayoutDateTime = defaultLayoutDateTime
	}
	if baseConfig.Common.LayoutDateTime64 == "" {
		baseConfig.Common.LayoutDateTime64 = defaultLayoutDateTime64
	}
	for _, taskConfig := range config.Tasks {
		if taskConfig.FlushInterval <= 0 {
			taskConfig.FlushInterval = config.Common.FlushInterval
		}
		if taskConfig.BufferSize <= 0 {
			taskConfig.BufferSize = config.Common.BufferSize
		}
		if taskConfig.MinBufferSize <= 0 {
			taskConfig.MinBufferSize = config.Common.MinBufferSize
		}
		if taskConfig.MsgSizeHint <= 0 {
			taskConfig.MsgSizeHint = config.Common.MsgSizeHint
		}
		if taskConfig.ConcurrentParsers <= 0 {
			taskConfig.ConcurrentParsers = config.Common.ConcurrentParsers
		}
		if taskConfig.LayoutDate == "" {
			taskConfig.LayoutDate = baseConfig.Common.LayoutDate
		}
		if taskConfig.LayoutDateTime == "" {
			taskConfig.LayoutDateTime = baseConfig.Common.LayoutDateTime
		}
		if taskConfig.LayoutDateTime64 == "" {
			taskConfig.LayoutDateTime64 = baseConfig.Common.LayoutDateTime64
		}
	}
}

// KafkaConfig configuration parameters
type KafkaConfig struct {
	Brokers string
	Sasl    struct {
		Password string
		Username string
	}
	Version string
}

// ClickHouseConfig configuration parameters
type ClickHouseConfig struct {
	DB   string
	Host string
	Port int

	Username    string
	Password    string
	DsnParams   string
	MaxLifeTime int
	RetryTimes  int
}

// Task configuration parameters
type TaskConfig struct {
	Name string

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

	Dims []struct {
		Name       string
		Type       string
		SourceName string
	} `json:"dims"`

	FlushInterval     int    `json:"flushInterval,omitempty"`
	BufferSize        int    `json:"bufferSize,omitempty"`
	MinBufferSize     int    `json:"minBufferSize,omitempty"`
	MsgSizeHint       int    `json:"msgSizeHint,omitempty"`
	ConcurrentParsers int    `json:"concurrentParsers,omitempty"`
	LayoutDate        string `json:"layoutDate,omitempty"`
	LayoutDateTime    string `json:"layoutDateTime,omitempty"`
	LayoutDateTime64  string `json:"layoutDateTime64,omitempty"`
}

var (
	defaultFlushInterval     = 3
	defaultBufferSize        = 100000
	defaultMinBufferSize     = 10000
	defaultMsgSizeHint       = 1000
	defaultConcurrentParsers = 5
	defaultLayoutDate        = "2006-01-02"
	defaultLayoutDateTime    = time.RFC3339
	defaultLayoutDateTime64  = time.RFC3339
)
