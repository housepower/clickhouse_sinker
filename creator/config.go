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

package creator

import (
	"encoding/json"
	"io/ioutil"
	"path/filepath"
	"strings"

	"github.com/k0kubun/pp"
	"github.com/sundy-li/go_commons/log"
	"github.com/sundy-li/go_commons/utils"
)

// Config struct used for different configurations use
type Config struct {
	Kafka      map[string]*KafkaConfig
	Clickhouse map[string]*ClickHouseConfig

	Tasks []*Task

	Statistics struct {
		Enable bool
		PushGateWayAddrs []string
		PushInterval     int
	}

	Common struct {
		FlushInterval int
		BufferSize    int
		MinBufferSize int
		LogLevel      string
	}
}

var (
	baseConfig *Config
)

// InitConfig must run before the server start
func InitConfig(dir string) *Config {
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
	if baseConfig.Common.FlushInterval < 1 {
		baseConfig.Common.FlushInterval = defaultFlushInterval
	}

	if baseConfig.Common.BufferSize < 1 {
		baseConfig.Common.BufferSize = defaultBufferSize
	}
	err = baseConfig.LoadTasks(filepath.Join(confPath, "tasks"))
	if err != nil {
		panic(err)
	}

	log.SetLevelStr(baseConfig.Common.LogLevel)
	_, _ = pp.Println(baseConfig)
	return baseConfig
}

// LoadTasks read the task definition from json configuration and load
func (config *Config) LoadTasks(dir string) error {
	// Check if the configuration is correct
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return err
	}
	config.Tasks = make([]*Task, 0, len(files))
	for _, f := range files {
		if strings.HasSuffix(f.Name(), ".json") {
			s, err := utils.ExtendFile(filepath.Join(dir, f.Name()))
			if err != nil {
				return err
			}
			taskConfig := &Task{}
			err = json.Unmarshal([]byte(s), taskConfig)
			if err != nil {
				return err
			}
			config.Tasks = append(config.Tasks, taskConfig)
		}
	}
	return nil
}

// Conf returns the instance of configuration
func Conf() Config {
	return *baseConfig
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
	Db   string
	Host string
	Port int

	Username    string
	Password    string
	MaxLifeTime int
	RetryTimes  int
}

// Task configuration parameters
type Task struct {
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
		Name string
		Type string
	} `json:"dims"`
	Metrics []struct {
		Name string
		Type string
	} `json:"metrics"`

	FlushInterval int `json:"flushInterval,omitempty"`
	BufferSize    int `json:"bufferSize,omitempty"`
	MinBufferSize int `json:"minBufferSize,omitempty"`
}

var (
	defaultFlushInterval = 3
	defaultBufferSize    = 10000
)
