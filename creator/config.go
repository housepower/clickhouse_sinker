package creator

import (
	"encoding/json"
	"io/ioutil"
	"path/filepath"
	"strings"

	"github.com/k0kubun/pp"
	"github.com/wswz/go_commons/log"
	"github.com/wswz/go_commons/utils"
)

type Config struct {
	Kafka      map[string]*KafkaConfig
	Clickhouse map[string]*ClickHouseConfig

	Tasks []*Task

	Common struct {
		FlushInterval int
		BufferSize    int
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
	pp.Println(baseConfig)
	return baseConfig
}

func (cfg *Config) LoadTasks(dir string) error {
	//检测配置是否正确
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return err
	}
	cfg.Tasks = make([]*Task, 0, len(files))
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
			cfg.Tasks = append(cfg.Tasks, taskConfig)
		}
	}
	return nil
}

func Conf() Config {
	return *baseConfig
}

type KafkaConfig struct {
	Brokers string
	Sasl    struct {
		Password string
		Username string
	}
	Version string
}

type ClickHouseConfig struct {
	Db   string
	Host string
	Port int

	Username    string
	Password    string
	MaxLifeTime int
	DnsLoop     bool
}

type Task struct {
	Name string

	Kafka         string
	Topic         string
	ConsumerGroup string

	// Earliest set to true to consume the message from oldest position
	Earliest bool
	Parser   string

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
}

var (
	defaultFlushInterval = 3
	defaultBufferSize    = 10000
)
