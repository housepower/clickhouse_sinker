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

package main

import (
	"flag"
	"fmt"
	"os"
	"reflect"
	"time"

	_ "github.com/ClickHouse/clickhouse-go"
	"github.com/housepower/clickhouse_sinker/config"
	"github.com/housepower/clickhouse_sinker/util"
	"github.com/jinzhu/copier"
	"go.uber.org/zap"
)

var (
	nacosAddr = flag.String("nacos-addr", "127.0.0.1:8848",
		"a list of comma-separated nacos server addresses")
	nacosUsername = flag.String("nacos-username", "username",
		"nacos username")
	nacosPassword = flag.String("nacos-password", "password",
		"nacos password")
	nacosNamespaceID = flag.String("nacos-namespace-id", "",
		`nacos namespace ID. Neither DEFAULT_NAMESPACE_ID("public") nor namespace name work!`)
	nacosGroup = flag.String("nacos-group", "DEFAULT_GROUP",
		`nacos group name. Empty string doesn't work!`)
	nacosDataID = flag.String("nacos-dataid", "clickhouse_sinker.json",
		`nacos data id`)

	localCfgFile = flag.String("local-cfg-file", "/etc/clickhouse_sinker.json", "local config file")
	replicas     = flag.Int("replicas", 1, "replicate each task to multiple ones with the same config except task name, consumer group and table name")
	maxOpenConns = flag.Int("max-open-conns", 0, "max open connections per shard")
)

// Empty is not valid namespaceID
func getProperties() map[string]interface{} {
	properties := make(map[string]interface{})
	properties["serverAddrs"] = *nacosAddr
	properties["username"] = *nacosUsername
	properties["password"] = *nacosPassword
	properties["namespaceId"] = *nacosNamespaceID
	properties["group"] = *nacosGroup
	properties["dataId"] = *nacosDataID
	return properties
}

func PublishSinkerConfig() {
	var err error
	var cfg *config.Config
	if _, err = os.Stat(*localCfgFile); err == nil {
		if cfg, err = config.ParseLocalCfgFile(*localCfgFile); err != nil {
			util.Logger.Fatal("config.ParseLocalCfgFile failed", zap.Error(err))
			return
		}
	} else {
		util.Logger.Fatal("expect --local-cfg-file")
		return
	}

	if err = cfg.Normallize(); err != nil {
		util.Logger.Fatal("cfg.Normallize failed", zap.Error(err))
		return
	}
	tasks := cfg.Tasks
	for i := 1; i < *replicas; i++ {
		for j := 0; j < len(tasks); j++ {
			taskCfg := &config.TaskConfig{}
			if err = copier.Copy(taskCfg, tasks[j]); err != nil {
				util.Logger.Fatal("copier.Copy failed", zap.Error(err))
			}
			taskCfg.Name = fmt.Sprintf("%s_r%d", taskCfg.Name, i)
			taskCfg.ConsumerGroup = fmt.Sprintf("%s_r%d", taskCfg.ConsumerGroup, i)
			taskCfg.TableName = fmt.Sprintf("%s_r%d", taskCfg.TableName, i)
			cfg.Tasks = append(cfg.Tasks, taskCfg)
		}
	}
	if *maxOpenConns > 0 {
		cfg.Clickhouse.MaxOpenConns = *maxOpenConns
	}

	ncm := config.NacosConfManager{}
	properties := getProperties()
	if err = ncm.Init(properties); err != nil {
		util.Logger.Fatal("ncm.Init failed", zap.Error(err))
	}

	if err = ncm.PublishConfig(cfg); err != nil {
		util.Logger.Fatal("ncm.PublishConfig failed", zap.Error(err))
	}
	util.Logger.Info("sleep a while")
	time.Sleep(10 * time.Second)

	var newCfg *config.Config
	if newCfg, err = ncm.GetConfig(); err != nil {
		util.Logger.Fatal("ncm.GetConfig failed", zap.Error(err))
	}
	if !reflect.DeepEqual(newCfg, cfg) {
		util.Logger.Fatal("BUG: got different config", zap.Reflect("cfg", cfg), zap.Reflect("newCfg", newCfg))
	}
}

func main() {
	util.InitLogger("info", []string{"stdout"})
	flag.Parse()
	PublishSinkerConfig()
}
