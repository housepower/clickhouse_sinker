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
	"os"
	"reflect"
	"time"

	_ "github.com/ClickHouse/clickhouse-go"
	"github.com/housepower/clickhouse_sinker/config"
	log "github.com/sirupsen/logrus"
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
	nacosDataID = flag.String("nacos-dataid", "",
		`nacos data id, the task name`)

	localCfgFile = flag.String("local-cfg-file", "/etc/clickhouse_sinker.json", "local config file")
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
			log.Fatalf("%+v", err)
			return
		}
	} else {
		log.Fatalf("expect --local-cfg-file")
		return
	}

	if err = cfg.Normallize(); err != nil {
		log.Fatalf("%+v", err)
		return
	}

	ncm := config.NacosConfManager{}
	properties := getProperties()
	if err = ncm.Init(properties); err != nil {
		log.Fatalf("%+v", err)
	}

	if err = ncm.PublishConfig(cfg); err != nil {
		log.Fatalf("%+v", err)
	}
	log.Infof("sleep a while")
	time.Sleep(10 * time.Second)

	var newCfg *config.Config
	if newCfg, err = ncm.GetConfig(); err != nil {
		log.Fatalf("%+v", err)
	}
	if !reflect.DeepEqual(newCfg, cfg) {
		log.Fatalf("got different config: %+v", newCfg)
	}
}

func main() {
	log.SetFormatter(&log.TextFormatter{
		FullTimestamp: true,
	})
	flag.Parse()
	PublishSinkerConfig()
}
