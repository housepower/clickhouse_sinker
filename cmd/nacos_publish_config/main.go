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
	"runtime"
	"strconv"
	"strings"
	"time"

	_ "github.com/ClickHouse/clickhouse-go"
	"github.com/housepower/clickhouse_sinker/config"
	"github.com/housepower/clickhouse_sinker/util"
	"github.com/k0kubun/pp"
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

	localCfgDir  = flag.String("local-cfg-dir", "/etc/clickhouse_sinker", "local config dir")
	localCfgFile = flag.String("local-cfg-file", "/etc/clickhouse_sinker.json", "local config file")
	instances    = flag.String("instances", "", "a list of comma-separated ip:port[@weight]")
	testRegister = flag.Bool("test-register", false, "whether run TestRegister")
)

// Empty is not valid namespaceID
func getProperties() map[string]interface{} {
	properties := make(map[string]interface{})
	properties["serverAddrs"] = *nacosAddr
	properties["username"] = *nacosUsername
	properties["password"] = *nacosPassword
	properties["namespaceId"] = *nacosNamespaceID
	properties["group"] = *nacosGroup
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
	} else if _, err = os.Stat(*localCfgDir); err == nil {
		if cfg, err = config.ParseLocalCfgDir(*localCfgDir); err != nil {
			log.Fatalf("%+v", err)
			return
		}
	} else {
		log.Fatalf("expect --local-cfg-file or --local-cfg-dir")
		return
	}

	if err = cfg.Normallize(); err != nil {
		log.Fatalf("%+v", err)
		return
	}

	var insts []config.Instance
	if *instances == "" {
		sinkerAddr := fmt.Sprintf("%s:%d", util.GetOutboundIP().String(), 2112)
		insts = []config.Instance{{Addr: sinkerAddr, Weight: 1}}
	} else {
		for _, instInfo := range strings.Split(*instances, ",") {
			var inst config.Instance
			fields := strings.Split(instInfo, "@")
			inst.Addr = fields[0]
			if len(fields) >= 2 {
				if inst.Weight, err = strconv.Atoi(fields[1]); err != nil {
					log.Fatalf("%+v", err)
				}
			}
			insts = append(insts, inst)
		}
	}
	cfg.AssignTasks(insts)
	_, _ = pp.Println("going to publish following config: ", cfg)

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

func TestRegister() {
	var err error
	ncm := config.NacosConfManager{}
	properties := getProperties()
	if err = ncm.Init(properties); err != nil {
		log.Fatalf("%+v", err)
	}

	var insts []config.Instance
	log.Infof("nacos try to deregister all existing instances")
	if insts, err = ncm.GetInstances(); err == nil {
		for _, inst := range insts {
			fields := strings.Split(inst.Addr, ":")
			ip := fields[0]
			var port int
			if port, err = strconv.Atoi(fields[1]); err != nil {
				log.Fatalf("failed to parse address %+v", inst.Addr)
			}
			log.Infof("nacos try to deregister %s:%d", ip, port)
			if err = ncm.Deregister(ip, port); err != nil {
				log.Warnf("ncm.Deregister(%s, %d) failed. %+v", ip, port, err)
			}
		}
	} else {
		log.Warnf("ncm.GetInstances failed. %+v", err)
	}

	ip := "127.0.0.1"
	port := 22
	//naming_client.NamingClient.SelectInstances() throws errors if "do not have useful host, ignore it", "instance list is empty!"
	//So there shall be at leas one alive instance during the test.
	log.Infof("nacos register %s:%d", ip, port)
	if err = ncm.Register(ip, port); err != nil {
		log.Fatalf("%+v", err)
	}
	log.Infof("nacos register %s:%d", ip, port+1)
	if err = ncm.Register(ip, port+1); err != nil {
		log.Fatalf("%+v", err)
	}

	expInsts := []config.Instance{
		{Addr: fmt.Sprintf("%s:%d", ip, port), Weight: runtime.NumCPU()},
		{Addr: fmt.Sprintf("%s:%d", ip, port+1), Weight: runtime.NumCPU()},
	}
	//naming_client.HostReactor.asyncUpdateService() updates cache every 10s.
	//So we need sleep a while to ensure at leas one update occurred.
	time.Sleep(10 * time.Second)
	if insts, err = ncm.GetInstances(); err != nil {
		log.Fatalf("%+v", err)
	}
	if !reflect.DeepEqual(expInsts, insts) {
		log.Fatalf("got different instances: %+v", insts)
	}

	log.Infof("nacos deregister %s:%d", ip, port)
	if err = ncm.Deregister(ip, port); err != nil {
		log.Fatalf("%+v", err)
	}
	expInsts = []config.Instance{
		{Addr: fmt.Sprintf("%s:%d", ip, port+1), Weight: runtime.NumCPU()},
	}
	time.Sleep(10 * time.Second)
	if insts, err = ncm.GetInstances(); err != nil {
		log.Fatalf("%+v", err)
	}
	if !reflect.DeepEqual(expInsts, insts) {
		log.Fatalf("got different instances: %+v", insts)
	}

	if err = ncm.Deregister(ip, port); err != nil {
		log.Fatalf("%+v", err)
	}
	//naming_client.NamingClient.SelectInstances() throws errors "instance list is empty!"
}

func main() {
	log.SetFormatter(&log.TextFormatter{
		FullTimestamp: true,
	})
	flag.Parse()
	if *testRegister {
		TestRegister()
	}
	PublishSinkerConfig()
}
