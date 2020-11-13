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
	"reflect"
	"runtime"
	"time"

	_ "github.com/ClickHouse/clickhouse-go"
	"github.com/housepower/clickhouse_sinker/config"
	"github.com/k0kubun/pp"
	log "github.com/sirupsen/logrus"
)

var (
	nacosAddr = flag.String("nacos-addr", "127.0.0.1:8848",
		"nacos server addresses, separated with comma")
	nacosUsername = flag.String("nacos-username", "username",
		"nacos username")
	nacosPassword = flag.String("nacos-password", "password",
		"nacos password")
	nacosNamespaceID = flag.String("nacos-namespace-id", "",
		`nacos namespace ID. Neither DEFAULT_NAMESPACE_ID("public") nor namespace name work!`)
	nacosGroup = flag.String("nacos-group", "DEFAULT_GROUP",
		`nacos group name. Empty string doesn't work!`)

	sinkerAddr   = flag.String("sinker-addr", "127.0.0.1:2112", "sinker address")
	sinkerCfgDir = flag.String("sinker-conf", "conf", "assign this config to the given sinker")
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
	if cfg, err = config.ParseLocalConfig(*sinkerCfgDir, *sinkerAddr); err != nil {
		log.Fatalf("%+v", err)
	}
	_, _ = pp.Println(cfg)

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

func TestRegister(){
	var err error
	ncm := config.NacosConfManager{}
	properties := getProperties()
	err = ncm.Init(properties)
	if err = ncm.Init(properties); err != nil {
		log.Fatalf("%+v", err)
	}

	ip := "127.0.0.1"
	port := 22
	//naming_client.NamingClient.SelectInstances() throws errors if "do not have useful host, ignore it", "instance list is empty!"
	//So there shall be at leas one alive instance during the test.
	var insts []config.Instance
	log.Infof("nacos register")
	if err = ncm.Register(ip, port); err != nil {
		log.Fatalf("%+v", err)
	}
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
	if insts, err = ncm.GetInstances(); err!= nil {
		log.Fatalf("%+v", err)
	}
	if !reflect.DeepEqual(expInsts, insts) {
		log.Fatalf("got different instances: %+v", insts)
	}

	log.Infof("nacos deregister")
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
	return
}

func main() {
	flag.Parse()
	TestRegister()
	PublishSinkerConfig()
}
