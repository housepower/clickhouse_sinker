package config

import (
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/housepower/clickhouse_sinker/util"
	"github.com/nacos-group/nacos-sdk-go/common/constant"
	"github.com/stretchr/testify/assert"
)

var (
	nacosAddr      = "192.168.21.42:8848,192.168.31.46:8848,192.168.32.25:8848"
	nacosNamespace = ""                     //Neither DEFAULT_NAMESPACE_ID("public") nor namespace name work!
	nacosGroup     = constant.DEFAULT_GROUP //Empty string doesn't work!
	nacosUsername  = "nacos"
	nacosPassword  = "0192023A7BBD73250516F069DF18B500"
	ip             = util.GetOutboundIP().String()
	port           = 9090
)

// Empty is not valid namespaceID
func _getProperties() map[string]interface{} {
	properties := make(map[string]interface{})
	properties["serverAddrs"] = nacosAddr
	properties["namespaceId"] = nacosNamespace
	properties["username"] = nacosUsername
	properties["password"] = nacosPassword
	properties["group"] = nacosGroup
	return properties
}

func TestNacosRegister(t *testing.T) {
	var err error
	ncm := NacosConfManager{}
	properties := _getProperties()
	err = ncm.Init(properties)
	assert.Nil(t, err)

	//naming_client.NamingClient.SelectInstances() throws errors if "do not have useful host, ignore it", "instance list is empty!"
	//So there shall be at leas one alive instance during the test.
	var insts []Instance

	t.Logf("nacos register")
	err = ncm.Register(ip, port)
	assert.Nil(t, err)
	err = ncm.Register(ip, port+1)
	assert.Nil(t, err)

	expInsts := []Instance{
		{Addr: fmt.Sprintf("%s:%d", ip, port), Weight: runtime.NumCPU()},
		{Addr: fmt.Sprintf("%s:%d", ip, port+1), Weight: runtime.NumCPU()},
	}
	//naming_client.HostReactor.asyncUpdateService() updates cache every 10s.
	//So we need sleep a while to ensure at leas one update occurred.
	time.Sleep(10 * time.Second)
	insts, err = ncm.GetInstances()
	if err != nil {
		assert.FailNow(t, err.Error())
	}
	assert.Equal(t, expInsts, insts)

	t.Logf("nacos deregister")
	err = ncm.Deregister(ip, port)
	assert.Nil(t, err)

	expInsts = []Instance{
		{Addr: fmt.Sprintf("%s:%d", ip, port+1), Weight: runtime.NumCPU()},
	}
	time.Sleep(20 * time.Second)
	insts, err = ncm.GetInstances()
	assert.Nil(t, err)
	assert.Equal(t, expInsts, insts)
}

func TestNacosConfig(t *testing.T) {
	var err error
	ncm := NacosConfManager{}
	properties := _getProperties()
	err = ncm.Init(properties)
	assert.Nil(t, err)

	expConf := &Config{}
	expConf.Common.MinBufferSize = 13
	var conf *Config
	err = ncm.PublishConfig(expConf)
	assert.Nil(t, err)

	time.Sleep(5 * time.Second)
	conf, err = ncm.GetConfig()
	assert.Nil(t, err)
	assert.Equal(t, expConf, conf)
}
