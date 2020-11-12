package config

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"

	"github.com/nacos-group/nacos-sdk-go/clients"
	"github.com/nacos-group/nacos-sdk-go/clients/config_client"
	"github.com/nacos-group/nacos-sdk-go/clients/naming_client"
	"github.com/nacos-group/nacos-sdk-go/common/constant"
	"github.com/nacos-group/nacos-sdk-go/model"
	"github.com/nacos-group/nacos-sdk-go/vo"
	"github.com/pkg/errors"
)

var _ RemoteConfManager = (*NacosConfManager)(nil)

const (
	ServiceName = "clickhouse_sinker"
	DataID      = "clickhouse_sinker.json"
)

type NacosConfManager struct {
	configClient config_client.IConfigClient
	namingClient naming_client.INamingClient
	group        string
	ip           string
	port         int
}

func (ncm *NacosConfManager) Init(properties map[string]interface{}) (err error) {
	var sc []constant.ServerConfig
	serverAddrs := strings.Split(properties["serverAddrs"].(string), ",")
	for _, serverAddr := range serverAddrs {
		serverAddrFields := strings.SplitN(serverAddr, ":", 2)
		var nacosPort uint64
		if nacosPort, err = strconv.ParseUint(serverAddrFields[1], 10, 64); err != nil {
			err = errors.Wrapf(err, "")
			return
		}
		sc = append(sc, constant.ServerConfig{
			IpAddr: serverAddrFields[0],
			Port:   nacosPort,
		})
	}

	var clientDir string
	if v, ok := properties["clientDir"]; ok {
		clientDir = v.(string)
	} else {
		clientDir = "/tmp/nacos"
	}
	namespaceID := constant.DEFAULT_NAMESPACE_ID
	group := constant.DEFAULT_GROUP
	var ok bool
	if _, ok = properties["namespaceId"]; ok {
		ns := properties["namespaceId"].(string)
		if ns != "" {
			namespaceID = ns
		}
	}
	if _, ok = properties["group"]; ok {
		grp := properties["group"].(string)
		if grp != "" {
			group = grp
		}
	}
	cc := constant.ClientConfig{
		NamespaceId:         namespaceID,
		TimeoutMs:           5000,
		ListenInterval:      10000,
		NotLoadCacheAtStart: true,
		LogDir:              filepath.Join(clientDir, "log"),
		CacheDir:            filepath.Join(clientDir, "cache"),
		RotateTime:          "1h",
		MaxAge:              3,
		LogLevel:            "debug",
		Username:            properties["username"].(string),
		Password:            properties["password"].(string),
	}

	ncm.configClient, err = clients.CreateConfigClient(map[string]interface{}{
		"serverConfigs": sc,
		"clientConfig":  cc,
	})
	if err != nil {
		err = errors.Wrapf(err, "")
		return
	}

	ncm.namingClient, err = clients.CreateNamingClient(map[string]interface{}{
		"serverConfigs": sc,
		"clientConfig":  cc,
	})
	if err != nil {
		err = errors.Wrapf(err, "")
		return
	}

	ncm.group = group
	ncm.ip = properties["ip"].(string)
	ncm.port = properties["port"].(int)
	return
}

func (ncm *NacosConfManager) Register() (err error) {
	_, err = ncm.namingClient.RegisterInstance(vo.RegisterInstanceParam{
		Ip:          ncm.ip,
		Port:        uint64(ncm.port),
		ServiceName: ServiceName,
		Weight:      float64(runtime.NumCPU()),
		GroupName:   ncm.group,
		Enable:      true,
		Healthy:     true,
		Ephemeral:   true,
	})
	if err != nil {
		err = errors.Wrapf(err, "")
	}
	return
}

func (ncm *NacosConfManager) Deregister() (err error) {
	_, err = ncm.namingClient.DeregisterInstance(
		vo.DeregisterInstanceParam{
			Ip:          ncm.ip,
			Port:        uint64(ncm.port),
			ServiceName: ServiceName,
			GroupName:   ncm.group,
			Ephemeral:   true,
		})
	if err != nil {
		err = errors.Wrapf(err, "")
	}
	return
}

func (ncm *NacosConfManager) GetInstances() (instances []Instance, err error) {
	var insts []model.Instance
	insts, err = ncm.namingClient.SelectInstances(vo.SelectInstancesParam{
		ServiceName: ServiceName,
		GroupName:   ncm.group,
		HealthyOnly: true,
	})
	//SelectInstances throws errors if "do not have useful host, ignore it", "instance list is empty!"
	if err != nil {
		err = errors.Wrapf(err, "")
		return
	}
	for _, inst := range insts {
		instances = append(instances, Instance{Addr: fmt.Sprintf("%s:%d", inst.Ip, inst.Port), Weight: int(inst.Weight)})
	}
	return
}

func (ncm *NacosConfManager) GetConfig() (conf *Config, err error) {
	var content string
	content, err = ncm.configClient.GetConfig(vo.ConfigParam{
		DataId: DataID,
		Group:  ncm.group,
	})
	if err != nil {
		err = errors.Wrapf(err, "")
		return
	}
	conf = &Config{}
	if err = json.Unmarshal([]byte(content), conf); err != nil {
		err = errors.Wrapf(err, "")
		return
	}
	return
}

func (ncm *NacosConfManager) PublishConfig(conf *Config) (err error) {
	var bs []byte
	if bs, err = json.Marshal(*conf); err != nil {
		err = errors.Wrapf(err, "")
		return
	}
	content := string(bs)
	_, err = ncm.configClient.PublishConfig(vo.ConfigParam{
		DataId:  DataID,
		Group:   ncm.group,
		Content: content,
	})
	if err != nil {
		err = errors.Wrapf(err, "")
		return
	}
	return
}
