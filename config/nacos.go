package config

import (
	"encoding/json"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/nacos-group/nacos-sdk-go/clients"
	"github.com/nacos-group/nacos-sdk-go/clients/config_client"
	"github.com/nacos-group/nacos-sdk-go/common/constant"
	"github.com/nacos-group/nacos-sdk-go/vo"
	"github.com/pkg/errors"
)

var _ RemoteConfManager = (*NacosConfManager)(nil)

type NacosConfManager struct {
	configClient config_client.IConfigClient
	group        string
	dataID       string
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
		clientDir, _ = v.(string)
	} else {
		clientDir = "/tmp/nacos"
	}
	var namespaceID string          //Neither DEFAULT_NAMESPACE_ID("public") nor namespace name work!
	group := constant.DEFAULT_GROUP //Empty string doesn't work!
	var ok bool
	if _, ok = properties["namespaceId"]; ok {
		namespaceID, _ = properties["namespaceId"].(string)
	}
	if _, ok = properties["group"]; ok {
		group, _ = properties["group"].(string)
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

	ncm.group = group
	if _, ok = properties["dataId"]; ok {
		ncm.dataID, _ = properties["dataId"].(string)
	}
	return
}

func (ncm *NacosConfManager) GetConfig() (conf *Config, err error) {
	var content string
	content, err = ncm.configClient.GetConfig(vo.ConfigParam{
		DataId: ncm.dataID,
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
	if ncm.dataID != conf.Task.Name {
		err = errors.Errorf("DataId %s doesn't match with config: %s", ncm.dataID, content)
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
		DataId:  conf.Task.Name,
		Group:   ncm.group,
		Content: content,
	})
	if err != nil {
		err = errors.Wrapf(err, "")
		return
	}
	ncm.dataID = conf.Task.Name
	return
}
