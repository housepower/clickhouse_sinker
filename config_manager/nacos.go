package rcm

import (
	"path/filepath"
	"strconv"
	"strings"

	"github.com/hjson/hjson-go/v4"
	"github.com/housepower/clickhouse_sinker/config"
	"github.com/housepower/clickhouse_sinker/util"
	"github.com/nacos-group/nacos-sdk-go/clients"
	"github.com/nacos-group/nacos-sdk-go/clients/config_client"
	"github.com/nacos-group/nacos-sdk-go/common/constant"
	"github.com/nacos-group/nacos-sdk-go/vo"
	"github.com/thanos-io/thanos/pkg/errors"
	"go.uber.org/zap"
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
		var nacosPort uint64 = 8848
		if len(serverAddrFields) >= 2 {
			if nacosPort, err = strconv.ParseUint(serverAddrFields[1], 10, 64); err != nil {
				err = errors.Wrapf(err, "invalid nacos serverAddrs")
				return
			}
		}
		sc = append(sc, constant.ServerConfig{
			IpAddr: serverAddrFields[0],
			Port:   nacosPort,
		})
	}

	var namespaceID string //Neither DEFAULT_NAMESPACE_ID("public") nor namespace name work!
	logDir := "."
	group := constant.DEFAULT_GROUP //Empty string doesn't work!
	if pop, ok := properties["namespaceId"]; ok {
		namespaceID, _ = pop.(string)
	}
	if pop, ok := properties["group"]; ok {
		group, _ = pop.(string)
	}
	if pop, ok := properties["logDir"]; ok {
		logDir, _ = pop.(string)
	}
	if logDir, err = filepath.Abs(logDir); err != nil {
		return errors.Wrapf(err, "")
	}
	cc := constant.ClientConfig{
		NamespaceId:         namespaceID,
		TimeoutMs:           5000,
		ListenInterval:      10000,
		NotLoadCacheAtStart: true,
		CacheDir:            filepath.Join(logDir, "nacos_cache"),
		CustomLogger:        &NacosLogger{util.Logger.Sugar()},
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
	if pop, ok := properties["dataId"]; ok {
		ncm.dataID, _ = pop.(string)
	}
	return
}

func (ncm *NacosConfManager) GetConfig() (conf *config.Config, err error) {
	var content string
	content, err = ncm.configClient.GetConfig(vo.ConfigParam{
		DataId: ncm.dataID,
		Group:  ncm.group,
	})
	if err != nil {
		err = errors.Wrapf(err, "")
		return
	}
	conf = &config.Config{Groups: make(map[string]*config.GroupConfig)}
	if err = hjson.Unmarshal([]byte(content), conf); err != nil {
		err = errors.Wrapf(err, "")
		return
	}
	return
}

func (ncm *NacosConfManager) PublishConfig(conf *config.Config) (err error) {
	util.Logger.Info("publish config to nacos")
	var bs []byte
	if bs, err = hjson.Marshal(*conf); err != nil {
		err = errors.Wrapf(err, "")
		return
	}
	content := string(bs)
	_, err = ncm.configClient.PublishConfig(vo.ConfigParam{
		DataId:  ncm.dataID,
		Group:   ncm.group,
		Content: content,
	})
	if err != nil {
		err = errors.Wrapf(err, "")
		return
	}
	// update the conf so that properties with default values are no longer nil
	if err = hjson.Unmarshal(bs, conf); err != nil {
		err = errors.Wrapf(err, "")
	}
	return
}

func (ncm *NacosConfManager) Stop() {
	cleanupKafkaClient()
	util.Logger.Info("stopped nacos config manager")
}

type NacosLogger struct {
	*zap.SugaredLogger
}

// override the loglevel to avoid log blow up
func (nlog *NacosLogger) Info(args ...interface{}) {
	nlog.Debug(args...)
}

// override the loglevel to avoid log blow up
func (nlog *NacosLogger) Infof(fmt string, args ...interface{}) {
	nlog.Debugf(fmt, args...)
}
