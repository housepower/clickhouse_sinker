package config

import (
	"fmt"
	"runtime"

	"github.com/google/uuid"
	"github.com/hashicorp/consul/api"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

var _ RemoteConfManager = (*ConsulConfManager)(nil)

type ConsulConfManager struct {
	consulAgent                    *api.Agent
	appID                          string
	deregisterCriticalServiceAfter string
	ip                             string
	port                           int
}

func (ccm *ConsulConfManager) Init(properties map[string]interface{}) (err error) {
	ccm.ip = properties["ip"].(string)
	ccm.port = properties["port"].(int)
	appID, _ := uuid.NewUUID()
	ccm.appID = fmt.Sprintf("clickhouse_sinker-%s", appID.String())
	ccm.deregisterCriticalServiceAfter = properties["deregisterCriticalServiceAfter"].(string)
	consulConfig := api.DefaultConfig()
	consulConfig.Address = properties["consulAddr"].(string)
	consulClient, _ := api.NewClient(consulConfig)
	ccm.consulAgent = consulClient.Agent()
	return
}

func (ccm *ConsulConfManager) Register() (err error) {
	log.Infof("Consul: register service")
	err = ccm.consulAgent.ServiceRegister(&api.AgentServiceRegistration{
		Name:    "clickhouse_sinker",
		ID:      ccm.appID,
		Port:    ccm.port,
		Address: ccm.ip,
		Weights: &api.AgentWeights{Passing: runtime.NumCPU(), Warning: 0},
		Check: &api.AgentServiceCheck{
			CheckID:                        ccm.appID + "-http-heath",
			Name:                           "/ready",
			Interval:                       "15s",
			Timeout:                        "15s",
			HTTP:                           fmt.Sprintf("http://%s:%d/ready?full=1", ccm.ip, ccm.port),
			DeregisterCriticalServiceAfter: ccm.deregisterCriticalServiceAfter,
		},
	})
	if err != nil {
		err = errors.Wrapf(err, "")
	}
	return
}

func (ccm *ConsulConfManager) Deregister() (err error) {
	log.Debug("Consul: deregister service")
	err = ccm.consulAgent.ServiceDeregister(ccm.appID)
	if err != nil {
		err = errors.Wrapf(err, "")
	}
	return
}

func (ccm *ConsulConfManager) GetInstances() (instances []Instance, err error) {
	//FIXME: implement it!
	return
}

func (ccm *ConsulConfManager) GetConfig() (conf *Config, err error) {
	//FIXME: implement it!
	return
}

func (ccm *ConsulConfManager) PublishConfig(conf *Config) (err error) {
	//FIXME: implement it!
	return
}
