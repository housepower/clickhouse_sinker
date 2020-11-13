package config

import (
	"fmt"
	"runtime"

	"github.com/hashicorp/consul/api"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

var _ RemoteConfManager = (*ConsulConfManager)(nil)

type ConsulConfManager struct {
	consulAgent                    *api.Agent
	deregisterCriticalServiceAfter string
}

func (ccm *ConsulConfManager) Init(properties map[string]interface{}) (err error) {
	consulConfig := api.DefaultConfig()
	consulConfig.Address = properties["consulAddr"].(string)
	consulClient, _ := api.NewClient(consulConfig)
	ccm.consulAgent = consulClient.Agent()
	ccm.deregisterCriticalServiceAfter = properties["deregisterCriticalServiceAfter"].(string)
	return
}

func (ccm *ConsulConfManager) Register(ip string, port int) (err error) {
	log.Infof("Consul: register service")
	appID := fmt.Sprintf("clickhouse_sinker-%s-%d", ip, port)
	err = ccm.consulAgent.ServiceRegister(&api.AgentServiceRegistration{
		Name:    "clickhouse_sinker",
		ID:      appID,
		Port:    port,
		Address: ip,
		Weights: &api.AgentWeights{Passing: runtime.NumCPU(), Warning: 0},
		Check: &api.AgentServiceCheck{
			CheckID:                        appID + "-http-heath",
			Name:                           "/ready",
			Interval:                       "15s",
			Timeout:                        "15s",
			HTTP:                           fmt.Sprintf("http://%s:%d/ready?full=1", ip, port),
			DeregisterCriticalServiceAfter: ccm.deregisterCriticalServiceAfter,
		},
	})
	if err != nil {
		err = errors.Wrapf(err, "")
	}
	return
}

func (ccm *ConsulConfManager) Deregister(ip string, port int) (err error) {
	log.Debug("Consul: deregister service")
	appID := fmt.Sprintf("clickhouse_sinker-%s-%d", ip, port)
	err = ccm.consulAgent.ServiceDeregister(appID)
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
