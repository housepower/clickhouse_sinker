package rcm

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/housepower/clickhouse_sinker/config"
	"github.com/housepower/clickhouse_sinker/util"
	"github.com/nacos-group/nacos-sdk-go/v2/clients"
	"github.com/nacos-group/nacos-sdk-go/v2/clients/config_client"
	"github.com/nacos-group/nacos-sdk-go/v2/clients/naming_client"
	"github.com/nacos-group/nacos-sdk-go/v2/common/constant"
	"github.com/nacos-group/nacos-sdk-go/v2/model"
	"github.com/nacos-group/nacos-sdk-go/v2/vo"
	"github.com/thanos-io/thanos/pkg/errors"
	"go.uber.org/zap"
)

var _ RemoteConfManager = (*NacosConfManager)(nil)

type NacosConfManager struct {
	configClient config_client.IConfigClient
	namingClient naming_client.INamingClient
	group        string
	dataID       string
	serviceName  string
	instance     string // ip:port

	// state of assignment loop
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
	mux      sync.Mutex //protect curInsts, curCfg, curVer
	curInsts []string
	curCfg   *config.Config
	curVer   int
}

func toInstanceID(ip string, port int) string {
	return fmt.Sprintf("%s:%d", ip, port)
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

	var clientDir string
	if v, ok := properties["clientDir"]; ok {
		clientDir, _ = v.(string)
	} else {
		clientDir = "/tmp/nacos"
	}
	var namespaceID string          //Neither DEFAULT_NAMESPACE_ID("public") nor namespace name work!
	group := constant.DEFAULT_GROUP //Empty string doesn't work!
	if pop, ok := properties["namespaceId"]; ok {
		namespaceID, _ = pop.(string)
	}
	if pop, ok := properties["group"]; ok {
		group, _ = pop.(string)
	}
	cc := constant.ClientConfig{
		NamespaceId:         namespaceID,
		TimeoutMs:           5000,
		ListenInterval:      10000,
		NotLoadCacheAtStart: true,
		LogDir:              filepath.Join(clientDir, "log"),
		CacheDir:            filepath.Join(clientDir, "cache"),
		LogLevel:            "debug",
		Username:            properties["username"].(string),
		Password:            properties["password"].(string),
		LogRollingConfig: &constant.ClientLogRollingConfig{
			MaxAge: 3,
		},
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
	if pop, ok := properties["dataId"]; ok {
		ncm.dataID, _ = pop.(string)
	}
	if pop, ok := properties["serviceName"]; ok {
		ncm.serviceName, _ = pop.(string)
	}
	ncm.ctx, ncm.cancel = context.WithCancel(context.Background())
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
	conf = &config.Config{}
	if err = json.Unmarshal([]byte(content), conf); err != nil {
		err = errors.Wrapf(err, "")
		return
	}
	return
}

func (ncm *NacosConfManager) PublishConfig(conf *config.Config) (err error) {
	var bs []byte
	if bs, err = json.Marshal(*conf); err != nil {
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
	return
}

func (ncm *NacosConfManager) Register(ip string, port int) (err error) {
	_, err = ncm.namingClient.RegisterInstance(vo.RegisterInstanceParam{
		Ip:          ip,
		Port:        uint64(port),
		ServiceName: ncm.serviceName,
		GroupName:   ncm.group,
		Enable:      true,
		Healthy:     true,
		Ephemeral:   true,
	})
	if err != nil {
		err = errors.Wrapf(err, "")
	}
	ncm.instance = toInstanceID(ip, port)
	return
}

func (ncm *NacosConfManager) Deregister(ip string, port int) (err error) {
	_, err = ncm.namingClient.DeregisterInstance(
		vo.DeregisterInstanceParam{
			Ip:          ip,
			Port:        uint64(port),
			ServiceName: ncm.serviceName,
			GroupName:   ncm.group,
			Ephemeral:   true,
		})
	if err != nil {
		err = errors.Wrapf(err, "")
	}
	return
}

func (ncm *NacosConfManager) Run() {
	var err error
	ncm.wg.Add(1)
	defer ncm.wg.Done()
	// Assign the first time
	util.Logger.Debug("assign first")
	if err = ncm.assign(); err != nil {
		util.Logger.Error("first assign failed", zap.Error(err))
	}

	// Listen to service and config change, and assign if necessary
	configParam := vo.ConfigParam{
		DataId:   ncm.dataID,
		Group:    ncm.group,
		OnChange: ncm.configOnChange,
	}
	if err = ncm.configClient.ListenConfig(configParam); err != nil {
		util.Logger.Fatal("ncm.configClient.ListenConfig failed with permanent error", zap.Error(err))
	}

	subParam := vo.SubscribeParam{
		GroupName:         ncm.group,
		ServiceName:       ncm.serviceName,
		SubscribeCallback: ncm.serviceOnChange,
	}
	if err = ncm.namingClient.Subscribe(&subParam); err != nil {
		util.Logger.Fatal("ncm.namingClient.Subscribe failed with permanent error", zap.Error(err))
	}

	// Assign regularly to handle lag change
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()
LOOP_FOR:
	for {
		select {
		case <-ncm.ctx.Done():
			util.Logger.Info("NacosConfManager.Run quit due to context has been canceled")
			break LOOP_FOR
		case <-ticker.C:
			util.Logger.Debug("assign triggered by 5 min timer")
			if err := ncm.assign(); err != nil {
				util.Logger.Error("assign failed", zap.Error(err))
			}
		}
	}
}

func (ncm *NacosConfManager) Stop() {
	ncm.cancel()
	ncm.wg.Wait()
	var err error
	configParam := vo.ConfigParam{
		DataId:   ncm.dataID,
		Group:    ncm.group,
		OnChange: ncm.configOnChange,
	}
	if err = ncm.configClient.CancelListenConfig(configParam); err != nil {
		util.Logger.Error("ncm.configClient.CancelListenConfig failed", zap.Error(err))
	}

	subParam := vo.SubscribeParam{
		GroupName:         ncm.group,
		ServiceName:       ncm.serviceName,
		SubscribeCallback: ncm.serviceOnChange,
	}
	if err = ncm.namingClient.Unsubscribe(&subParam); err != nil {
		util.Logger.Error("ncm.namingClient.Unsubscribe failed", zap.Error(err))
	}
	util.Logger.Info("stopped nacos config manager")
}

func (ncm *NacosConfManager) configOnChange(namespace, group, dataID, data string) {
	if group != ncm.group || dataID != ncm.dataID {
		return
	}
	util.Logger.Debug("assign triggered by config change")
	if err := ncm.assign(); err != nil {
		util.Logger.Error("configOnChange failed", zap.Error(err))
	}
}

func (ncm *NacosConfManager) serviceOnChange(services []model.Instance, err error) {
	if err != nil {
		return
	}
	util.Logger.Debug("assign triggered by service change")
	if err = ncm.assign(); err != nil {
		util.Logger.Error("serviceOnChange failed", zap.Error(err))
	}
}

type InstanceAssignment struct {
	Instance string
	TotalLag int64
	TaskLags []TaskLag
}

type TaskLag struct {
	Task string
	Lag  int64
}

func (ncm *NacosConfManager) assign() (err error) {
	ncm.mux.Lock()
	defer ncm.mux.Unlock()
	getServiceParam := vo.GetServiceParam{
		GroupName:   ncm.group,
		ServiceName: ncm.serviceName,
	}
	var service model.Service
	if service, err = ncm.namingClient.GetService(getServiceParam); err != nil {
		err = errors.Wrapf(err, "ncm.namingClient.GetService failed")
		return
	}
	var newInsts []string
	for _, inst := range service.Hosts {
		newInsts = append(newInsts, toInstanceID(inst.Ip, int(inst.Port)))
	}
	sort.Strings(newInsts)
	if newInsts == nil || newInsts[0] != ncm.instance {
		// Only the first instance is capable to assgin
		return
	}

	var newCfg *config.Config
	if newCfg, err = ncm.GetConfig(); err != nil {
		err = errors.Wrapf(err, "ncm.GetConfig failed")
		return
	}
	if reflect.DeepEqual(ncm.curInsts, newInsts) &&
		reflect.DeepEqual(ncm.curCfg, newCfg) &&
		ncm.curCfg.Assignment.UpdatedBy == ncm.instance &&
		time.Unix(ncm.curCfg.Assignment.UpdatedAt, 0).Add(10*time.Minute).After(time.Now()) {
		util.Logger.Info("Both instances and config are up-to-date, and the config was published by myself in less than 10 minutes.")
		return
	}

	var stateLags map[string]StateLag
	if stateLags, err = GetTaskStateAndLags(newCfg); err != nil {
		return
	}
	util.Logger.Debug(fmt.Sprintf("task state and lags %+v", stateLags))

	var validTasks []string
	for _, taskCfg := range newCfg.Tasks {
		if _, ok := stateLags[taskCfg.Name]; ok {
			validTasks = append(validTasks, taskCfg.Name)
		}
	}
	sort.Slice(validTasks, func(i, j int) bool {
		taskNameI := validTasks[i]
		lagI := stateLags[taskNameI].Lag
		taskNameJ := validTasks[j]
		lagJ := stateLags[taskNameJ].Lag
		return (lagI > lagJ) || (lagI == lagJ && taskNameI < taskNameJ)
	})

	instAgs := make([]*InstanceAssignment, len(newInsts))
	for i, instance := range newInsts {
		instAgs[i] = &InstanceAssignment{
			Instance: instance,
		}
	}
	// distribute tasks in snake way
	for idxTask := 0; idxTask < len(validTasks); idxTask++ {
		idxInst := idxTask % len(newInsts)
		if (idxTask/len(newInsts))%2 == 1 {
			idxInst = len(newInsts) - 1 - idxInst
		}
		taskName := validTasks[idxTask]
		taskLag := stateLags[taskName].Lag
		instAg := instAgs[idxInst]
		instAg.TotalLag += taskLag
		instAg.TaskLags = append(instAg.TaskLags, TaskLag{Task: taskName, Lag: taskLag})
	}
	// balance
	if len(newInsts) >= 2 && len(validTasks) > len(newInsts) {
		last := len(newInsts) - 1
		for {
			sort.Slice(instAgs, func(i, j int) bool {
				return (instAgs[i].TotalLag > instAgs[j].TotalLag) || (instAgs[i].TotalLag == instAgs[j].TotalLag && instAgs[i].Instance < instAgs[j].Instance)
			})
			diffLag := float64(instAgs[0].TotalLag - instAgs[last].TotalLag)
			diffLagAbs := math.Abs(diffLag)
			if diffLag == 0.0 {
				break
			}
			var moved bool
			for idx := 0; idx < len(instAgs[0].TaskLags); idx++ {
				movingTask := instAgs[0].TaskLags[idx]
				if math.Abs(diffLag-float64(2*movingTask.Lag)) < diffLagAbs {
					instAgs[0].TotalLag -= movingTask.Lag
					instAgs[last].TotalLag += movingTask.Lag
					instAgs[0].TaskLags = append(instAgs[0].TaskLags[:idx], instAgs[0].TaskLags[idx+1:]...)
					instAgs[last].TaskLags = append(instAgs[last].TaskLags, movingTask)
					sort.Slice(instAgs[last].TaskLags, func(i, j int) bool {
						return instAgs[last].TaskLags[i].Lag > instAgs[last].TaskLags[j].Lag
					})
					moved = true
					break
				}
			}
			if !moved {
				break
			}
		}
	}

	// publish assignment
	newVer := ncm.curVer + 1
	util.Logger.Debug("going to publish assignment", zap.Int("version", newVer), zap.Reflect("assignment", instAgs))
	newCfg.Assignment.Map = make(map[string][]string)
	for _, instAg := range instAgs {
		var tasks []string
		for _, taskLag := range instAg.TaskLags {
			tasks = append(tasks, taskLag.Task)
		}
		sort.Strings(tasks)
		newCfg.Assignment.Map[instAg.Instance] = tasks
	}
	newCfg.Assignment.Version = newVer
	newCfg.Assignment.UpdatedBy = ncm.instance
	newCfg.Assignment.UpdatedAt = time.Now().Unix()
	if err = ncm.PublishConfig(newCfg); err != nil {
		return
	}
	ncm.curCfg = newCfg
	ncm.curInsts = newInsts
	ncm.curVer = newVer

	return
}
