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
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"net/http/pprof"
	"os"
	"reflect"
	"runtime"
	"strings"
	"time"

	_ "github.com/ClickHouse/clickhouse-go"
	"github.com/google/gops/agent"
	"github.com/housepower/clickhouse_sinker/config"
	"github.com/housepower/clickhouse_sinker/health"
	"github.com/housepower/clickhouse_sinker/input"
	"github.com/housepower/clickhouse_sinker/output"
	"github.com/housepower/clickhouse_sinker/parser"
	"github.com/housepower/clickhouse_sinker/pool"
	"github.com/housepower/clickhouse_sinker/statistics"
	"github.com/housepower/clickhouse_sinker/task"
	"github.com/housepower/clickhouse_sinker/util"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	log "github.com/sirupsen/logrus"
	"github.com/sundy-li/go_commons/app"
)

type CmdOptions struct {
	ShowVer                              bool
	HTTPPort                             int
	PushGatewayAddrs                     string
	PushInterval                         int
	LocalCfgDir                          string
	ConsulRegister                       bool
	ConsulAddr                           string
	ConsulDeregisterCriticalServiceAfter string
	NacosRegister                        bool
	NacosAddr                            string
	NacosNamespaceID                     string
	NacosGroup                           string
	NacosUsername                        string
	NacosPassword                        string
}

var (
	cmdOps      CmdOptions
	selfIP      string
	selfAddr    string
	httpMetrics = promhttp.Handler()
	runner      *Sinker
)

func initCmdOptions() {
	// 1. Set options to default value.
	cmdOps = CmdOptions{
		ShowVer:                              false,
		HTTPPort:                             2112,
		PushGatewayAddrs:                     "",
		PushInterval:                         10,
		LocalCfgDir:                          "/etc/clickhouse_sinker",
		ConsulRegister:                       false,
		ConsulAddr:                           "http://127.0.0.1:8500",
		ConsulDeregisterCriticalServiceAfter: "30m",
		NacosRegister:                        false,
		NacosAddr:                            "127.0.0.1:8848",
		NacosNamespaceID:                     "",
		NacosGroup:                           "DEFAULT_GROUP",
		NacosUsername:                        "nacos",
		NacosPassword:                        "nacos",
	}

	// 2. Replace options with the corresponding env variable if present.
	util.EnvBoolVar(&cmdOps.ShowVer, "v")
	util.EnvIntVar(&cmdOps.HTTPPort, "http-port")
	util.EnvStringVar(&cmdOps.PushGatewayAddrs, "metric-push-gateway-addrs")
	util.EnvIntVar(&cmdOps.PushInterval, "push-interval")
	util.EnvStringVar(&cmdOps.LocalCfgDir, "local-cfg-dir")

	util.EnvBoolVar(&cmdOps.ConsulRegister, "consul-register-enable")
	util.EnvStringVar(&cmdOps.ConsulAddr, "consul-addr")
	util.EnvStringVar(&cmdOps.ConsulDeregisterCriticalServiceAfter, "consul-deregister-critical-services-after")

	util.EnvBoolVar(&cmdOps.NacosRegister, "nacos-register-enable")
	util.EnvStringVar(&cmdOps.NacosAddr, "nacos-addr")
	util.EnvStringVar(&cmdOps.NacosNamespaceID, "nacos-namespace-id")
	util.EnvStringVar(&cmdOps.NacosGroup, "nacos-group")
	util.EnvStringVar(&cmdOps.NacosUsername, "nacos-username")
	util.EnvStringVar(&cmdOps.NacosPassword, "nacos-password")

	// 3. Replace options with the corresponding CLI parameter if present.
	flag.BoolVar(&cmdOps.ShowVer, "v", cmdOps.ShowVer, "show build version and quit")
	flag.IntVar(&cmdOps.HTTPPort, "http-port", cmdOps.HTTPPort, "http listen port")
	flag.StringVar(&cmdOps.PushGatewayAddrs, "metric-push-gateway-addrs", cmdOps.PushGatewayAddrs, "a list of comma-separated prometheus push gatway address")
	flag.IntVar(&cmdOps.PushInterval, "push-interval", cmdOps.PushInterval, "push interval in seconds")
	flag.StringVar(&cmdOps.LocalCfgDir, "local-cfg-dir", cmdOps.LocalCfgDir, "local config dir")

	flag.BoolVar(&cmdOps.ConsulRegister, "consul-register-enable", cmdOps.ConsulRegister, "register current instance in consul")
	flag.StringVar(&cmdOps.ConsulAddr, "consul-addr", cmdOps.ConsulAddr, "consul api interface address")
	flag.StringVar(&cmdOps.ConsulDeregisterCriticalServiceAfter, "consul-deregister-critical-services-after", cmdOps.ConsulDeregisterCriticalServiceAfter,
		"configure service check DeregisterCriticalServiceAfter")

	flag.BoolVar(&cmdOps.NacosRegister, "nacos-register-enable", cmdOps.NacosRegister, "register current instance in nacos")
	flag.StringVar(&cmdOps.NacosAddr, "nacos-addr", cmdOps.NacosAddr, "a list of comma-separated nacos server addresses")
	flag.StringVar(&cmdOps.NacosNamespaceID, "nacos-namespace-id", cmdOps.NacosNamespaceID,
		`nacos namespace ID. Neither DEFAULT_NAMESPACE_ID("public") nor namespace name work!`)
	flag.StringVar(&cmdOps.NacosGroup, "nacos-group", cmdOps.NacosGroup, `nacos group name. Empty string doesn't work!`)
	flag.StringVar(&cmdOps.NacosUsername, "nacos-username", cmdOps.NacosUsername, "nacos username")
	flag.StringVar(&cmdOps.NacosPassword, "nacos-password", cmdOps.NacosPassword, "nacos password")
	flag.Parse()
}

func init() {
	initCmdOptions()
	if cmdOps.ShowVer {
		config.PrintSinkerInfo()
		os.Exit(0)
	}
	selfIP = util.GetOutboundIP().String()
	cmdOps.HTTPPort = util.GetSpareTCPPort(selfIP, cmdOps.HTTPPort)
	selfAddr = fmt.Sprintf("%s:%d", selfIP, cmdOps.HTTPPort)
}

// GenTask generate a task via config
func GenTask(cfg *config.Config, taskName string) (taskImpl *task.Service) {
	taskCfg := cfg.Tasks[taskName]
	ck := output.NewClickHouse(cfg, taskName)
	pp := parser.NewParserPool(taskCfg.Parser, taskCfg.CsvFormat, taskCfg.Delimiter, []string{taskCfg.LayoutDate, taskCfg.LayoutDateTime, taskCfg.LayoutDateTime64})
	var inputer input.Inputer
	if taskCfg.Kafka != "" {
		inputer = input.NewInputer(taskCfg.KafkaClient)
	}
	taskImpl = task.NewTaskService(inputer, ck, pp, cfg, taskName)
	return
}

func main() {
	log.SetFormatter(&log.TextFormatter{
		FullTimestamp: true,
	})
	if err := agent.Listen(agent.Options{}); err != nil {
		log.Fatalf("%+v", err)
	}

	app.Run("clickhouse_sinker", func() error {
		var rcm config.RemoteConfManager
		var properties map[string]interface{}
		if cmdOps.ConsulRegister {
			rcm = &config.ConsulConfManager{}
			properties = make(map[string]interface{})
			properties["consulAddr"] = cmdOps.ConsulAddr
			properties["deregisterCriticalServiceAfter"] = cmdOps.ConsulDeregisterCriticalServiceAfter
		} else if cmdOps.NacosRegister {
			rcm = &config.NacosConfManager{}
			properties = make(map[string]interface{})
			properties["serverAddrs"] = cmdOps.NacosAddr
			properties["username"] = cmdOps.NacosUsername
			properties["password"] = cmdOps.NacosPassword
			properties["namespaceId"] = cmdOps.NacosNamespaceID
			properties["group"] = cmdOps.NacosGroup
		}
		if rcm != nil {
			if err := rcm.Init(properties); err != nil {
				log.Fatalf("%+v", err)
			}
			if err := rcm.Register(selfIP, cmdOps.HTTPPort); err != nil {
				log.Fatalf("%+v", err)
			}
		}
		runner = NewSinker(rcm)
		return runner.Init()
	}, func() error {
		go func() {
			mux := http.NewServeMux()
			mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
				_, _ = w.Write([]byte(`
				<html><head><title>ClickHouse Sinker</title></head>
				<body>
					<h1>ClickHouse Sinker</h1>
					<p><a href="/metrics">Metrics</a></p>
					<p><a href="/ready">Ready</a></p>
					<p><a href="/ready?full=1">Ready Full</a></p>
					<p><a href="/live">Live</a></p>
					<p><a href="/live?full=1">Live Full</a></p>
				</body></html>`))
			})

			mux.Handle("/metrics", httpMetrics)
			mux.HandleFunc("/ready", health.Health.ReadyEndpoint) // GET /ready?full=1
			mux.HandleFunc("/live", health.Health.LiveEndpoint)   // GET /live?full=1

			mux.HandleFunc("/debug/pprof/", pprof.Index)
			mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
			mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
			mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
			mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

			log.Infof("Run http server http://%s", selfAddr)
			log.Error(http.ListenAndServe(selfAddr, mux))
		}()

		runner.Run()
		return nil
	}, func() error {
		runner.Close()
		return nil
	})
}

// Sinker object maintains number of task for each partition
type Sinker struct {
	curCfg *config.Config
	pusher *statistics.Pusher
	tasks  map[string]*task.Service
	rcm    config.RemoteConfManager
	ctx    context.Context
	cancel context.CancelFunc
}

// NewSinker get an instance of sinker with the task list
func NewSinker(rcm config.RemoteConfManager) *Sinker {
	parent := context.Background()
	ctx, cancel := context.WithCancel(parent)
	s := &Sinker{rcm: rcm, ctx: ctx, cancel: cancel}
	return s
}

// Init initializes the list of tasks
func (s *Sinker) Init() (err error) {
	return
}

// Run rull all tasks in different go routines
func (s *Sinker) Run() {
	var err error
	var newCfg *config.Config
	if cmdOps.PushGatewayAddrs != "" {
		addrs := strings.Split(cmdOps.PushGatewayAddrs, ",")
		s.pusher = statistics.NewPusher(addrs, cmdOps.PushInterval)
		if err = s.pusher.Init(); err != nil {
			return
		}
		go s.pusher.Run(s.ctx)
	}
	if s.rcm == nil {
		if newCfg, err = config.ParseLocalConfig(cmdOps.LocalCfgDir); err != nil {
			log.Fatalf("%+v", err)
			return
		}
		if err = newCfg.Normallize(); err != nil {
			log.Fatalf("%+v", err)
			return
		}
		// Assign all tasks to myself.
		newCfg.AssignTasks([]config.Instance{{Addr: selfAddr, Weight: 1}})
		if err = s.applyConfig(newCfg); err != nil {
			log.Fatalf("%+v", err)
			return
		}
		<-s.ctx.Done()
	} else {
		for {
			select {
			case <-s.ctx.Done():
				return
			case <-time.After(5 * time.Second):
				if newCfg, err = s.rcm.GetConfig(); err != nil {
					log.Fatalf("%+v", err)
					return
				}
				if err = newCfg.Normallize(); err != nil {
					log.Fatalf("%+v", err)
					return
				}
				if err = s.applyConfig(newCfg); err != nil {
					log.Fatalf("%+v", err)
					return
				}
			}
		}
	}
}

// Close shutdown tasks
func (s *Sinker) Close() {
	s.cancel()
	for _, task := range s.tasks {
		task.Stop()
	}

	util.GlobalParsingPool.StopWait()
	util.GlobalWritingPool.StopWait()
	util.GlobalTimerWheel.Stop()

	if s.pusher != nil {
		s.pusher.Stop()
	}
	if s.rcm != nil {
		_ = s.rcm.Deregister(selfIP, cmdOps.HTTPPort)
	}
}

func (s *Sinker) applyConfig(newCfg *config.Config) (err error) {
	var lvl log.Level
	if lvl, err = log.ParseLevel(newCfg.Common.LogLevel); err != nil {
		err = errors.Wrapf(err, "")
		return
	}
	log.SetLevel(lvl)
	if s.curCfg == nil {
		// The first time invoking of applyConfig
		err = s.applyFirstConfig(newCfg)
	} else if !reflect.DeepEqual(newCfg, s.curCfg) {
		err = s.applyAnotherConfig(newCfg)
	}
	return
}

func (s *Sinker) applyFirstConfig(newCfg *config.Config) (err error) {
	var bsNewCfg []byte
	if bsNewCfg, err = json.Marshal(newCfg); err != nil {
		err = errors.Wrapf(err, "")
		return
	}
	log.Infof("going to apply the first config: %+v", string(bsNewCfg))

	util.InitGlobalTimerWheel()
	concurrentParsers := 10
	s.tasks = make(map[string]*task.Service)
	if taskNames, ok := newCfg.Assignment[selfAddr]; ok {
		for _, taskName := range taskNames {
			t := GenTask(newCfg, taskName)
			if err = t.Init(); err != nil {
				return
			}
			s.tasks[taskName] = t
		}
		concurrentParsers = len(taskNames) * 10
		if concurrentParsers > runtime.NumCPU()/2 {
			concurrentParsers = runtime.NumCPU() / 2
		}
	}
	util.InitGlobalParsingPool(concurrentParsers)
	totalConn := pool.GetTotalConn()
	util.InitGlobalWritingPool(totalConn)

	for _, t := range s.tasks {
		go t.Run(s.ctx)
	}
	s.curCfg = newCfg
	return
}

func (s *Sinker) applyAnotherConfig(newCfg *config.Config) (err error) {
	var bsNewCfg []byte
	if bsNewCfg, err = json.Marshal(newCfg); err != nil {
		err = errors.Wrapf(err, "")
		return
	}
	log.Infof("going to apply a different config: %+v", string(bsNewCfg))
	//1. Found all tasks need to stop.
	// Each such task matches at least one of the following conditions:
	// - task not in new assignment
	// - task config differ
	// - task clickhouse config differ
	// - task kafka config differ
	var tasksToStop []string
	if taskNames, ok := s.curCfg.Assignment[selfAddr]; ok {
		for _, taskName := range taskNames {
			var needStop bool
			curTaskCfg := s.curCfg.Tasks[taskName]
			if newTaskCfg, ok2 := newCfg.Tasks[taskName]; ok2 {
				if !reflect.DeepEqual(newTaskCfg, curTaskCfg) {
					needStop = true
				} else {
					chName := curTaskCfg.Clickhouse
					curChCfg := s.curCfg.Clickhouse[chName]
					newChCfg := newCfg.Clickhouse[chName]
					if !reflect.DeepEqual(newChCfg, curChCfg) {
						needStop = true
					}
					kfkName := curTaskCfg.Kafka
					curKfkCfg := s.curCfg.Kafka[kfkName]
					newKfkCfg := newCfg.Kafka[kfkName]
					if !reflect.DeepEqual(newKfkCfg, curKfkCfg) {
						needStop = true
					}
				}
			} else {
				needStop = true
			}
			if needStop {
				tasksToStop = append(tasksToStop, taskName)
			}
		}
	}
	// 2. Stop all tasks in parallel found at previous step.
	for _, taskName := range tasksToStop {
		if task, ok := s.tasks[taskName]; ok {
			task.NotifyStop()
		}
	}
	for _, taskName := range tasksToStop {
		if task, ok := s.tasks[taskName]; ok {
			task.Stop()
			delete(s.tasks, taskName)
		} else {
			log.Warnf("Failed to stop task %s. It's disappeared.", taskName)
		}
	}
	// 3. Initailize all tasks which are new or their config differ.
	var newTasks []*task.Service
	if taskNames, ok := newCfg.Assignment[selfAddr]; ok {
		for _, taskName := range taskNames {
			if _, ok2 := s.tasks[taskName]; !ok2 {
				t := GenTask(newCfg, taskName)
				if err = t.Init(); err != nil {
					return
				}
				s.tasks[taskName] = t
				newTasks = append(newTasks, t)
			}
		}
	}
	// 4. Resize goroutine pools.
	concurrentParsers := len(s.tasks) * 10
	if concurrentParsers > runtime.NumCPU()/2 {
		concurrentParsers = runtime.NumCPU() / 2
	}
	util.GlobalParsingPool.Resize(concurrentParsers)
	totalConn := pool.GetTotalConn()
	util.GlobalWritingPool.Resize(totalConn)

	// 5. Start new tasks. We don't do it at step 3 in order to avoid goroutine leak due to errors raised by later steps.
	for _, t := range newTasks {
		go t.Run(s.ctx)
	}

	// 6. Record the new config.
	s.curCfg = newCfg
	return
}
