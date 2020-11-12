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
	"flag"
	"fmt"
	"net/http"
	"net/http/pprof"
	"os"
	"reflect"
	"runtime"
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
	"github.com/k0kubun/pp"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	log "github.com/sirupsen/logrus"
	"github.com/sundy-li/go_commons/app"
)

var (
	v        = flag.Bool("v", false, "show build version")
	cfgDir   = flag.String("conf", "", "config dir")
	httpPort = flag.Int("http-port", 2112, "http listen port")

	consulRegister = flag.Bool("consul-register-enable", false,
		"register current instance in consul")
	consulAddr = flag.String("consul-addr", "http://127.0.0.1:8500",
		"consul api interface address")
	consulDeregisterCriticalServiceAfter = flag.String("consul-deregister-critical-services-after", "30m",
		"configure service check DeregisterCriticalServiceAfter")

	nacosRegister = flag.Bool("nacos-register-enable", false,
		"register current instance in nacos")
	nacosAddr = flag.String("nacos-addr", "127.0.0.1:8848",
		"nacos server addresses, separated with comma")
	nacosNamespaceID = flag.String("nacos-namespace-id", "",
		"nacos namespace ID")
	nacosGroup = flag.String("nacos-group", "",
		"nacos group name")
	nacosUsername = flag.String("nacos-username", "username",
		"nacos username")
	nacosPassword = flag.String("nacos-password", "password",
		"nacos password")

	httpMetrics = promhttp.Handler()
	runner      *Sinker
	ip          string
	selfAddr    string
)

func init() {
	flag.Parse()
	if *v {
		config.PrintSinkerInfo()
		os.Exit(0)
	}
	ip = util.GetOutboundIP().String()
	*httpPort = util.GetSpareTCPPort(ip, *httpPort)
	selfAddr = fmt.Sprintf("%s:%d", ip, *httpPort)
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
		if *consulRegister {
			rcm = &config.ConsulConfManager{}
			properties = make(map[string]interface{})
			properties["consulAddr"] = *consulAddr
			properties["deregisterCriticalServiceAfter"] = *consulDeregisterCriticalServiceAfter
			properties["ip"] = ip
			properties["port"] = *httpPort
		} else if *nacosRegister {
			rcm = &config.NacosConfManager{}
			properties = make(map[string]interface{})
			properties["serverAddrs"] = *nacosAddr
			properties["namespaceId"] = *nacosNamespaceID
			properties["username"] = *nacosUsername
			properties["password"] = *nacosPassword
			properties["group"] = *nacosGroup
			properties["ip"] = ip
			properties["port"] = *httpPort
		}
		if rcm != nil {
			if err := rcm.Init(properties); err != nil {
				log.Fatalf("%+v", err)
			}
			if err := rcm.Register(); err != nil {
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
	if s.rcm == nil {
		if newCfg, err = config.ParseLocalConfig(*cfgDir, selfAddr); err != nil {
			log.Fatalf("%+v", err)
			return
		}
		if err = s.applyConfig(newCfg); err != nil {
			log.Fatalf("%+v", err)
			return
		}
		<-s.ctx.Done()
	} else {
		t := time.NewTicker(5 * time.Second)
		for {
			select {
			case <-s.ctx.Done():
				return
			case <-t.C:
				if newCfg, err = s.rcm.GetConfig(); err != nil {
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
		_ = s.rcm.Deregister()
	}
}

func (s *Sinker) applyConfig(newCfg *config.Config) (err error) {
	if newCfg == nil {
		err = errors.Errorf("applyConfig got a nil config")
		return
	}
	if err = newCfg.Normallize(); err != nil {
		return
	}
	var lvl log.Level
	if lvl, err = log.ParseLevel(newCfg.Common.LogLevel); err != nil {
		err = errors.Wrapf(err, "")
		return
	}
	log.SetLevel(lvl)
	if s.curCfg == nil {
		// The first time invoking of applyConfig
		err = s.applyFirstConfig(newCfg)
	} else {
		err = s.applyAnotherConfig(newCfg)
	}
	return
}

func (s *Sinker) applyFirstConfig(newCfg *config.Config) (err error) {
	log.Infof("going to apply the first config: %+v", pp.Sprint(newCfg))
	if newCfg.Statistics.Enable {
		s.pusher = statistics.NewPusher(newCfg.Statistics.PushGateWayAddrs,
			newCfg.Statistics.PushInterval)
		if err = s.pusher.Init(); err != nil {
			return
		}
	}

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

	if s.pusher != nil {
		go s.pusher.Run()
	}
	for _, t := range s.tasks {
		go t.Run(s.ctx)
	}
	s.curCfg = newCfg
	return
}

func (s *Sinker) applyAnotherConfig(newCfg *config.Config) (err error) {
	// 1. Quit if the two configs are the same.
	if reflect.DeepEqual(newCfg, s.curCfg) {
		return
	}
	log.Infof("going to apply a different config: %+v", pp.Sprint(newCfg))
	//2. Found all tasks need to stop.
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
	// 3. Stop all tasks found at the step 2.
	for _, taskName := range tasksToStop {
		if task, ok := s.tasks[taskName]; ok {
			task.Stop()
			delete(s.tasks, taskName)
		} else {
			log.Warnf("Failed to stop task %s. It's disappeared.", taskName)
		}
	}
	// 4. Initailize all tasks which is new or its config differ.
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
	// 5. Resize goroutine pools.
	concurrentParsers := len(s.tasks) * 10
	if concurrentParsers > runtime.NumCPU()/2 {
		concurrentParsers = runtime.NumCPU() / 2
	}
	util.GlobalParsingPool.Resize(concurrentParsers)
	totalConn := pool.GetTotalConn()
	util.GlobalWritingPool.Resize(totalConn)

	// 6. Handle pusher config
	if !reflect.DeepEqual(newCfg.Statistics, s.curCfg.Statistics) {
		if s.pusher != nil {
			s.pusher.Stop()
			s.pusher = nil
		}
		if newCfg.Statistics.Enable {
			s.pusher = statistics.NewPusher(newCfg.Statistics.PushGateWayAddrs,
				newCfg.Statistics.PushInterval)
			if err = s.pusher.Init(); err != nil {
				return
			}
			go s.pusher.Run()
		}
	}

	// 7. Start new tasks. We don't do it at step 4 in order to avoid goroutine leak due to errors raised by later steps.
	for _, t := range newTasks {
		go t.Run(s.ctx)
	}

	// 8. Record the new config.
	s.curCfg = newCfg
	return
}
