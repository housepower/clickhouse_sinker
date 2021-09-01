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
	"log"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/housepower/clickhouse_sinker/config"
	cm "github.com/housepower/clickhouse_sinker/config_manager"
	"github.com/housepower/clickhouse_sinker/health"
	"github.com/housepower/clickhouse_sinker/pool"
	"github.com/housepower/clickhouse_sinker/statistics"
	"github.com/housepower/clickhouse_sinker/task"
	"github.com/housepower/clickhouse_sinker/util"
	"go.uber.org/zap"

	_ "github.com/ClickHouse/clickhouse-go"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type CmdOptions struct {
	ShowVer          bool
	LogLevel         string // "debug", "info", "warn", "error", "dpanic", "panic", "fatal"
	LogPaths         string // comma-separated paths. "stdout" means the console stdout
	HTTPPort         int    // 0 menas a randomly OS chosen port
	PushGatewayAddrs string
	PushInterval     int
	LocalCfgFile     string
	NacosAddr        string
	NacosNamespaceID string
	NacosGroup       string
	NacosUsername    string
	NacosPassword    string
	NacosDataID      string
	NacosServiceName string // participate in assignment management if not empty
}

var (
	//goreleaser fill following info per https://goreleaser.com/customization/build/.
	version = "None"
	commit  = "None"
	date    = "None"
	builtBy = "None"

	cmdOps      CmdOptions
	selfIP      string
	httpAddr    string
	httpMetrics = promhttp.Handler()
	runner      *Sinker
)

func initCmdOptions() {
	// 1. Set options to default value.
	cmdOps = CmdOptions{
		ShowVer:          false,
		LogLevel:         "info",
		LogPaths:         "stdout,clickhouse_sinker.log",
		HTTPPort:         0,
		PushGatewayAddrs: "",
		PushInterval:     10,
		LocalCfgFile:     "/etc/clickhouse_sinker.json",
		NacosAddr:        "127.0.0.1:8848",
		NacosNamespaceID: "",
		NacosGroup:       "DEFAULT_GROUP",
		NacosUsername:    "nacos",
		NacosPassword:    "nacos",
		NacosDataID:      "",
		NacosServiceName: "",
	}

	// 2. Replace options with the corresponding env variable if present.
	util.EnvBoolVar(&cmdOps.ShowVer, "v")
	util.EnvStringVar(&cmdOps.LogLevel, "log-level")
	util.EnvStringVar(&cmdOps.LogPaths, "log-paths")
	util.EnvIntVar(&cmdOps.HTTPPort, "http-port")
	util.EnvStringVar(&cmdOps.PushGatewayAddrs, "metric-push-gateway-addrs")
	util.EnvIntVar(&cmdOps.PushInterval, "push-interval")

	util.EnvStringVar(&cmdOps.NacosAddr, "nacos-addr")
	util.EnvStringVar(&cmdOps.NacosUsername, "nacos-username")
	util.EnvStringVar(&cmdOps.NacosPassword, "nacos-password")
	util.EnvStringVar(&cmdOps.NacosNamespaceID, "nacos-namespace-id")
	util.EnvStringVar(&cmdOps.NacosGroup, "nacos-group")
	util.EnvStringVar(&cmdOps.NacosDataID, "nacos-dataid")
	util.EnvStringVar(&cmdOps.NacosServiceName, "nacos-service-name")

	// 3. Replace options with the corresponding CLI parameter if present.
	flag.BoolVar(&cmdOps.ShowVer, "v", cmdOps.ShowVer, "show build version and quit")
	flag.StringVar(&cmdOps.LogLevel, "log-level", cmdOps.LogLevel, "one of debug, info, warn, error, dpanic, panic, fatal")
	flag.StringVar(&cmdOps.LogPaths, "log-paths", cmdOps.LogPaths, "a list of comma-separated log file path. stdout means the console stdout")
	flag.IntVar(&cmdOps.HTTPPort, "http-port", cmdOps.HTTPPort, "http listen port")
	flag.StringVar(&cmdOps.PushGatewayAddrs, "metric-push-gateway-addrs", cmdOps.PushGatewayAddrs, "a list of comma-separated prometheus push gatway address")
	flag.IntVar(&cmdOps.PushInterval, "push-interval", cmdOps.PushInterval, "push interval in seconds")
	flag.StringVar(&cmdOps.LocalCfgFile, "local-cfg-file", cmdOps.LocalCfgFile, "local config file")

	flag.StringVar(&cmdOps.NacosAddr, "nacos-addr", cmdOps.NacosAddr, "a list of comma-separated nacos server addresses")
	flag.StringVar(&cmdOps.NacosUsername, "nacos-username", cmdOps.NacosUsername, "nacos username")
	flag.StringVar(&cmdOps.NacosPassword, "nacos-password", cmdOps.NacosPassword, "nacos password")
	flag.StringVar(&cmdOps.NacosNamespaceID, "nacos-namespace-id", cmdOps.NacosNamespaceID,
		`nacos namespace ID. Neither DEFAULT_NAMESPACE_ID("public") nor namespace name work!`)
	flag.StringVar(&cmdOps.NacosGroup, "nacos-group", cmdOps.NacosGroup, `nacos group name. Empty string doesn't work!`)
	flag.StringVar(&cmdOps.NacosDataID, "nacos-dataid", cmdOps.NacosDataID, "nacos dataid")
	flag.StringVar(&cmdOps.NacosServiceName, "nacos-service-name", cmdOps.NacosServiceName, "nacos service name")
	flag.Parse()
}

func getVersion() string {
	return fmt.Sprintf("version %s, commit %s, date %s, builtBy %s", version, commit, date, builtBy)
}

func init() {
	initCmdOptions()
	logPaths := strings.Split(cmdOps.LogPaths, ",")
	util.InitLogger(logPaths)
	util.SetLogLevel(cmdOps.LogLevel)
	util.Logger.Info(getVersion())
	if cmdOps.ShowVer {
		os.Exit(0)
	}
	var err error
	var ip net.IP
	if ip, err = util.GetOutboundIP(); err != nil {
		log.Fatal("unable to determine self ip", err)
	}
	selfIP = ip.String()
}

func main() {
	util.Run("clickhouse_sinker", func() error {
		// Initialize http server for metrics and debug
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
					<p><a href="/debug/pprof/">pprof</a></p>
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

		// cmdOps.HTTPPort=0: let OS choose the listen port, and record the exact metrics URL to log.
		httpPort := cmdOps.HTTPPort
		if httpPort != 0 {
			httpPort = util.GetSpareTCPPort(httpPort)
		}
		httpAddr = fmt.Sprintf(":%d", httpPort)
		listener, err := net.Listen("tcp", httpAddr)
		if err != nil {
			util.Logger.Fatal("net.Listen failed", zap.String("httpAddr", httpAddr), zap.Error(err))
		}
		httpPort = util.GetNetAddrPort(listener.Addr())
		httpAddr = fmt.Sprintf("%s:%d", selfIP, httpPort)
		util.Logger.Info(fmt.Sprintf("Run http server at http://%s/", httpAddr))

		go func() {
			if err := http.Serve(listener, mux); err != nil {
				util.Logger.Error("http.ListenAndServe failed", zap.Error(err))
			}
		}()

		var rcm cm.RemoteConfManager
		var properties map[string]interface{}
		if cmdOps.NacosDataID != "" {
			util.Logger.Info(fmt.Sprintf("get config from nacos serverAddrs %s, namespaceId %s, group %s, dataId %s",
				cmdOps.NacosAddr, cmdOps.NacosNamespaceID, cmdOps.NacosGroup, cmdOps.NacosDataID))
			rcm = &cm.NacosConfManager{}
			properties = make(map[string]interface{})
			properties["serverAddrs"] = cmdOps.NacosAddr
			properties["username"] = cmdOps.NacosUsername
			properties["password"] = cmdOps.NacosPassword
			properties["namespaceId"] = cmdOps.NacosNamespaceID
			properties["group"] = cmdOps.NacosGroup
			properties["dataId"] = cmdOps.NacosDataID
			properties["serviceName"] = cmdOps.NacosServiceName
		} else {
			util.Logger.Info(fmt.Sprintf("get config from local file %s", cmdOps.LocalCfgFile))
		}
		if rcm != nil {
			if err := rcm.Init(properties); err != nil {
				util.Logger.Fatal("rcm.Init failed", zap.Error(err))
			}
			if cmdOps.NacosServiceName != "" {
				if err := rcm.Register(selfIP, httpPort); err != nil {
					util.Logger.Fatal("rcm.Init failed", zap.Error(err))
				}
			}
		}
		runner = NewSinker(rcm)
		return runner.Init()
	}, func() error {
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
	numCfg int
	pusher *statistics.Pusher
	tasks  map[string]*task.Service
	rcm    cm.RemoteConfManager
	ctx    context.Context
	cancel context.CancelFunc
}

// NewSinker get an instance of sinker with the task list
func NewSinker(rcm cm.RemoteConfManager) *Sinker {
	ctx, cancel := context.WithCancel(context.Background())
	s := &Sinker{tasks: make(map[string]*task.Service), rcm: rcm, ctx: ctx, cancel: cancel}
	return s
}

func (s *Sinker) Init() (err error) {
	return
}

// Run rull task in different go routines
func (s *Sinker) Run() {
	var err error
	var newCfg *config.Config
	if cmdOps.PushGatewayAddrs != "" {
		addrs := strings.Split(cmdOps.PushGatewayAddrs, ",")
		s.pusher = statistics.NewPusher(addrs, cmdOps.PushInterval, httpAddr)
		if err = s.pusher.Init(); err != nil {
			return
		}
		go s.pusher.Run(s.ctx)
	}
	if s.rcm == nil {
		if _, err = os.Stat(cmdOps.LocalCfgFile); err == nil {
			if newCfg, err = config.ParseLocalCfgFile(cmdOps.LocalCfgFile); err != nil {
				util.Logger.Fatal("config.ParseLocalCfgFile failed", zap.Error(err))
				return
			}
		} else {
			util.Logger.Fatal("expect --local-cfg-file or --nacos-dataid")
			return
		}
		if err = newCfg.Normallize(); err != nil {
			util.Logger.Fatal("newCfg.Normallize failed", zap.Error(err))
			return
		}
		if err = s.applyConfig(newCfg); err != nil {
			util.Logger.Fatal("s.applyConfig failed", zap.Error(err))
			return
		}
		<-s.ctx.Done()
	} else {
		if cmdOps.NacosServiceName != "" {
			go s.rcm.Run(s.ctx)
		}
		for {
			select {
			case <-s.ctx.Done():
				return
			case <-time.After(10 * time.Second):
				if newCfg, err = s.rcm.GetConfig(); err != nil {
					util.Logger.Error("s.rcm.GetConfig failed", zap.Error(err))
					continue
				}
				if err = newCfg.Normallize(); err != nil {
					util.Logger.Error("newCfg.Normallize failed", zap.Error(err))
					continue
				}
				if err = s.applyConfig(newCfg); err != nil {
					util.Logger.Error("s.applyConfig failed", zap.Error(err))
					continue
				}
			}
		}
	}
}

// Close shutdown task
func (s *Sinker) Close() {
	// 1. Stop tasks gracefully.
	s.stopAllTasks()
	// 2. Stop rcm
	if s.rcm != nil {
		s.rcm.Stop()
	}
	// 3. Stop pusher
	if s.pusher != nil {
		s.pusher.Stop()
		s.pusher = nil
	}
}

func (s *Sinker) stopAllTasks() {
	var wg sync.WaitGroup
	for _, tsk := range s.tasks {
		wg.Add(1)
		go func(tsk *task.Service) {
			tsk.Stop()
			wg.Done()
		}(tsk)
	}
	wg.Wait()
	for taskName := range s.tasks {
		delete(s.tasks, taskName)
	}
	util.Logger.Info("stopping parsing pool")
	if util.GlobalParsingPool != nil {
		util.GlobalParsingPool.StopWait()
	}
	util.Logger.Info("stopping timer wheel")
	if util.GlobalTimerWheel != nil {
		util.GlobalTimerWheel.Stop()
	}
	util.Logger.Info("stopping writing pool")
	if util.GlobalWritingPool != nil {
		util.GlobalWritingPool.StopWait()
	}
}

func (s *Sinker) applyConfig(newCfg *config.Config) (err error) {
	util.SetLogLevel(newCfg.LogLevel)
	if s.curCfg == nil {
		// The first time invoking of applyConfig
		err = s.applyFirstConfig(newCfg)
	} else if !reflect.DeepEqual(newCfg.Clickhouse, s.curCfg.Clickhouse) ||
		!reflect.DeepEqual(newCfg.Kafka, s.curCfg.Kafka) ||
		!reflect.DeepEqual(newCfg.Tasks, s.curCfg.Tasks) ||
		!reflect.DeepEqual(newCfg.Assignment.Map, s.curCfg.Assignment.Map) {
		err = s.applyAnotherConfig(newCfg)
	}
	return
}

func (s *Sinker) applyFirstConfig(newCfg *config.Config) (err error) {
	util.Logger.Info("going to apply the first config", zap.Reflect("config", newCfg))

	// 1. Initialize clickhouse connections
	chCfg := &newCfg.Clickhouse
	if err = pool.InitClusterConn(chCfg.Hosts, chCfg.Port, chCfg.DB, chCfg.Username, chCfg.Password,
		chCfg.DsnParams, chCfg.Secure, chCfg.InsecureSkipVerify, chCfg.MaxOpenConns); err != nil {
		return
	}

	// 2. Start goroutine pools.
	util.InitGlobalTimerWheel()
	util.InitGlobalParsingPool()
	util.InitGlobalWritingPool(len(chCfg.Hosts) * chCfg.MaxOpenConns)

	// 3. Generate, initialize and run task
	for _, taskCfg := range newCfg.Tasks {
		if cmdOps.NacosServiceName != "" && !newCfg.IsAssigned(httpAddr, taskCfg.Name) {
			continue
		}
		task := task.NewTaskService(newCfg, taskCfg)
		if err = task.Init(); err != nil {
			return
		}
		s.tasks[taskCfg.Name] = task
	}
	for _, task := range s.tasks {
		go task.Run(s.ctx)
	}
	s.curCfg = newCfg
	return
}

func (s *Sinker) applyAnotherConfig(newCfg *config.Config) (err error) {
	util.Logger.Info("going to apply another config", zap.Int("number", s.numCfg), zap.Reflect("config", newCfg))
	s.numCfg++

	if !reflect.DeepEqual(newCfg.Kafka, s.curCfg.Kafka) || !reflect.DeepEqual(newCfg.Clickhouse, s.curCfg.Clickhouse) {
		// 1. Stop tasks gracefully. Wait until all flying data be processed (write to CH and commit to Kafka).
		s.stopAllTasks()
		// 2. Initialize clickhouse connections.
		chCfg := &newCfg.Clickhouse
		if err = pool.InitClusterConn(chCfg.Hosts, chCfg.Port, chCfg.DB, chCfg.Username, chCfg.Password,
			chCfg.DsnParams, chCfg.Secure, chCfg.InsecureSkipVerify, chCfg.MaxOpenConns); err != nil {
			return
		}

		// 3. Restart goroutine pools.
		util.Logger.Info("restarting parsing, writing and timer pool")
		util.GlobalTimerWheel = nil
		util.InitGlobalTimerWheel()
		util.GlobalParsingPool.Restart()
		maxWorkers := len(newCfg.Clickhouse.Hosts) * newCfg.Clickhouse.MaxOpenConns
		util.GlobalWritingPool.Resize(maxWorkers)
		util.GlobalWritingPool.Restart()
		util.Logger.Info("resized writing pool", zap.Int("maxWorkers", maxWorkers))

		// 4. Generate, initialize and run tasks.
		for _, taskCfg := range newCfg.Tasks {
			if cmdOps.NacosServiceName != "" && !newCfg.IsAssigned(httpAddr, taskCfg.Name) {
				continue
			}
			task := task.NewTaskService(newCfg, taskCfg)
			if err = task.Init(); err != nil {
				return
			}
			s.tasks[taskCfg.Name] = task
		}
		for _, task := range s.tasks {
			go task.Run(s.ctx)
		}
	} else if !reflect.DeepEqual(newCfg.Tasks, s.curCfg.Tasks) || !reflect.DeepEqual(newCfg.Assignment.Map, s.curCfg.Assignment.Map) {
		//1. Find tasks need to stop.
		var tasksToStop []string
		curCfgTasks := make(map[string]*config.TaskConfig)
		newCfgTasks := make(map[string]*config.TaskConfig)
		for _, taskCfg := range s.curCfg.Tasks {
			curCfgTasks[taskCfg.Name] = taskCfg
		}
		for _, taskCfg := range newCfg.Tasks {
			if cmdOps.NacosServiceName != "" && !newCfg.IsAssigned(httpAddr, taskCfg.Name) {
				continue
			}
			newCfgTasks[taskCfg.Name] = taskCfg
		}
		for taskName := range s.tasks {
			curTaskCfg := curCfgTasks[taskName]
			newTaskCfg, ok := newCfgTasks[taskName]
			if !ok || !reflect.DeepEqual(newTaskCfg, curTaskCfg) {
				tasksToStop = append(tasksToStop, taskName)
			}
		}
		// 2. Stop tasks in parallel found at the previous step.
		// They must drain flying batchs as quickly as possible to allow another clickhouse_sinker
		// instance take over partitions safely.
		var wg sync.WaitGroup
		for _, taskName := range tasksToStop {
			wg.Add(1)
			go func(tsk *task.Service) {
				tsk.Stop()
				wg.Done()
			}(s.tasks[taskName])
		}
		wg.Wait()
		for _, taskName := range tasksToStop {
			delete(s.tasks, taskName)
		}
		// 3. Initailize tasks which are new or their config differ.
		var newTasks []*task.Service
		for taskName, taskCfg := range newCfgTasks {
			if _, ok := s.tasks[taskName]; ok {
				continue
			}
			task := task.NewTaskService(newCfg, taskCfg)
			if err = task.Init(); err != nil {
				return
			}
			s.tasks[taskName] = task
			newTasks = append(newTasks, task)
		}

		// 4. Start new tasks. We don't do it at step 3 in order to avoid goroutine leak due to errors raised by later steps.
		for _, task := range newTasks {
			go task.Run(s.ctx)
		}
	}
	// Record the new config
	s.curCfg = newCfg
	return
}
