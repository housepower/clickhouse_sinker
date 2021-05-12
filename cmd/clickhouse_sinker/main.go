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
	"runtime"
	"strings"
	"time"

	"github.com/housepower/clickhouse_sinker/config"
	"github.com/housepower/clickhouse_sinker/health"
	"github.com/housepower/clickhouse_sinker/input"
	"github.com/housepower/clickhouse_sinker/output"
	"github.com/housepower/clickhouse_sinker/parser"
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
	HTTPPort         int
	PushGatewayAddrs string
	PushInterval     int
	LocalCfgFile     string
	NacosAddr        string
	NacosNamespaceID string
	NacosGroup       string
	NacosUsername    string
	NacosPassword    string
	NacosDataID      string
}

var (
	//goreleaser fill following info per https://goreleaser.com/customization/build/.
	version = "None"
	commit  = "None"
	date    = "None"
	builtBy = "None"

	cmdOps      CmdOptions
	selfIP      string
	httpMetrics = promhttp.Handler()
	runner      *Sinker
)

func initCmdOptions() {
	// 1. Set options to default value.
	cmdOps = CmdOptions{
		ShowVer:          false,
		HTTPPort:         2112,
		PushGatewayAddrs: "",
		PushInterval:     10,
		LocalCfgFile:     "/etc/clickhouse_sinker.json",
		NacosAddr:        "127.0.0.1:8848",
		NacosNamespaceID: "",
		NacosGroup:       "DEFAULT_GROUP",
		NacosUsername:    "nacos",
		NacosPassword:    "nacos",
		NacosDataID:      "",
	}

	// 2. Replace options with the corresponding env variable if present.
	util.EnvBoolVar(&cmdOps.ShowVer, "v")
	util.EnvIntVar(&cmdOps.HTTPPort, "http-port")
	util.EnvStringVar(&cmdOps.PushGatewayAddrs, "metric-push-gateway-addrs")
	util.EnvIntVar(&cmdOps.PushInterval, "push-interval")

	util.EnvStringVar(&cmdOps.NacosAddr, "nacos-addr")
	util.EnvStringVar(&cmdOps.NacosUsername, "nacos-username")
	util.EnvStringVar(&cmdOps.NacosPassword, "nacos-password")
	util.EnvStringVar(&cmdOps.NacosNamespaceID, "nacos-namespace-id")
	util.EnvStringVar(&cmdOps.NacosGroup, "nacos-group")
	util.EnvStringVar(&cmdOps.NacosDataID, "nacos-dataid")

	// 3. Replace options with the corresponding CLI parameter if present.
	flag.BoolVar(&cmdOps.ShowVer, "v", cmdOps.ShowVer, "show build version and quit")
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
	flag.Parse()
}

func getVersion() string {
	return fmt.Sprintf("version %s, commit %s, date %s, builtBy %s", version, commit, date, builtBy)
}

func init() {
	util.InitLogger("info", []string{"stdout"})
	initCmdOptions()
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
	cmdOps.HTTPPort = util.GetSpareTCPPort(cmdOps.HTTPPort)
}

// GenTask generate a task via config
func GenTask(cfg *config.Config) (taskImpl *task.Service) {
	taskCfg := &cfg.Task
	ck := output.NewClickHouse(cfg)
	pp, _ := parser.NewParserPool(taskCfg.Parser, taskCfg.CsvFormat, taskCfg.Delimiter, taskCfg.TimeZone)
	inputer := input.NewInputer(taskCfg.KafkaClient)
	taskImpl = task.NewTaskService(inputer, ck, pp, cfg)
	return
}

func main() {
	util.Run("clickhouse_sinker", func() error {
		var rcm config.RemoteConfManager
		var properties map[string]interface{}
		if cmdOps.NacosDataID != "" {
			util.Logger.Info(fmt.Sprintf("get config from nacos serverAddrs %s, namespaceId %s, group %s, dataId %s",
				cmdOps.NacosAddr, cmdOps.NacosNamespaceID, cmdOps.NacosGroup, cmdOps.NacosDataID))
			rcm = &config.NacosConfManager{}
			properties = make(map[string]interface{})
			properties["serverAddrs"] = cmdOps.NacosAddr
			properties["username"] = cmdOps.NacosUsername
			properties["password"] = cmdOps.NacosPassword
			properties["namespaceId"] = cmdOps.NacosNamespaceID
			properties["group"] = cmdOps.NacosGroup
			properties["dataId"] = cmdOps.NacosDataID
		} else {
			util.Logger.Info(fmt.Sprintf("get config from local file %s", cmdOps.LocalCfgFile))
		}
		if rcm != nil {
			if err := rcm.Init(properties); err != nil {
				util.Logger.Fatal("rcm.Init failed", zap.Error(err))
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

			util.Logger.Info(fmt.Sprintf("Run http server http://%s:%d", selfIP, cmdOps.HTTPPort))
			if err := http.ListenAndServe(fmt.Sprintf(":%d", cmdOps.HTTPPort), mux); err != nil {
				util.Logger.Error("http.ListenAndServe failed", zap.Error(err))
			}
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
	task   *task.Service
	rcm    config.RemoteConfManager
	ctx    context.Context
	cancel context.CancelFunc
}

// NewSinker get an instance of sinker with the task list
func NewSinker(rcm config.RemoteConfManager) *Sinker {
	ctx, cancel := context.WithCancel(context.Background())
	s := &Sinker{rcm: rcm, ctx: ctx, cancel: cancel}
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
		s.pusher = statistics.NewPusher(addrs, cmdOps.PushInterval)
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
			util.Logger.Fatal("expect --local-cfg-file or --local-cfg-dir")
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
		for {
			select {
			case <-s.ctx.Done():
				return
			case <-time.After(5 * time.Second):
				if newCfg, err = s.rcm.GetConfig(); err != nil {
					util.Logger.Error("s.rcm.GetConfig failed", zap.Error(err))
					return
				}
				if err = newCfg.Normallize(); err != nil {
					util.Logger.Error("newCfg.Normallize failed", zap.Error(err))
					return
				}
				if err = s.applyConfig(newCfg); err != nil {
					util.Logger.Error("s.applyConfig failed", zap.Error(err))
					return
				}
			}
		}
	}
}

// Close shutdown task
func (s *Sinker) Close() {
	// Stop task gracefully. Wait until all flying data be processed (write to CH and commit to Kafka).
	util.Logger.Info("stopping parsing pool", zap.String("task", s.curCfg.Task.Name))
	util.GlobalParsingPool.StopWait()
	util.Logger.Info("stopping writing pool", zap.String("task", s.curCfg.Task.Name))
	util.GlobalWritingPool.StopWait()
	util.Logger.Info("stopping timer wheel", zap.String("task", s.curCfg.Task.Name))
	util.GlobalTimerWheel.Stop()

	s.task.Stop()
	if s.pusher != nil {
		s.pusher.Stop()
	}
	s.cancel()
}

func (s *Sinker) applyConfig(newCfg *config.Config) (err error) {
	if s.curCfg == nil || newCfg.LogLevel != s.curCfg.LogLevel || !reflect.DeepEqual(newCfg.LogPaths, s.curCfg.LogPaths) {
		util.InitLogger(newCfg.LogLevel, newCfg.LogPaths)
	}
	if s.curCfg == nil {
		// The first time invoking of applyConfig
		err = s.applyFirstConfig(newCfg)
	} else if !reflect.DeepEqual(newCfg.Clickhouse, s.curCfg.Clickhouse) || !reflect.DeepEqual(newCfg.Kafka, s.curCfg.Kafka) || !reflect.DeepEqual(newCfg.Task, s.curCfg.Task) {
		err = s.applyAnotherConfig(newCfg)
	}
	return
}

func (s *Sinker) applyFirstConfig(newCfg *config.Config) (err error) {
	util.Logger.Info("going to apply the first config", zap.Reflect("config", newCfg))
	util.InitGlobalTimerWheel()
	t := GenTask(newCfg)
	if err = t.Init(); err != nil {
		return
	}
	s.task = t
	concurrentParsers := 10
	if runtime.NumCPU() >= 2 {
		if concurrentParsers > runtime.NumCPU()/2 {
			concurrentParsers = runtime.NumCPU() / 2
		}
	} else {
		concurrentParsers = 1
	}
	util.InitGlobalParsingPool(concurrentParsers)
	totalConn := pool.NumShard()
	util.InitGlobalWritingPool(totalConn)

	go s.task.Run(s.ctx)
	s.curCfg = newCfg
	return
}

func (s *Sinker) applyAnotherConfig(newCfg *config.Config) (err error) {
	util.Logger.Info("going to apply another config", zap.Reflect("config", newCfg))

	if !reflect.DeepEqual(newCfg.Kafka, s.curCfg.Kafka) || !reflect.DeepEqual(newCfg.Clickhouse, s.curCfg.Clickhouse) || !reflect.DeepEqual(newCfg.Task, s.curCfg.Task) {
		// 1. Stop task gracefully. Wait until all flying data be processed (write to CH and commit to Kafka).
		util.Logger.Info("stopping parsing pool", zap.String("task", s.curCfg.Task.Name))
		util.GlobalParsingPool.StopWait()
		util.Logger.Info("stopping writing pool", zap.String("task", s.curCfg.Task.Name))
		util.GlobalWritingPool.StopWait()
		util.Logger.Info("stopping timer wheel", zap.String("task", s.curCfg.Task.Name))
		util.GlobalTimerWheel.Stop()
		s.task.Stop()

		// 2. Generate and initialize task
		t := GenTask(newCfg)
		if err = t.Init(); err != nil {
			return
		}

		// 3. Restart goroutine pools.
		util.InitGlobalTimerWheel()
		util.GlobalParsingPool.Restart()
		totalConn := pool.NumShard()
		util.GlobalWritingPool.Resize(totalConn)
		util.GlobalWritingPool.Restart()

		// 4. Start task
		go t.Run(s.ctx)
		s.task = t
	}
	// Record the new config
	s.curCfg = newCfg
	return
}
