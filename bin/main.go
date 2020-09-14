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
	"flag"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"net/http/pprof"

	"github.com/google/uuid"
	"github.com/hashicorp/consul/api"
	"github.com/housepower/clickhouse_sinker/creator"
	"github.com/housepower/clickhouse_sinker/health"
	"github.com/housepower/clickhouse_sinker/prom"
	"github.com/housepower/clickhouse_sinker/statistics"
	"github.com/housepower/clickhouse_sinker/task"
	_ "github.com/kshvakov/clickhouse"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sundy-li/go_commons/app"
	"github.com/sundy-li/go_commons/log"
)

var (
	config     = flag.String("conf", "", "config dir")
	httpAddr   = flag.String("http-addr", "0.0.0.0:2112", "http interface")
	consulAddr = flag.String("consul-addr", "http://127.0.0.1:8500", "consul api interface address")

	httpMetrcs = promhttp.Handler()
	cfg        creator.Config
	runner     *Sinker
	ip         string
	port       int
	appID, _   = uuid.NewUUID()
	appIDStr   = fmt.Sprintf("clickhouse_sinker-%s", appID.String())
)

func parseAddr(addr string) (string, int) {
	ip, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		panic(err)
	}

	port, err := strconv.Atoi(portStr)
	if err != nil {
		panic(err)
	}

	if ip == "0.0.0.0" {
		ip = ""
	}

	return ip, port
}

func serviceRegister(agent *api.Agent) {
	log.Debug("Consul: register service")
	err := agent.ServiceRegister(&api.AgentServiceRegistration{
		Name:    "clickhouse_sinker",
		ID:      appIDStr,
		Port:    port,
		Address: ip,
		Check: &api.AgentServiceCheck{
			CheckID:  appIDStr + "-http-heath",
			Name:     "/ready",
			Interval: "15s",
			Timeout:  "15s",
			HTTP:     fmt.Sprintf("http://%s/ready?full=1", *httpAddr),
		},
	})
	if err != nil {
		log.Warnf("Consul: %s", err)
	}
}
func init() {
	flag.Parse()
	ip, port = parseAddr(*httpAddr)
}

func main() {
	consulConfig := api.DefaultConfig()
	consulConfig.Address = *consulAddr
	consulClient, _ := api.NewClient(consulConfig)
	consulAgent := consulClient.Agent()

	prometheus.MustRegister(prometheus.NewBuildInfoCollector())
	prometheus.MustRegister(prom.ClickhouseReconnectTotal)
	prometheus.MustRegister(prom.ClickhouseEventsSuccess)
	prometheus.MustRegister(prom.ClickhouseEventsErrors)
	prometheus.MustRegister(prom.ClickhouseEventsTotal)
	prometheus.MustRegister(prom.KafkaConsumerErrors)

	serviceRegister(consulAgent)
	defer func() {
		log.Debug("Consul: de-register service")
		err := consulAgent.ServiceDeregister(appIDStr)
		if err != nil {
			log.Warnf("Consul: %s", err)
		}
	}()

	app.Run("clickhouse_sinker", func() error {
		cfg = *creator.InitConfig(*config)
		runner = NewSinker(cfg)
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

			mux.Handle("/metrics", httpMetrcs)
			mux.HandleFunc("/ready", health.Health.ReadyEndpoint) // GET /ready?full=1
			mux.HandleFunc("/live", health.Health.LiveEndpoint)   // GET /live?full=1

			mux.HandleFunc("/debug/pprof/", pprof.Index)
			mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
			mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
			mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
			mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

			log.Infof("Run http server http://%s", *httpAddr)
			log.Error(http.ListenAndServe(*httpAddr, mux))
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
	pusher  *statistics.Pusher
	tasks   []*task.Service
	config  creator.Config
	stopped chan struct{}
}

// NewSinker get an instance of sinker with the task list
func NewSinker(config creator.Config) *Sinker {
	s := &Sinker{config: config, stopped: make(chan struct{})}
	return s
}

// Init initializes the list of tasks
func (s *Sinker) Init() error {
	if s.config.Statistics.Enable {
		s.pusher = statistics.NewPusher(s.config.Statistics.PushGateWayAddrs,
			s.config.Statistics.PushInterval)
		err := s.pusher.Init()
		if err != nil {
			return err
		}
	}
	s.tasks = s.config.GenTasks()
	for _, t := range s.tasks {
		if err := t.Init(); err != nil {
			return err
		}
	}
	return nil
}

// Run rull all tasks in different go routines
func (s *Sinker) Run() {
	if s.pusher != nil {
		go s.pusher.Run()
	}
	for i := range s.tasks {
		go s.tasks[i].Run()
	}
	<-s.stopped
}

// Close shutdown tasks
func (s *Sinker) Close() {
	for i := range s.tasks {
		s.tasks[i].Stop()
	}

	if s.pusher != nil {
		s.pusher.Stop()
	}
	close(s.stopped)
}
