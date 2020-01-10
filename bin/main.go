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

	_ "github.com/kshvakov/clickhouse"
	"github.com/sundy-li/go_commons/app"

	"github.com/housepower/clickhouse_sinker/creator"
	"github.com/housepower/clickhouse_sinker/statistics"
	"github.com/housepower/clickhouse_sinker/task"
)

var (
	config string
)

func init() {
	flag.StringVar(&config, "conf", "", "config dir")

	flag.Parse()
}

func main() {

	var cfg creator.Config
	var runner *Sinker

	app.Run("clickhouse_sinker", func() error {
		cfg = *creator.InitConfig(config)
		runner = NewSinker(cfg)
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
	tasks   []*task.TaskService
	pusher  *statistics.Pusher
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
		s.pusher.Run()
	}
	for i := range s.tasks {
		go s.tasks[i].Run()
	}
	<-s.stopped
}

// Close shoutdown tasks
func (s *Sinker) Close() {
	for i := range s.tasks {
		s.tasks[i].Stop()
	}

	if s.pusher != nil {
		s.pusher.Stop()
	}
	close(s.stopped)
}
