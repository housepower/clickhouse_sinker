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

package task

import (
	"context"
	"fmt"
	"runtime"
	"strconv"
	"strings"

	"github.com/housepower/clickhouse_sinker/config"
	"github.com/housepower/clickhouse_sinker/input"
	"github.com/housepower/clickhouse_sinker/output"

	"github.com/sundy-li/go_commons/log"
)

// TaskService holds the configuration for each task
type Service struct {
	started    bool
	stopped    chan struct{}
	kafka      *input.Kafka
	clickhouse *output.ClickHouse
	taskCfg    *config.TaskConfig
}

// NewTaskService creates an instance of new tasks with kafka, clickhouse and paser instances
func NewTaskService(kafka *input.Kafka, clickhouse *output.ClickHouse, taskCfg *config.TaskConfig) *Service {
	return &Service{
		stopped:    make(chan struct{}),
		kafka:      kafka,
		clickhouse: clickhouse,
		started:    false,
		taskCfg:    taskCfg,
	}
}

// Init initializes the kafak and clickhouse task associated with this service

func (service *Service) Init() (err error) {
	if err = service.clickhouse.Init(); err != nil {
		return
	}
	err = service.kafka.Init(service.clickhouse.Dims)
	return
}

// Run starts the task
func (service *Service) Run(ctx context.Context) {
	service.started = true
	log.Infof("task %s has started", service.taskCfg.Name)
	go service.kafka.Run(ctx)
LOOP:
	for {
		select {
		case <-ctx.Done():
			break LOOP
		case batch := <-service.kafka.BatchCh():
			service.flush(batch)
		}
	}
	service.stopped <- struct{}{}
}

func (service *Service) flush(batch input.Batch) {
	log.Infof("%s: buf size:%d", service.taskCfg.Name, len(batch.MsgRows))
	service.clickhouse.Send(batch)
}

// Stop stop kafka and clickhouse client
func (service *Service) Stop() {
	log.Infof("%s: stopping task...", service.taskCfg.Name)
	if err := service.kafka.Stop(); err != nil {
		panic(err)
	}
	log.Infof("%s: stopped kafka", service.taskCfg.Name)

	_ = service.clickhouse.Close()
	log.Infof("%s: closed clickhouse", service.taskCfg.Name)

	if service.started {
		<-service.stopped
	}
	log.Infof("%s: got notify from service.stopped", service.taskCfg.Name)
}

// GoID returns go routine id 获取goroutine的id
func GoID() int {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	idField := strings.Fields(strings.TrimPrefix(string(buf[:n]), "goroutine "))[0]
	id, err := strconv.Atoi(idField)
	if err != nil {
		panic(fmt.Sprintf("cannot get goroutine id: %v", err))
	}
	return id
}
