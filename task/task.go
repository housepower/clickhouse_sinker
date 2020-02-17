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
	"fmt"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/housepower/clickhouse_sinker/input"
	"github.com/housepower/clickhouse_sinker/model"
	"github.com/housepower/clickhouse_sinker/output"
	"github.com/housepower/clickhouse_sinker/parser"

	"github.com/sundy-li/go_commons/log"
)

// TaskService holds the configuration for each task
type Service struct {
	stopped    chan struct{}
	kafka      *input.Kafka
	clickhouse *output.ClickHouse
	p          parser.Parser

	FlushInterval int
	BufferSize    int
	MinBufferSize int
}

// NewTaskService creates an instance of new tasks with kafka, clickhouse and paser instances
func NewTaskService(kafka *input.Kafka, clickhouse *output.ClickHouse, p parser.Parser) *Service {
	return &Service{
		stopped:    make(chan struct{}),
		kafka:      kafka,
		clickhouse: clickhouse,
		p:          p,
	}
}

// Init initializes the kafak and clickhouse task associated with this service

func (service *Service) Init() error {
	err := service.kafka.Init()
	if err != nil {
		return err
	}
	return service.clickhouse.Init()
}

// Run starts the task
func (service *Service) Run() {
	if err := service.kafka.Start(); err != nil {
		panic(err)
	}

	log.Infof("TaskService %s TaskService has started", service.clickhouse.GetName())
	tick := time.NewTicker(time.Duration(service.FlushInterval) * time.Second)
	msgs := make([]model.Metric, 0, service.BufferSize)
FOR:
	for {
		select {
		case msg, more := <-service.kafka.Msgs():
			if !more {
				break FOR
			}
			msgs = append(msgs, service.parse(msg))
			if len(msgs) >= service.BufferSize {
				service.flush(msgs)
				msgs = msgs[:0]
				tick = time.NewTicker(time.Duration(service.FlushInterval) * time.Second)
			}
		case <-tick.C:
			log.Infof("%s: tick", service.clickhouse.GetName())
			if len(msgs) == 0 || len(msgs) < service.MinBufferSize {
				continue
			}
			service.flush(msgs)
			msgs = msgs[:0]
		}
	}
	service.flush(msgs)
	service.stopped <- struct{}{}
}

func (service *Service) parse(data []byte) model.Metric {
	return service.p.Parse(data)
}
func (service *Service) flush(metrics []model.Metric) {
	log.Infof("%s: buf size:%d", service.clickhouse.GetName(), len(metrics))
	service.clickhouse.LoopWrite(metrics)
}

// Stop stop kafak and clickhouse client
func (service *Service) Stop() {
	log.Infof("%s: close TaskService", service.clickhouse.GetName())
	if err := service.kafka.Stop(); err != nil {
		panic(err)
	}
	<-service.stopped
	_ = service.clickhouse.Close()
	log.Infof("%s: closed TaskService", service.clickhouse.GetName())
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
