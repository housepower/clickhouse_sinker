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
	"sync"
	"time"

	"github.com/housepower/clickhouse_sinker/config"
	"github.com/housepower/clickhouse_sinker/input"
	"github.com/housepower/clickhouse_sinker/model"
	"github.com/housepower/clickhouse_sinker/output"
	"github.com/housepower/clickhouse_sinker/parser"
	"github.com/housepower/clickhouse_sinker/statistics"
	"github.com/housepower/clickhouse_sinker/util"
	"github.com/pkg/errors"
	"golang.org/x/time/rate"

	"github.com/sundy-li/go_commons/log"
)

// TaskService holds the configuration for each task
type Service struct {
	sync.Mutex

	ctx        context.Context
	started    bool
	stopped    chan struct{}
	inputer    input.Inputer
	clickhouse *output.ClickHouse
	taskCfg    *config.TaskConfig
	pp         *parser.Pool
	dims       []*model.ColumnWithType

	rings     []*Ring
	batchChan chan *model.Batch
	limiter1  *rate.Limiter
	limiter2  *rate.Limiter
	limiter3  *rate.Limiter
}

// NewTaskService creates an instance of new tasks with kafka, clickhouse and paser instances
func NewTaskService(inputer input.Inputer, clickhouse *output.ClickHouse, taskCfg *config.TaskConfig, pp *parser.Pool) *Service {
	return &Service{
		stopped:    make(chan struct{}),
		inputer:    inputer,
		clickhouse: clickhouse,
		started:    false,
		taskCfg:    taskCfg,
		pp:         pp,
	}
}

// Init initializes the kafak and clickhouse task associated with this service

func (service *Service) Init() (err error) {
	if err = service.clickhouse.Init(); err != nil {
		return
	}

	service.dims = service.clickhouse.Dims
	service.batchChan = make(chan *model.Batch, 32)
	service.limiter1 = rate.NewLimiter(rate.Every(10*time.Second), 1)
	service.limiter2 = rate.NewLimiter(rate.Every(10*time.Second), 1)
	service.limiter3 = rate.NewLimiter(rate.Every(10*time.Second), 1)

	err = service.inputer.Init(service.taskCfg, service.put)
	return
}

// Run starts the task
func (service *Service) Run(ctx context.Context) {
	service.started = true
	service.ctx = ctx
	log.Infof("%s: task started", service.taskCfg.Name)
	go service.inputer.Run(ctx)
LOOP:
	for {
		select {
		case <-ctx.Done():
			break LOOP
		case batch := <-service.batchChan:
			if err := service.flush(batch); err != nil {
				log.Errorf("%s: got error %+v", service.taskCfg.Name, err)
			}
		}
	}
	service.stopped <- struct{}{}
}

func (service *Service) fnCommit(partition int, offset int64) error {
	msg := model.InputMessage{Topic: service.taskCfg.Topic, Partition: partition, Offset: offset}
	return service.inputer.CommitMessages(service.ctx, &msg)
}

func (service *Service) put(msg model.InputMessage) {
	statistics.ConsumeMsgsTotal.WithLabelValues(service.taskCfg.Name).Inc()
	// ensure ring for this message exist
	service.Lock()
	var ring *Ring
	if msg.Partition < len(service.rings) {
		ring = service.rings[msg.Partition]
	} else {
		for i := len(service.rings); i < msg.Partition+1; i++ {
			service.rings = append(service.rings, nil)
		}
	}

	var err error
	if ring == nil {
		batchSizeShift := util.GetShift(service.taskCfg.BufferSize)
		ringCap := int64(1 << (batchSizeShift + 1))
		ring := &Ring{
			ringBuf:          make([]model.MsgRow, ringCap),
			ringCap:          ringCap,
			ringGroundOff:    msg.Offset,
			ringCeilingOff:   msg.Offset,
			ringFilledOffset: msg.Offset,
			batchSizeShift:   batchSizeShift,
			idleCnt:          0,
			isIdle:           false,
			partition:        msg.Partition,
			batchSys:         model.NewBatchSys(service.fnCommit),
			service:          service,
		}
		// schedule a delayed ForceBatch
		if ring.tid, err = util.GlobalTimerWheel.Schedule(time.Duration(service.taskCfg.FlushInterval)*time.Second, ring.ForceBatch, nil); err != nil {
			err = errors.Wrap(err, "")
			log.Criticalf("%s: got error %+v", service.taskCfg.Name, err)
		}
		service.rings[msg.Partition] = ring
		service.Unlock()
	} else {
		service.Unlock()
		var ringGroundOff, ringFilledOffset int64
		ring.mux.Lock()
		ringGroundOff, ringFilledOffset = ring.ringGroundOff, ring.ringFilledOffset
		ring.mux.Unlock()
		if msg.Offset < ringFilledOffset {
			statistics.RingMsgsOffTooSmallErrorTotal.WithLabelValues(service.taskCfg.Name).Inc()
			if service.limiter2.Allow() {
				log.Warnf("%s: got a message(topic %v, partition %d, offset %v) left to %v",
					service.taskCfg.Name, msg.Topic, msg.Partition, msg.Offset, ringFilledOffset)
			}
			return
		}
		if msg.Offset >= ringGroundOff+ring.ringCap {
			statistics.RingMsgsOffTooLargeErrorTotal.WithLabelValues(service.taskCfg.Name).Inc()
			if service.limiter3.Allow() {
				log.Warnf("%s: got a message(topic %v, partition %d, offset %v) right to the range [%v, %v)",
					service.taskCfg.Name, msg.Topic, msg.Partition, msg.Offset, ring.ringGroundOff, ring.ringGroundOff+ring.ringCap)
			}
			time.Sleep(1 * time.Second)
			ring.ForceBatch(&msg)
		}
	}

	// submit message to a goroutine pool
	statistics.ParseMsgsBacklog.WithLabelValues(service.taskCfg.Name).Inc()
	_ = util.GlobalParsingPool.Submit(func() {
		var row []interface{}
		p := service.pp.Get()
		metric, err := p.Parse(msg.Value)
		if err != nil {
			statistics.ParseMsgsErrorTotal.WithLabelValues(service.taskCfg.Name).Inc()
			if service.limiter1.Allow() {
				log.Errorf("%s: failed to parse message(topic %v, partition %d, offset %v) %+v, string(value) <<<%+v>>>, got error %+v",
					service.taskCfg.Name, msg.Topic, msg.Partition, msg.Offset, msg, string(msg.Value), err)
			}
		} else {
			row = model.MetricToRow(metric, msg, service.dims)
		}

		service.pp.Put(p)
		var ring *Ring
		service.Lock()
		ring = service.rings[msg.Partition]
		service.Unlock()
		ring.PutElem(model.MsgRow{Msg: &msg, Row: row})
	})
}

func (service *Service) flush(batch *model.Batch) (err error) {
	if (len(batch.MsgRows)) == 0 {
		return batch.Commit()
	}
	service.clickhouse.Send(batch, func(batch *model.Batch) error {
		lastMsg := batch.MsgRows[len(batch.MsgRows)-1].Msg
		statistics.ConsumeOffsets.WithLabelValues(service.taskCfg.Name, strconv.Itoa(lastMsg.Partition), lastMsg.Topic).Set(float64(lastMsg.Offset))
		return batch.Commit()
	})
	return nil
}

// Stop stop kafka and clickhouse client
func (service *Service) Stop() {
	log.Infof("%s: stopping task service...", service.taskCfg.Name)
	if err := service.inputer.Stop(); err != nil {
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

// GoID returns goroutine id
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
