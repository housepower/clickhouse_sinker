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
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fagongzi/goetty"
	"github.com/housepower/clickhouse_sinker/config"
	"github.com/housepower/clickhouse_sinker/input"
	"github.com/housepower/clickhouse_sinker/model"
	"github.com/housepower/clickhouse_sinker/output"
	"github.com/housepower/clickhouse_sinker/parser"
	"github.com/housepower/clickhouse_sinker/statistics"
	"github.com/housepower/clickhouse_sinker/util"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

// TaskService holds the configuration for each task
type Service struct {
	sync.Mutex

	parentCtx  context.Context
	ctx        context.Context
	cancel     context.CancelFunc
	started    bool
	stopped    chan struct{}
	inputer    input.Inputer
	clickhouse *output.ClickHouse
	pp         *parser.Pool
	cfg        *config.Config
	taskCfg    *config.TaskConfig
	dims       []*model.ColumnWithType

	knownKeys  sync.Map
	newKeys    sync.Map
	cntNewKeys int32 // size of newKeys
	tid        goetty.Timeout

	rings     []*Ring
	sharder   *Sharder
	batchChan chan *model.Batch
	limiter1  *rate.Limiter
	limiter2  *rate.Limiter
	limiter3  *rate.Limiter
}

// NewTaskService creates an instance of new tasks with kafka, clickhouse and paser instances
func NewTaskService(cfg *config.Config, taskCfg *config.TaskConfig) *Service {
	ck := output.NewClickHouse(cfg, taskCfg)
	pp, _ := parser.NewParserPool(taskCfg.Parser, taskCfg.CsvFormat, taskCfg.Delimiter, taskCfg.TimeZone)
	inputer := input.NewInputer(taskCfg.KafkaClient)
	return &Service{
		stopped:    make(chan struct{}),
		inputer:    inputer,
		clickhouse: ck,
		started:    false,
		pp:         pp,
		cfg:        cfg,
		taskCfg:    taskCfg,
	}
}

// Init initializes the kafak and clickhouse task associated with this service
func (service *Service) Init() (err error) {
	taskCfg := service.taskCfg
	util.Logger.Info("task initializing", zap.String("task", taskCfg.Name))
	if err = service.clickhouse.Init(); err != nil {
		return
	}

	service.dims = service.clickhouse.Dims
	service.batchChan = make(chan *model.Batch, 32)
	service.limiter1 = rate.NewLimiter(rate.Every(10*time.Second), 1)
	service.limiter2 = rate.NewLimiter(rate.Every(10*time.Second), 1)
	service.limiter3 = rate.NewLimiter(rate.Every(10*time.Second), 1)

	service.rings = make([]*Ring, 0)
	if taskCfg.ShardingKey != "" {
		if service.sharder, err = NewSharder(service); err != nil {
			return
		}
	}

	if err = service.inputer.Init(service.cfg, taskCfg, service.put); err != nil {
		return
	}

	if taskCfg.DynamicSchema.Enable {
		maxDims := math.MaxInt16
		if taskCfg.DynamicSchema.MaxDims > 0 {
			maxDims = taskCfg.DynamicSchema.MaxDims
		}
		if maxDims <= len(service.dims) {
			taskCfg.DynamicSchema.Enable = false
			util.Logger.Warn(fmt.Sprintf("disabled DynamicSchema since the number of columns reaches upper limit %d", maxDims), zap.String("task", taskCfg.Name))
		} else {
			for _, dim := range service.dims {
				service.knownKeys.Store(dim.SourceName, nil)
			}
			for _, dim := range taskCfg.ExcludeColumns {
				service.knownKeys.Store(dim, nil)
			}
			service.newKeys = sync.Map{}
			atomic.StoreInt32(&service.cntNewKeys, 0)
		}
	}
	return
}

// Run starts the task
func (service *Service) Run(ctx context.Context) {
	var err error
	taskCfg := service.taskCfg
	service.started = true
	service.parentCtx = ctx
	service.ctx, service.cancel = context.WithCancel(ctx)
	util.Logger.Info("task started", zap.String("task", taskCfg.Name))
	go service.inputer.Run(service.ctx)
	if service.sharder != nil {
		// schedule a delayed ForceFlush
		if service.sharder.tid, err = util.GlobalTimerWheel.Schedule(time.Duration(taskCfg.FlushInterval)*time.Second, service.sharder.ForceFlush, nil); err != nil {
			if errors.Is(err, goetty.ErrSystemStopped) {
				util.Logger.Info("Service.Run scheduling timer to a stopped timer wheel")
			} else {
				err = errors.Wrap(err, "")
				util.Logger.Fatal("scheduling timer filed", zap.String("task", taskCfg.Name), zap.Error(err))
			}
		}
	}

LOOP:
	for {
		select {
		case <-service.ctx.Done():
			util.Logger.Info("Service.Run quit due to context has been canceled", zap.String("task", service.taskCfg.Name))
			break LOOP
		case batch := <-service.batchChan:
			if err := service.flush(batch); err != nil {
				util.Logger.Fatal("service.flush failed", zap.String("task", taskCfg.Name), zap.Error(err))
			}
		}
	}
	service.stopped <- struct{}{}
}

func (service *Service) fnCommit(partition int, offset int64) error {
	msg := model.InputMessage{Topic: service.taskCfg.Topic, Partition: partition, Offset: offset}
	return service.inputer.CommitMessages(service.parentCtx, &msg)
}

func (service *Service) put(msg model.InputMessage) {
	taskCfg := service.taskCfg
	statistics.ConsumeMsgsTotal.WithLabelValues(taskCfg.Name).Inc()
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
		batchSizeShift := util.GetShift(taskCfg.BufferSize)
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
			batchSys:         model.NewBatchSys(taskCfg, service.fnCommit),
			service:          service,
		}
		// schedule a delayed ForceBatchOrShard
		if ring.tid, err = util.GlobalTimerWheel.Schedule(time.Duration(taskCfg.FlushInterval)*time.Second, ring.ForceBatchOrShard, nil); err != nil {
			if errors.Is(err, goetty.ErrSystemStopped) {
				util.Logger.Info("Service.put scheduling timer to a stopped timer wheel")
			} else {
				err = errors.Wrap(err, "")
				util.Logger.Fatal("scheduling timer filed", zap.String("task", taskCfg.Name), zap.Error(err))
			}
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
			statistics.RingMsgsOffTooSmallErrorTotal.WithLabelValues(taskCfg.Name).Inc()
			if service.limiter2.Allow() {
				util.Logger.Warn(fmt.Sprintf("got a message(topic %v, partition %d, offset %v) left to %v",
					msg.Topic, msg.Partition, msg.Offset, ringFilledOffset), zap.String("task", taskCfg.Name))
			}
			return
		}
		if msg.Offset >= ringGroundOff+ring.ringCap && atomic.LoadInt32(&service.cntNewKeys) == 0 {
			statistics.RingMsgsOffTooLargeErrorTotal.WithLabelValues(taskCfg.Name).Inc()
			if service.limiter3.Allow() {
				util.Logger.Warn(fmt.Sprintf("got a message(topic %v, partition %d, offset %v) right to the range [%v, %v)",
					msg.Topic, msg.Partition, msg.Offset, ring.ringGroundOff, ring.ringGroundOff+ring.ringCap), zap.String("task", taskCfg.Name))
			}
			time.Sleep(1 * time.Second)
			ring.ForceBatchOrShard(&msg)
		}
	}

	// submit message to a goroutine pool
	statistics.ParsingPoolBacklog.WithLabelValues(taskCfg.Name).Inc()
	_ = util.GlobalParsingPool.Submit(func() {
		var row *model.Row
		var foundNewKeys bool
		var metric model.Metric
		defer statistics.ParsingPoolBacklog.WithLabelValues(taskCfg.Name).Dec()
		p := service.pp.Get()
		metric, err = p.Parse(msg.Value)
		// WARNNING: Always PutElem even if there's parsing error, so that this message can be acked to Kafka and skipped writing to ClickHouse.
		if err != nil {
			statistics.ParseMsgsErrorTotal.WithLabelValues(taskCfg.Name).Inc()
			if service.limiter1.Allow() {
				util.Logger.Error(fmt.Sprintf("failed to parse message(topic %v, partition %d, offset %v)",
					msg.Topic, msg.Partition, msg.Offset), zap.String("message value", string(msg.Value)), zap.String("task", taskCfg.Name), zap.Error(err))
			}
		} else {
			row = model.MetricToRow(metric, msg, service.dims)
			if taskCfg.DynamicSchema.Enable {
				foundNewKeys = metric.GetNewKeys(&service.knownKeys, &service.newKeys)
			}
			// Dumping message and result
			//util.Logger.Debug("parsed kafka message", zap.Int("partition", msg.Partition), zap.Int64("offset", msg.Offset),
			//	zap.String("message value", string(msg.Value)), zap.String("row(spew)", spew.Sdump(row)))
		}
		// WARNNING: metric.GetXXX may depend on p. Don't call them after p been freed.
		service.pp.Put(p)

		if foundNewKeys {
			cntNewKeys := atomic.AddInt32(&service.cntNewKeys, 1)
			if cntNewKeys == 1 {
				// The first message which contains new keys triggers flushing
				// all messages and scheduling a delayed func to apply schema change.
				for _, ring := range service.rings {
					if ring != nil {
						ring.ForceBatchOrShard(nil)
					}
				}
				if service.sharder != nil {
					service.sharder.ForceFlush(nil)
				}
				if service.tid, err = util.GlobalTimerWheel.Schedule(time.Duration(taskCfg.FlushInterval)*time.Second, service.changeSchema, nil); err != nil {
					if errors.Is(err, goetty.ErrSystemStopped) {
						util.Logger.Info("Service.put scheduling timer to a stopped timer wheel")
					} else {
						err = errors.Wrap(err, "")
						util.Logger.Fatal("scheduling timer failed", zap.String("task", taskCfg.Name), zap.Error(err))
					}
				}
			}
		}
		if atomic.LoadInt32(&service.cntNewKeys) == 0 {
			var ring *Ring
			service.Lock()
			ring = service.rings[msg.Partition]
			service.Unlock()
			ring.PutElem(model.MsgRow{Msg: &msg, Row: row})
		}
	})
}

func (service *Service) flush(batch *model.Batch) (err error) {
	if (len(*batch.Rows)) == 0 {
		return batch.Commit()
	}
	service.clickhouse.Send(batch)
	return nil
}

func (service *Service) changeSchema(arg interface{}) {
	var err error
	taskCfg := service.taskCfg
	// change schema
	if err = service.clickhouse.ChangeSchema(&service.newKeys); err != nil {
		util.Logger.Fatal("clickhouse.ChangeSchema failed", zap.String("task", taskCfg.Name), zap.Error(err))
	}
	// restart myself
	service.Stop()
	if err = service.Init(); err != nil {
		util.Logger.Fatal("service.Init failed", zap.String("task", taskCfg.Name), zap.Error(err))
	}
	go service.Run(service.parentCtx)
}

// NotifyStop notify task to stop, This is non-blocking.
func (service *Service) NotifyStop() {
	taskCfg := service.taskCfg
	util.Logger.Info("notified stopping task service...", zap.String("task", taskCfg.Name))
	service.cancel()
}

// Stop stop kafka and clickhouse client. This is blocking.
func (service *Service) Stop() {
	taskCfg := service.taskCfg
	if !service.started {
		util.Logger.Info("stopped a already stopped task service", zap.String("task", taskCfg.Name))
		return
	}
	util.Logger.Info("stopping task service...", zap.String("task", taskCfg.Name))
	service.cancel()
	service.clickhouse.Stop()
	util.Logger.Info("stopped output", zap.String("task", taskCfg.Name))
	if err := service.inputer.Stop(); err != nil {
		util.Logger.Fatal("service.inputer.Stop failed", zap.Error(err))
	}
	util.Logger.Info("stopped input", zap.String("task", taskCfg.Name))

	if service.sharder != nil {
		service.sharder.tid.Stop()
	}
	for _, ring := range service.rings {
		if ring != nil {
			ring.tid.Stop()
		}
	}
	service.tid.Stop()
	util.Logger.Info("stopped internal timers", zap.String("task", taskCfg.Name))

	if service.started {
		<-service.stopped
	}
	util.Logger.Info("stopped", zap.String("task", taskCfg.Name))
}
