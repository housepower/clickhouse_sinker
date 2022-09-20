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
	"math"
	"regexp"
	"sync"
	"sync/atomic"
	"time"

	"github.com/confluentinc/confluent-kafka-go/schemaregistry"
	"github.com/fagongzi/goetty"
	"github.com/thanos-io/thanos/pkg/errors"
	"github.com/viru-tech/clickhouse_sinker/config"
	"github.com/viru-tech/clickhouse_sinker/input"
	"github.com/viru-tech/clickhouse_sinker/model"
	"github.com/viru-tech/clickhouse_sinker/output"
	"github.com/viru-tech/clickhouse_sinker/parser"
	"github.com/viru-tech/clickhouse_sinker/statistics"
	"github.com/viru-tech/clickhouse_sinker/util"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

// TaskService holds the configuration for each task
type Service struct {
	sync.Mutex
	inputer        input.Inputer
	clickhouse     *output.ClickHouse
	schemaRegistry schemaregistry.Client
	pp             *parser.Pool
	cfg            *config.Config
	taskCfg        *config.TaskConfig
	whiteList      *regexp.Regexp
	blackList      *regexp.Regexp
	lblBlkList     *regexp.Regexp
	dims           []*model.ColumnWithType

	idxSerID int
	nameKey  string

	knownKeys  sync.Map
	newKeys    sync.Map
	warnKeys   sync.Map
	cntNewKeys int32 // size of newKeys
	tid        goetty.Timeout

	rings    []*Ring
	sharder  *Sharder
	limiter1 *rate.Limiter
	limiter2 *rate.Limiter

	wgRun     sync.WaitGroup
	state     uint32
	numFlying int32
	taskDone  *sync.Cond
}

// NewTaskService creates an instance of new tasks with kafka, clickhouse and paser instances
func NewTaskService(cfg *config.Config, taskCfg *config.TaskConfig) (service *Service) {
	ck := output.NewClickHouse(cfg, taskCfg)
	inputer := input.NewInputer(taskCfg.KafkaClient)
	service = &Service{
		inputer:    inputer,
		clickhouse: ck,
		cfg:        cfg,
		taskCfg:    taskCfg,
	}
	service.taskDone = sync.NewCond(service)
	if taskCfg.DynamicSchema.WhiteList != "" {
		service.whiteList = regexp.MustCompile(taskCfg.DynamicSchema.WhiteList)
	}
	if taskCfg.DynamicSchema.BlackList != "" {
		service.blackList = regexp.MustCompile(taskCfg.DynamicSchema.BlackList)
	}
	if taskCfg.PromLabelsBlackList != "" {
		service.lblBlkList = regexp.MustCompile(taskCfg.PromLabelsBlackList)
	}
	return
}

// Init initializes the kafka and clickhouse task associated with this service
func (service *Service) Init() (err error) {
	taskCfg := service.taskCfg
	util.Logger.Info("task initializing", zap.String("task", taskCfg.Name))
	service.numFlying = 0
	atomic.StoreUint32(&service.state, util.StateRunning)
	if err = service.clickhouse.Init(); err != nil {
		return
	}

	if service.cfg.SchemaRegistry.URL != "" {
		service.schemaRegistry, err = schemaregistry.NewClient(schemaregistry.NewConfig(service.cfg.SchemaRegistry.URL))
		if err != nil {
			return
		}
	}
	service.pp, _ = parser.NewParserPool(taskCfg.Parser, taskCfg.CsvFormat, taskCfg.Delimiter, taskCfg.TimeZone, taskCfg.TimeUnit, taskCfg.Topic, service.schemaRegistry)

	service.dims = service.clickhouse.Dims
	service.idxSerID = service.clickhouse.IdxSerID
	service.nameKey = service.clickhouse.NameKey
	service.limiter1 = rate.NewLimiter(rate.Every(10*time.Second), 1)
	service.limiter2 = rate.NewLimiter(rate.Every(10*time.Second), 1)

	service.rings = make([]*Ring, 0)
	if taskCfg.ShardingKey != "" {
		if service.sharder, err = NewSharder(service); err != nil {
			return
		}
	}

	if err = service.inputer.Init(service.cfg, taskCfg, service.put, service.drain); err != nil {
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
			service.knownKeys.Store("", nil) // column name shall not be empty string
			service.newKeys = sync.Map{}
			atomic.StoreInt32(&service.cntNewKeys, 0)
		}
	}
	return
}

// Run starts the task
func (service *Service) Run() {
	var err error
	service.wgRun.Add(1)
	defer service.wgRun.Done()
	taskCfg := service.taskCfg
	if service.sharder != nil {
		// schedule a delayed ForceFlush
		if service.sharder.tid, err = util.GlobalTimerWheel.Schedule(time.Duration(taskCfg.FlushInterval)*time.Second, service.sharder.ForceFlush, nil); err != nil {
			if errors.Is(err, goetty.ErrSystemStopped) {
				util.Logger.Info("Service.Run scheduling timer to a stopped timer wheel")
			} else {
				err = errors.Wrapf(err, "")
				util.Logger.Fatal("scheduling timer filed", zap.String("task", taskCfg.Name), zap.Error(err))
			}
		}
	}
	service.inputer.Run()
}

func (service *Service) fnCommit(partition int, offset int64) (err error) {
	msg := model.InputMessage{Topic: service.taskCfg.Topic, Partition: partition, Offset: offset}
	if err = service.inputer.CommitMessages(&msg); err != nil {
		return
	}
	util.Logger.Debug(fmt.Sprintf("committed topic %s, partition %d, offset %d", msg.Topic, msg.Partition, msg.Offset+1), zap.String("task", service.taskCfg.Name))
	return
}

func (service *Service) putToRing(msg *model.InputMessage) (ok bool) {
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

	if ring == nil {
		batchSizeShift := util.GetShift(taskCfg.BufferSize)
		ringCap := int64(1 << (batchSizeShift + 1))
		ring := &Ring{
			ringBuf:          nil,
			ringCap:          ringCap,
			ringCapMask:      ringCap - 1,
			ringGroundOff:    msg.Offset,
			ringCeilingOff:   msg.Offset,
			ringFilledOffset: msg.Offset,
			batchSizeShift:   batchSizeShift,
			idleCnt:          0,
			isIdle:           true,
			partition:        msg.Partition,
			batchSys:         model.NewBatchSys(taskCfg, service.fnCommit),
			service:          service,
		}
		ring.available = sync.NewCond(&ring.mux)
		ring.PutMsgNolock(msg)
		service.rings[msg.Partition] = ring
		service.Unlock()
		ok = true
	} else {
		service.Unlock()
		ring.mux.Lock()
		ring.QuitIdle()
		if msg.Offset < ring.ringFilledOffset {
			statistics.RingMsgsOffTooSmallErrorTotal.WithLabelValues(taskCfg.Name).Inc()
			if service.limiter2.Allow() {
				util.Logger.Warn(fmt.Sprintf("got a message(topic %v, partition %d, offset %v) left to %v",
					msg.Topic, msg.Partition, msg.Offset, ring.ringFilledOffset), zap.String("task", taskCfg.Name))
			}
			ring.mux.Unlock()
		} else if msg.Offset < ring.ringGroundOff+ring.ringCap {
			ring.PutMsgNolock(msg)
			ring.mux.Unlock()
			ok = true
		} else {
			prevMsgOff := msg.Offset - 1
			for atomic.LoadUint32(&service.state) == util.StateRunning && !ring.isIdle &&
				msg.Offset == ring.ringGroundOff+ring.ringCap && ring.ringBuf[prevMsgOff&ring.ringCapMask].Msg != nil {
				// wait ring.PutElem/ring.ForceBatchOrShard to make room
				util.Logger.Debug(fmt.Sprintf("got a message(topic %v, partition %d, offset %v) while the ring is full, waiting...",
					msg.Topic, msg.Partition, msg.Offset), zap.String("task", taskCfg.Name))
				ring.available.Wait()
				util.Logger.Debug(fmt.Sprintf("got a message(topic %v, partition %d, offset %v) while the ring is full, wake-up",
					msg.Topic, msg.Partition, msg.Offset), zap.String("task", taskCfg.Name))
			}
			if atomic.LoadUint32(&service.state) != util.StateRunning || ring.isIdle {
				util.Logger.Debug(fmt.Sprintf("got a message(topic %v, partition %d, offset %v) while the ring.isIdle %v, service.state %v",
					msg.Topic, msg.Partition, msg.Offset, ring.isIdle, atomic.LoadUint32(&service.state)), zap.String("task", taskCfg.Name))
				ring.mux.Unlock()
			} else if msg.Offset == ring.ringGroundOff || (msg.Offset < ring.ringGroundOff+ring.ringCap && ring.ringBuf[prevMsgOff&ring.ringCapMask].Msg != nil) {
				ring.PutMsgNolock(msg)
				ring.mux.Unlock()
				ok = true
			} else {
				// discard messages to make room
				ring.mux.Unlock()
				statistics.RingMsgsOffTooLargeErrorTotal.WithLabelValues(taskCfg.Name).Inc()
				util.Logger.Warn(fmt.Sprintf("got a message(topic %v, partition %d, offset %v) which's previous one is absent in ring offsets [%v, %v)",
					msg.Topic, msg.Partition, msg.Offset, ring.ringGroundOff, ring.ringGroundOff+ring.ringCap), zap.String("task", taskCfg.Name))
				ring.MakeRoom(msg)
				ring.PutMsgNolock(msg)
				ok = true
			}
		}
	}
	return
}

func (service *Service) put(msg *model.InputMessage) {
	if atomic.LoadUint32(&service.state) != util.StateRunning {
		return
	}
	if !service.putToRing(msg) {
		return
	}
	// submit message to the parsing pool
	taskCfg := service.taskCfg
	service.Lock()
	service.numFlying++
	service.Unlock()
	statistics.ParsingPoolBacklog.WithLabelValues(taskCfg.Name).Inc()
	_ = util.GlobalParsingPool.Submit(func() {
		var err error
		var row *model.Row
		var foundNewKeys bool
		var metric model.Metric
		defer func() {
			service.Lock()
			service.numFlying--
			if service.numFlying == 0 {
				service.taskDone.Broadcast()
			}
			service.Unlock()
			statistics.ParsingPoolBacklog.WithLabelValues(taskCfg.Name).Dec()
		}()
		p := service.pp.Get()
		metric, err = p.Parse(msg.Value)
		// WARNNING: Always PutElem even if there's parsing error, so that this message can be acked to Kafka and skipped writing to ClickHouse.
		if err != nil {
			row = &model.FakedRow
			statistics.ParseMsgsErrorTotal.WithLabelValues(taskCfg.Name).Inc()
			if service.limiter1.Allow() {
				util.Logger.Error(fmt.Sprintf("failed to parse message(topic %v, partition %d, offset %v)",
					msg.Topic, msg.Partition, msg.Offset), zap.String("message value", string(msg.Value)), zap.String("task", taskCfg.Name), zap.Error(err))
			}
		} else {
			row = model.MetricToRow(metric, msg, service.dims, service.idxSerID, service.nameKey, service.lblBlkList)
			if taskCfg.DynamicSchema.Enable {
				foundNewKeys = metric.GetNewKeys(&service.knownKeys, &service.newKeys, &service.warnKeys, service.whiteList, service.blackList, msg.Partition, msg.Offset)
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
						err = errors.Wrapf(err, "")
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
			ring.PutElem(model.MsgRow{Msg: msg, Row: row})
		}
	})
}

// drain ensure we have completeted procession(discard or write&commit) for all received messages, and cleared service state.
func (service *Service) drain() {
	savedState := atomic.SwapUint32(&service.state, util.StateStopped)
	defer atomic.CompareAndSwapUint32(&service.state, util.StateStopped, savedState)
	begin := time.Now()
	service.Lock()
	for service.numFlying != 0 {
		service.taskDone.Wait()
	}
	for _, ring := range service.rings {
		if ring != nil {
			ring.ForceBatchOrShard(nil)
		}
	}
	service.rings = make([]*Ring, 0)
	service.Unlock()
	if service.sharder != nil {
		service.sharder.ForceFlush(nil)
	}
	util.Logger.Debug("generated flying batches",
		zap.String("task", service.taskCfg.Name),
		zap.Duration("cost", time.Since(begin)))
	service.clickhouse.Drain()
	util.Logger.Debug("drained flying batches",
		zap.String("task", service.taskCfg.Name),
		zap.Duration("cost", time.Since(begin)))
}

func (service *Service) Flush(batch *model.Batch) (err error) {
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
	go service.Run()
}

// Stop stop kafka and clickhouse client. This is blocking.
func (service *Service) Stop() {
	taskCfg := service.taskCfg

	util.Logger.Debug("stopping task service...", zap.String("task", taskCfg.Name))
	atomic.StoreUint32(&service.state, util.StateStopped)
	for _, ring := range service.rings {
		if ring != nil {
			ring.mux.Lock()
			ring.available.Broadcast()
			ring.mux.Unlock()
		}
	}

	if service.sharder != nil {
		service.sharder.tid.Stop()
	}
	service.tid.Stop()
	util.Logger.Debug("stopped internal timers", zap.String("task", taskCfg.Name))

	if err := service.inputer.Stop(); err != nil {
		util.Logger.Fatal("service.inputer.Stop failed", zap.Error(err))
	}
	util.Logger.Debug("stopped input", zap.String("task", taskCfg.Name))

	service.wgRun.Wait()
	util.Logger.Debug("stopped task", zap.String("task", taskCfg.Name))
}
