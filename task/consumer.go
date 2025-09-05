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
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/viru-tech/clickhouse_sinker/config"
	"github.com/viru-tech/clickhouse_sinker/input"
	"github.com/viru-tech/clickhouse_sinker/model"
	"github.com/viru-tech/clickhouse_sinker/util"
	"go.uber.org/zap"

	_ "github.com/ClickHouse/clickhouse-go/v2"
)

type Commit struct {
	group    string
	offsets  model.RecordMap
	wg       *sync.WaitGroup
	consumer *Consumer
}

type Consumer struct {
	sinker    *Sinker
	inputer   *input.KafkaFranz
	tasks     sync.Map
	grpConfig *config.GroupConfig
	fetchesCh chan input.Fetches
	processWg sync.WaitGroup
	ctx       context.Context
	cancel    context.CancelFunc
	state     atomic.Uint32
	errCommit bool

	numFlying  int32
	mux        sync.Mutex
	commitDone *sync.Cond
}

const (
	MaxCountInBuf  = 1 << 27
	MaxParallelism = 10
)

func newConsumer(s *Sinker, gCfg *config.GroupConfig) *Consumer {
	c := &Consumer{
		sinker:    s,
		numFlying: 0,
		errCommit: false,
		grpConfig: gCfg,
		fetchesCh: make(chan input.Fetches),
	}
	c.state.Store(util.StateStopped)
	c.commitDone = sync.NewCond(&c.mux)
	return c
}

func (c *Consumer) addTask(tsk *Service) {
	c.tasks.Store(tsk.taskCfg.Name, tsk)
}

func (c *Consumer) start() {
	if c.state.Load() == util.StateRunning {
		return
	}
	c.ctx, c.cancel = context.WithCancel(context.Background())
	c.inputer = input.NewKafkaFranz()
	c.state.Store(util.StateRunning)
	if err := c.inputer.Init(c.sinker.curCfg, c.grpConfig, c.fetchesCh, c.cleanupFn); err == nil {
		go c.inputer.Run()
		go c.processFetch()
	} else {
		util.Logger.Fatal("failed to init consumer", zap.String("consumer", c.grpConfig.Name), zap.Error(err))
	}
}

func (c *Consumer) stop() {
	if c.state.Load() == util.StateStopped {
		return
	}
	c.state.Store(util.StateStopped)

	// stop the processFetch routine, make sure no more input to the commit chan & writing pool
	c.cancel()
	c.processWg.Wait()
	c.inputer.Stop()
}

func (c *Consumer) restart() {
	c.stop()
	c.start()
}

func (c *Consumer) cleanupFn() {
	// ensure the completion of writing to ck
	var wg sync.WaitGroup
	c.tasks.Range(func(key, value any) bool {
		wg.Add(1)
		go func(t *Service) {
			// drain ensure we have completeted persisting all received messages
			t.clickhouse.Drain()
			wg.Done()
		}(value.(*Service))
		return true
	})
	wg.Wait()

	// ensure the completion of offset submission
	c.mux.Lock()
	for c.numFlying != 0 {
		util.Logger.Debug("draining flying pending commits", zap.String("consumergroup", c.grpConfig.Name), zap.Int32("pending", c.numFlying))
		c.commitDone.Wait()
	}
	c.mux.Unlock()
}

func (c *Consumer) updateGroupConfig(g *config.GroupConfig) {
	if c.state.Load() == util.StateStopped {
		return
	}
	c.grpConfig = g
	// restart the processFetch routine because of potential BufferSize or FlushInterval change
	// make sure no more input to the commit chan & writing pool
	c.cancel()
	c.processWg.Wait()
	c.ctx, c.cancel = context.WithCancel(context.Background())
	go c.processFetch()
}

func (c *Consumer) processFetch() {
	c.processWg.Add(1)
	defer c.processWg.Done()
	recMap := make(model.RecordMap)

	type messageWithTrace struct {
		traceID string
		msg     *model.InputMessage
	}
	// TODO: redo?
	type thresholder struct {
		duration  time.Duration
		ticker    *time.Ticker
		threshold uint64
		current   uint64
		inputC    chan messageWithTrace
		task      *Service
	}

	flushFn := func(traceId, with string, worker *thresholder) {
		if len(recMap) == 0 {
			return
		}
		bufLength := atomic.LoadUint64(&worker.current)
		if bufLength > 0 {
			util.LogTrace(traceId, util.TraceKindProcessEnd, zap.String("with", with), zap.Uint64("bufLength", bufLength))
		}
		var wg sync.WaitGroup
		worker.task.sharder.Flush(c.ctx, &wg, recMap[worker.task.taskCfg.Topic], traceId)

		util.Logger.Info("flushed", zap.Uint64("count", bufLength), zap.String("trace_id", traceId), zap.String("topic", worker.task.taskCfg.Topic))

		c.mux.Lock()
		c.numFlying++
		c.mux.Unlock()
		c.sinker.commitsCh <- &Commit{group: c.grpConfig.Name, offsets: recMap, wg: &wg, consumer: c}
		recMap = make(model.RecordMap)
	}

	thresholds := make(map[string]*thresholder)

	c.tasks.Range(func(key, value any) bool {
		task := value.(*Service)
		bufSize := uint64(task.taskCfg.BufferSize * len(c.sinker.curCfg.Clickhouse.Hosts) * 4 / 5)
		threshold := &thresholder{
			threshold: bufSize,
			inputC:    make(chan messageWithTrace, bufSize),
			task:      task,
		}
		if task.taskCfg.FlushInterval != 0 {
			threshold.duration = time.Duration(task.taskCfg.FlushInterval) * time.Second
			threshold.ticker = time.NewTicker(threshold.duration)
		}
		if task.taskCfg.Topic != "" {
			thresholds[task.taskCfg.Topic] = threshold
			return true
		}
		if task.clickhouse.TableName == "" {
			util.Logger.Warn("can't find topic or tablename for task",
				zap.String("topic", task.taskCfg.Topic),
			)
			return true
		}
		thresholds[task.clickhouse.TableName] = threshold

		return true
	})

	for i := range thresholds {
		topic := i
		go func() {
			task := thresholds[topic]
			var traceID string
			for {
				select {
				case msg, ok := <-task.inputC:
					if !ok {
						return
					}
					traceID = msg.traceID
					err := task.task.Put(msg.msg, traceID, func(traceId, with string) {
						flushFn(traceId, with, task)
					})
					if err != nil {
						// decrease the error record
						util.Rs.Dec(1)
					}
				case <-task.ticker.C:
					flushFn(traceID, "ticker.C triggered", task)
				case <-c.ctx.Done():
					close(task.inputC)
					return
				}
			}
		}()
	}

	ticker := time.NewTicker(time.Duration(c.grpConfig.FlushInterval) * time.Second)
	defer ticker.Stop()
	traceId := "NO_RECORDS_FETCHED"
	wait := false
	for {
		select {
		case fetches := <-c.fetchesCh:
			if c.state.Load() == util.StateStopped {
				continue
			}
			fetch := fetches.Fetch.Records()
			if wait {
				util.LogTrace(fetches.TraceId,
					util.TraceKindProcessing,
					zap.String("message", "bufThreshold not reached, use old traceId"),
					zap.String("old_trace_id", traceId),
					zap.Int("records", len(fetch)),
					zap.Any("bufThresholds", thresholds),
				)
			} else {
				traceId = fetches.TraceId
				util.LogTrace(traceId, util.TraceKindProcessStart, zap.Int("records", len(fetch)))
			}
			items, done := int64(len(fetch)), int64(-1)
			var err error

			for {
				index := atomic.AddInt64(&done, 1)
				if index >= items || c.state.Load() == util.StateStopped {
					break
				}

				rec := fetch[index]
				msg := &model.InputMessage{
					Topic:     rec.Topic,
					Partition: int(rec.Partition),
					Key:       rec.Key,
					Value:     rec.Value,
					Offset:    rec.Offset,
					Timestamp: &rec.Timestamp,
				}

				tablename := ""
				for _, it := range rec.Headers {
					if it.Key == "__table_name" {
						tablename = string(it.Value)
						break
					}
				}
				worker, ok := thresholds[rec.Topic]
				if !ok && tablename != "" {
					worker, ok = thresholds[tablename]
				}
				if ok {
					select {
					case worker.inputC <- messageWithTrace{
						msg:     msg,
						traceID: traceId,
					}:
					case <-c.ctx.Done():
						util.Logger.Info("stopped processing loop", zap.String("group", c.grpConfig.Name))
						return
					}
				} else {
					util.Logger.Warn("topic not found", zap.String("topic", rec.Topic))
				}
			}

			// record the latest offset in order
			// assume the c.state was reset to stopped when facing error, so that further fetch won't get processed
			if err == nil {
				for _, f := range *fetches.Fetch {
					for i := range f.Topics {
						ft := &f.Topics[i]
						if recMap[ft.Topic] == nil {
							recMap[ft.Topic] = make(map[int32]*model.BatchRange)
						}
						for j := range ft.Partitions {
							fpr := ft.Partitions[j].Records
							if len(fpr) == 0 {
								continue
							}
							lastOff := fpr[len(fpr)-1].Offset
							firstOff := fpr[0].Offset
							if thresholds[ft.Topic] == nil {
								util.Logger.Info("topic not found", zap.String("topic", ft.Topic))
								continue
							}
							thresholds[ft.Topic].current += uint64(len(fpr))
							or, ok := recMap[ft.Topic][ft.Partitions[j].Partition]
							if !ok {
								or = &model.BatchRange{Begin: math.MaxInt64, End: -1}
								recMap[ft.Topic][ft.Partitions[j].Partition] = or
							}
							if or.End < lastOff {
								or.End = lastOff
							}
							if or.Begin > firstOff {
								or.Begin = firstOff
							}
						}
					}
				}
			}
			wait = true
			for i := range thresholds {
				if thresholds[i].current >= thresholds[i].threshold {
					flushFn(traceId, "bufLength reached", thresholds[i])
					thresholds[i].current = 0
					thresholds[i].ticker.Reset(thresholds[i].duration)
					wait = false
				}
			}
		case <-c.ctx.Done():
			util.Logger.Info("stopped processing loop", zap.String("group", c.grpConfig.Name))
			return
		}
	}
}
