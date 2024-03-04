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

	"github.com/housepower/clickhouse_sinker/config"
	"github.com/housepower/clickhouse_sinker/input"
	"github.com/housepower/clickhouse_sinker/model"
	"github.com/housepower/clickhouse_sinker/util"
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
	var bufLength int64

	flushFn := func(traceId, with string) {
		if len(recMap) == 0 {
			return
		}
		if bufLength > 0 {
			util.LogTrace(traceId, util.TraceKindProcessEnd, zap.String("with", with), zap.Int64("bufLength", bufLength))
		}
		var wg sync.WaitGroup
		c.tasks.Range(func(key, value any) bool {
			// flush to shard, ck
			task := value.(*Service)
			task.sharder.Flush(c.ctx, &wg, recMap[task.taskCfg.Topic], traceId)
			return true
		})
		bufLength = 0

		c.mux.Lock()
		c.numFlying++
		c.mux.Unlock()
		c.sinker.commitsCh <- &Commit{group: c.grpConfig.Name, offsets: recMap, wg: &wg, consumer: c}
		recMap = make(model.RecordMap)
	}

	bufThreshold := c.grpConfig.BufferSize * len(c.sinker.curCfg.Clickhouse.Hosts) * 4 / 5
	if bufThreshold > MaxCountInBuf {
		bufThreshold = MaxCountInBuf
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
					zap.Int("bufThreshold", bufThreshold),
					zap.Int64("totalLength", bufLength))
			} else {
				traceId = fetches.TraceId
				util.LogTrace(traceId, util.TraceKindProcessStart, zap.Int("records", len(fetch)))
			}
			items, done := int64(len(fetch)), int64(-1)
			var concurrency int
			if concurrency = int(items/1000) + 1; concurrency > MaxParallelism {
				concurrency = MaxParallelism
			}

			var wg sync.WaitGroup
			var err error
			wg.Add(concurrency)
			for i := 0; i < concurrency; i++ {
				go func() {
					for {
						index := atomic.AddInt64(&done, 1)
						if index >= items || c.state.Load() == util.StateStopped {
							wg.Done()
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

						c.tasks.Range(func(key, value any) bool {
							tsk := value.(*Service)
							if (tablename != "" && tsk.clickhouse.TableName == tablename) || tsk.taskCfg.Topic == rec.Topic {
								//bufLength++
								atomic.AddInt64(&bufLength, 1)
								if e := tsk.Put(msg, traceId, flushFn); e != nil {
									atomic.StoreInt64(&done, items)
									err = e
									// decrise the error record
									util.Rs.Dec(1)
									return false
								}
							}
							return true
						})
					}
				}()
			}
			wg.Wait()

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

			if bufLength > int64(bufThreshold) {
				flushFn(traceId, "bufLength reached")
				ticker.Reset(time.Duration(c.grpConfig.FlushInterval) * time.Second)
				wait = false
			} else {
				wait = true
			}
		case <-ticker.C:
			flushFn(traceId, "ticker.C triggered")
		case <-c.ctx.Done():
			util.Logger.Info("stopped processing loop", zap.String("group", c.grpConfig.Name))
			return
		}
	}
}
