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
	"encoding/json"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"github.com/housepower/clickhouse_sinker/config"
	"github.com/housepower/clickhouse_sinker/input"
	"github.com/housepower/clickhouse_sinker/model"
	"github.com/housepower/clickhouse_sinker/util"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"

	_ "github.com/ClickHouse/clickhouse-go/v2"
)

type Commit struct {
	group    string
	offsets  []byte
	wg       *sync.WaitGroup
	consumer *Consumer
}

type Consumer struct {
	sinker      *Sinker
	inputer     *input.KafkaFranz
	tasks       sync.Map
	cfgs        []*config.TaskConfig
	fetches     chan []*kgo.Record
	processWg   sync.WaitGroup
	stopProcess chan struct{}
	state       atomic.Uint32

	recMap map[string]map[int32]int64 // committed RecMap
	recMux sync.Mutex

	numFlying  int32
	mux        sync.Mutex
	commitDone *sync.Cond
}

const (
	MaxCountInBuf  = 1 << 27
	MaxParallelism = 10
)

func newConsumer(s *Sinker) *Consumer {
	c := &Consumer{
		sinker:      s,
		numFlying:   0,
		stopProcess: make(chan struct{}),
		fetches:     make(chan []*kgo.Record),
		recMap:      make(map[string]map[int32]int64),
	}
	c.state.Store(util.StateStopped)
	c.commitDone = sync.NewCond(&c.mux)
	return c
}

func (c *Consumer) start() {
	c.inputer = input.NewKafkaFranz()
	c.state.Store(util.StateRunning)
	if err := c.inputer.Init(c.sinker.curCfg, c.cfgs, c.fetches, c.cleanupFn); err == nil {
		go c.inputer.Run()
		go c.processFetch()
	} else {
		util.Logger.Fatal("failed to init consumer", zap.String("consumer", c.cfgs[0].ConsumerGroup), zap.Error(err))
	}
}

func (c *Consumer) stop(force bool) (err error) {
	c.state.Store(util.StateStopped)

	// stop the processFetch routine, make sure no more input to the- commit chan & writing pool
	c.stopProcess <- struct{}{}
	c.processWg.Wait()
	err = c.inputer.Stop()
	return err
}

func (c *Consumer) restart() {
	if err := c.stop(false); err != nil {
		util.Logger.Fatal("failed to restart consumer group", zap.String("group", c.cfgs[0].ConsumerGroup))
	}
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
		util.Logger.Debug("draining flying pending commits", zap.String("consumergroup", c.cfgs[0].ConsumerGroup), zap.Int32("pending", c.numFlying))
		c.commitDone.Wait()
	}
	c.mux.Unlock()
}

func (c *Consumer) processFetch() {
	c.processWg.Add(1)
	defer c.processWg.Done()
	recMap := make(map[string]map[int32]int64)
	var bufLength, bufThreshold int64

	flushFn := func() {
		c.recMux.Lock()
		ok := reflect.DeepEqual(recMap, c.recMap)
		c.recMux.Unlock()
		if ok {
			return
		}
		var wg sync.WaitGroup
		c.tasks.Range(func(key, value any) bool {
			// flush to shard, ck
			value.(*Service).sharder.Flush(&wg)
			return true
		})
		bufLength = 0
		off, err := json.Marshal(recMap)
		if err != nil {
			return
		}
		c.mux.Lock()
		c.numFlying++
		c.mux.Unlock()
		c.sinker.commits <- &Commit{group: c.cfgs[0].ConsumerGroup, offsets: off, wg: &wg, consumer: c}
	}

	for _, it := range c.cfgs {
		bufThreshold += int64(it.BufferSize)
	}
	bufThreshold = bufThreshold * int64(len(c.sinker.curCfg.Clickhouse.Hosts)) * 4 / 5
	if bufThreshold > MaxCountInBuf {
		bufThreshold = MaxCountInBuf
	}

	ticker := time.NewTicker(time.Duration(c.cfgs[0].FlushInterval) * time.Second)
	defer ticker.Stop()
	for {
		select {
		case fetch := <-c.fetches:
			if c.state.Load() == util.StateStopped {
				continue
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
							if (tablename != "" && tsk.taskCfg.TableName == tablename) || tsk.taskCfg.Topic == rec.Topic {
								bufLength++
								if e := tsk.Put(msg, flushFn); e != nil {
									atomic.StoreInt64(&done, items)
									err = e
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
				for i, it := range fetch {
					if recMap[it.Topic] == nil {
						recMap[it.Topic] = make(map[int32]int64)
					}
					recMap[it.Topic][it.Partition] = it.Offset
					fetch[i] = nil
				}
			}

			if bufLength > bufThreshold {
				flushFn()
			}
		case <-ticker.C:
			flushFn()
		case <-c.stopProcess:
			flushFn()
			util.Logger.Info("stopped processing loop", zap.String("group", c.cfgs[0].ConsumerGroup))
			return
		}
	}
}
