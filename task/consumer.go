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
	offsets  map[string]map[int32]int64
	wg       *sync.WaitGroup
	consumer *Consumer
}

type Consumer struct {
	sinker    *Sinker
	inputer   *input.KafkaFranz
	tasks     sync.Map
	grpConfig *config.GroupConfig
	fetchesCh chan *kgo.Fetches
	processWg sync.WaitGroup
	stopCh    chan struct{}
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
		stopCh:    make(chan struct{}),
		fetchesCh: make(chan *kgo.Fetches),
	}
	c.state.Store(util.StateStopped)
	c.commitDone = sync.NewCond(&c.mux)
	return c
}

func (c *Consumer) addTask(tsk *Service) {
	c.tasks.Store(tsk.taskCfg.Name, tsk)
}

func (c *Consumer) start() {
	c.inputer = input.NewKafkaFranz()
	c.state.Store(util.StateRunning)
	if err := c.inputer.Init(c.sinker.curCfg, c.grpConfig, c.fetchesCh, c.cleanupFn); err == nil {
		go c.inputer.Run()
		go c.processFetch()
	} else {
		util.Logger.Fatal("failed to init consumer", zap.String("consumer", c.grpConfig.Name), zap.Error(err))
	}
}

func (c *Consumer) stop() (err error) {
	if c.state.Load() == util.StateStopped {
		return
	}
	c.state.Store(util.StateStopped)

	// stop the processFetch routine, make sure no more input to the commit chan & writing pool
	c.stopCh <- struct{}{}
	c.processWg.Wait()
	err = c.inputer.Stop()
	return err
}

func (c *Consumer) restart() {
	if err := c.stop(); err != nil {
		util.Logger.Fatal("failed to restart consumer group", zap.String("group", c.grpConfig.Name), zap.Error(err))
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
	c.stopCh <- struct{}{}
	c.processWg.Wait()
	go c.processFetch()
}

func (c *Consumer) processFetch() {
	c.processWg.Add(1)
	defer c.processWg.Done()
	recMap := make(map[string]map[int32]int64)
	var bufLength int

	flushFn := func() {
		if len(recMap) == 0 {
			return
		}
		var wg sync.WaitGroup
		c.tasks.Range(func(key, value any) bool {
			// flush to shard, ck
			value.(*Service).sharder.Flush(&wg)
			return true
		})
		bufLength = 0

		c.mux.Lock()
		c.numFlying++
		c.mux.Unlock()
		c.sinker.commitsCh <- &Commit{group: c.grpConfig.Name, offsets: recMap, wg: &wg, consumer: c}
		recMap = make(map[string]map[int32]int64)
	}

	bufThreshold := c.grpConfig.BufferSize * len(c.sinker.curCfg.Clickhouse.Hosts) * 4 / 5
	if bufThreshold > MaxCountInBuf {
		bufThreshold = MaxCountInBuf
	}

	ticker := time.NewTicker(time.Duration(c.grpConfig.FlushInterval) * time.Second)
	defer ticker.Stop()
	for {
		select {
		case fetches := <-c.fetchesCh:
			if c.state.Load() == util.StateStopped {
				continue
			}

			fetch := fetches.Records()
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
				for _, f := range *fetches {
					for i := range f.Topics {
						ft := &f.Topics[i]
						if recMap[ft.Topic] == nil {
							recMap[ft.Topic] = make(map[int32]int64)
						}
						for j := range ft.Partitions {
							fpr := ft.Partitions[j].Records
							if len(fpr) == 0 {
								continue
							}
							lastOff := fpr[len(fpr)-1].Offset

							old, ok := recMap[ft.Topic][ft.Partitions[j].Partition]
							if !ok || old < lastOff {
								recMap[ft.Topic][ft.Partitions[j].Partition] = lastOff
							}
						}
					}
				}
			}

			if bufLength > bufThreshold {
				flushFn()
				ticker.Reset(time.Duration(c.grpConfig.FlushInterval) * time.Second)
			}
		case <-ticker.C:
			flushFn()
		case <-c.stopCh:
			flushFn()
			util.Logger.Info("stopped processing loop", zap.String("group", c.grpConfig.Name))
			return
		}
	}
}
