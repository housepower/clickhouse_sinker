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
	"encoding/json"
	"os"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/housepower/clickhouse_sinker/config"
	cm "github.com/housepower/clickhouse_sinker/config_manager"
	"github.com/housepower/clickhouse_sinker/model"
	"github.com/housepower/clickhouse_sinker/pool"
	"github.com/housepower/clickhouse_sinker/statistics"
	"github.com/housepower/clickhouse_sinker/util"
	"go.uber.org/zap"

	_ "github.com/ClickHouse/clickhouse-go/v2"
)

// Sinker object maintains number of task for each partition
type Sinker struct {
	curCfg   *config.Config
	cmdOps   *util.CmdOptions
	httpAddr string
	numCfg   int
	pusher   *statistics.Pusher
	rcm      cm.RemoteConfManager
	ctx      context.Context
	cancel   context.CancelFunc

	consumers         map[string]*Consumer
	commitsCh         chan *Commit
	exitCh            chan struct{}
	stopCommitCh      chan struct{}
	consumerRestartCh chan *Consumer
}

// NewSinker get an instance of sinker with the task list
func NewSinker(rcm cm.RemoteConfManager, http string, cmd *util.CmdOptions) *Sinker {
	ctx, cancel := context.WithCancel(context.Background())
	s := &Sinker{
		rcm:               rcm,
		ctx:               ctx,
		cmdOps:            cmd,
		cancel:            cancel,
		commitsCh:         make(chan *Commit, 10),
		exitCh:            make(chan struct{}),
		stopCommitCh:      make(chan struct{}),
		consumerRestartCh: make(chan *Consumer),
		consumers:         make(map[string]*Consumer),
		httpAddr:          http,
	}
	return s
}

func (s *Sinker) Init() (err error) {
	return
}

func (s *Sinker) GetCurrentConfig() *config.Config {
	return s.curCfg
}

// Run is the mainloop to get and apply config
func (s *Sinker) Run() {
	var err error
	var newCfg *config.Config
	defer func() {
		s.exitCh <- struct{}{}
	}()
	if s.cmdOps.PushGatewayAddrs != "" {
		addrs := strings.Split(s.cmdOps.PushGatewayAddrs, ",")
		s.pusher = statistics.NewPusher(addrs, s.cmdOps.PushInterval, s.httpAddr)
		if err = s.pusher.Init(); err != nil {
			return
		}
		go s.pusher.Run()
	}
	if s.rcm == nil {
		if _, err = os.Stat(s.cmdOps.LocalCfgFile); err == nil {
			if newCfg, err = config.ParseLocalCfgFile(s.cmdOps.LocalCfgFile); err != nil {
				util.Logger.Fatal("config.ParseLocalCfgFile failed", zap.Error(err))
				return
			}
		} else {
			util.Logger.Fatal("expect --local-cfg-file or --nacos-dataid")
			return
		}
		if err = newCfg.Normallize(); err != nil {
			util.Logger.Fatal("newCfg.Normallize failed", zap.Error(err))
			return
		}
		if err = s.applyConfig(newCfg); err != nil {
			util.Logger.Fatal("s.applyConfig failed", zap.Error(err))
			return
		}
		for {
			select {
			case <-s.ctx.Done():
				util.Logger.Info("Sinker.Run quit due to context has been canceled")
				return
			case c := <-s.consumerRestartCh:
				// only restart the consumer which was not changed in applyAnotherConfig
				if c == s.consumers[c.grpConfig.Name] {
					newGroup := newConsumer(s)
					s.consumers[c.grpConfig.Name] = newGroup
					c.tasks.Range(func(key, value any) bool {
						cloneTask(value.(*Service), newGroup)
						return true
					})
					newGroup.start()
					util.Logger.Info("consumer restarted because of previous offset commit error",
						zap.String("consumer", c.grpConfig.Name))
				} else {
					util.Logger.Info("consumer restarted when applying another config",
						zap.String("consumer", c.grpConfig.Name))
				}
			}
		}
	} else {
		if s.cmdOps.NacosServiceName != "" {
			go s.rcm.Run()
		}
		// Golang <-time.After() is not garbage collected before expiry.
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-s.ctx.Done():
				util.Logger.Info("Sinker.Run quit due to context has been canceled")
				return
			case <-ticker.C:
				if newCfg, err = s.rcm.GetConfig(); err != nil {
					util.Logger.Error("s.rcm.GetConfig failed", zap.Error(err))
					continue
				}
				if err = newCfg.Normallize(); err != nil {
					util.Logger.Error("newCfg.Normallize failed", zap.Error(err))
					continue
				}
				if err = s.applyConfig(newCfg); err != nil {
					util.Logger.Error("s.applyConfig failed", zap.Error(err))
					continue
				}
			case c := <-s.consumerRestartCh:
				// only restart the consumer which was not changed in applyAnotherConfig
				if c == s.consumers[c.grpConfig.Name] {
					newGroup := newConsumer(s)
					s.consumers[c.grpConfig.Name] = newGroup
					c.tasks.Range(func(key, value any) bool {
						cloneTask(value.(*Service), newGroup)
						return true
					})
					newGroup.start()
					util.Logger.Info("consumer restarted because of previous offset commit error",
						zap.String("consumer", c.grpConfig.Name))
				} else {
					util.Logger.Info("consumer restarted when applying another config",
						zap.String("consumer", c.grpConfig.Name))
				}
			}
		}
	}
}

// Close shutdown task
func (s *Sinker) Close() {
	// 1. Stop rcm
	if s.rcm != nil {
		s.rcm.Stop()
		s.rcm = nil
	}
	// 2. Quit Run mainloop
	s.cancel()
	<-s.exitCh
	// 3. Stop tasks gracefully.
	s.stopAllTasks()
	// 4. Stop pusher
	if s.pusher != nil {
		s.pusher.Stop()
		s.pusher = nil
	}
}

func (s *Sinker) stopAllTasks() {
	// stop the input to prevent further kafka consumption, but will drain all the parsed messages in memory
	var wg sync.WaitGroup
	for _, v := range s.consumers {
		if v.state.Load() == util.StateRunning {
			wg.Add(1)
			go func(c *Consumer) {
				c.stop()
				wg.Done()
			}(v)
		}
	}
	wg.Wait()
	util.Logger.Info("stopped all consumers")

	select {
	case s.stopCommitCh <- struct{}{}:
	default:
	}
	util.Logger.Debug("stopped commit session")

	for name := range s.consumers {
		delete(s.consumers, name)
	}

	if util.GlobalWritingPool != nil {
		util.GlobalWritingPool.StopWait()
	}
	util.Logger.Debug("stopped writing pool")
}

func (s *Sinker) applyConfig(newCfg *config.Config) (err error) {
	util.SetLogLevel(newCfg.LogLevel)
	if s.curCfg == nil {
		// The first time invoking of applyConfig
		err = s.applyFirstConfig(newCfg)
	} else if !reflect.DeepEqual(newCfg.Clickhouse, s.curCfg.Clickhouse) ||
		!reflect.DeepEqual(newCfg.Kafka, s.curCfg.Kafka) ||
		!reflect.DeepEqual(newCfg.Tasks, s.curCfg.Tasks) ||
		!reflect.DeepEqual(newCfg.Assignment.Map, s.curCfg.Assignment.Map) {
		err = s.applyAnotherConfig(newCfg)
	}
	return
}

func (s *Sinker) applyFirstConfig(newCfg *config.Config) (err error) {
	util.Logger.Info("going to apply the first config", zap.Reflect("config", newCfg))
	// 1. Initialize clickhouse connections
	chCfg := &newCfg.Clickhouse
	if err = pool.InitClusterConn(chCfg.Hosts, chCfg.Port, chCfg.DB, chCfg.Username, chCfg.Password,
		chCfg.DsnParams, chCfg.Secure, chCfg.InsecureSkipVerify, chCfg.MaxOpenConns, chCfg.DialTimeout); err != nil {
		return
	}

	// 2. Start goroutine pools.
	util.InitGlobalWritingPool(len(chCfg.Hosts) * chCfg.MaxOpenConns)
	go s.commitFn()

	// 3. Generate, initialize and run task
	for _, taskCfg := range newCfg.Tasks {
		if s.cmdOps.NacosServiceName != "" && !newCfg.IsAssigned(s.httpAddr, taskCfg.Name) {
			continue
		}
		var c *Consumer
		var ok bool
		if c, ok = s.consumers[taskCfg.ConsumerGroup]; !ok {
			c = newConsumer(s)
			s.consumers[taskCfg.ConsumerGroup] = c
		}
		task := NewTaskService(newCfg, taskCfg, c)
		if err = task.Init(); err != nil {
			return
		}
	}

	// 4. Start fetching messages
	s.curCfg = newCfg
	for _, v := range s.consumers {
		v.start()
	}

	util.Logger.Info("applied the first config")
	return
}

func (s *Sinker) applyAnotherConfig(newCfg *config.Config) (err error) {
	util.Logger.Info("going to apply another config", zap.Int("number", s.numCfg), zap.Reflect("config", newCfg))
	if !reflect.DeepEqual(newCfg.Kafka, s.curCfg.Kafka) || !reflect.DeepEqual(newCfg.Clickhouse, s.curCfg.Clickhouse) {
		// 1. Stop tasks gracefully. Wait until all flying data be processed (write to CH and commit to Kafka).
		s.stopAllTasks()
		// 2. Initialize clickhouse connections.
		chCfg := &newCfg.Clickhouse
		if err = pool.InitClusterConn(chCfg.Hosts, chCfg.Port, chCfg.DB, chCfg.Username, chCfg.Password,
			chCfg.DsnParams, chCfg.Secure, chCfg.InsecureSkipVerify, chCfg.MaxOpenConns, chCfg.DialTimeout); err != nil {
			return
		}

		// 3. Restart goroutine pools.
		maxWorkers := len(newCfg.Clickhouse.Hosts) * newCfg.Clickhouse.MaxOpenConns
		util.GlobalWritingPool.Resize(maxWorkers)
		util.GlobalWritingPool.Restart()
		util.Logger.Info("resized writing pool", zap.Int("maxWorkers", maxWorkers))
		go s.commitFn()

		// 4. Generate, initialize and run task
		var tasksToStart []string
		for _, taskCfg := range newCfg.Tasks {
			if s.cmdOps.NacosServiceName != "" && !newCfg.IsAssigned(s.httpAddr, taskCfg.Name) {
				continue
			}
			var c *Consumer
			var ok bool
			if c, ok = s.consumers[taskCfg.ConsumerGroup]; !ok {
				c = newConsumer(s)
				s.consumers[taskCfg.ConsumerGroup] = c
			}
			task := NewTaskService(newCfg, taskCfg, c)
			if err = task.Init(); err != nil {
				return
			}
			tasksToStart = append(tasksToStart, taskCfg.Name)
		}

		// 5. Start fetching messages, only after all the taskCfg loaded to consumer.cfgs
		s.curCfg = newCfg
		for _, v := range s.consumers {
			v.start()
		}

		util.Logger.Info("started tasks", zap.Reflect("tasks", tasksToStart))
	} else if !reflect.DeepEqual(newCfg.Tasks, s.curCfg.Tasks) || !reflect.DeepEqual(newCfg.Assignment.Map, s.curCfg.Assignment.Map) {
		// 1. Find the config difference
		newConsumers := make(map[string]map[string]*config.TaskConfig)
		for _, taskCfg := range newCfg.Tasks {
			if s.cmdOps.NacosServiceName != "" && !newCfg.IsAssigned(s.httpAddr, taskCfg.Name) {
				continue
			}
			var group map[string]*config.TaskConfig
			var ok bool
			if group, ok = newConsumers[taskCfg.ConsumerGroup]; !ok {
				group = make(map[string]*config.TaskConfig)
				newConsumers[taskCfg.ConsumerGroup] = group
			}
			group[taskCfg.Name] = taskCfg
		}

		toCreate := make(map[string]map[string]*config.TaskConfig)
		var deleteConsumers []string
		for name, c := range s.consumers {
			var group map[string]*config.TaskConfig
			var ok bool
			if group, ok = newConsumers[name]; !ok {
				// consumer group no longer with this client, stop it
				go c.stop()
				deleteConsumers = append(deleteConsumers, name)
			} else {
				// find the one that need to be restart
				var groupChanged bool
				if len(group) != len(c.grpConfig.Topics) {
					groupChanged = true
				} else {
					c.tasks.Range(func(key, value any) bool {
						cfg := value.(*Service).taskCfg
						var it *config.TaskConfig
						var ok bool
						if it, ok = group[cfg.Name]; !ok {
							groupChanged = true
							return false
						} else if !reflect.DeepEqual(it, cfg) {
							groupChanged = true
							return false
						}
						return true
					})
				}
				// restart the group accordingly
				if groupChanged {
					go c.stop()
					toCreate[name] = group
				}
				delete(newConsumers, name)
			}
		}
		// create new consumers
		for name, cfgs := range newConsumers {
			toCreate[name] = cfgs
		}
		for _, name := range deleteConsumers {
			delete(s.consumers, name)
		}
		for name, cfgs := range toCreate {
			c := newConsumer(s)
			s.consumers[name] = c
			for _, cfg := range cfgs {
				task := NewTaskService(newCfg, cfg, c)
				if err = task.Init(); err != nil {
					return
				}
			}
			c.start()
		}
	}
	// Record the new config
	s.curCfg = newCfg
	util.Logger.Info("applied another config", zap.Int("number", s.numCfg))
	s.numCfg++
	return
}

func (s *Sinker) commitFn() {
	for {
		select {
		case com := <-s.commitsCh:
			com.wg.Wait()
			c := com.consumer

			if !c.errCommit {
				c.recMux.Lock()
				err := json.Unmarshal(com.offsets, &c.recMap)
				c.recMux.Unlock()
				if err != nil {
					util.Logger.Fatal("Failed to unmarshall offsets", zap.ByteString("offsets", com.offsets), zap.Error(err))
				}
			LOOP:
				for i, value := range c.recMap {
					for k, v := range value {
						if err := c.inputer.CommitMessages(&model.InputMessage{Topic: i, Partition: int(k), Offset: v}); err != nil {
							c.errCommit = true
							// restart the consumer when facing commit error, avoid change the s.consumers outside of s.Run
							// error could be RebalanceInProgress, IllegalGeneration, UnknownMemberID
							go func() {
								c.stop()
								s.consumerRestartCh <- c
							}()
							util.Logger.Warn("Batch.Commit failed, will restart later", zap.Error(err))
							break LOOP
						}
					}
				}
			}
			c.mux.Lock()
			c.numFlying--
			if c.numFlying == 0 {
				c.commitDone.Broadcast()
			}
			c.mux.Unlock()
		case <-s.stopCommitCh:
			util.Logger.Info("stopped committing loop")
			return
		}
	}
}
