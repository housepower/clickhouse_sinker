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
	"os"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/google/uuid"
	"github.com/housepower/clickhouse_sinker/config"
	cm "github.com/housepower/clickhouse_sinker/config_manager"
	"github.com/housepower/clickhouse_sinker/model"
	"github.com/housepower/clickhouse_sinker/output"
	"github.com/housepower/clickhouse_sinker/pool"
	"github.com/housepower/clickhouse_sinker/statistics"
	"github.com/housepower/clickhouse_sinker/util"
	"github.com/pkg/errors"
	"go.uber.org/zap"
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

	seriesQuotas sync.Map
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
			util.Logger.Error("failed to initialize connection to the specified push gateway address", zap.Error(err))
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
		ha := ""
		if s.cmdOps.NacosServiceName != "" {
			ha = s.httpAddr
		}
		if err = newCfg.Normallize(true, ha); err != nil {
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
					newGroup := newConsumer(s, c.grpConfig)
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
				ha := ""
				if s.cmdOps.NacosServiceName != "" {
					ha = s.httpAddr
				}
				if err = newCfg.Normallize(true, ha); err != nil {
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
					newGroup := newConsumer(s, c.grpConfig)
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

	if len(s.consumers) == 0 && s.cmdOps.NacosServiceName != "" {
		util.Logger.Warn("No task fetched from Nacos, make sure the program is running with correct commandline option!")
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
	s.curCfg = newCfg
	for group, grpCfg := range newCfg.Groups {
		c := newConsumer(s, grpCfg)
		for _, tsk := range grpCfg.Configs {
			task := NewTaskService(newCfg, tsk, c)
			if err = task.Init(); err != nil {
				return
			}
		}
		s.consumers[group] = c
	}

	if err = s.loadBmSeries(); err != nil {
		return
	}
	for _, c := range s.consumers {
		c.start()
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
		s.curCfg = newCfg
		for group, grpCfg := range newCfg.Groups {
			c := newConsumer(s, grpCfg)
			for _, tsk := range grpCfg.Configs {
				task := NewTaskService(newCfg, tsk, c)
				if err = task.Init(); err != nil {
					return
				}
				tasksToStart = append(tasksToStart, tsk.Name)
			}
			s.consumers[group] = c
		}

		if err = s.loadBmSeries(); err != nil {
			return
		}
		for _, c := range s.consumers {
			c.start()
		}

		util.Logger.Info("started tasks", zap.Reflect("tasks", tasksToStart))
	} else if !reflect.DeepEqual(newCfg.Tasks, s.curCfg.Tasks) || !reflect.DeepEqual(newCfg.Assignment.Map, s.curCfg.Assignment.Map) {
		// Find the consumer difference
		var deleteConsumers []string
		for name, c := range s.consumers {
			var group *config.GroupConfig
			var ok bool
			if group, ok = newCfg.Groups[name]; !ok {
				deleteConsumers = append(deleteConsumers, name)
			} else {
				if len(c.grpConfig.Topics) != len(group.Topics) {
					deleteConsumers = append(deleteConsumers, name)
				} else {
					sort.Strings(c.grpConfig.Topics)
					sort.Strings(group.Topics)
					if !reflect.DeepEqual(c.grpConfig.Topics, group.Topics) ||
						c.grpConfig.BufferSize != group.BufferSize {
						deleteConsumers = append(deleteConsumers, name)
					} else {
						// apply TaskConfig Change
						for _, tCfg := range group.Configs {
							if !reflect.DeepEqual(tCfg, s.consumers[name].grpConfig.Configs[tCfg.Name]) {
								task := NewTaskService(newCfg, tCfg, s.consumers[name])
								if task.taskCfg.PrometheusSchema {
									// stop the task for reloading the bmseries
									s.consumers[name].stop()
								}
								if err = task.Init(); err != nil {
									return
								}
							}
						}
						c.updateGroupConfig(group)
					}
				}
			}
		}
		// 1) stop consumers no longer with newcfg
		var wg sync.WaitGroup
		for _, v := range deleteConsumers {
			c := s.consumers[v]
			if c.state.Load() == util.StateRunning {
				wg.Add(1)
				go func(c *Consumer) {
					c.stop()
					wg.Done()
				}(c)
			}
			delete(s.consumers, v)
		}
		wg.Wait()

		// 2) fire up new consumers
		// Record the new config
		s.curCfg = newCfg
		for _, v := range newCfg.Groups {
			if s.consumers[v.Name] == nil {
				c := newConsumer(s, v)
				for _, tCfg := range v.Configs {
					task := NewTaskService(newCfg, tCfg, c)
					if err = task.Init(); err != nil {
						return
					}
				}
				s.consumers[v.Name] = c
			}
		}

		if err = s.loadBmSeries(); err != nil {
			return
		}
		for _, c := range s.consumers {
			c.start()
		}
	}
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
			LOOP:
				for i, value := range com.offsets {
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
						} else {
							statistics.ConsumeOffsets.WithLabelValues(com.consumer.grpConfig.Name, i, strconv.Itoa(int(k))).Set(float64(v))
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

func (s *Sinker) loadBmSeries() (err error) {
	// series table could be shared between multiple tasks
	tables := make(map[string][]*Service)
	for _, c := range s.consumers {
		c.tasks.Range(func(key, value any) bool {
			k := value.(*Service).clickhouse.GetSeriesQuotaKey()
			if k != "" {
				tables[k] = append(tables[k], value.(*Service))
			}
			return true
		})
	}

	// delete the seriesQuota which no longer being used
	s.seriesQuotas.Range(func(key, value any) bool {
		k := key.(string)
		if _, ok := tables[k]; !ok {
			s.seriesQuotas.Delete(k)
			util.Logger.Info("dropping seriesQuota", zap.String("series table", k))
		}
		return true
	})

	// initialize seriesQuota for new series tables
	for k, v := range tables {
		var query, dbname, mergetable string
		var conn clickhouse.Conn
		sq := &model.SeriesQuota{}
		mysq := sq
		if s, ok := s.seriesQuotas.LoadOrStore(k, sq); ok {
			q := s.(*model.SeriesQuota)
			mysq = q
			for _, svc := range v {
				svc.clickhouse.SetSeriesQuota(q)
			}
			mysq.Lock()
			defer mysq.Unlock()
			if len(mysq.BmSeries) > 0 {
				// only reload the map when the map is empty
				return
			}
		} else {
			var count uint64
			var reg string
			for _, svc := range v {
				r := svc.clickhouse.GetDistMetricTable()
				if r != "" {
					reg += ("^" + r + "$|")
				}
			}
			reg = reg[:len(reg)-1]
			mergetable = strings.ReplaceAll(k+uuid.New().String(), "-", "_")
			dbname = strings.Split(k, ".")[0]

			query = fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s as %s ENGINE=Merge('%s', '%s')`,
				mergetable, v[0].clickhouse.GetDistMetricTable(), dbname, reg)

			sc := pool.GetShardConn(0)
			if conn, _, err = sc.NextGoodReplica(0); err != nil {
				return
			}
			output.Execute(conn, context.Background(), output.EXEC, query, []zap.Field{zap.String("task", v[0].taskCfg.Name)})

			query = fmt.Sprintf(`WITH (SELECT max(timestamp) FROM %s) AS m
			SELECT count() FROM %s FINAL WHERE __series_id GLOBAL IN (
			SELECT DISTINCT __series_id FROM %s WHERE timestamp >= addDays(m, -1));`, mergetable, k, mergetable)
			output.Execute(conn, context.Background(), output.QUERYROW, query, []zap.Field{zap.String("task", v[0].taskCfg.Name)}).(driver.Row).Scan(&count)

			mysq.Lock()
			mysq.BmSeries = make(map[int64]int64, count)
			mysq.NextResetQuota = time.Now().Add(10 * time.Second)
			mysq.Unlock()
			for _, svc := range v {
				svc.clickhouse.SetSeriesQuota(mysq)
			}
		}

		query = fmt.Sprintf(`WITH (SELECT max(timestamp) FROM %s) AS m
		SELECT toInt64(__series_id) AS sid, toInt64(__mgmt_id) AS mid FROM %s FINAL WHERE sid GLOBAL IN (
		SELECT DISTINCT toInt64(__series_id) FROM %s WHERE timestamp >= addDays(m, -1)
		) ORDER BY sid;`, mergetable, k, mergetable)
		rs := output.Execute(conn, context.Background(), output.QUERY, query, []zap.Field{zap.String("task", v[0].taskCfg.Name)}).(driver.Rows)
		defer rs.Close()

		mysq.Lock()
		defer mysq.Unlock()
		var seriesID, mgmtID int64
		for rs.Next() {
			if err = rs.Scan(&seriesID, &mgmtID); err != nil {
				return errors.Wrapf(err, "")
			}
			mysq.BmSeries[seriesID] = mgmtID
		}
		util.Logger.Info(fmt.Sprintf("loaded %d series from %v", len(mysq.BmSeries), k))

		query = fmt.Sprintf(`DROP TABLE IF EXISTS %s `, mergetable)
		output.Execute(conn, context.Background(), output.EXEC, query, []zap.Field{zap.String("task", v[0].taskCfg.Name)})
	}

	return
}
