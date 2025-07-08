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

	"github.com/viru-tech/clickhouse_sinker/config"
	cm "github.com/viru-tech/clickhouse_sinker/config_manager"
	"github.com/viru-tech/clickhouse_sinker/discovery"
	"github.com/viru-tech/clickhouse_sinker/input"
	"github.com/viru-tech/clickhouse_sinker/model"
	"github.com/viru-tech/clickhouse_sinker/output"
	"github.com/viru-tech/clickhouse_sinker/pool"
	"github.com/viru-tech/clickhouse_sinker/statistics"
	"github.com/viru-tech/clickhouse_sinker/util"
	"go.uber.org/zap"
)

var (
	countSeriesSQL = `SELECT DISTINCT count() FROM %s WHERE %s GLOBAL IN (
	SELECT DISTINCT %s FROM %s WHERE timestamp >= addSeconds(now(), -%d));`
	loadSeriesSQL = `SELECT DISTINCT toInt64(%s) AS sid, toInt64(%s) AS mid FROM %s WHERE sid GLOBAL IN (
	SELECT DISTINCT toInt64(%s) FROM %s WHERE timestamp >= addSeconds(now(), -%d)
	) ORDER BY sid;`
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

	if s.cmdOps.PushGatewayAddrs != "" {
		addrs := strings.Split(s.cmdOps.PushGatewayAddrs, ",")
		s.pusher = statistics.NewPusher(addrs, s.cmdOps.PushInterval, s.httpAddr)
		if err = s.pusher.Init(); err != nil {
			util.Logger.Fatal("failed to initialize connection to the specified push gateway address", zap.Error(err))
			return
		}
		go s.pusher.Run()
	}

	if s.rcm == nil {
		if _, err := os.Stat(s.cmdOps.LocalCfgFile); err != nil {
			util.Logger.Fatal("expect --local-cfg-file or --nacos-dataid")
			return
		}

		if newCfg, err = config.ParseLocalCfgFile(s.cmdOps.LocalCfgFile); err != nil {
			util.Logger.Fatal("config.ParseLocalCfgFile failed", zap.Error(err))
			return
		}

		ha := ""
		if s.cmdOps.NacosServiceName != "" {
			ha = s.httpAddr
		}
		if err = newCfg.Normallize(true, ha, s.cmdOps.Credentials); err != nil {
			util.Logger.Fatal("newCfg.Normallize failed", zap.Error(err))
			return
		}
		if err = s.applyConfig(newCfg); err != nil {
			util.Logger.Fatal("s.applyConfig failed", zap.Error(err))
			return
		}
		pollTicker := time.NewTicker(time.Duration(s.curCfg.Kafka.Properties.MaxPollInterval) * time.Millisecond)
		defer pollTicker.Stop()
		reloadBmSeriesTicker := time.NewTicker(time.Duration(s.curCfg.ReloadSeriesMapInterval) * time.Second)
		defer reloadBmSeriesTicker.Stop()
	LOOP:
		for {
			select {
			case <-s.ctx.Done():
				util.Logger.Info("Sinker.Run quit due to context has been canceled")
				break LOOP
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
			case <-reloadBmSeriesTicker.C:
				needReload := false
				output.SeriesQuotas.Range(func(key, value any) bool {
					needReload = true
					return false
				})
				if needReload {
					util.Logger.Info("offloading out-of-date series record")
					if err = s.reloadBmSeries(); err != nil {
						util.Logger.Error("reloadBmSeries failed", zap.Error(err))
					}
				}
			case <-pollTicker.C:
				consumerName := input.Walk(s.curCfg.Kafka.Properties.MaxPollInterval)
				if consumerName != "" {
					if consumer, ok := s.consumers[consumerName]; ok {
						util.Logger.Warn("consumer restarted because of max.poll.interval.ms changed", zap.String("consumer", consumerName), zap.Int("max.poll.interval.ms", s.curCfg.Kafka.Properties.MaxPollInterval))
						go func() {
							consumer.stop()
							s.consumerRestartCh <- consumer
						}()
					}
				}
			}
		}
	} else {
		if s.cmdOps.NacosServiceName != "" {
			go s.rcm.Run()
		}
		sd := discovery.NewDiscovery(s.curCfg, s.rcm, s.rcm != nil)
		// Golang <-time.After() is not garbage collected before expiry.
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		curInterval := 3600
		reloadBmSeriesTicker := time.NewTicker(time.Second * time.Duration(curInterval))
		defer reloadBmSeriesTicker.Stop()
		pollInterval := config.DefaultMaxPollIntervalMs
		pollTicker := time.NewTicker(time.Duration(pollInterval) * time.Millisecond)
		defer pollTicker.Stop()
		sdInterval := config.DefaultDiscoveryIntervalSec
		sdTicker := time.NewTicker(time.Duration(sdInterval) * time.Second)
		defer sdTicker.Stop()
	WORKLOOP:
		for {
			select {
			case <-s.ctx.Done():
				util.Logger.Info("Sinker.Run quit due to context has been canceled")
				break WORKLOOP
			case <-ticker.C:
				if newCfg, err = s.rcm.GetConfig(); err != nil {
					util.Logger.Error("s.rcm.GetConfig failed", zap.Error(err))
					continue
				}
				ha := ""
				if s.cmdOps.NacosServiceName != "" {
					ha = s.httpAddr
				}
				if err = newCfg.Normallize(true, ha, s.cmdOps.Credentials); err != nil {
					util.Logger.Error("newCfg.Normallize failed", zap.Error(err))
					continue
				}
				if s.curCfg != nil {
					pollInterval = s.curCfg.Kafka.Properties.MaxPollInterval
					sdInterval = s.curCfg.Discovery.CheckInterval
					curInterval = s.curCfg.ReloadSeriesMapInterval
				}
				if err = s.applyConfig(newCfg); err != nil {
					util.Logger.Error("s.applyConfig failed", zap.Error(err))
					continue
				}
				if curInterval != newCfg.Kafka.Properties.MaxPollInterval {
					pollTicker.Reset(time.Duration(newCfg.Kafka.Properties.MaxPollInterval) * time.Millisecond)
				}
				if sdInterval != newCfg.Discovery.CheckInterval {
					sdTicker.Reset(time.Duration(newCfg.Discovery.CheckInterval) * time.Second)
				}
				if curInterval != newCfg.ReloadSeriesMapInterval {
					reloadBmSeriesTicker.Reset(time.Second * time.Duration(newCfg.ReloadSeriesMapInterval))
				}
			case c := <-s.consumerRestartCh:
				// only restart the consumer which was not changed in applyAnotherConfig
				util.Logger.Info("consumer going to restart", zap.String("consumer", c.grpConfig.Name))
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
			case <-reloadBmSeriesTicker.C:
				needReload := false
				output.SeriesQuotas.Range(func(key, value any) bool {
					needReload = true
					return false
				})
				if needReload {
					util.Logger.Info("offloading out-of-date series record")
					if err = s.reloadBmSeries(); err != nil {
						util.Logger.Error("reloadBmSeries failed", zap.Error(err))
					}
				}
			case <-pollTicker.C:
				consumerName := input.Walk(curInterval)
				if consumerName != "" {
					if consumer, ok := s.consumers[consumerName]; ok {
						util.Logger.Warn("consumer restarted because of max.poll.interval.ms changed", zap.String("consumer", consumerName), zap.Int("max.poll.interval.ms", curInterval))
						go func() {
							consumer.stop()
							s.consumerRestartCh <- consumer
						}()
					}
				}
			case <-sdTicker.C:
				sd.SetConfig(*s.curCfg)
				if sd.IsEnabled() {
					if err = sd.GetCKConn(); err == nil {
						if err = sd.Dispatcher(); err != nil {
							util.Logger.Error("sd.Dispatcher failed", zap.Error(err))
						}
					}
				}
			}
		}
	}

	s.exitCh <- struct{}{}
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

	pool.CloseAll()
	util.Logger.Debug("stopped writing pool")
}

func (s *Sinker) applyConfig(newCfg *config.Config) (err error) {
	util.SetLogLevel(newCfg.LogLevel)
	util.SetLogTrace(newCfg.LogTrace)
	util.Rs.SetPoolSize(newCfg.RecordPoolSize)
	util.Rs.Reset()
	if err := util.Gsypt.Unmarshal(&newCfg.Clickhouse); err != nil {
		util.Logger.Error("failed to decrypt config password", zap.Error(err))
		return err
	}
	if s.curCfg == nil {
		// The first time invoking of applyConfig
		err = s.applyFirstConfig(newCfg)
	} else if !reflect.DeepEqual(newCfg.Clickhouse, s.curCfg.Clickhouse) ||
		!reflect.DeepEqual(newCfg.Kafka, s.curCfg.Kafka) ||
		!reflect.DeepEqual(newCfg.Tasks, s.curCfg.Tasks) ||
		!reflect.DeepEqual(newCfg.Assignment.Map, s.curCfg.Assignment.Map) {
		err = s.applyAnotherConfig(newCfg)
	}
	s.curCfg.ActiveSeriesRange = newCfg.ActiveSeriesRange
	s.curCfg.ReloadSeriesMapInterval = newCfg.ReloadSeriesMapInterval

	if len(s.consumers) == 0 && s.cmdOps.NacosServiceName != "" {
		util.Logger.Warn("No task fetched from Nacos, make sure the program is running with correct commandline option!")
	}
	return
}

func (s *Sinker) applyFirstConfig(newCfg *config.Config) (err error) {
	util.Logger.Info("going to apply the first config", zap.Any("config", newCfg))
	// 1. Initialize clickhouse connections
	chCfg := &newCfg.Clickhouse
	if err = pool.InitClusterConn(chCfg); err != nil {
		return
	}

	// 2. Start goroutine pools.
	go s.commitFn()

	// 3. Generate, initialize and run task
	s.curCfg = newCfg
	for group, grpCfg := range newCfg.Groups {
		c := newConsumer(s, grpCfg)
		for _, tsk := range grpCfg.Configs {
			task := NewTaskService(newCfg, tsk, c)
			if err = task.Init(); err != nil {
				util.Logger.Warn("task init failed", zap.String("name", tsk.Name), zap.Error(err))
				continue
			}
		}
		s.consumers[group] = c
	}

	if err = s.initBmSeries(); err != nil {
		return
	}
	for _, c := range s.consumers {
		c.start()
	}

	util.Logger.Info("applied the first config")
	return
}

func (s *Sinker) applyAnotherConfig(newCfg *config.Config) (err error) {
	util.Logger.Info("going to apply another config", zap.Int("number", s.numCfg), zap.Any("config", newCfg))
	if !reflect.DeepEqual(newCfg.Kafka, s.curCfg.Kafka) || !reflect.DeepEqual(newCfg.Clickhouse, s.curCfg.Clickhouse) {
		// 1. Stop tasks gracefully. Wait until all flying data be processed (write to CH and commit to Kafka).
		s.stopAllTasks()

		// 2. Initialize clickhouse connections.
		chCfg := &newCfg.Clickhouse
		if err = pool.InitClusterConn(chCfg); err != nil {
			return
		}

		// 3. Restart goroutine pools.
		maxWorkers := len(newCfg.Clickhouse.Hosts) * newCfg.Clickhouse.MaxOpenConns
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
					util.Logger.Warn("task init failed", zap.String("name", tsk.Name), zap.Error(err))
					continue
				}
				tasksToStart = append(tasksToStart, tsk.Name)
			}
			s.consumers[group] = c
		}

		if err = s.initBmSeries(); err != nil {
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
						util.Logger.Warn("task init failed", zap.String("name", v.Name), zap.Error(err))
						continue
					}
				}
				s.consumers[v.Name] = c
			}
		}

		if err = s.initBmSeries(); err != nil {
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
						if err := c.inputer.CommitMessages(&model.InputMessage{Topic: i, Partition: int(k), Offset: v.End}); err != nil {
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
							statistics.ConsumeOffsets.WithLabelValues(com.consumer.grpConfig.Name, i, strconv.Itoa(int(k))).Set(float64(v.End))
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

func (s *Sinker) initBmSeries() (err error) {
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
	output.SeriesQuotas.Range(func(key, value any) bool {
		k := key.(string)
		if _, ok := tables[k]; !ok {
			output.SeriesQuotas.Delete(k)
			util.Logger.Info("dropping seriesQuota", zap.String("series table", k))
		}
		return true
	})

	var conn *pool.Conn
	if conn, _, err = pool.GetShardConn(0).NextGoodReplica(s.curCfg.Clickhouse.Ctx, 0); err != nil {
		return
	}

	// initialize seriesQuota for series tables
	for k, v := range tables {
		var sq *model.SeriesQuota
		if sqAny, ok := output.SeriesQuotas.Load(k); ok {
			sq = sqAny.(*model.SeriesQuota)
			sq.Lock()
			res := len(sq.BmSeries) > 0
			sq.Unlock()
			if res {
				continue
			}
		}

		seriesMap, err := loadBmSeries(conn, k, v, s.curCfg.ActiveSeriesRange)
		if err != nil {
			return err
		}

		sq.Lock()
		sq.BmSeries = seriesMap
		sq.Birth = time.Now()
		sq.Unlock()
	}

	return
}

func (s *Sinker) reloadBmSeries() (err error) {
	// find the out-of-date SeriesQuota
	sqMap := make(map[string]*model.SeriesQuota)
	now := time.Now()
	output.SeriesQuotas.Range(func(key, value any) bool {
		k := key.(string)
		v := value.(*model.SeriesQuota)
		v.Lock()
		if now.Sub(v.Birth).Seconds() > float64(s.curCfg.ReloadSeriesMapInterval) {
			sqMap[k] = v
		}
		v.Unlock()
		return true
	})

	if len(sqMap) == 0 {
		util.Logger.Info("SeriesMap cache is up to date!")
		return
	}

	// series table could be shared between multiple tasks
	tables := make(map[string][]*Service)
	for _, c := range s.consumers {
		c.tasks.Range(func(key, value any) bool {
			k := value.(*Service).clickhouse.GetSeriesQuotaKey()
			if _, ok := sqMap[k]; ok {
				tables[k] = append(tables[k], value.(*Service))
			}
			return true
		})
	}

	if len(tables) == 0 {
		return
	}

	var conn *pool.Conn
	if conn, _, err = pool.GetShardConn(0).NextGoodReplica(s.curCfg.Clickhouse.Ctx, 0); err != nil {
		return
	}

	// reload seriesQuotas which is out-of-date
	for k, v := range tables {
		seriesMap, err := loadBmSeries(conn, k, v, s.curCfg.ActiveSeriesRange)
		if err != nil {
			return err
		}

		sq := sqMap[k]
		sq.Lock()
		/*
			FIXME: BmSeries and seriesMap will stored in memory at the same time
		*/
		sq.BmSeries = seriesMap
		sq.Birth = time.Now()
		sq.Unlock()
		util.Logger.Info(fmt.Sprintf("reloaded %d series from %v, SeriesMap cache is up to date!", len(seriesMap), k))
	}

	return
}

func loadBmSeries(conn *pool.Conn, sqKey string, tasks []*Service, activeSeriesRange int) (result map[int64]int64, err error) {
	// merge all metric tables to get the latest timestamp
	// old bmseries record won't be loaded into memory to avoid OOM
	var dimSerID, dimMgmtID string

	/* FIXME: We can't assume that the series_id and mgmt_id of all tasks are the same,
	because some tasks may be old and some are newly created
	*/
	svc := tasks[0]
	dbname := strings.Split(sqKey, ".")[0]
	dimSerID, dimMgmtID = svc.clickhouse.DimSerID, svc.clickhouse.DimMgmtID
	metricTable := dbname + "." + svc.clickhouse.GetMetricTable()

	var count uint64
	query := fmt.Sprintf(countSeriesSQL, sqKey, dimSerID, dimSerID, metricTable, activeSeriesRange)
	util.Logger.Info(fmt.Sprintf("executing sql=> %s", query), zap.String("task", tasks[0].taskCfg.Name))
	if err = conn.QueryRow(query).Scan(&count); err != nil {
		return
	}
	if count < 1<<20 {
		count = 1 << 20 //100w allocate 38M memory
	}
	seriesMap := make(map[int64]int64, count)
	query = fmt.Sprintf(loadSeriesSQL, dimSerID, dimMgmtID, sqKey, dimSerID, metricTable, activeSeriesRange)
	util.Logger.Info(fmt.Sprintf("executing sql=> %s", query), zap.String("task", tasks[0].taskCfg.Name))
	rs, err := conn.Query(query)
	if err != nil {
		return nil, err
	}
	defer rs.Close()

	var seriesID, mgmtID int64
	for rs.Next() {
		if err = rs.Scan(&seriesID, &mgmtID); err != nil {
			return nil, err
		}
		seriesMap[seriesID] = mgmtID
	}
	util.Logger.Info(fmt.Sprintf("loaded %d series for %s", len(seriesMap), sqKey))
	return seriesMap, err
}
