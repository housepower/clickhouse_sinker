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
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/housepower/clickhouse_sinker/config"
	"github.com/housepower/clickhouse_sinker/model"
	"github.com/housepower/clickhouse_sinker/output"
	"github.com/housepower/clickhouse_sinker/parser"
	"github.com/housepower/clickhouse_sinker/statistics"
	"github.com/housepower/clickhouse_sinker/util"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

// TaskService holds the configuration for each task
type Service struct {
	clickhouse *output.ClickHouse
	pp         *parser.Pool
	taskCfg    *config.TaskConfig
	whiteList  *regexp.Regexp
	blackList  *regexp.Regexp
	lblBlkList *regexp.Regexp
	dims       []*model.ColumnWithType
	numDims    int

	idxSerID int
	nameKey  string

	knownKeys  sync.Map
	newKeys    sync.Map
	warnKeys   sync.Map
	cntNewKeys int32 // size of newKeys

	sharder  *Sharder
	limiter1 *rate.Limiter
	limiter2 *rate.Limiter
	consumer *Consumer
}

// cloneTask create a new task by steal members from s instead of creating a new one
func cloneTask(s *Service, newGroup *Consumer) (service *Service) {
	service = &Service{
		clickhouse: s.clickhouse,
		pp:         s.pp,
		taskCfg:    s.taskCfg,
		consumer:   s.consumer,
		whiteList:  s.whiteList,
		blackList:  s.blackList,
		lblBlkList: s.lblBlkList,
	}
	if newGroup != nil {
		service.consumer = newGroup
	}
	if err := service.Init(); err != nil {
		util.Logger.Fatal("failed to clone task", zap.String("group", service.taskCfg.ConsumerGroup), zap.String("task", service.taskCfg.Name), zap.Error(err))
	}

	return
}

// NewTaskService creates an instance of new tasks with kafka, clickhouse and paser instances
func NewTaskService(cfg *config.Config, taskCfg *config.TaskConfig, c *Consumer) (service *Service) {
	ck := output.NewClickHouse(cfg, taskCfg)
	pp, _ := parser.NewParserPool(taskCfg.Parser, taskCfg.CsvFormat, taskCfg.Delimiter, taskCfg.TimeZone, taskCfg.TimeUnit)
	service = &Service{
		clickhouse: ck,
		pp:         pp,
		taskCfg:    taskCfg,
		consumer:   c,
	}
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

// Init initializes the kafak and clickhouse task associated with this service
func (service *Service) Init() (err error) {
	taskCfg := service.taskCfg
	util.Logger.Info("task initializing", zap.String("task", taskCfg.Name))
	if err = service.clickhouse.Init(); err != nil {
		return
	}

	service.dims = service.clickhouse.Dims
	service.numDims = len(service.dims)
	service.idxSerID = service.clickhouse.IdxSerID
	service.nameKey = service.clickhouse.NameKey
	service.limiter1 = rate.NewLimiter(rate.Every(10*time.Second), 1)
	service.limiter2 = rate.NewLimiter(rate.Every(10*time.Second), 1)

	if service.sharder, err = NewSharder(service); err != nil {
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
	service.consumer.addTask(service)

	return
}

func (service *Service) Put(msg *model.InputMessage, flushFn func()) error {
	taskCfg := service.taskCfg
	statistics.ConsumeMsgsTotal.WithLabelValues(taskCfg.Name).Inc()
	var err error
	var row *model.Row
	var foundNewKeys bool
	var metric model.Metric

	p := service.pp.Get()
	if metric, err = p.Parse(msg.Value); err != nil {
		// directly return, ignore the row with parsing errors
		statistics.ParseMsgsErrorTotal.WithLabelValues(taskCfg.Name).Inc()
		if service.limiter1.Allow() {
			util.Logger.Error(fmt.Sprintf("failed to parse message(topic %v, partition %d, offset %v)",
				msg.Topic, msg.Partition, msg.Offset), zap.String("message value", string(msg.Value)), zap.String("task", taskCfg.Name), zap.Error(err))
		}
		return nil
	} else {
		row = service.metric2Row(metric, msg)
		if taskCfg.DynamicSchema.Enable {
			foundNewKeys = metric.GetNewKeys(&service.knownKeys, &service.newKeys, &service.warnKeys, service.whiteList, service.blackList, msg.Partition, msg.Offset)
		}
	}
	// WARNNING: metric.GetXXX may depend on p. Don't call them after p been freed.
	service.pp.Put(p)

	if foundNewKeys {
		cntNewKeys := atomic.AddInt32(&service.cntNewKeys, 1)
		if cntNewKeys == 1 {
			// the first message which contains new keys triggers the following:
			// 1) restart the consumer group
			// 	 1) stop the consumer to prevent blocking other consumers, stop will process until ChangeSchema completed
			// 2) flush the shards
			// 3) apply the schema change.
			// 4) recreate the service
			util.Logger.Warn("new key detected, consumer is going to restart", zap.String("consumer group", service.taskCfg.ConsumerGroup), zap.Error(err))
			service.consumer.state.Store(util.StateStopped)
			go service.consumer.restart()
			flushFn()
			if err = service.clickhouse.ChangeSchema(&service.newKeys); err != nil {
				util.Logger.Fatal("clickhouse.ChangeSchema failed", zap.String("task", taskCfg.Name), zap.Error(err))
			}
			cloneTask(service, nil)

			return fmt.Errorf("consumer restart required due to new key")
		}
	}

	if atomic.LoadInt32(&service.cntNewKeys) == 0 && service.consumer.state.Load() == util.StateRunning {
		msgRow := model.MsgRow{Msg: msg, Row: row}
		if service.sharder.policy != nil {
			if msgRow.Shard, err = service.sharder.Calc(msgRow.Row); err != nil {
				util.Logger.Fatal("shard number calculation failed", zap.String("task", taskCfg.Name), zap.Error(err))
			}
		} else {
			msgRow.Shard = int(msgRow.Msg.Offset>>17) % service.sharder.shards
		}
		service.sharder.PutElement(&msgRow)
	}

	return nil
}

func (service *Service) metric2Row(metric model.Metric, msg *model.InputMessage) (row *model.Row) {
	row = model.GetRow()
	if service.idxSerID >= 0 {
		var seriesID, mgmtID int64
		var labels []string
		// If some labels are not Prometheus native, ETL shall calculate and pass "__series_id" and "__mgmt_id".
		val := metric.GetInt64("__series_id", false)
		seriesID = val.(int64)
		val = metric.GetInt64("__mgmt_id", false)
		mgmtID = val.(int64)
		for i := 0; i < service.idxSerID; i++ {
			dim := service.dims[i]
			val := model.GetValueByType(metric, dim)
			*row = append(*row, val)
		}
		*row = append(*row, seriesID) // __series_id
		newSeries := service.clickhouse.AllowWriteSeries(seriesID, mgmtID)
		if newSeries {
			*row = append(*row, mgmtID, nil) // __mgmt_id, labels
			for i := service.idxSerID + 3; i < service.numDims; i++ {
				dim := service.dims[i]
				val := model.GetValueByType(metric, dim)
				*row = append(*row, val)
				if val != nil && dim.Type.Type == model.String && dim.Name != service.nameKey && dim.Name != "le" && (service.lblBlkList == nil || !service.lblBlkList.MatchString(dim.Name)) {
					// "labels" JSON excludes "le", so that "labels" can be used as group key for histogram queries.
					labelVal := val.(string)
					labels = append(labels, fmt.Sprintf(`%s: %s`, strconv.Quote(dim.Name), strconv.Quote(labelVal)))
				}
			}
			(*row)[service.idxSerID+2] = fmt.Sprintf("{%s}", strings.Join(labels, ", "))
		}
	} else {
		for _, dim := range service.dims {
			if strings.HasPrefix(dim.Name, "__kafka") {
				if strings.HasSuffix(dim.Name, "_topic") {
					*row = append(*row, msg.Topic)
				} else if strings.HasSuffix(dim.Name, "_partition") {
					*row = append(*row, msg.Partition)
				} else if strings.HasSuffix(dim.Name, "_offset") {
					*row = append(*row, msg.Offset)
				} else if strings.HasSuffix(dim.Name, "_key") {
					*row = append(*row, string(msg.Key))
				} else if strings.HasSuffix(dim.Name, "_timestamp") {
					*row = append(*row, *msg.Timestamp)
				} else {
					*row = append(*row, nil)
				}
			} else {
				val := model.GetValueByType(metric, dim)
				*row = append(*row, val)
			}
		}
	}
	return
}
