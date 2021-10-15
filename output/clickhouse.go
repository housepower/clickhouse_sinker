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

package output

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"math"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/housepower/clickhouse_sinker/config"
	"github.com/housepower/clickhouse_sinker/model"
	"github.com/housepower/clickhouse_sinker/pool"
	"github.com/housepower/clickhouse_sinker/statistics"
	"github.com/housepower/clickhouse_sinker/util"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

var (
	ErrTblNotExist       = errors.Errorf("table doesn't exist")
	selectSQLTemplate    = `select name, type, default_kind from system.columns where database = '%s' and table = '%s'`
	lowCardinalityRegexp = regexp.MustCompile(`LowCardinality\((.+)\)`)

	// https://github.com/ClickHouse/ClickHouse/issues/24036
	// src/Common/ErrorCodes.cpp
	// src/Storages/MergeTree/ReplicatedMergeTreeBlockOutputStream.cpp
	replicaSpecificErrorCodes = []int32{164, 225, 319, 1000} //READONLY, NO_ZOOKEEPER, UNKNOWN_STATUS_OF_INSERT, POCO_EXCEPTION
)

// ClickHouse is an output service consumers from kafka messages
type ClickHouse struct {
	Dims       []*model.ColumnWithType
	Dms        []string
	IdxSerID   int
	idxLabels  []int
	cfg        *config.Config
	taskCfg    *config.TaskConfig
	prepareSQL string
	promSerSQL string
	seriesTbl  string
	bmSeries   *roaring64.Bitmap
	numFlying  int32
	mux        sync.Mutex
	taskDone   *sync.Cond
}

// NewClickHouse new a clickhouse instance
func NewClickHouse(cfg *config.Config, taskCfg *config.TaskConfig) *ClickHouse {
	ck := &ClickHouse{cfg: cfg, taskCfg: taskCfg}
	ck.taskDone = sync.NewCond(&ck.mux)
	return ck
}

// Init the clickhouse intance
func (c *ClickHouse) Init() (err error) {
	return c.initSchema()
}

// Drain drains flying batchs
func (c *ClickHouse) Drain() {
	c.mux.Lock()
	for c.numFlying != 0 {
		c.taskDone.Wait()
	}
	c.mux.Unlock()
}

// Send a batch to clickhouse
func (c *ClickHouse) Send(batch *model.Batch) {
	c.mux.Lock()
	c.numFlying++
	c.mux.Unlock()
	statistics.WritingPoolBacklog.WithLabelValues(c.taskCfg.Name).Inc()
	_ = util.GlobalWritingPool.Submit(func() {
		c.loopWrite(batch)
		c.mux.Lock()
		c.numFlying--
		if c.numFlying == 0 {
			c.taskDone.Broadcast()
		}
		c.mux.Unlock()
		statistics.WritingPoolBacklog.WithLabelValues(c.taskCfg.Name).Dec()
	})
}

// Write a batch to clickhouse
func (c *ClickHouse) write(batch *model.Batch, sc *pool.ShardConn, dbVer *int) (err error) {
	if len(*batch.Rows) == 0 {
		return
	}
	var conn *sql.DB
	if conn, *dbVer, err = sc.NextGoodReplica(*dbVer); err != nil {
		return
	}

	if err = writeRows(c.prepareSQL, *batch.Rows, conn); err != nil {
		return
	}
	if c.IdxSerID >= 0 {
		var seriesRows model.Rows
		var labels []string
		c.mux.Lock()
		for _, row := range *batch.Rows {
			seriesID := (*row)[c.IdxSerID].(uint64)
			if !c.bmSeries.Contains(seriesID) {
				seriesRow := make(model.Row, 2+len(c.idxLabels)) //__series_id, lables, ...
				if labels == nil {
					labels = make([]string, len(c.idxLabels))
				}
				seriesRow[0] = seriesID
				for i, idxLabel := range c.idxLabels {
					seriesRow[2+i] = (*row)[idxLabel]
					labelKey := c.Dims[idxLabel].Name
					labelVal := (*row)[idxLabel].(string)
					labels[i] = fmt.Sprintf(`"%s": "%s"`, labelKey, labelVal)
				}
				seriesRow[1] = fmt.Sprintf("{%s}", strings.Join(labels, ", "))
				seriesRows = append(seriesRows, &seriesRow)
			}
		}
		c.mux.Unlock()
		if len(seriesRows) != 0 {
			if err = writeRows(c.promSerSQL, seriesRows, conn); err != nil {
				return
			}
			c.mux.Lock()
			for _, seriesRow := range seriesRows {
				seriesID := (*seriesRow)[0].(uint64)
				c.bmSeries.Add(seriesID)
			}
			c.mux.Unlock()
		}
	}

	statistics.FlushMsgsTotal.WithLabelValues(c.taskCfg.Name).Add(float64(batch.RealSize))
	return
}

// LoopWrite will dead loop to write the records
func (c *ClickHouse) loopWrite(batch *model.Batch) {
	var err error
	var times int
	var reconnect bool
	var dbVer int
	sc := pool.GetShardConn(batch.BatchIdx)
	for {
		if err = c.write(batch, sc, &dbVer); err == nil {
			if err = batch.Commit(); err == nil {
				return
			}
			// Note: kafka_go and sarama commit give different error when context is cancceled.
			if errors.Is(err, context.Canceled) || errors.Is(err, io.ErrClosedPipe) {
				util.Logger.Warn("Batch.Commit failed due to the context has been cancelled", zap.String("task", c.taskCfg.Name))
				return
			}
			util.Logger.Fatal("Batch.Commit failed with permanent error", zap.String("task", c.taskCfg.Name), zap.Error(err))
		}
		if errors.Is(err, context.Canceled) {
			util.Logger.Info("ClickHouse.write failed due to the context has been cancelled", zap.String("task", c.taskCfg.Name))
			return
		}
		util.Logger.Error("flush batch failed", zap.String("task", c.taskCfg.Name), zap.Int("try", times), zap.Error(err))
		statistics.FlushMsgsErrorTotal.WithLabelValues(c.taskCfg.Name).Add(float64(batch.RealSize))
		times++
		reconnect = shouldReconnect(err, sc)
		if reconnect && (c.cfg.Clickhouse.RetryTimes <= 0 || times < c.cfg.Clickhouse.RetryTimes) {
			time.Sleep(10 * time.Second)
		} else {
			util.Logger.Fatal("ClickHouse.loopWrite failed", zap.String("task", c.taskCfg.Name))
		}
	}
}

func (c *ClickHouse) initBmSeries(conn *sql.DB) (err error) {
	c.bmSeries = roaring64.New()
	allSeriesSQL := fmt.Sprintf("SELECT __series_id FROM %s.dist_%s", c.cfg.Clickhouse.DB, c.seriesTbl)
	var rs *sql.Rows
	var seriesID uint64
	if rs, err = conn.Query(allSeriesSQL); err != nil {
		err = errors.Wrapf(err, "")
		return err
	}
	defer rs.Close()
	for rs.Next() {
		if err = rs.Scan(&seriesID); err != nil {
			err = errors.Wrapf(err, "")
			return err
		}
		c.bmSeries.Add(seriesID)
	}
	return
}

func (c *ClickHouse) initSchema() (err error) {
	c.IdxSerID = -1
	if c.taskCfg.AutoSchema {
		sc := pool.GetShardConn(0)
		var conn *sql.DB
		if conn, _, err = sc.NextGoodReplica(0); err != nil {
			return
		}
		if c.Dims, err = getDims(c.cfg.Clickhouse.DB, c.taskCfg.TableName, c.taskCfg.ExcludeColumns, conn); err != nil {
			return
		}
		for i, dim := range c.Dims {
			if dim.Name == "__series_id" {
				c.IdxSerID = i
			}
		}
		if c.IdxSerID >= 0 {
			expSeriesDims := []*model.ColumnWithType{
				{Name: "__series_id", Type: model.Int},
				{Name: "labels", Type: model.String},
			}
			expSeriesDms := "__series_id UInt64, labels String"
			expSeriesDmsQuoted := []string{"__series_id", "labels"}
			for i, dim := range c.Dims {
				if dim.Type == model.String {
					c.idxLabels = append(c.idxLabels, i)
					expSeriesDims = append(expSeriesDims, dim)
					if dim.Nullable {
						expSeriesDms += fmt.Sprintf(", `%s` Nullable(String)", dim.Name)
					} else {
						expSeriesDms += fmt.Sprintf(", `%s` String", dim.Name)
					}
					expSeriesDmsQuoted = append(expSeriesDmsQuoted, fmt.Sprintf("`%s`", dim.Name))
				}
			}
			// Check the series table schema
			c.seriesTbl = c.taskCfg.TableName + "_series"
			var seriesDims []*model.ColumnWithType
			if seriesDims, err = getDims(c.cfg.Clickhouse.DB, c.taskCfg.TableName, c.taskCfg.ExcludeColumns, conn); err != nil {
				if errors.Is(err, ErrTblNotExist) {
					err = errors.Wrapf(err, "Please create table %s(and distributed table) with the following columns: %s", c.seriesTbl, expSeriesDms)
					return
				}
				return
			}
			if len(seriesDims) != 2+len(c.idxLabels) {
				err = errors.Errorf("Missed %d columns in %s. Please modify table %s(and distributed table) to the following columns: %s", 2+len(c.idxLabels)-len(seriesDims), c.seriesTbl, c.seriesTbl, expSeriesDms)
				return
			}
			for i, idxLabel := range c.idxLabels {
				dim := c.Dims[idxLabel]
				seriesDim := seriesDims[i]
				if seriesDim.Name != dim.Name || seriesDim.Type != model.String {
					err = errors.Errorf("Column %s(#%d) in %s doesn't math expection. Please modify table %s(and distributed table) to the following columns: %s", seriesDim.Name, i, c.seriesTbl, c.seriesTbl, expSeriesDms)
					return
				}
			}
			// Generate SQL for series INSERT
			var params = make([]string, len(c.Dims))
			for i := range params {
				params[i] = "?"
			}
			c.promSerSQL = "INSERT INTO " + c.cfg.Clickhouse.DB + "." + c.seriesTbl + " (" + strings.Join(expSeriesDmsQuoted, ",") + ") " +
				"VALUES (" + strings.Join(params, ",") + ")"
			// Initialize bmSeries
			if err = c.initBmSeries(conn); err != nil {
				return
			}
		}
	} else {
		c.Dims = make([]*model.ColumnWithType, 0)
		for _, dim := range c.taskCfg.Dims {
			tp, nullable := model.WhichType(dim.Type)
			c.Dims = append(c.Dims, &model.ColumnWithType{
				Name:       dim.Name,
				Type:       tp,
				Nullable:   nullable,
				SourceName: dim.SourceName,
			})
		}
	}
	// Generate SQL for INSERT
	c.Dms = make([]string, 0, len(c.Dims))
	quotedDms := make([]string, 0, len(c.Dims))
	for _, d := range c.Dims {
		c.Dms = append(c.Dms, d.Name)
		quotedDms = append(quotedDms, fmt.Sprintf("`%s`", d.Name))
	}
	var params = make([]string, len(c.Dims))
	for i := range params {
		params[i] = "?"
	}
	c.prepareSQL = "INSERT INTO " + c.cfg.Clickhouse.DB + "." + c.taskCfg.TableName + " (" + strings.Join(quotedDms, ",") + ") " +
		"VALUES (" + strings.Join(params, ",") + ")"
	util.Logger.Info(fmt.Sprintf("Prepare sql=> %s", c.prepareSQL), zap.String("task", c.taskCfg.Name))
	return nil
}

func (c *ClickHouse) ChangeSchema(newKeys *sync.Map) (err error) {
	var queries []string
	var onCluster string
	taskCfg := c.taskCfg
	chCfg := &c.cfg.Clickhouse
	if chCfg.Cluster != "" {
		onCluster = fmt.Sprintf("ON CLUSTER %s", chCfg.Cluster)
	}
	maxDims := math.MaxInt16
	if taskCfg.DynamicSchema.MaxDims > 0 {
		maxDims = taskCfg.DynamicSchema.MaxDims
	}
	newKeysQuota := maxDims - len(c.Dims)
	if newKeysQuota <= 0 {
		util.Logger.Warn("number of columns reaches upper limit", zap.Int("limit", maxDims), zap.Int("current", len(c.Dims)))
		return
	}
	var i int
	newKeys.Range(func(key, value interface{}) bool {
		i++
		if i > newKeysQuota {
			util.Logger.Warn("number of columns reaches upper limit", zap.Int("limit", maxDims), zap.Int("current", i))
			return false
		}
		strKey, _ := key.(string)
		intVal := value.(int)
		var strVal string
		switch intVal {
		case model.Int:
			strVal = "Nullable(Int64)"
		case model.Float:
			strVal = "Nullable(Float64)"
		case model.String:
			strVal = "Nullable(String)"
		case model.DateTime:
			strVal = "Nullable(DateTime64(3))"
		case model.IntArray:
			strVal = "Array(Int64)"
		case model.FloatArray:
			strVal = "Array(Float64)"
		case model.StringArray:
			strVal = "Array(String)"
		case model.DateTimeArray:
			strVal = "Array(DateTime64(3))"
		default:
			err = errors.Errorf("%s: BUG: unsupported column type %s", taskCfg.Name, strVal)
			return false
		}
		query := fmt.Sprintf("ALTER TABLE %s.%s %s ADD COLUMN IF NOT EXISTS `%s` %s", chCfg.DB, taskCfg.TableName, onCluster, strKey, strVal)
		queries = append(queries, query)
		if c.IdxSerID >= 0 && intVal == model.String {
			query := fmt.Sprintf("ALTER TABLE %s.%s %s ADD COLUMN IF NOT EXISTS `%s` %s", chCfg.DB, c.seriesTbl, onCluster, strKey, strVal)
			queries = append(queries, query)
		}
		return true
	})
	if err != nil {
		return
	}
	sort.Strings(queries)
	if chCfg.Cluster != "" {
		for _, table := range []string{taskCfg.TableName, c.seriesTbl} {
			var distTbls []string
			if distTbls, err = c.getDistTbls(table); err != nil {
				return
			}
			for _, distTbl := range distTbls {
				queries = append(queries, fmt.Sprintf("DROP TABLE IF EXISTS %s.%s %s", chCfg.DB, distTbl, onCluster))
				queries = append(queries, fmt.Sprintf("CREATE TABLE %s.%s %s AS %s ENGINE = Distributed(%s, %s, %s);",
					chCfg.DB, distTbl, onCluster, table,
					chCfg.Cluster, chCfg.DB, table))
			}
		}
	}
	sc := pool.GetShardConn(0)
	var conn *sql.DB
	if conn, _, err = sc.NextGoodReplica(0); err != nil {
		return
	}
	for _, query := range queries {
		util.Logger.Info(fmt.Sprintf("executing sql=> %s", query), zap.String("task", taskCfg.Name))
		if _, err = conn.Exec(query); err != nil {
			err = errors.Wrapf(err, query)
			return
		}
	}
	return
}

func (c *ClickHouse) getDistTbls(table string) (distTbls []string, err error) {
	taskCfg := c.taskCfg
	chCfg := &c.cfg.Clickhouse
	sc := pool.GetShardConn(0)
	var conn *sql.DB
	if conn, _, err = sc.NextGoodReplica(0); err != nil {
		return
	}
	query := fmt.Sprintf(`SELECT name FROM system.tables WHERE engine='Distributed' AND database='%s' AND match(create_table_query, 'Distributed.*\'%s\',\s*\'%s\'')`,
		chCfg.DB, chCfg.DB, table)
	util.Logger.Info(fmt.Sprintf("executing sql=> %s", query), zap.String("task", taskCfg.Name))

	var rows *sql.Rows
	if rows, err = conn.Query(query); err != nil {
		err = errors.Wrapf(err, "")
		return
	}
	defer rows.Close()
	for rows.Next() {
		var name string
		if err = rows.Scan(&name); err != nil {
			err = errors.Wrapf(err, "")
			return
		}
		distTbls = append(distTbls, name)
	}
	return
}
