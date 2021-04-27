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
	std_errors "errors"
	"fmt"
	"io"
	"math"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/housepower/clickhouse_sinker/config"
	"github.com/housepower/clickhouse_sinker/model"
	"github.com/housepower/clickhouse_sinker/pool"
	"github.com/housepower/clickhouse_sinker/statistics"
	"github.com/housepower/clickhouse_sinker/util"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

var (
	selectSQLTemplate    = `select name, type, default_kind from system.columns where database = '%s' and table = '%s'`
	lowCardinalityRegexp = regexp.MustCompile(`LowCardinality\((.+)\)`)
)

// ClickHouse is an output service consumers from kafka messages
type ClickHouse struct {
	Dims []*model.ColumnWithType
	Dms  []string
	// Table Configs
	cfg *config.Config

	prepareSQL string
}

// NewClickHouse new a clickhouse instance
func NewClickHouse(cfg *config.Config) *ClickHouse {
	return &ClickHouse{cfg: cfg}
}

// Init the clickhouse intance
func (c *ClickHouse) Init() (err error) {
	chCfg := &c.cfg.Clickhouse
	if err = pool.InitConn(chCfg.Hosts, chCfg.Port, chCfg.DB, chCfg.Username, chCfg.Password, chCfg.DsnParams, chCfg.Secure, chCfg.InsecureSkipVerify); err != nil {
		return
	}
	return c.initSchema()
}

// Send a batch to clickhouse
func (c *ClickHouse) Send(batch *model.Batch) {
	statistics.WritingPoolBacklog.WithLabelValues(c.cfg.Task.Name).Inc()
	_ = util.GlobalWritingPool.Submit(func() {
		c.loopWrite(batch)
		statistics.WritingPoolBacklog.WithLabelValues(c.cfg.Task.Name).Dec()
	})
}

// Write kvs to clickhouse
func (c *ClickHouse) write(batch *model.Batch) error {
	var numErr int
	var err, tmpErr error
	var stmt *sql.Stmt
	var tx *sql.Tx
	if len(*batch.Rows) == 0 {
		return nil
	}

	conn := pool.GetConn(batch.BatchIdx)
	if tx, err = conn.Begin(); err != nil {
		goto ERR
	}
	if stmt, err = tx.Prepare(c.prepareSQL); err != nil {
		goto ERR
	}
	defer stmt.Close()
	for _, row := range *batch.Rows {
		if _, tmpErr = stmt.Exec(*row...); tmpErr != nil {
			numErr++
			err = tmpErr
		}
	}
	if err != nil {
		util.Logger.Error("stmt.Exec failed", zap.String("task", c.cfg.Task.Name), zap.Int("times", numErr), zap.Error(err))
		goto ERR
	}

	if err = tx.Commit(); err != nil {
		goto ERR
	}
	statistics.FlushMsgsTotal.WithLabelValues(c.cfg.Task.Name).Add(float64(batch.RealSize))
	return err
ERR:
	if shouldReconnect(err) {
		_ = conn.ReConnect()
		statistics.ClickhouseReconnectTotal.WithLabelValues(c.cfg.Task.Name).Inc()
	}
	return err
}

func shouldReconnect(err error) bool {
	if err == nil {
		return false
	}
	if strings.Contains(err.Error(), "connection refused") || strings.Contains(err.Error(), "bad connection") {
		return true
	}
	util.Logger.Info("this is a permanent error", zap.Error(err))
	return false
}

// LoopWrite will dead loop to write the records
func (c *ClickHouse) loopWrite(batch *model.Batch) {
	var err error
	var times int
	for {
		if err = c.write(batch); err == nil {
			if err = batch.Commit(); err == nil {
				return
			}
			// TODO: kafka_go and sarama commit give different error when context is cancceled.
			// How to unify them?
			if std_errors.Is(err, context.Canceled) || std_errors.Is(err, io.ErrClosedPipe) {
				util.Logger.Info("ClickHouse.loopWrite quit due to the context has been cancelled", zap.String("task", c.cfg.Task.Name))
				return
			}
			util.Logger.Fatal("committing offset failed with permanent error %+v", zap.String("task", c.cfg.Task.Name), zap.Error(err))
		}
		if std_errors.Is(err, context.Canceled) {
			util.Logger.Info("ClickHouse.loopWrite quit due to the context has been cancelled", zap.String("task", c.cfg.Task.Name))
			return
		}
		util.Logger.Error("flush batch failed", zap.String("task", c.cfg.Task.Name), zap.Int("try", c.cfg.Clickhouse.RetryTimes-times), zap.Error(err))
		statistics.FlushMsgsErrorTotal.WithLabelValues(c.cfg.Task.Name).Add(float64(batch.RealSize))
		times++
		if shouldReconnect(err) && (c.cfg.Clickhouse.RetryTimes <= 0 || times < c.cfg.Clickhouse.RetryTimes) {
			time.Sleep(10 * time.Second)
		} else {
			util.Logger.Fatal("ClickHouse.loopWrite failed", zap.String("task", c.cfg.Task.Name))
		}
	}
}

// Stop free clickhouse connections
func (c *ClickHouse) Stop() error {
	pool.FreeConn()
	return nil
}

func (c *ClickHouse) initSchema() (err error) {
	if c.cfg.Task.AutoSchema {
		conn := pool.GetConn(0)
		var rs *sql.Rows
		if rs, err = conn.Query(fmt.Sprintf(selectSQLTemplate, c.cfg.Clickhouse.DB, c.cfg.Task.TableName)); err != nil {
			err = errors.Wrapf(err, "")
			return err
		}
		defer rs.Close()

		c.Dims = make([]*model.ColumnWithType, 0, 10)
		var name, typ, defaultKind string
		for rs.Next() {
			if err = rs.Scan(&name, &typ, &defaultKind); err != nil {
				err = errors.Wrapf(err, "")
				return err
			}
			typ = lowCardinalityRegexp.ReplaceAllString(typ, "$1")
			if !util.StringContains(c.cfg.Task.ExcludeColumns, name) && defaultKind != "MATERIALIZED" {
				tp, nullable := model.WhichType(typ)
				c.Dims = append(c.Dims, &model.ColumnWithType{Name: name, Type: tp, Nullable: nullable, SourceName: util.GetSourceName(name)})
			}
		}
	} else {
		c.Dims = make([]*model.ColumnWithType, 0)
		for _, dim := range c.cfg.Task.Dims {
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
	c.prepareSQL = "INSERT INTO " + c.cfg.Clickhouse.DB + "." + c.cfg.Task.TableName + " (" + strings.Join(quotedDms, ",") + ") " +
		"VALUES (" + strings.Join(params, ",") + ")"

	util.Logger.Info(fmt.Sprintf("Prepare sql=> %s", c.prepareSQL), zap.String("task", c.cfg.Task.Name))
	return nil
}

func (c *ClickHouse) ChangeSchema(newKeys *sync.Map) (err error) {
	var queries []string
	var onCluster string
	taskCfg := &c.cfg.Task
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
		var strVal string
		switch value.(int) {
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
		return true
	})
	if err != nil {
		return
	}
	sort.Strings(queries)
	if chCfg.Cluster != "" {
		var distTbls []string
		if distTbls, err = c.getDistTbls(); err != nil {
			return
		}
		for _, distTbl := range distTbls {
			queries = append(queries, fmt.Sprintf("DROP TABLE IF EXISTS %s.%s %s", chCfg.DB, distTbl, onCluster))
			queries = append(queries, fmt.Sprintf("CREATE TABLE %s.%s %s AS %s ENGINE = Distributed(%s, %s, %s);",
				chCfg.DB, distTbl, onCluster, taskCfg.TableName,
				chCfg.Cluster, chCfg.DB, taskCfg.TableName))
		}
	}
	conn := pool.GetConn(0)
	for _, query := range queries {
		util.Logger.Info(fmt.Sprintf("executing sql=> %s", query), zap.String("task", taskCfg.Name))
		if _, err = conn.Exec(query); err != nil {
			err = errors.Wrapf(err, query)
			return
		}
	}
	return
}

func (c *ClickHouse) getDistTbls() (distTbls []string, err error) {
	taskCfg := &c.cfg.Task
	chCfg := &c.cfg.Clickhouse
	conn := pool.GetConn(0)
	query := fmt.Sprintf(`SELECT name FROM system.tables WHERE engine='Distributed' AND database='%s' AND match(create_table_query, 'Distributed.*\'%s\',\s*\'%s\'')`,
		chCfg.DB, chCfg.DB, taskCfg.TableName)
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
