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
	std_errors "errors"
	"fmt"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/housepower/clickhouse_sinker/config"
	"github.com/housepower/clickhouse_sinker/model"
	"github.com/housepower/clickhouse_sinker/pool"
	"github.com/housepower/clickhouse_sinker/statistics"
	"github.com/housepower/clickhouse_sinker/util"
	"github.com/pkg/errors"

	"github.com/sundy-li/go_commons/log"
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
	taskCfg *config.TaskConfig
	chCfg   *config.ClickHouseConfig

	prepareSQL string
}

// NewClickHouse new a clickhouse instance
func NewClickHouse(taskCfg *config.TaskConfig) *ClickHouse {
	cfg := config.GetConfig()
	return &ClickHouse{taskCfg: taskCfg, chCfg: cfg.Clickhouse[taskCfg.Clickhouse]}
}

// Init the clickhouse intance
func (c *ClickHouse) Init() error {
	if err := c.initSchema(); err != nil {
		return err
	}
	return nil
}

// Send a batch to clickhouse
func (c *ClickHouse) Send(batch *model.Batch, callback func(batch *model.Batch) error) {
	// TODO workerpool parallel
	statistics.WritingPoolBacklog.WithLabelValues(c.taskCfg.Name).Inc()
	_ = util.GlobalWritingPool.Submit(func() {
		c.loopWrite(batch, callback)
		statistics.WritingPoolBacklog.WithLabelValues(c.taskCfg.Name).Dec()
	})
}

// Write kvs to clickhouse
func (c *ClickHouse) write(batch *model.Batch) error {
	if len(*batch.Rows) == 0 {
		return nil
	}

	conn := pool.GetConn(c.taskCfg.Clickhouse, batch.BatchIdx)
	tx, err := conn.Begin()
	if err != nil {
		if shouldReconnect(err) {
			_ = conn.ReConnect()
			statistics.ClickhouseReconnectTotal.WithLabelValues(c.taskCfg.Name).Inc()
		}
		return err
	}

	stmt, err := tx.Prepare(c.prepareSQL)
	if err != nil {
		log.Error("%s: tx.Prepare failed with error %+v", c.taskCfg.Name, err.Error())

		if shouldReconnect(err) {
			_ = conn.ReConnect()
			statistics.ClickhouseReconnectTotal.WithLabelValues(c.taskCfg.Name).Inc()
		}
		return err
	}

	defer stmt.Close()
	var numErr int
	for _, row := range *batch.Rows {
		if _, err = stmt.Exec(*row...); err != nil {
			err = errors.Wrap(err, "")
			numErr++
		}
	}
	if err != nil {
		log.Errorf("%s: stmt.Exec failed %d times with errors %+v", c.taskCfg.Name, numErr, err)
		return err
	}

	if err = tx.Commit(); err != nil {
		err = errors.Wrap(err, "")
		if shouldReconnect(err) {
			_ = conn.ReConnect()
			statistics.ClickhouseReconnectTotal.WithLabelValues(c.taskCfg.Name).Inc()
		}
		return err
	}
	statistics.FlushMsgsTotal.WithLabelValues(c.taskCfg.Name).Add(float64(batch.RealSize))
	return err
}

func shouldReconnect(err error) bool {
	if strings.Contains(err.Error(), "connection refused") || strings.Contains(err.Error(), "bad connection") {
		return true
	}
	log.Info("not match reconnect rules", err.Error())
	return false
}

// LoopWrite will dead loop to write the records
func (c *ClickHouse) loopWrite(batch *model.Batch, callback func(batch *model.Batch) error) {
	var err error
	times := c.chCfg.RetryTimes
	for {
		if err = c.write(batch); err == nil {
			for {
				if err = callback(batch); err == nil {
					return
				}
				if std_errors.Is(err, context.Canceled) {
					log.Infof("%s: ClickHouse.loopWrite quit due to the context has been cancelled", c.taskCfg.Name)
					return
				}
				log.Criticalf("%s: committing offset(try %%d) failed with error %+v", c.taskCfg.Name, c.chCfg.RetryTimes-times, err)
				if c.chCfg.RetryTimes > 0 {
					times--
					if times <= 0 {
						os.Exit(-1)
					}
					time.Sleep(10 * time.Second)
				}
			}
		}
		if std_errors.Is(err, context.Canceled) {
			log.Infof("%s: ClickHouse.loopWrite quit due to the context has been cancelled", c.taskCfg.Name)
			return
		}
		log.Errorf("%s: flush batch(try #%d) failed with error %+v", c.taskCfg.Name, c.chCfg.RetryTimes-times, err)
		statistics.FlushMsgsErrorTotal.WithLabelValues(c.taskCfg.Name).Add(float64(batch.RealSize))
		if c.chCfg.RetryTimes > 0 {
			times--
			if times <= 0 {
				os.Exit(-1)
			}
			time.Sleep(10 * time.Second)
		}
	}
}

// Close does nothing, place holder for handling close
func (c *ClickHouse) Close() error {
	return nil
}

func (c *ClickHouse) initSchema() (err error) {
	if c.taskCfg.AutoSchema {
		conn := pool.GetConn(c.taskCfg.Clickhouse, 0)
		rs, err := conn.Query(fmt.Sprintf(selectSQLTemplate, c.chCfg.DB, c.taskCfg.TableName))
		if err != nil {
			return err
		}
		defer rs.Close()

		c.Dims = make([]*model.ColumnWithType, 0, 10)
		var name, typ, defaultKind string
		for rs.Next() {
			_ = rs.Scan(&name, &typ, &defaultKind)
			typ = lowCardinalityRegexp.ReplaceAllString(typ, "$1")
			if !util.StringContains(c.taskCfg.ExcludeColumns, name) && defaultKind != "MATERIALIZED" {
				c.Dims = append(c.Dims, &model.ColumnWithType{Name: name, Type: typ, SourceName: util.GetSourceName(name)})
			}
		}
	} else {
		c.Dims = make([]*model.ColumnWithType, 0)
		for _, dim := range c.taskCfg.Dims {
			c.Dims = append(c.Dims, &model.ColumnWithType{
				Name:       dim.Name,
				Type:       dim.Type,
				SourceName: dim.SourceName,
			})
		}
	}
	//根据 Dms 生成prepare的sql语句
	c.Dms = make([]string, 0, len(c.Dims))
	for _, d := range c.Dims {
		c.Dms = append(c.Dms, d.Name)
	}
	var params = make([]string, len(c.Dims))
	for i := range params {
		params[i] = "?"
	}
	c.prepareSQL = "INSERT INTO " + c.chCfg.DB + "." + c.taskCfg.TableName + " (" + strings.Join(c.Dms, ",") + ") " +
		"VALUES (" + strings.Join(params, ",") + ")"

	log.Infof("%s: Prepare sql=> %s", c.taskCfg.Name, c.prepareSQL)
	return nil
}
