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
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/housepower/clickhouse_sinker/config"
	"github.com/housepower/clickhouse_sinker/model"
	"github.com/housepower/clickhouse_sinker/pool"
	"github.com/housepower/clickhouse_sinker/statistics"
	"github.com/housepower/clickhouse_sinker/util"
	"github.com/thanos-io/thanos/pkg/errors"
	"go.uber.org/zap"
)

var (
	ErrTblNotExist    = errors.Newf("table doesn't exist")
	selectSQLTemplate = `select name, type, default_kind from system.columns where database = '%s' and table = '%s'`

	// https://github.com/ClickHouse/ClickHouse/issues/24036
	// src/Common/ErrorCodes.cpp
	// src/Storages/MergeTree/ReplicatedMergeTreeBlockOutputStream.cpp
	// ZooKeeper issues(https://issues.apache.org/jira/browse/ZOOKEEPER-4410) can cause ClickHouse exeception: "Code": 999, "Message": "Cannot allocate block number..."
	// CKServer too many parts possibly reason: https://github.com/ClickHouse/ClickHouse/issues/6720#issuecomment-526045768
	// zooKeeper Connection loss issue: https://cwiki.apache.org/confluence/display/ZOOKEEPER/FAQ#:~:text=How%20should%20I%20handle%20the%20CONNECTION_LOSS%20error%3F
	// zooKeeper Session expired issue: https://cwiki.apache.org/confluence/display/ZOOKEEPER/FAQ#:~:text=How%20should%20I%20handle%20SESSION_EXPIRED%3F
	replicaSpecificErrorCodes = []int32{225, 242, 252, 319, 999, 1000} //NO_ZOOKEEPER, TABLE_IS_READ_ONLY, TOO_MANY_PARTS, UNKNOWN_STATUS_OF_INSERT, KEEPER_EXCEPTION, POCO_EXCEPTION
	wrSeriesQuota             = 16384
	seriesQuotas              sync.Map
)

type seriesQuota struct {
	sync.Mutex
	nextResetQuota time.Time
	bmSeries       map[int64]int64
	wrSeries       int
}

// ClickHouse is an output service consumers from kafka messages
type ClickHouse struct {
	Dims      []*model.ColumnWithType
	NumDims   int
	IdxSerID  int
	NameKey   string
	cfg       *config.Config
	taskCfg   *config.TaskConfig
	TableName string
	dbName    string

	prepareSQL string
	promSerSQL string
	seriesTbl  string

	distMetricTbls []string
	distSeriesTbls []string

	seriesQuota *seriesQuota

	numFlying int32
	mux       sync.Mutex
	taskDone  *sync.Cond
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
		util.Logger.Debug("draining flying batches",
			zap.String("task", c.taskCfg.Name),
			zap.Int32("pending", c.numFlying))
		c.taskDone.Wait()
	}
	c.mux.Unlock()
}

// Send a batch to clickhouse
func (c *ClickHouse) Send(batch *model.Batch) {
	if err := util.GlobalWritingPool.Submit(func() {
		c.loopWrite(batch)
		batch.Wg.Done()
		c.mux.Lock()
		c.numFlying--
		if c.numFlying == 0 {
			c.taskDone.Broadcast()
		}
		c.mux.Unlock()
		statistics.WritingPoolBacklog.WithLabelValues(c.taskCfg.Name).Dec()
	}); err != nil {
		return
	}

	c.mux.Lock()
	c.numFlying++
	c.mux.Unlock()
	statistics.WritingPoolBacklog.WithLabelValues(c.taskCfg.Name).Inc()
}

func (c *ClickHouse) AllowWriteSeries(sid, mid int64) (allowed bool) {
	c.seriesQuota.Lock()
	defer c.seriesQuota.Unlock()
	mid2, loaded := c.seriesQuota.bmSeries[sid]
	if !loaded {
		allowed = true
		statistics.WriteSeriesAllowNew.WithLabelValues(c.taskCfg.Name).Inc()
	} else if mid != mid2 {
		if c.seriesQuota.wrSeries < wrSeriesQuota {
			c.seriesQuota.wrSeries++
			allowed = true
		} else {
			now := time.Now()
			if now.After(c.seriesQuota.nextResetQuota) {
				c.seriesQuota.nextResetQuota = now.Add(10 * time.Second)
				c.seriesQuota.wrSeries = 1
				allowed = true
			}
		}
		if allowed {
			statistics.WriteSeriesAllowChanged.WithLabelValues(c.taskCfg.Name).Inc()
		} else {
			statistics.WriteSeriesDropQuota.WithLabelValues(c.taskCfg.Name).Inc()
		}
	} else {
		statistics.WriteSeriesDropUnchanged.WithLabelValues(c.taskCfg.Name).Inc()
	}
	return
}

func (c *ClickHouse) writeSeries(rows model.Rows, conn clickhouse.Conn) (err error) {
	var seriesRows model.Rows
	for _, row := range rows {
		if len(*row) != c.NumDims {
			continue
		}
		seriesRows = append(seriesRows, row)
	}
	if len(seriesRows) != 0 {
		begin := time.Now()
		var numBad int
		if numBad, err = writeRows(c.promSerSQL, seriesRows, c.IdxSerID, c.NumDims, conn); err != nil {
			return
		}
		// update c.bmSeries **after** writing series
		c.seriesQuota.Lock()
		for _, row := range seriesRows {
			sid := (*row)[c.IdxSerID].(int64)
			mid := (*row)[c.IdxSerID+1].(int64)
			if _, loaded := c.seriesQuota.bmSeries[sid]; loaded {
				c.seriesQuota.wrSeries--
			}
			c.seriesQuota.bmSeries[sid] = mid
		}
		c.seriesQuota.Unlock()
		util.Logger.Info("ClickHouse.writeSeries succeeded", zap.Int("series", len(seriesRows)), zap.String("task", c.taskCfg.Name))
		statistics.WriteSeriesSucceed.WithLabelValues(c.taskCfg.Name).Add(float64(len(seriesRows)))
		if numBad != 0 {
			statistics.ParseMsgsErrorTotal.WithLabelValues(c.taskCfg.Name).Add(float64(numBad))
		}
		statistics.WritingDurations.WithLabelValues(c.taskCfg.Name, c.seriesTbl).Observe(time.Since(begin).Seconds())
	}
	return
}

// Write a batch to clickhouse
func (c *ClickHouse) write(batch *model.Batch, sc *pool.ShardConn, dbVer *int) (err error) {
	if len(*batch.Rows) == 0 {
		return
	}
	var conn clickhouse.Conn
	if conn, *dbVer, err = sc.NextGoodReplica(*dbVer); err != nil {
		return
	}
	util.Logger.Debug("writing batch", zap.String("task", c.taskCfg.Name), zap.String("replica", sc.GetReplica()), zap.Int("dbVer", *dbVer))

	//row[:c.IdxSerID+1] is for metric table
	//row[c.IdxSerID:] is for series table
	numDims := c.NumDims
	if c.taskCfg.PrometheusSchema {
		numDims = c.IdxSerID + 1
		if err = c.writeSeries(*batch.Rows, conn); err != nil {
			return
		}
	}
	begin := time.Now()
	var numBad int
	if numBad, err = writeRows(c.prepareSQL, *batch.Rows, 0, numDims, conn); err != nil {
		return
	}
	statistics.WritingDurations.WithLabelValues(c.taskCfg.Name, c.TableName).Observe(time.Since(begin).Seconds())
	if numBad != 0 {
		statistics.ParseMsgsErrorTotal.WithLabelValues(c.taskCfg.Name).Add(float64(numBad))
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
			return
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

func (c *ClickHouse) initBmSeries(conn clickhouse.Conn) (err error) {
	tbl := c.seriesTbl
	if c.cfg.Clickhouse.Cluster != "" {
		tbl = c.distSeriesTbls[0]
	}

	var count uint64
	conn.QueryRow(context.Background(), fmt.Sprintf("SELECT count() FROM %s.%s FINAL", c.dbName, tbl)).Scan(&count)
	sq := &seriesQuota{}
	if v, ok := seriesQuotas.LoadOrStore(c.GetSeriesQuotaKey(), sq); ok {
		c.seriesQuota = v.(*seriesQuota)
		c.seriesQuota.Lock()
		defer c.seriesQuota.Unlock()
		if len(c.seriesQuota.bmSeries) == int(count) {
			// only reload the map when there is difference detected
			return
		}
	} else {
		sq.Lock()
		sq.bmSeries = make(map[int64]int64, count)
		sq.nextResetQuota = time.Now().Add(10 * time.Second)
		sq.Unlock()
		c.seriesQuota = sq
	}

	query := fmt.Sprintf("SELECT toInt64(__series_id) AS sid, toInt64(__mgmt_id) AS mid FROM %s.%s FINAL ORDER BY sid", c.dbName, tbl)
	util.Logger.Info(fmt.Sprintf("executing sql=> %s", query), zap.String("task", c.taskCfg.Name))
	var rs driver.Rows
	var seriesID, mgmtID int64
	if rs, err = conn.Query(context.Background(), query); err != nil {
		err = errors.Wrapf(err, "")
		return err
	}
	defer rs.Close()

	c.seriesQuota.Lock()
	defer c.seriesQuota.Unlock()

	for rs.Next() {
		if err = rs.Scan(&seriesID, &mgmtID); err != nil {
			err = errors.Wrapf(err, "")
			return err
		}
		c.seriesQuota.bmSeries[seriesID] = mgmtID
	}
	util.Logger.Info(fmt.Sprintf("loaded %d series from %v", len(c.seriesQuota.bmSeries), tbl), zap.String("task", c.taskCfg.Name))
	return
}

func (c *ClickHouse) initSeriesSchema(conn clickhouse.Conn) (err error) {
	if !c.taskCfg.PrometheusSchema {
		c.IdxSerID = -1
		return
	}
	// Strip away string columns from metric table, and move column "__series_id" to the last.
	var dimSerID *model.ColumnWithType
	for i := 0; i < len(c.Dims); {
		dim := c.Dims[i]
		if dim.Name == "__series_id" && dim.Type.Type == model.Int64 {
			dimSerID = dim
			c.Dims = append(c.Dims[:i], c.Dims[i+1:]...)
		} else if dim.Type.Type == model.String {
			c.Dims = append(c.Dims[:i], c.Dims[i+1:]...)
			util.Logger.Warn("non-numeric type metric ignored", zap.String("metric name", dim.Name))
		} else {
			i++
		}
	}
	if dimSerID == nil {
		err = errors.Newf("Metric table %s.%s shall have column `__series_id UInt64`.", c.dbName, c.TableName)
		return
	}
	c.IdxSerID = len(c.Dims)
	c.Dims = append(c.Dims, dimSerID)

	// Add string columns from series table
	if c.seriesTbl == "" {
		c.seriesTbl = c.TableName + "_series"
	}
	expSeriesDims := []*model.ColumnWithType{
		{Name: "__series_id", Type: &model.TypeInfo{Type: model.Int64}},
		{Name: "__mgmt_id", Type: &model.TypeInfo{Type: model.Int64}},
		{Name: "labels", Type: &model.TypeInfo{Type: model.String}},
	}
	var seriesDims []*model.ColumnWithType
	if seriesDims, err = getDims(c.dbName, c.seriesTbl, nil, c.taskCfg.Parser, conn); err != nil {
		if errors.Is(err, ErrTblNotExist) {
			err = errors.Wrapf(err, "Please create series table for %s.%s", c.dbName, c.TableName)
			return
		}
		return
	}
	var badFirst bool
	if len(seriesDims) < len(expSeriesDims) {
		badFirst = true
	} else {
		for i := range expSeriesDims {
			if seriesDims[i].Name != expSeriesDims[i].Name ||
				seriesDims[i].Type.Type != expSeriesDims[i].Type.Type {
				badFirst = true
				break
			}
		}
	}
	if badFirst {
		err = errors.Newf(`First columns of %s are expect to be "__series_id Int64, __mgmt_id Int64, labels String".`, c.seriesTbl)
		return
	}
	c.NameKey = "__name__" // prometheus uses internal "__name__" label for metric name
	for i := len(expSeriesDims); i < len(seriesDims); i++ {
		serDim := seriesDims[i]
		if serDim.Type.Type == model.String {
			c.NameKey = serDim.Name // opentsdb uses "metric" tag for metric name
			break
		}
	}
	c.Dims = append(c.Dims, seriesDims[1:]...)

	// Generate SQL for series INSERT
	serDimsQuoted := make([]string, len(seriesDims))
	for i, serDim := range seriesDims {
		serDimsQuoted[i] = fmt.Sprintf("`%s`", serDim.Name)
	}
	c.promSerSQL = fmt.Sprintf("INSERT INTO `%s`.`%s` (%s)",
		c.dbName,
		c.seriesTbl,
		strings.Join(serDimsQuoted, ","))

	// Check distributed series table
	if chCfg := &c.cfg.Clickhouse; chCfg.Cluster != "" {
		if c.distSeriesTbls, err = c.getDistTbls(c.seriesTbl); err != nil {
			return
		}
		if c.distSeriesTbls == nil {
			err = errors.Newf("Please create distributed table for %s in cluster '%s'.", c.seriesTbl, c.cfg.Clickhouse.Cluster)
			return
		}
	}

	// Initialize bmSeries
	if err = c.initBmSeries(conn); err != nil {
		return
	}
	return
}

func (c *ClickHouse) initSchema() (err error) {
	if idx := strings.Index(c.taskCfg.TableName, "."); idx > 0 {
		c.TableName = c.taskCfg.TableName[idx+1:]
		c.dbName = c.taskCfg.TableName[0:idx]
	} else {
		c.TableName = c.taskCfg.TableName[idx+1:]
		c.dbName = c.cfg.Clickhouse.DB
	}
	c.seriesTbl = c.taskCfg.SeriesTableName

	sc := pool.GetShardConn(0)
	var conn clickhouse.Conn
	if conn, _, err = sc.NextGoodReplica(0); err != nil {
		return
	}
	if c.taskCfg.AutoSchema {
		if c.Dims, err = getDims(c.dbName, c.TableName, c.taskCfg.ExcludeColumns, c.taskCfg.Parser, conn); err != nil {
			return
		}
	} else {
		c.Dims = make([]*model.ColumnWithType, 0, len(c.taskCfg.Dims))
		for _, dim := range c.taskCfg.Dims {
			c.Dims = append(c.Dims, &model.ColumnWithType{
				Name:       dim.Name,
				Type:       model.WhichType(dim.Type),
				SourceName: dim.SourceName,
			})
		}
	}
	if err = c.initSeriesSchema(conn); err != nil {
		return
	}
	// Generate SQL for INSERT
	c.NumDims = len(c.Dims)
	numDims := c.NumDims
	if c.taskCfg.PrometheusSchema {
		numDims = c.IdxSerID + 1
	}
	quotedDms := make([]string, numDims)
	for i := 0; i < numDims; i++ {
		quotedDms[i] = fmt.Sprintf("`%s`", c.Dims[i].Name)
	}
	c.prepareSQL = fmt.Sprintf("INSERT INTO `%s`.`%s` (%s)",
		c.dbName,
		c.TableName,
		strings.Join(quotedDms, ","))
	util.Logger.Info(fmt.Sprintf("Prepare sql=> %s", c.prepareSQL), zap.String("task", c.taskCfg.Name))

	// Check distributed metric table
	if chCfg := &c.cfg.Clickhouse; chCfg.Cluster != "" {
		if c.distMetricTbls, err = c.getDistTbls(c.TableName); err != nil {
			return
		}
		if c.distMetricTbls == nil {
			err = errors.Newf("Please create distributed table for %s in cluster '%s'.", c.seriesTbl, c.cfg.Clickhouse.Cluster)
			return
		}
	}
	return nil
}

func (c *ClickHouse) ChangeSchema(newKeys *sync.Map) (err error) {
	var queries []string
	var onCluster string
	taskCfg := c.taskCfg
	chCfg := &c.cfg.Clickhouse
	if chCfg.Cluster != "" {
		onCluster = fmt.Sprintf("ON CLUSTER `%s`", chCfg.Cluster)
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
	var affectDistMetric, affectDistSeries bool
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
		case model.Bool:
			strVal = "Nullable(Bool)"
		case model.Int64:
			strVal = "Nullable(Int64)"
		case model.Float64:
			strVal = "Nullable(Float64)"
		case model.String:
			strVal = "Nullable(String)"
		case model.DateTime:
			strVal = "Nullable(DateTime64(3))"
		case model.Object:
			strVal = model.GetTypeName(intVal)
		default:
			err = errors.Newf("%s: BUG: unsupported column type %s", taskCfg.Name, model.GetTypeName(intVal))
			return false
		}
		if c.taskCfg.PrometheusSchema && intVal == model.String {
			query := fmt.Sprintf("ALTER TABLE `%s`.`%s` %s ADD COLUMN IF NOT EXISTS `%s` %s", c.dbName, c.seriesTbl, onCluster, strKey, strVal)
			queries = append(queries, query)
			affectDistSeries = true
		} else {
			if c.taskCfg.PrometheusSchema && intVal > model.String {
				util.Logger.Fatal("unsupported metric value type", zap.String("type", strVal), zap.String("name", strKey), zap.String("task", c.taskCfg.Name))
			}
			query := fmt.Sprintf("ALTER TABLE `%s`.`%s` %s ADD COLUMN IF NOT EXISTS `%s` %s", c.dbName, c.TableName, onCluster, strKey, strVal)
			queries = append(queries, query)
			affectDistMetric = true
		}
		return true
	})
	if err != nil {
		return
	}
	sort.Strings(queries)
	sc := pool.GetShardConn(0)
	var conn clickhouse.Conn
	if conn, _, err = sc.NextGoodReplica(0); err != nil {
		return
	}
	for _, query := range queries {
		util.Logger.Info(fmt.Sprintf("executing sql=> %s", query), zap.String("task", taskCfg.Name))
		if err = conn.Exec(context.Background(), query); err != nil {
			err = errors.Wrapf(err, query)
			return
		}
	}
	if chCfg.Cluster != "" {
		if affectDistMetric {
			if err = recreateDistTbls(chCfg.Cluster, c.dbName, c.TableName, c.distMetricTbls, conn); err != nil {
				return
			}
		}
		if affectDistSeries {
			if err = recreateDistTbls(chCfg.Cluster, c.dbName, c.seriesTbl, c.distSeriesTbls, conn); err != nil {
				return
			}
		}
	}
	return
}

func (c *ClickHouse) getDistTbls(table string) (distTbls []string, err error) {
	taskCfg := c.taskCfg
	chCfg := &c.cfg.Clickhouse
	sc := pool.GetShardConn(0)
	var conn clickhouse.Conn
	if conn, _, err = sc.NextGoodReplica(0); err != nil {
		return
	}
	query := fmt.Sprintf(`SELECT name FROM system.tables WHERE engine='Distributed' AND database='%s' AND match(create_table_query, 'Distributed\(\'%s\', \'%s\', \'%s\'.*\)')`,
		c.dbName, chCfg.Cluster, c.dbName, table)
	util.Logger.Info(fmt.Sprintf("executing sql=> %s", query), zap.String("task", taskCfg.Name))

	var rows driver.Rows
	if rows, err = conn.Query(context.Background(), query); err != nil {
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

func UpdateSeriesQuotas(c map[string]struct{}) {
	seriesQuotas.Range(func(key, value any) bool {
		k := key.(string)
		if _, ok := c[k]; !ok {
			seriesQuotas.Delete(k)
		}
		return true
	})
}

func (c *ClickHouse) GetSeriesQuotaKey() string {
	if c.taskCfg.PrometheusSchema {
		return c.dbName + "." + c.seriesTbl
	}
	return ""
}
