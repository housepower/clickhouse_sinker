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
	"encoding/json"
	"expvar"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/avast/retry-go/v4"
	"github.com/thanos-io/thanos/pkg/errors"
	"github.com/viru-tech/clickhouse_sinker/config"
	"github.com/viru-tech/clickhouse_sinker/model"
	"github.com/viru-tech/clickhouse_sinker/pool"
	"github.com/viru-tech/clickhouse_sinker/statistics"
	"github.com/viru-tech/clickhouse_sinker/util"
	"go.uber.org/zap"
)

var (
	ErrTblNotExist     = errors.Newf("table doesn't exist")
	selectSQLTemplate  = `select name, type, default_kind from system.columns where database = '%s' and table = '%s'`
	referedSQLTemplate = `SELECT 
    current_col.default_expression,
    referenced_col.type AS referenced_col_type,
    current_col.name,
    current_col.type
FROM 
    system.columns AS current_col
JOIN 
    system.columns AS referenced_col 
ON 
    current_col.database = referenced_col.database 
    AND current_col.table = referenced_col.table 
    AND current_col.default_expression = referenced_col.name 
WHERE 
    current_col.database = '%s' 
    AND
    current_col.table = '%s';`
	wrSeriesQuota int = 16384

	SeriesQuotas sync.Map
)

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
	DimSerID       string
	DimMgmtID      string

	seriesQuota *model.SeriesQuota

	numFlying int32
	mux       sync.Mutex
	taskDone  *sync.Cond
}

type DistTblInfo struct {
	name    string
	cluster string
}

func init() {
	expvar.Publish("SeriesMap", expvar.Func(func() interface{} {
		var result = make(map[string]string)
		SeriesQuotas.Range(func(key, value interface{}) bool {
			if sq, ok := value.(*model.SeriesQuota); ok {
				sq.RLock()
				if bs, err := json.Marshal(sq); err == nil {
					result[key.(string)] = string(bs)
				}
				sq.RUnlock()
			}
			return true
		})
		return result
	}))
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
func (c *ClickHouse) Send(batch *model.Batch, traceId string) {
	sc := pool.GetShardConn(batch.BatchIdx)
	if err := sc.SubmitTask(func() {
		c.loopWrite(batch, sc, traceId)
		batch.Wg.Done()
		c.mux.Lock()
		c.numFlying--
		if c.numFlying == 0 {
			c.taskDone.Broadcast()
		}
		c.mux.Unlock()
		statistics.WritingPoolBacklog.WithLabelValues(c.taskCfg.Name).Dec()
	}); err != nil {
		batch.Wg.Done()
		util.Rs.Dec(int64(batch.RealSize))
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
	mid2, loaded := c.seriesQuota.BmSeries[sid]
	if !loaded {
		util.Logger.Debug("found new series", zap.Int64("mid", mid), zap.Int64("sid", sid))
		allowed = true
		statistics.WriteSeriesAllowNew.WithLabelValues(c.taskCfg.Name).Inc()
	} else if mid != mid2 {
		util.Logger.Debug("found new series map", zap.Int64("mid", mid), zap.Int64("sid", sid))
		if c.seriesQuota.WrSeries < wrSeriesQuota {
			c.seriesQuota.WrSeries++
			allowed = true
		} else {
			now := time.Now()
			if now.After(c.seriesQuota.NextResetQuota) {
				c.seriesQuota.NextResetQuota = now.Add(10 * time.Second)
				c.seriesQuota.WrSeries = 1
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

func (c *ClickHouse) writeSeries(rows model.Rows, conn *pool.Conn) (err error) {
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
			if _, loaded := c.seriesQuota.BmSeries[sid]; loaded {
				c.seriesQuota.WrSeries--
			}
			c.seriesQuota.BmSeries[sid] = mid
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
	var conn *pool.Conn
	if conn, *dbVer, err = sc.NextGoodReplica(c.cfg.Clickhouse.Ctx, *dbVer); err != nil {
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
func (c *ClickHouse) loopWrite(batch *model.Batch, sc *pool.ShardConn, traceId string) {
	var retrycount int
	var dbVer int

	util.LogTrace(traceId, util.TraceKindWriteStart, zap.Int("realsize", batch.RealSize))
	defer func() {
		util.Rs.Dec(int64(batch.RealSize))
		util.LogTrace(traceId, util.TraceKindWriteEnd, zap.Int("success", batch.RealSize))
	}()
	times := c.cfg.Clickhouse.RetryTimes
	if times <= 0 {
		times = 0
	}
	if err := retry.Do(
		func() error { return c.write(batch, sc, &dbVer) },
		retry.LastErrorOnly(true),
		retry.Attempts(uint(times)),
		retry.Delay(10*time.Second),
		retry.MaxDelay(1*time.Minute),
		retry.OnRetry(func(n uint, err error) {
			retrycount++
			util.Logger.Error("flush batch failed",
				zap.String("task", c.taskCfg.Name),
				zap.String("group", batch.GroupId),
				zap.Int("try", int(retrycount)),
				zap.Error(err))
			statistics.FlushMsgsErrorTotal.WithLabelValues(c.taskCfg.Name).Add(float64(batch.RealSize))
		}),
	); err != nil {
		util.Logger.Fatal("ClickHouse.loopWrite failed", zap.String("task", c.taskCfg.Name), zap.Error(err))
	}
}

func (c *ClickHouse) getSeriesDims(dims []*model.ColumnWithType, conn *pool.Conn) {
	for _, dim := range dims {
		if strings.Contains(dim.Name, "series_id") {
			c.DimSerID = dim.Name
		}
		if strings.Contains(dim.Name, "mgmt_id") {
			c.DimMgmtID = dim.Name
		}
	}
}

func (c *ClickHouse) initSeriesSchema(conn *pool.Conn) (err error) {
	if !c.taskCfg.PrometheusSchema {
		c.IdxSerID = -1
		return
	}

	// Add string columns from series table
	if c.seriesTbl == "" {
		c.seriesTbl = c.TableName + "_series"
	}

	var seriesDims []*model.ColumnWithType
	if seriesDims, err = getDims(c.dbName, c.seriesTbl, nil, c.taskCfg.Parser, conn); err != nil {
		if errors.Is(err, ErrTblNotExist) {
			err = errors.Wrapf(err, "Please create series table for %s.%s", c.dbName, c.TableName)
			return
		}
		return
	}

	c.getSeriesDims(seriesDims, conn)

	// Move column "__series_id__" to the last.
	var dimSerID *model.ColumnWithType
	for i := 0; i < len(c.Dims); {
		dim := c.Dims[i]
		if dim.Name == c.DimSerID && dim.Type.Type == model.Int64 {
			dimSerID = dim
			c.Dims = append(c.Dims[:i], c.Dims[i+1:]...)
			break
		} else {
			i++
		}
	}
	if dimSerID == nil {
		err = errors.Newf("Metric table %s.%s shall have column `%s Int64`.", c.dbName, c.TableName, c.DimSerID)
		return
	}
	c.IdxSerID = len(c.Dims)
	c.Dims = append(c.Dims, dimSerID)

	expSeriesDims := []*model.ColumnWithType{
		{Name: c.DimSerID, Type: &model.TypeInfo{Type: model.Int64}},
		{Name: c.DimMgmtID, Type: &model.TypeInfo{Type: model.Int64}},
		{Name: "labels", Type: &model.TypeInfo{Type: model.String}},
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
		err = errors.Newf(`First columns of %s are expect to be %s Int64, %s Int64, labels String".`, c.seriesTbl, c.DimSerID, c.DimMgmtID)
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
	if c.cfg.Clickhouse.Protocol == clickhouse.HTTP.String() {
		serDimsQuoted := make([]string, len(seriesDims))
		for i, serDim := range seriesDims {
			serDimsQuoted[i] = fmt.Sprintf("`%s`", serDim.Name)
		}
		var params = make([]string, len(seriesDims))
		for i := range params {
			params[i] = "?"
		}
		c.promSerSQL = "INSERT INTO " + c.cfg.Clickhouse.DB + "." + c.TableName + " (" + strings.Join(serDimsQuoted, ",") + ") " +
			"VALUES (" + strings.Join(params, ",") + ")"
	} else {
		serDimsQuoted := make([]string, len(seriesDims))
		for i, serDim := range seriesDims {
			serDimsQuoted[i] = fmt.Sprintf("`%s`", serDim.Name)
		}
		c.promSerSQL = fmt.Sprintf("INSERT INTO `%s`.`%s` (%s)",
			c.dbName,
			c.seriesTbl,
			strings.Join(serDimsQuoted, ","))
	}
	util.Logger.Info(fmt.Sprintf("promSer sql=> %s", c.promSerSQL), zap.String("task", c.taskCfg.Name))

	// Check distributed series table
	if chCfg := &c.cfg.Clickhouse; chCfg.Cluster != "" {
		withDistTable := false
		info, e := c.getDistTbls(c.seriesTbl, chCfg.Cluster)
		if e != nil {
			return e
		}
		c.distSeriesTbls = make([]string, 0)
		for _, i := range info {
			c.distSeriesTbls = append(c.distSeriesTbls, i.name)
			if i.cluster == c.cfg.Clickhouse.Cluster {
				withDistTable = true
			}
		}
		if !withDistTable {
			err = errors.Newf("Please create distributed table for %s in cluster '%s'.", c.seriesTbl, c.cfg.Clickhouse.Cluster)
			return
		}
	}

	sq, _ := SeriesQuotas.LoadOrStore(c.GetSeriesQuotaKey(),
		&model.SeriesQuota{
			NextResetQuota: time.Now().Add(10 * time.Second),
			Birth:          time.Now(),
		})
	c.seriesQuota = sq.(*model.SeriesQuota)

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
	var conn *pool.Conn
	if conn, _, err = sc.NextGoodReplica(c.cfg.Clickhouse.Ctx, 0); err != nil {
		return
	}
	if c.taskCfg.AutoSchema {
		if c.Dims, err = getDims(c.dbName, c.TableName, c.taskCfg.ExcludeColumns, c.taskCfg.Parser, conn); err != nil {
			return
		}
	} else {
		c.Dims = make([]*model.ColumnWithType, 0, len(c.taskCfg.Dims))
		for _, dim := range c.taskCfg.Dims {
			mtype := model.WhichType(dim.Type)
			if dim.Const != "" && (mtype.Type != model.String || mtype.Array) {
				return fmt.Errorf("only non-array %s columns can be set as a constant", model.GetTypeName(mtype.Type))
			}

			c.Dims = append(c.Dims, &model.ColumnWithType{
				Name:       dim.Name,
				Type:       model.WhichType(dim.Type),
				SourceName: dim.SourceName,
				Const:      dim.Const,
			})
		}
	}

	if err = c.initSeriesSchema(conn); err != nil {
		return
	}
	// Generate SQL for INSERT
	if c.cfg.Clickhouse.Protocol == clickhouse.HTTP.String() {
		c.NumDims = len(c.Dims)
		numDims := c.NumDims
		if c.taskCfg.PrometheusSchema {
			numDims = c.IdxSerID + 1
		}
		quotedDms := make([]string, numDims)
		for i := 0; i < numDims; i++ {
			quotedDms[i] = fmt.Sprintf("`%s`", c.Dims[i].Name)
		}
		var params = make([]string, c.NumDims)
		for i := range params {
			params[i] = "?"
		}
		c.prepareSQL = "INSERT INTO " + c.cfg.Clickhouse.DB + "." + c.TableName + " (" + strings.Join(quotedDms, ",") + ") " +
			"VALUES (" + strings.Join(params, ",") + ")"

	} else {
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
	}
	util.Logger.Info(fmt.Sprintf("Prepare sql=> %s", c.prepareSQL), zap.String("task", c.taskCfg.Name))

	// Check distributed metric table
	if chCfg := &c.cfg.Clickhouse; chCfg.Cluster != "" {
		withDistTable := false
		info, e := c.getDistTbls(c.TableName, chCfg.Cluster)
		if e != nil {
			return e
		}
		c.distMetricTbls = make([]string, 0)
		for _, i := range info {
			c.distMetricTbls = append(c.distMetricTbls, i.name)
			if i.cluster == c.cfg.Clickhouse.Cluster {
				withDistTable = true
			}
		}
		if !withDistTable {
			err = errors.Newf("Please create distributed table for %s in cluster '%s'.", c.TableName, c.cfg.Clickhouse.Cluster)
			return
		}
	}
	return nil
}

func (c *ClickHouse) ChangeSchema(newKeys *sync.Map) (err error) {
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
	var alterSeries, alterMetric []string
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
			strVal = "Bool"
		case model.Int64:
			strVal = "Int64"
		case model.Float64:
			strVal = "Float64"
		case model.String:
			strVal = "String"
		case model.DateTime:
			strVal = "DateTime64(3)"
		case model.Object:
			strVal = model.GetTypeName(intVal)
		default:
			err = errors.Newf("%s: BUG: unsupported column type %s", taskCfg.Name, model.GetTypeName(intVal))
			return false
		}

		if !taskCfg.DynamicSchema.NotNullable {
			strVal = fmt.Sprintf("Nullable(%v)", strVal)
		}

		if c.taskCfg.PrometheusSchema && intVal == model.String {
			alterSeries = append(alterSeries, fmt.Sprintf("ADD COLUMN IF NOT EXISTS `%s` %s", strKey, strVal))
		} else {
			if c.taskCfg.PrometheusSchema && intVal > model.String {
				util.Logger.Fatal("unsupported metric value type", zap.String("type", strVal), zap.String("name", strKey), zap.String("task", c.taskCfg.Name))
			}
			alterMetric = append(alterMetric, fmt.Sprintf("ADD COLUMN IF NOT EXISTS `%s` %s", strKey, strVal))
		}
		return true
	})
	if err != nil {
		return
	}

	sc := pool.GetShardConn(0)
	var conn *pool.Conn
	if conn, _, err = sc.NextGoodReplica(c.cfg.Clickhouse.Ctx, 0); err != nil {
		return
	}

	var version string
	if err = conn.QueryRow("SELECT version()").Scan(&version); err != nil {
		version = "1.0.0.0"
	}
	alterTable := func(tbl, col string) error {
		query := fmt.Sprintf("ALTER TABLE `%s`.`%s` %s %s", c.dbName, tbl, onCluster, col)
		if util.CompareClickHouseVersion(version, "23.3") >= 0 {
			query += " SETTINGS alter_sync = 0"
		}
		util.Logger.Info(fmt.Sprintf("executing sql=> %s", query), zap.String("task", taskCfg.Name))
		return conn.Exec(query)
	}

	if len(alterSeries) != 0 {
		sort.Strings(alterSeries)
		columns := strings.Join(alterSeries, ",")
		if err = alterTable(c.seriesTbl, columns); err != nil {
			return err
		}
		for _, distTbl := range c.distSeriesTbls {
			if err = alterTable(distTbl, columns); err != nil {
				return err
			}
		}
	}
	if len(alterMetric) != 0 {
		sort.Strings(alterMetric)
		columns := strings.Join(alterMetric, ",")
		if err = alterTable(c.TableName, columns); err != nil {
			return err
		}
		for _, distTbl := range c.distMetricTbls {
			if err = alterTable(distTbl, columns); err != nil {
				return err
			}
		}
	}

	return
}

func (c *ClickHouse) getDistTbls(table, clusterName string) (distTbls []DistTblInfo, err error) {
	taskCfg := c.taskCfg
	sc := pool.GetShardConn(0)
	var conn *pool.Conn
	if conn, _, err = sc.NextGoodReplica(c.cfg.Clickhouse.Ctx, 0); err != nil {
		return
	}
	query := fmt.Sprintf(`SELECT name, (extractAllGroups(engine_full, '(Distributed\\(\')(.*)\',\\s+\'(.*)\',\\s+\'(.*)\'(.*)')[1])[2] AS cluster
	 FROM system.tables WHERE engine='Distributed' AND database='%s' AND match(engine_full, 'Distributed\(\'.*\', \'%s\', \'%s\'.*\)')`,
		c.dbName, c.dbName, table)
	util.Logger.Info(fmt.Sprintf("executing sql=> %s", query), zap.String("task", taskCfg.Name))
	var rows *pool.Rows
	if rows, err = conn.Query(query); err != nil {
		err = errors.Wrapf(err, "")
		return
	}
	defer rows.Close()
	var curInfo DistTblInfo
	for rows.Next() {
		var name, cluster string
		if err = rows.Scan(&name, &cluster); err != nil {
			err = errors.Wrapf(err, "")
			return
		}
		if cluster == clusterName {
			// distributed table
			curInfo = DistTblInfo{name: name, cluster: cluster}
		} else {
			// logic table
			distTbls = append(distTbls, DistTblInfo{name: name, cluster: cluster})
		}
	}
	// dist table always in the end
	distTbls = append(distTbls, curInfo)
	return
}

func (c *ClickHouse) GetSeriesQuotaKey() string {
	if c.taskCfg.PrometheusSchema {
		if c.cfg.Clickhouse.Cluster != "" {
			return c.dbName + "." + c.distSeriesTbls[len(c.distSeriesTbls)-1]
		} else {
			return c.dbName + "." + c.seriesTbl
		}
	}
	return ""
}

func (c *ClickHouse) GetMetricTable() string {
	if c.taskCfg.PrometheusSchema {
		if c.cfg.Clickhouse.Cluster != "" {
			return c.distMetricTbls[len(c.distMetricTbls)-1]
		} else {
			return c.TableName
		}
	}
	return ""
}

func (c *ClickHouse) SetSeriesQuota(sq *model.SeriesQuota) {
	c.seriesQuota = sq
}
