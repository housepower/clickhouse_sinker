package output

import (
	"context"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/RoaringBitmap/roaring"
	"github.com/housepower/clickhouse_sinker/model"
	"github.com/housepower/clickhouse_sinker/pool"
	"github.com/housepower/clickhouse_sinker/util"
	"github.com/thanos-io/thanos/pkg/errors"
	"go.uber.org/zap"
)

func shouldReconnect(err error, sc *pool.ShardConn) bool {
	var exp *clickhouse.Exception
	if errors.As(err, &exp) {
		util.Logger.Error("this is an exception from clickhouse-server", zap.String("replica", sc.GetReplica()), zap.Reflect("exception", exp))
		var replicaSpecific bool
		for _, ec := range replicaSpecificErrorCodes {
			if ec == exp.Code {
				replicaSpecific = true
				break
			}
		}
		return replicaSpecific
	}
	return true
}

func writeRows(prepareSQL string, rows model.Rows, idxBegin, idxEnd int, conn clickhouse.Conn) (numBad int, err error) {
	var errExec error
	var batch driver.Batch
	if batch, err = conn.PrepareBatch(context.Background(), prepareSQL); err != nil {
		err = errors.Wrapf(err, "clickhouse.Conn.PrepareBatch %s", prepareSQL)
		return
	}
	defer func() {
		_ = batch.Abort()
	}()
	var bmBad *roaring.Bitmap
	for i, row := range rows {
		if err = batch.Append((*row)[idxBegin:idxEnd]...); err != nil {
			if bmBad == nil {
				errExec = errors.Wrapf(err, "driver.Batch.Append")
				bmBad = roaring.NewBitmap()
			}
			bmBad.AddInt(i)
		}
	}
	if errExec != nil {
		_ = batch.Abort()
		numBad = int(bmBad.GetCardinality())
		util.Logger.Warn(fmt.Sprintf("writeRows skipped %d rows of %d due to invalid content", numBad, len(rows)), zap.Error(errExec))
		// write rows again, skip bad ones
		if batch, err = conn.PrepareBatch(context.Background(), prepareSQL); err != nil {
			err = errors.Wrapf(err, "clickhouse.Conn.PrepareBatch %s", prepareSQL)
			return
		}
		defer func() {
			_ = batch.Abort()
		}()
		for i, row := range rows {
			if !bmBad.ContainsInt(i) {
				if err = batch.Append((*row)[idxBegin:idxEnd]...); err != nil {
					err = errors.Wrapf(err, "stmt.Exec")
					break
				}
			}
		}
		if err = batch.Send(); err != nil {
			err = errors.Wrapf(err, "driver.Batch.Send")
			return
		}
		return
	}
	if err = batch.Send(); err != nil {
		err = errors.Wrapf(err, "driver.Batch.Send")
		return
	}
	return
}

func getDims(database, table string, excludedColumns []string, parser string, conn clickhouse.Conn) (dims []*model.ColumnWithType, err error) {
	var rs driver.Rows
	if rs, err = conn.Query(context.Background(), fmt.Sprintf(selectSQLTemplate, database, table)); err != nil {
		err = errors.Wrapf(err, "")
		return
	}
	defer rs.Close()

	dims = make([]*model.ColumnWithType, 0, 10)
	var name, typ, defaultKind string
	for rs.Next() {
		if err = rs.Scan(&name, &typ, &defaultKind); err != nil {
			err = errors.Wrapf(err, "")
			return
		}
		typ = lowCardinalityRegexp.ReplaceAllString(typ, "$1")
		if !util.StringContains(excludedColumns, name) && defaultKind != "MATERIALIZED" {
			tp, nullable, array := model.WhichType(typ)
			dims = append(dims, &model.ColumnWithType{Name: name, Type: tp, Nullable: nullable, Array: array, SourceName: util.GetSourceName(parser, name)})
		}
	}
	if len(dims) == 0 {
		err = errors.Wrapf(ErrTblNotExist, "%s.%s", database, table)
		return
	}
	return
}

func recreateDistTbls(cluster, database, table string, distTbls []string, conn clickhouse.Conn) (err error) {
	var queries []string
	for _, distTbl := range distTbls {
		queries = append(queries, fmt.Sprintf("DROP TABLE IF EXISTS `%s`.`%s` ON CLUSTER `%s` SYNC", database, distTbl, cluster))
		queries = append(queries, fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%s`.`%s` ON CLUSTER `%s` AS `%s`.`%s` ENGINE = Distributed(`%s`, `%s`, `%s`);",
			database, distTbl, cluster, database, table,
			cluster, database, table))
	}
	for _, query := range queries {
		util.Logger.Info(fmt.Sprintf("executing sql=> %s", query))
		if err = conn.Exec(context.Background(), query); err != nil {
			err = errors.Wrapf(err, "")
			return
		}
	}
	return
}
