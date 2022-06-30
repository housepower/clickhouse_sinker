package output

import (
	"database/sql"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/RoaringBitmap/roaring"
	"github.com/housepower/clickhouse_sinker/model"
	"github.com/housepower/clickhouse_sinker/pool"
	"github.com/housepower/clickhouse_sinker/util"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

func shouldReconnect(err error, sc *pool.ShardConn) bool {
	var exp *clickhouse.Exception
	if errors.As(err, &exp) {
		util.Logger.Error("this is an exception from clickhouse-server", zap.String("dsn", sc.GetDsn()), zap.Reflect("exception", exp))
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

func writeRows(prepareSQL string, rows model.Rows, idxBegin, idxEnd int, conn *sql.DB) (numBad int, err error) {
	var stmt *sql.Stmt
	var tx *sql.Tx
	var errExec error
	if tx, err = conn.Begin(); err != nil {
		err = errors.Wrapf(err, "conn.Begin %s", prepareSQL)
		return
	}
	if stmt, err = tx.Prepare(prepareSQL); err != nil {
		err = errors.Wrapf(err, "tx.Prepare %s", prepareSQL)
		_ = tx.Rollback()
		return
	}
	defer stmt.Close()
	var bmBad *roaring.Bitmap
	for i, row := range rows {
		if _, err = stmt.Exec((*row)[idxBegin:idxEnd]...); err != nil {
			if bmBad == nil {
				errExec = errors.Wrapf(err, "stmt.Exec")
				bmBad = roaring.NewBitmap()
			}
			bmBad.AddInt(i)
		}
	}
	if errExec != nil {
		stmt.Close()
		_ = tx.Rollback()
		numBad = int(bmBad.GetCardinality())
		util.Logger.Warn(fmt.Sprintf("writeRows skipped %d rows of %d due to invalid content", numBad, len(rows)), zap.Error(errExec))
		// write rows again, skip bad ones
		if tx, err = conn.Begin(); err != nil {
			err = errors.Wrapf(err, "conn.Begin %s", prepareSQL)
			return
		}
		if stmt, err = tx.Prepare(prepareSQL); err != nil {
			err = errors.Wrapf(err, "tx.Prepare %s", prepareSQL)
			_ = tx.Rollback()
			return
		}
		defer stmt.Close()
		for i, row := range rows {
			if !bmBad.ContainsInt(i) {
				if _, err = stmt.Exec((*row)[idxBegin:idxEnd]...); err != nil {
					err = errors.Wrapf(err, "stmt.Exec")
					break
				}
			}
		}
		if err != nil {
			_ = tx.Rollback()
			return
		}
		if err = tx.Commit(); err != nil {
			err = errors.Wrapf(err, "tx.Commit")
			return
		}
		return
	}
	if err = tx.Commit(); err != nil {
		err = errors.Wrapf(err, "tx.Commit")
		return
	}
	return
}

func getDims(database, table string, excludedColumns []string, conn *sql.DB) (dims []*model.ColumnWithType, err error) {
	var rs *sql.Rows
	if rs, err = conn.Query(fmt.Sprintf(selectSQLTemplate, database, table)); err != nil {
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
			tp, nullable := model.WhichType(typ)
			dims = append(dims, &model.ColumnWithType{Name: name, Type: tp, Nullable: nullable, SourceName: util.GetSourceName(name)})
		}
	}
	if len(dims) == 0 {
		err = errors.Wrapf(ErrTblNotExist, "%s.%s", database, table)
		return
	}
	return
}

func recreateDistTbls(cluster, database, table string, distTbls []string, conn *sql.DB) (err error) {
	var queries []string
	for _, distTbl := range distTbls {
		queries = append(queries, fmt.Sprintf("DROP TABLE IF EXISTS `%s`.`%s` ON CLUSTER `%s`", database, distTbl, cluster))
		queries = append(queries, fmt.Sprintf("CREATE TABLE `%s`.`%s` ON CLUSTER `%s` AS `%s`.`%s` ENGINE = Distributed(`%s`, `%s`, `%s`);",
			database, distTbl, cluster, database, table,
			cluster, database, table))
	}
	for _, query := range queries {
		util.Logger.Info(fmt.Sprintf("executing sql=> %s", query))
		if _, err = conn.Exec(query); err != nil {
			err = errors.Wrapf(err, "")
			return
		}
	}
	return
}
