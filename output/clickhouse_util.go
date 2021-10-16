package output

import (
	"database/sql"
	"fmt"
	"regexp"

	"github.com/ClickHouse/clickhouse-go"
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

func writeRows(prepareSQL string, rows model.Rows, conn *sql.DB) (err error) {
	var stmt *sql.Stmt
	var tx *sql.Tx
	if tx, err = conn.Begin(); err != nil {
		err = errors.Wrapf(err, "conn.Begin %s", prepareSQL)
		return
	}
	if stmt, err = tx.Prepare(prepareSQL); err != nil {
		err = errors.Wrapf(err, "tx.Prepare %s", prepareSQL)
		return
	}
	defer stmt.Close()
	for _, row := range rows {
		if _, err = stmt.Exec(*row...); err != nil {
			err = errors.Wrapf(err, "stmt.Exec")
			break
		}
	}
	if err != nil {
		_ = tx.Rollback()
		return err
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
		queries = append(queries, fmt.Sprintf("DROP TABLE IF EXISTS %s.%s ON CLUSTER %s", database, distTbl, cluster))
		queries = append(queries, fmt.Sprintf("CREATE TABLE %s.%s ON CLUSTER %s AS %s ENGINE = Distributed(%s, %s, %s);",
			database, distTbl, cluster, table,
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

func isReplicated(database, table string, conn *sql.DB) (yes bool, err error) {
	var rs *sql.Rows
	if rs, err = conn.Query(fmt.Sprintf("SHOW CREATE TABLE %s.%s", database, table)); err != nil {
		err = errors.Wrapf(err, "")
		return
	}
	defer rs.Close()
	var statement string
	var matched bool
	for rs.Next() {
		if err = rs.Scan(&statement); err != nil {
			err = errors.Wrapf(err, "")
			return
		}
		if matched, err = regexp.Match(`ENGINE\s*=\s*Replicated`, []byte(statement)); err != nil {
			err = errors.Wrapf(err, "")
			return
		}
		yes = matched
		return
	}
	return
}
