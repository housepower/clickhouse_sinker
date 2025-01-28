package output

import (
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/thanos-io/thanos/pkg/errors"
	"github.com/viru-tech/clickhouse_sinker/model"
	"github.com/viru-tech/clickhouse_sinker/pool"
	"github.com/viru-tech/clickhouse_sinker/util"
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

func writeRows(prepareSQL string, rows model.Rows, idxBegin, idxEnd int, conn *pool.Conn) (numBad int, err error) {
	return conn.Write(prepareSQL, rows, idxBegin, idxEnd)
}

func getDims(database, table string, excludedColumns []string, parser string, conn *pool.Conn) (dims []*model.ColumnWithType, err error) {
	var rs *pool.Rows
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
		if !util.StringContains(excludedColumns, name) && defaultKind != "MATERIALIZED" {
			dims = append(dims, &model.ColumnWithType{Name: name, Type: model.WhichType(typ), SourceName: util.GetSourceName(parser, name)})
		}
	}
	if len(dims) == 0 {
		err = errors.Wrapf(ErrTblNotExist, "%s.%s", database, table)
		return
	}
	return
}
