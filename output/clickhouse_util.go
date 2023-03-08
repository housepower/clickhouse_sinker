package output

import (
	"context"
	"fmt"
	"time"

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
		for i, row := range rows {
			if !bmBad.ContainsInt(i) {
				if err = batch.Append((*row)[idxBegin:idxEnd]...); err != nil {
					break
				}
			}
		}
		if err = batch.Send(); err != nil {
			err = errors.Wrapf(err, "driver.Batch.Send")
			_ = batch.Abort()
			return
		}
		return
	}
	if err = batch.Send(); err != nil {
		err = errors.Wrapf(err, "driver.Batch.Send")
		_ = batch.Abort()
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

const (
	EXEC = iota
	QUERYROW
	QUERY
)

// LoopWrite will dead loop to write the records
func Execute(conn driver.Conn, ctx context.Context, querykind int, query string, fields []zap.Field, args ...interface{}) (result interface{}) {
	var times int
	util.Logger.Info(fmt.Sprintf("executing sql=> %s", query), fields...)
	var err error

	for {
		zapFields := fields
		switch querykind {
		case EXEC:
			err = conn.Exec(ctx, query, args...)
		case QUERYROW:
			result = conn.QueryRow(ctx, query, args...)
		case QUERY:
			result, err = conn.Query(ctx, query, args...)
		default:
			util.Logger.Fatal("unknown query kind specified")
		}

		if err == nil {
			return
		} else if errors.Is(err, context.Canceled) {
			util.Logger.Info("Execute failed due to the context has been cancelled", fields...)
			return
		} else if errors.Is(err, clickhouse.ErrAcquireConnTimeout) {
			time.Sleep(10 * time.Second)
		} else {
			zapFields = append(zapFields, zap.Error(err))
			util.Logger.Fatal("Query execution failed", zapFields...)
		}
		zapFields = append(zapFields, zap.Int("try", times), zap.Error(err))
		util.Logger.Error("Query execution failed", zapFields...)
		times++
	}
}
