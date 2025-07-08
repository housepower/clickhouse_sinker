package pool

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/RoaringBitmap/roaring"
	"github.com/thanos-io/thanos/pkg/errors"
	"github.com/viru-tech/clickhouse_sinker/model"
	"github.com/viru-tech/clickhouse_sinker/util"
	"go.uber.org/zap"
)

type Row struct {
	proto clickhouse.Protocol
	r1    *sql.Row
	r2    driver.Row
}

func (r *Row) Scan(dest ...any) error {
	if r.proto == clickhouse.HTTP {
		return r.r1.Scan(dest...)
	} else {
		return r.r2.Scan(dest...)
	}
}

type Rows struct {
	protocol clickhouse.Protocol
	rs1      *sql.Rows
	rs2      driver.Rows
}

func (r *Rows) Close() error {
	if r.protocol == clickhouse.HTTP {
		return r.rs1.Close()
	} else {
		return r.rs2.Close()
	}
}

func (r *Rows) Columns() ([]string, error) {
	if r.protocol == clickhouse.HTTP {
		return r.rs1.Columns()
	} else {
		return r.rs2.Columns(), nil
	}
}

func (r *Rows) Next() bool {
	if r.protocol == clickhouse.HTTP {
		return r.rs1.Next()
	} else {
		return r.rs2.Next()
	}
}

func (r *Rows) Scan(dest ...any) error {
	if r.protocol == clickhouse.HTTP {
		return r.rs1.Scan(dest...)
	} else {
		return r.rs2.Scan(dest...)
	}
}

type Conn struct {
	protocol clickhouse.Protocol
	c        driver.Conn
	db       *sql.DB
	ctx      context.Context
}

func (c *Conn) Query(query string, args ...any) (*Rows, error) {
	var rs Rows
	rs.protocol = c.protocol
	if c.protocol == clickhouse.HTTP {
		rows, err := c.db.Query(query, args...)
		if err != nil {
			return &rs, err
		} else {
			rs.rs1 = rows
		}
	} else {
		rows, err := c.c.Query(c.ctx, query, args...)
		if err != nil {
			return &rs, err
		} else {
			rs.rs2 = rows
		}
	}
	return &rs, nil
}

func (c *Conn) QueryRow(query string, args ...any) *Row {
	var row Row
	row.proto = c.protocol
	if c.protocol == clickhouse.HTTP {
		row.r1 = c.db.QueryRow(query, args...)
	} else {
		row.r2 = c.c.QueryRow(c.ctx, query, args...)
	}
	return &row
}

func (c *Conn) Exec(query string, args ...any) error {
	if c.protocol == clickhouse.HTTP {
		_, err := c.db.Exec(query, args...)
		return err
	} else {
		return c.c.Exec(c.ctx, query, args...)
	}
}

func (c *Conn) Ping() error {
	if c.protocol == clickhouse.HTTP {
		return c.db.Ping()
	} else {
		return c.c.Ping(c.ctx)
	}
}

func (c *Conn) write_v1(prepareSQL string, rows model.Rows, idxBegin, idxEnd int) (numBad int, err error) {
	var errExec error

	var stmt *sql.Stmt
	var tx *sql.Tx
	tx, err = c.db.Begin()
	if err != nil {
		err = errors.Wrapf(err, "pool.Conn.Begin")
		return
	}

	if stmt, err = tx.Prepare(prepareSQL); err != nil {
		err = errors.Wrapf(err, "tx.Prepare %s", prepareSQL)
		return
	}
	defer stmt.Close()

	var bmBad *roaring.Bitmap
	for i, row := range rows {
		if _, err = stmt.Exec((*row)[idxBegin:idxEnd]...); err != nil {
			if bmBad == nil {
				errExec = errors.Wrapf(err, "driver.Batch.Append")
				bmBad = roaring.NewBitmap()
			}
			bmBad.AddInt(i)
		}

	}
	if errExec != nil {
		_ = tx.Rollback()
		numBad = int(bmBad.GetCardinality())
		util.Logger.Warn(fmt.Sprintf("writeRows skipped %d rows of %d due to invalid content", numBad, len(rows)), zap.Error(errExec))
		// write rows again, skip bad ones
		if stmt, err = tx.Prepare(prepareSQL); err != nil {
			err = errors.Wrapf(err, "tx.Prepare %s", prepareSQL)
			return
		}
		for i, row := range rows {
			if !bmBad.ContainsInt(i) {
				if _, err = stmt.Exec((*row)[idxBegin:idxEnd]...); err != nil {
					break
				}
			}
		}
		if err = tx.Commit(); err != nil {
			err = errors.Wrapf(err, "tx.Commit")
			_ = tx.Rollback()
			return
		}
		return
	}
	if err = tx.Commit(); err != nil {
		err = errors.Wrapf(err, "tx.Commit")
		_ = tx.Rollback()
		return
	}
	return
}

func (c *Conn) write_v2(prepareSQL string, rows model.Rows, idxBegin, idxEnd int) (numBad int, err error) {
	var errExec error
	var batch driver.Batch
	if batch, err = c.c.PrepareBatch(c.ctx, prepareSQL); err != nil {
		err = errors.Wrapf(err, "pool.Conn.PrepareBatch %s", prepareSQL)
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
		if batch, err = c.c.PrepareBatch(c.ctx, prepareSQL); err != nil {
			err = errors.Wrapf(err, "pool.Conn.PrepareBatch %s", prepareSQL)
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

func (c *Conn) Write(prepareSQL string, rows model.Rows, idxBegin, idxEnd int) (numBad int, err error) {
	util.Logger.Debug("start write to ck", zap.Int("begin", idxBegin), zap.Int("end", idxEnd))
	if c.protocol == clickhouse.HTTP {
		numBad, err = c.write_v1(prepareSQL, rows, idxBegin, idxEnd)
	} else {
		numBad, err = c.write_v2(prepareSQL, rows, idxBegin, idxEnd)
	}
	util.Logger.Debug("loop write completed", zap.Int("numbad", numBad))
	return numBad, err
}

func (c *Conn) AsyncInsert(query string, wait bool) error {
	if c.protocol == clickhouse.HTTP {
		return fmt.Errorf("DO NOT SUPPORT THIS FUNCTION")
	} else {
		return c.c.AsyncInsert(c.ctx, query, wait)
	}
}

func (c *Conn) Close() error {
	if c.protocol == clickhouse.HTTP {
		return c.db.Close()
	} else {
		return c.c.Close()
	}
}
