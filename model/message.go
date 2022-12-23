package model

import (
	"sync"
	"time"
)

var (
	rowsPool sync.Pool
	FakedRow Row = make([]interface{}, 0)
)

// MsgWithMeta abstract messages
// We are not using interface because virtual call. See https://syslog.ravelin.com/go-interfaces-but-at-what-cost-961e0f58a07b?gi=58f6761d1d70
type InputMessage struct {
	Topic     string
	Partition int
	Key       []byte
	Value     []byte
	Offset    int64
	Timestamp *time.Time
}

type Row []interface{}
type Rows []*Row

type MsgRow struct {
	Msg   *InputMessage
	Row   *Row
	Shard int
}

type Batch struct {
	Rows     *Rows
	BatchIdx int64
	RealSize int

	Wg *sync.WaitGroup
}

func (b *Batch) Size() int {
	return len(*b.Rows)
}

func GetRows() (rs *Rows) {
	v := rowsPool.Get()
	if v == nil {
		rows := make(Rows, 0)
		return &rows
	}
	return v.(*Rows)
}

func PutRows(rs *Rows) {
	*rs = (*rs)[:0]
	rowsPool.Put(rs)
}

var rowPool sync.Pool

func GetRow() *Row {
	v := rowPool.Get()
	if v == nil {
		row := make(Row, 0)
		return &row
	}
	return v.(*Row)
}

func PutRow(r *Row) {
	*r = (*r)[:0]
	rowPool.Put(r)
}
