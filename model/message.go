package model

import (
	"sync"
	"time"
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
