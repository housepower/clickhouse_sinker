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
	Rows *Rows
	// Msgs 与 *Rows 1:1 对齐,携带原始 kafka 消息以支持死信重放;可能为 nil。
	Msgs     []*InputMessage
	BatchIdx int64
	GroupId  string
	RealSize int

	Wg *sync.WaitGroup
}

func (b *Batch) Size() int {
	return len(*b.Rows)
}

type BatchRange struct {
	Begin int64
	End   int64
}

type RecordMap = map[string]map[int32]*BatchRange
