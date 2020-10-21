package model

import (
	"strings"
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

type MsgRow struct {
	Msg *InputMessage
	Row []interface{}
}

type Batch struct {
	MsgRows  []MsgRow
	BatchIdx int64
	RealSize int
}

func (b Batch) Size() int {
	return len(b.MsgRows)
}

func NewBatch(batchSize int, cap int) (batch Batch) {
	return Batch{
		MsgRows: make([]MsgRow, batchSize, cap),
	}
}

func MetricToRow(metric Metric, msg InputMessage, dims []*ColumnWithType) (row []interface{}) {
	row = make([]interface{}, len(dims))
	for i, dim := range dims {
		if strings.HasPrefix(dim.Name, "__kafka") {
			if strings.HasSuffix(dim.Name, "_topic") {
				row[i] = msg.Topic
			} else if strings.HasSuffix(dim.Name, "_partition") {
				row[i] = msg.Partition
			} else {
				row[i] = msg.Offset
			}
		} else {
			row[i] = GetValueByType(metric, dim)
		}
	}
	return
}
