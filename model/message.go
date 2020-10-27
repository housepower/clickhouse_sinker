package model

import (
	"container/list"
	"strings"
	"sync"
	"sync/atomic"
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
	Msg   *InputMessage
	Row   []interface{}
	Shard int
}

type Batch struct {
	MsgRows  []MsgRow
	BatchIdx int64
	RealSize int
	Group    *BatchGroup
}

//BatchGroup consists of multiple batches.
//The `before` relationship could be impossilbe if messages of a partition are distributed to multiple batches.
//So those batches need to be committed after ALL of them have been written to clickhouse.
type BatchGroup struct {
	Batchs    []*Batch
	Offsets   []int64
	Sys       *BatchSys
	PendWrite int32 //how many batches in this group are pending to wirte to ClickHouse
}

type BatchSys struct {
	mux      sync.Mutex
	groups   list.List
	fnCommit func(partition int, offset int64) error
}

func NewBatchSys(fnCommit func(partition int, offset int64) error) *BatchSys {
	return &BatchSys{fnCommit: fnCommit}
}

func (bs *BatchSys) TryCommit() error {
	bs.mux.Lock()
	defer bs.mux.Unlock()
	// ensure groups be committed orderly
LOOP:
	for e := bs.groups.Front(); e != nil; {
		grp := e.Value.(*BatchGroup)
		if atomic.LoadInt32(&grp.PendWrite) != 0 {
			break LOOP
		}
		// commit the whole group
		for j, off := range grp.Offsets {
			if off >= 0 {
				if err := bs.fnCommit(j, off); err != nil {
					return err
				}
			}
		}
		eNext := e.Next()
		bs.groups.Remove(e)
		e = eNext
	}
	return nil
}

func (bs *BatchSys) CreateBatchGroupSingle(batch *Batch, partition int, offset int64) {
	bg := &BatchGroup{
		Sys:       bs,
		Batchs:    []*Batch{batch},
		Offsets:   make([]int64, partition+1),
		PendWrite: 1,
	}
	bg.Batchs[0].Group = bg
	for i := 0; i < partition; i++ {
		bg.Offsets[i] = -1
	}
	bg.Offsets[partition] = offset
	bs.mux.Lock()
	bs.groups.PushBack(bg)
	bs.mux.Unlock()
}

func (bs *BatchSys) CreateBatchGroupMulti(batches []*Batch, offsets []int64) {
	bg := &BatchGroup{Sys: bs, PendWrite: int32(len(batches))}
	bg.Batchs = append(bg.Batchs, batches...)
	bg.Offsets = append(bg.Offsets, offsets...)
	for _, batch := range bg.Batchs {
		batch.Group = bg
	}
	bs.mux.Lock()
	bs.groups.PushBack(bg)
	bs.mux.Unlock()
}

func NewBatch(cap int) (batch *Batch) {
	b := &Batch{
		MsgRows: make([]MsgRow, 0, cap),
	}
	return b
}

func (b *Batch) Size() int {
	return len(b.MsgRows)
}

func (b *Batch) Commit() error {
	atomic.AddInt32(&b.Group.PendWrite, -1)
	return b.Group.Sys.TryCommit()
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
