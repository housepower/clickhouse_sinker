package model

import (
	"container/list"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/housepower/clickhouse_sinker/config"
	"github.com/housepower/clickhouse_sinker/statistics"
)

var (
	rowsPool sync.Pool
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
	Group    *BatchGroup
}

//BatchGroup consists of multiple batches.
//The `before` relationship could be impossible if messages of a partition are distributed to multiple batches.
//So those batches need to be committed after ALL of them have been written to clickhouse.
type BatchGroup struct {
	Batchs    []*Batch
	Offsets   []int64
	Sys       *BatchSys
	PendWrite int32 //how many batches in this group are pending to wirte to ClickHouse
}

type BatchSys struct {
	taskCfg  *config.TaskConfig
	mux      sync.Mutex
	groups   list.List
	fnCommit func(partition int, offset int64) error
}

func NewBatchSys(taskCfg *config.TaskConfig, fnCommit func(partition int, offset int64) error) *BatchSys {
	return &BatchSys{taskCfg: taskCfg, fnCommit: fnCommit}
}

func (bs *BatchSys) TryCommit() error {
	bs.mux.Lock()
	defer bs.mux.Unlock()
	// ensure groups be committed orderly
LOOP:
	for e := bs.groups.Front(); e != nil; {
		grp, _ := e.Value.(*BatchGroup)
		if atomic.LoadInt32(&grp.PendWrite) != 0 {
			break LOOP
		}
		// commit the whole group
		for j, off := range grp.Offsets {
			if off >= 0 {
				if err := bs.fnCommit(j, off); err != nil {
					return err
				}
				statistics.ConsumeOffsets.WithLabelValues(bs.taskCfg.Name, bs.taskCfg.Topic, strconv.Itoa(j)).Set(float64(off))
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

func NewBatch() (b *Batch) {
	return &Batch{
		Rows: GetRows(),
	}
}

func (b *Batch) Size() int {
	return len(*b.Rows)
}

// Commit is not retry-able!
func (b *Batch) Commit() error {
	for _, row := range *b.Rows {
		PutRow(row)
	}
	PutRows(b.Rows)
	b.Rows = nil
	atomic.AddInt32(&b.Group.PendWrite, -1)
	return b.Group.Sys.TryCommit()
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

func MetricToRow(metric Metric, msg InputMessage, dims []*ColumnWithType) (row *Row) {
	row = GetRow()
	for _, dim := range dims {
		if strings.HasPrefix(dim.Name, "__kafka") {
			if strings.HasSuffix(dim.Name, "_topic") {
				*row = append(*row, msg.Topic)
			} else if strings.HasSuffix(dim.Name, "_partition") {
				*row = append(*row, msg.Partition)
			} else {
				*row = append(*row, msg.Offset)
			}
		} else {
			val := GetValueByType(metric, dim)
			*row = append(*row, val)
		}
	}
	return
}
