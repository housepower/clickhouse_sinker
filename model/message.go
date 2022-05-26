package model

import (
	"container/list"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/housepower/clickhouse_sinker/config"
	"github.com/housepower/clickhouse_sinker/statistics"
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
	Group    *BatchGroup
}

//BatchGroup consists of multiple batches.
//The `before` relationship could be impossible if messages of a partition are distributed to multiple batches.
//So those batches need to be committed after ALL of them have been written to clickhouse.
type BatchGroup struct {
	Batchs    []*Batch
	Offsets   map[int]int64
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
			if err := bs.fnCommit(j, off); err != nil {
				return err
			}
			statistics.ConsumeOffsets.WithLabelValues(bs.taskCfg.Name, bs.taskCfg.Topic, strconv.Itoa(j)).Set(float64(off))
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
		Offsets:   make(map[int]int64),
		PendWrite: 1,
	}
	bg.Batchs[0].Group = bg
	bg.Offsets[partition] = offset
	bs.mux.Lock()
	bs.groups.PushBack(bg)
	bs.mux.Unlock()
}

func (bs *BatchSys) CreateBatchGroupMulti(batches []*Batch, offsets map[int]int64) {
	bg := &BatchGroup{Sys: bs, PendWrite: int32(len(batches))}
	bg.Batchs = append(bg.Batchs, batches...)
	bg.Offsets = offsets
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

func MetricToRow(metric Metric, msg *InputMessage, dims []*ColumnWithType, idxSeriesID int, nameKey string) (row *Row) {
	row = GetRow()
	var dig *xxhash.Digest
	var labels []string
	var seriesID, mgmtID int64
	if idxSeriesID >= 0 {
		// If some labels are not Prometheus native, ETL shall calculate and pass "__series_id", otherwise clickhouse_sinker use "__mgmt_id".
		val := metric.GetInt("__series_id", false)
		seriesID = val.(int64)
		// clickhouse_sinker calculate "__mgmt_id" based on all labels.
		dig = xxhash.New()
	}
	for i, dim := range dims {
		if idxSeriesID >= 0 && i == idxSeriesID {
			*row = append(*row, int64(0))
		} else if idxSeriesID >= 0 && i == idxSeriesID+1 {
			*row = append(*row, int64(0))
		} else if idxSeriesID >= 0 && i == idxSeriesID+2 {
			*row = append(*row, "")
		} else if strings.HasPrefix(dim.Name, "__kafka") {
			if strings.HasSuffix(dim.Name, "_topic") {
				*row = append(*row, msg.Topic)
			} else if strings.HasSuffix(dim.Name, "_partition") {
				*row = append(*row, msg.Partition)
			} else if strings.HasSuffix(dim.Name, "_offset") {
				*row = append(*row, msg.Offset)
			} else if strings.HasSuffix(dim.Name, "_key") {
				*row = append(*row, string(msg.Key))
			} else if strings.HasSuffix(dim.Name, "_timestamp") {
				*row = append(*row, *msg.Timestamp)
			} else {
				*row = append(*row, nil)
			}
		} else {
			val := GetValueByType(metric, dim)
			*row = append(*row, val)
			if idxSeriesID >= 0 && dim.Type == String && val != nil {
				if labelVal := val.(string); labelVal != "" {
					// "__series_id" calculation envolves all Prometheus native labels (including metric name) in some order.
					if dig != nil {
						_, _ = dig.WriteString("###")
						_, _ = dig.WriteString(dim.Name)
						_, _ = dig.WriteString("###")
						_, _ = dig.WriteString(labelVal)
					}
					// "labels" JSON excludes "le", so that "labels" can be used as group key for histogram queries.
					if dim.Name != nameKey && dim.Name != "le" {
						labels = append(labels, fmt.Sprintf(`%s: %s`, strconv.Quote(dim.Name), strconv.Quote(labelVal)))
					}
				}
			}
		}
	}
	if idxSeriesID >= 0 {
		mgmtID = int64(dig.Sum64())
		if seriesID == 0 {
			seriesID = mgmtID
		}
		(*row)[idxSeriesID] = seriesID
		(*row)[idxSeriesID+1] = mgmtID
		(*row)[idxSeriesID+2] = fmt.Sprintf("{%s}", strings.Join(labels, ", "))
	}
	return
}
