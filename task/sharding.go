package task

import (
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cespare/xxhash"
	"github.com/fagongzi/goetty"
	"github.com/housepower/clickhouse_sinker/model"
	"github.com/housepower/clickhouse_sinker/pool"
	"github.com/housepower/clickhouse_sinker/statistics"
	"github.com/housepower/clickhouse_sinker/util"
	"github.com/pkg/errors"
	"github.com/sundy-li/go_commons/log"
)

type ShardingPolicy struct {
	ckNum  int    //number of clickhouse instances
	colSeq int    //shardingKey column seq, 0 based
	stripe uint64 //=0 means hash, >0 means stripe size
}

func NewShardingPolicy(shardingKey, shardingPolicy string, dims []string, ckNum int) (policy *ShardingPolicy, err error) {
	policy = &ShardingPolicy{ckNum: ckNum}
	colSeq := -1
	for i, dim := range dims {
		if dim == shardingKey {
			colSeq = i
		}
	}
	if colSeq < 0 {
		err = errors.Errorf("invalid shardingKey %s", shardingKey)
		return
	}
	policy.colSeq = colSeq
	if shardingPolicy == "hash" {
		policy.stripe = 0
	} else if strings.HasPrefix(shardingPolicy, "stripe,") {
		if policy.stripe, err = strconv.ParseUint(shardingPolicy[len("stripe,"):], 10, 64); err != nil {
			err = errors.Wrapf(err, "invalid shardingPolicy %s", shardingPolicy)
		}
	} else {
		err = errors.Errorf("invalid shardingPolicy %s", shardingPolicy)
	}
	return
}

func (policy *ShardingPolicy) Calc(row *model.Row) (shard int, err error) {
	val := (*row)[policy.colSeq]
	if policy.stripe > 0 {
		var valu64 uint64
		switch v := val.(type) {
		case int:
			valu64 = uint64(v)
		case int8:
			valu64 = uint64(v)
		case int16:
			valu64 = uint64(v)
		case int32:
			valu64 = uint64(v)
		case int64:
			valu64 = uint64(v)
		case uint:
			valu64 = uint64(v)
		case uint8:
			valu64 = uint64(v)
		case uint16:
			valu64 = uint64(v)
		case uint32:
			valu64 = uint64(v)
		case uint64:
			valu64 = v
		case float32:
			valu64 = uint64(v)
		case float64:
			valu64 = uint64(v)
		case time.Time:
			valu64 = uint64(v.Unix())
		default:
			err = errors.Errorf("failed to convert %+v to integer", v)
			return
		}
		shard = int((valu64 / policy.stripe) % uint64(policy.ckNum))
	} else {
		var valu64 uint64
		switch v := val.(type) {
		case []byte:
			valu64 = xxhash.Sum64(v)
		case string:
			valu64 = xxhash.Sum64String(v)
		default:
			err = errors.Errorf("failed to convert %+v to string", v)
			return
		}
		shard = int(valu64 % uint64(policy.ckNum))
	}
	return
}

type Sharder struct {
	service  *Service
	policy   *ShardingPolicy
	batchSys *model.BatchSys
	ckNum    int
	mux      sync.Mutex
	msgBuf   []*model.Rows
	offsets  []int64
	tid      goetty.Timeout
}

func NewSharder(service *Service) (sh *Sharder, err error) {
	var policy *ShardingPolicy
	ckNum := pool.GetNumConn(service.taskCfg.Clickhouse)
	if policy, err = NewShardingPolicy(service.taskCfg.ShardingKey, service.taskCfg.ShardingPolicy, service.clickhouse.Dms, ckNum); err != nil {
		return
	}
	sh = &Sharder{
		service:  service,
		policy:   policy,
		batchSys: model.NewBatchSys(service.taskCfg, service.fnCommit),
		ckNum:    ckNum,
		msgBuf:   make([]*model.Rows, ckNum),
		offsets:  make([]int64, 0),
	}
	for i := 0; i < ckNum; i++ {
		sh.msgBuf[i] = model.GetRows()
	}
	return
}

func (sh *Sharder) Calc(row *model.Row) (int, error) {
	return sh.policy.Calc(row)
}

func (sh *Sharder) PutElems(partition int, ringBuf []model.MsgRow, begOff, endOff, ringCap int64) (msgCnt int) {
	sh.mux.Lock()
	defer sh.mux.Unlock()
	var gaps []OffsetRange
	var parseErrs int
	gapBegOff := int64(-1)
	for i := begOff; i < endOff; i++ {
		msgRow := &ringBuf[i&(ringCap-1)]
		if msgRow.Msg != nil {
			msgCnt++
			//assert msg.Offset==i
			if msgRow.Row != nil {
				rows := sh.msgBuf[msgRow.Shard]
				*rows = append(*rows, msgRow.Row)
			} else {
				parseErrs++
			}
			if gapBegOff >= 0 {
				gaps = append(gaps, OffsetRange{Begin: gapBegOff, End: i})
				gapBegOff = -1
			}
		} else if gapBegOff < 0 {
			gapBegOff = i
		}
		msgRow.Msg = nil
		msgRow.Row = nil
		msgRow.Shard = -1
	}
	if gapBegOff >= 0 {
		gaps = append(gaps, OffsetRange{Begin: gapBegOff, End: endOff})
	}

	gap := partition + 1 - len(sh.offsets)
	for i := 0; i < gap; i++ {
		sh.offsets = append(sh.offsets, -1)
	}
	if msgCnt > 0 {
		sh.offsets[partition] = endOff - 1
		statistics.ShardMsgsBacklog.WithLabelValues(sh.service.taskCfg.Name).Add(float64(msgCnt))
	}
	var maxBatchSize int
	for i := 0; i < sh.ckNum; i++ {
		batchSize := len(*sh.msgBuf[i])
		if maxBatchSize < batchSize {
			maxBatchSize = batchSize
		}
	}
	log.Debugf("%s: sharded a batch for topic %v patittion %d, offset %d, messages %d, gaps: %+v, parse errors: %d",
		sh.service.taskCfg.Name, sh.service.taskCfg.Topic, partition, endOff-1,
		msgCnt, gaps, parseErrs)
	if maxBatchSize >= sh.service.taskCfg.BufferSize {
		sh.doFlush(nil)
	}
	return
}

func (sh *Sharder) ForceFlush(arg interface{}) {
	sh.mux.Lock()
	sh.doFlush(arg)
	sh.mux.Unlock()
}

// assmues sh.mux has been locked
func (sh *Sharder) doFlush(_ interface{}) {
	var err error
	var msgCnt int
	var batches []*model.Batch
	for i, rows := range sh.msgBuf {
		realSize := len(*rows)
		if realSize > 0 {
			msgCnt += realSize
			batch := &model.Batch{
				Rows:     rows,
				BatchIdx: int64(i),
				RealSize: realSize,
			}
			batches = append(batches, batch)
			sh.msgBuf[i] = model.GetRows()
		}
	}
	if msgCnt > 0 {
		log.Debugf("%s: going to flush batch group for topic %v, offsets %+v, messages %d", sh.service.taskCfg.Name, sh.service.taskCfg.Name, sh.offsets, msgCnt)
		sh.batchSys.CreateBatchGroupMulti(batches, sh.offsets)
		sh.offsets = sh.offsets[:0]
		// ALL batches in a group shall be populated before sending any one to next stage.
		for _, batch := range batches {
			sh.service.batchChan <- batch
		}
		statistics.ShardMsgsBacklog.WithLabelValues(sh.service.taskCfg.Name).Sub(float64(msgCnt))
	}

	// reschedule the delayed ForceFlush
	sh.tid.Stop()
	if sh.tid, err = util.GlobalTimerWheel.Schedule(time.Duration(sh.service.taskCfg.FlushInterval)*time.Second, sh.ForceFlush, nil); err != nil {
		err = errors.Wrap(err, "")
		log.Criticalf("%s: got error %+v", sh.service.taskCfg.Name, err)
	}
}
