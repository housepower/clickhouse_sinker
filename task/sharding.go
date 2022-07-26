package task

import (
	"fmt"
	"sync"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/fagongzi/goetty"
	"github.com/housepower/clickhouse_sinker/model"
	"github.com/housepower/clickhouse_sinker/pool"
	"github.com/housepower/clickhouse_sinker/statistics"
	"github.com/housepower/clickhouse_sinker/util"
	"github.com/shopspring/decimal"
	"github.com/thanos-io/thanos/pkg/errors"
	"go.uber.org/zap"
)

type ShardingPolicy struct {
	shards int    //number of clickhouse shards
	colSeq int    //shardingKey column seq, 0 based
	stripe uint64 //=0 means hash, >0 means stripe size
}

func NewShardingPolicy(shardingKey string, shardingStripe uint64, dims []*model.ColumnWithType, shards int) (policy *ShardingPolicy, err error) {
	policy = &ShardingPolicy{stripe: shardingStripe, shards: shards}
	colSeq := -1
	for i, dim := range dims {
		if dim.Name == shardingKey {
			if dim.Nullable || dim.Array {
				err = errors.Newf("invalid shardingKey %s, expect its type be numerical or string", shardingKey)
				return
			}
			colSeq = i
			switch dim.Type {
			case model.Int8, model.Int16, model.Int32, model.Int64, model.UInt8, model.UInt16, model.UInt32, model.UInt64, model.Float32, model.Float64, model.Decimal, model.DateTime:
				//numerical
				if policy.stripe <= 0 {
					policy.stripe = uint64(1)
				}
			case model.String:
				//string
				policy.stripe = 0
			default:
				err = errors.Newf("invalid shardingKey %s, expect its type be numerical or string", shardingKey)
				return
			}
		}
	}
	if colSeq < 0 {
		err = errors.Newf("invalid shardingKey %s, no such column", shardingKey)
		return
	}
	policy.colSeq = colSeq
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
		case decimal.Decimal:
			valu64 = uint64(v.IntPart())
		case time.Time:
			valu64 = uint64(v.Unix())
		default:
			err = errors.Newf("failed to convert %+v to integer", v)
			return
		}
		shard = int((valu64 / policy.stripe) % uint64(policy.shards))
	} else {
		var valu64 uint64
		switch v := val.(type) {
		case []byte:
			valu64 = xxhash.Sum64(v)
		case string:
			valu64 = xxhash.Sum64String(v)
		default:
			err = errors.Newf("failed to convert %+v to string", v)
			return
		}
		shard = int(valu64 % uint64(policy.shards))
	}
	return
}

type Sharder struct {
	service  *Service
	policy   *ShardingPolicy
	batchSys *model.BatchSys
	shards   int
	mux      sync.Mutex
	msgBuf   []*model.Rows
	offsets  map[int]int64
	tid      goetty.Timeout
}

func NewSharder(service *Service) (sh *Sharder, err error) {
	var policy *ShardingPolicy
	shards := pool.NumShard()
	taskCfg := service.taskCfg
	if policy, err = NewShardingPolicy(taskCfg.ShardingKey, taskCfg.ShardingStripe, service.clickhouse.Dims, shards); err != nil {
		return
	}
	sh = &Sharder{
		service:  service,
		policy:   policy,
		batchSys: model.NewBatchSys(taskCfg, service.fnCommit),
		shards:   shards,
		msgBuf:   make([]*model.Rows, shards),
		offsets:  make(map[int]int64),
	}
	for i := 0; i < shards; i++ {
		sh.msgBuf[i] = model.GetRows()
	}
	return
}

func (sh *Sharder) Calc(row *model.Row) (int, error) {
	return sh.policy.Calc(row)
}

func (sh *Sharder) PutElems(partition int, ringBuf []model.MsgRow, begOff, endOff, ringCapMask int64) {
	if begOff >= endOff {
		return
	}
	msgCnt := endOff - begOff
	sh.mux.Lock()
	defer sh.mux.Unlock()
	var parseErrs int
	taskCfg := sh.service.taskCfg
	for i := begOff; i < endOff; i++ {
		msgRow := &ringBuf[i&ringCapMask]
		//assert msg.Offset==i
		if msgRow.Row != &model.FakedRow {
			rows := sh.msgBuf[msgRow.Shard]
			*rows = append(*rows, msgRow.Row)
		} else {
			parseErrs++
		}
		msgRow.Msg = nil
		msgRow.Row = nil
		msgRow.Shard = -1
	}

	sh.offsets[partition] = endOff - 1
	statistics.ShardMsgs.WithLabelValues(taskCfg.Name).Add(float64(msgCnt))
	var maxBatchSize int
	for i := 0; i < sh.shards; i++ {
		batchSize := len(*sh.msgBuf[i])
		if maxBatchSize < batchSize {
			maxBatchSize = batchSize
		}
	}
	util.Logger.Debug(fmt.Sprintf("sharded a batch for topic %v patittion %d, offset [%d, %d), messages %d, parse errors: %d",
		taskCfg.Topic, partition, begOff, endOff, msgCnt, parseErrs),
		zap.String("task", taskCfg.Name))
	if maxBatchSize >= taskCfg.BufferSize {
		sh.doFlush(nil)
	}
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
	taskCfg := sh.service.taskCfg
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
		util.Logger.Info(fmt.Sprintf("created a batch group for topic %v, offsets %+v, messages %d", taskCfg.Topic, sh.offsets, msgCnt), zap.String("task", taskCfg.Name))
		sh.batchSys.CreateBatchGroupMulti(batches, sh.offsets)
		sh.offsets = make(map[int]int64)
		// ALL batches in a group shall be populated before sending any one to next stage.
		for _, batch := range batches {
			sh.service.Flush(batch)
		}
		statistics.ShardMsgs.WithLabelValues(taskCfg.Name).Sub(float64(msgCnt))
	}

	// reschedule the delayed ForceFlush
	sh.tid.Stop()
	if sh.tid, err = util.GlobalTimerWheel.Schedule(time.Duration(taskCfg.FlushInterval)*time.Second, sh.ForceFlush, nil); err != nil {
		if errors.Is(err, goetty.ErrSystemStopped) {
			util.Logger.Info("Sharder.doFlush scheduling timer to a stopped timer wheel")
		} else {
			err = errors.Wrapf(err, "")
			util.Logger.Fatal("scheduling timer filed", zap.String("task", taskCfg.Name), zap.Error(err))
		}
	}
}
