package task

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/housepower/clickhouse_sinker/model"
	"github.com/housepower/clickhouse_sinker/pool"
	"github.com/housepower/clickhouse_sinker/statistics"
	"github.com/housepower/clickhouse_sinker/util"
	nanoid "github.com/matoous/go-nanoid/v2"
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
			if dim.Type.Nullable || dim.Type.Array {
				err = errors.Newf("invalid shardingKey '%s', expect its type be numerical or string", shardingKey)
				return
			}
			colSeq = i
			switch dim.Type.Type {
			case model.Int8, model.Int16, model.Int32, model.Int64, model.UInt8, model.UInt16, model.UInt32, model.UInt64, model.Float32, model.Float64, model.Decimal, model.DateTime:
				//numerical
				if policy.stripe <= 0 {
					policy.stripe = uint64(1)
				}
			case model.String:
				//string
				policy.stripe = 0
			default:
				err = errors.Newf("invalid shardingKey '%s', expect its type be numerical or string", shardingKey)
				return
			}
		}
	}
	if colSeq < 0 {
		util.Logger.Info("shardingKey is __offset__, use offset as sharding key")
		if policy.stripe <= 0 {
			policy.stripe = uint64(1)
		}
	}
	policy.colSeq = colSeq
	return
}

func (policy *ShardingPolicy) Calc(row *model.Row, offset int64) (shard int, err error) {
	var val interface{}
	if policy.colSeq < 0 {
		val = offset
	} else {
		val = (*row)[policy.colSeq]
	}
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
	service *Service
	policy  *ShardingPolicy
	shards  int
	mux     sync.Mutex
	msgBuf  []*model.Rows
	msgMsgs [][]*model.InputMessage // 与 msgBuf 各 shard 1:1 对齐
}

func NewSharder(service *Service) (sh *Sharder, err error) {
	var policy *ShardingPolicy
	shards := pool.NumShard()
	if policy, err = NewShardingPolicy(service.shardingKey, service.shardingStripe, service.clickhouse.Dims, shards); err != nil {
		return sh, errors.Wrapf(err, "error when creating sharding policy for task '%s'", service.taskCfg.Name)
	}
	sh = &Sharder{
		service: service,
		policy:  policy,
		shards:  shards,
	}
	sh.reset(shards)
	return
}

// reset 重建所有 shard 的缓冲。调用者需持有 mux(或在构造期单线程)。
func (sh *Sharder) reset(shards int) {
	sh.msgBuf = make([]*model.Rows, shards)
	sh.msgMsgs = make([][]*model.InputMessage, shards)
	for i := 0; i < shards; i++ {
		rs := make(model.Rows, 0)
		sh.msgBuf[i] = &rs
		sh.msgMsgs[i] = make([]*model.InputMessage, 0)
	}
}

// putRaw 追加一行及其原始消息到指定 shard。调用者需持有 mux。
func (sh *Sharder) putRaw(shard int, row *model.Row, msg *model.InputMessage) {
	*sh.msgBuf[shard] = append(*sh.msgBuf[shard], row)
	sh.msgMsgs[shard] = append(sh.msgMsgs[shard], msg)
}

// takeShard 取出并清空指定 shard 的缓冲。调用者需持有 mux。
func (sh *Sharder) takeShard(i int) (*model.Rows, []*model.InputMessage) {
	rows, msgs := sh.msgBuf[i], sh.msgMsgs[i]
	rs := make(model.Rows, 0, len(*rows))
	sh.msgBuf[i] = &rs
	sh.msgMsgs[i] = make([]*model.InputMessage, 0, len(msgs))
	return rows, msgs
}

func (sh *Sharder) Calc(row *model.Row, offset int64) (int, error) {
	return sh.policy.Calc(row, offset)
}

func (sh *Sharder) PutElement(msgRow *model.MsgRow) {
	sh.mux.Lock()
	defer sh.mux.Unlock()
	sh.putRaw(msgRow.Shard, msgRow.Row, msgRow.Msg)
	statistics.ShardMsgs.WithLabelValues(sh.service.taskCfg.Name).Inc()
}

func (sh *Sharder) Flush(c context.Context, wg *sync.WaitGroup, rmap map[int32]*model.BatchRange, traceId string) {
	sh.mux.Lock()
	defer sh.mux.Unlock()
	select {
	case <-c.Done():
		util.Logger.Info("batch abandoned because of context canceled")
		return
	default:
		var msgCnt int
		util.Logger.Debug("flush records to ck")
		taskCfg := sh.service.taskCfg
		batchId, _ := nanoid.New()
		for i := range sh.msgBuf {
			realSize := len(*sh.msgBuf[i])
			if realSize > 0 {
				msgCnt += realSize
				rows, msgs := sh.takeShard(i)
				batch := &model.Batch{
					Rows:     rows,
					Msgs:     msgs,
					BatchIdx: int64(i),
					GroupId:  batchId,
					RealSize: realSize,
					Wg:       wg,
				}
				batch.Wg.Add(1)
				sh.service.clickhouse.Send(batch, traceId)
			}
		}
		if msgCnt > 0 {
			util.Logger.Info(fmt.Sprintf("created a batch group for task %v with %d shards, total messages %d", sh.service.taskCfg.Name, len(sh.msgBuf), msgCnt),
				zap.String("group", batchId),
				zap.Reflect("offsets", rmap))
			statistics.ShardMsgs.WithLabelValues(taskCfg.Name).Sub(float64(msgCnt))
		}
	}
}
