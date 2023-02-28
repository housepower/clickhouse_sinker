package task

import (
	"fmt"
	"sync"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/housepower/clickhouse_sinker/model"
	"github.com/housepower/clickhouse_sinker/pool"
	"github.com/housepower/clickhouse_sinker/statistics"
	"github.com/housepower/clickhouse_sinker/util"
	"github.com/shopspring/decimal"
	"github.com/thanos-io/thanos/pkg/errors"
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
				err = errors.Newf("invalid shardingKey %s, expect its type be numerical or string", shardingKey)
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
	service *Service
	policy  *ShardingPolicy
	shards  int
	mux     sync.Mutex
	msgBuf  []*model.Rows
}

func NewSharder(service *Service) (sh *Sharder, err error) {
	var policy *ShardingPolicy
	shards := pool.NumShard()
	taskCfg := service.taskCfg
	if taskCfg.ShardingKey != "" {
		if policy, err = NewShardingPolicy(taskCfg.ShardingKey, taskCfg.ShardingStripe, service.clickhouse.Dims, shards); err != nil {
			return
		}
	}
	sh = &Sharder{
		service: service,
		policy:  policy,
		shards:  shards,
		msgBuf:  make([]*model.Rows, shards),
	}
	for i := 0; i < shards; i++ {
		rs := make(model.Rows, 0)
		sh.msgBuf[i] = &rs
	}
	return
}

func (sh *Sharder) Calc(row *model.Row) (int, error) {
	return sh.policy.Calc(row)
}

func (sh *Sharder) PutElement(msgRow *model.MsgRow) {
	sh.mux.Lock()
	defer sh.mux.Unlock()
	rows := sh.msgBuf[msgRow.Shard]
	*rows = append(*rows, msgRow.Row)
	statistics.ShardMsgs.WithLabelValues(sh.service.taskCfg.Name).Inc()
}

func (sh *Sharder) Flush(wg *sync.WaitGroup) {
	sh.mux.Lock()
	defer sh.mux.Unlock()
	var msgCnt int
	taskCfg := sh.service.taskCfg
	for i, rows := range sh.msgBuf {
		realSize := len(*rows)
		if realSize > 0 {
			msgCnt += realSize
			batch := &model.Batch{
				Rows:     rows,
				BatchIdx: int64(i),
				RealSize: realSize,
				Wg:       wg,
			}
			batch.Wg.Add(1)
			sh.service.clickhouse.Send(batch)
			rs := make(model.Rows, 0)
			sh.msgBuf[i] = &rs
		}
	}
	if msgCnt > 0 {
		util.Logger.Info(fmt.Sprintf("created a batch group for task %v, messages %d", sh.service.taskCfg.Name, msgCnt))
		statistics.ShardMsgs.WithLabelValues(taskCfg.Name).Sub(float64(msgCnt))
	}
}
