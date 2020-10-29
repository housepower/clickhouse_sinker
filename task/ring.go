package task

import (
	"sync"
	"time"

	"github.com/fagongzi/goetty"
	"github.com/pkg/errors"
	"github.com/sundy-li/go_commons/log"

	"github.com/housepower/clickhouse_sinker/model"
	"github.com/housepower/clickhouse_sinker/statistics"
	"github.com/housepower/clickhouse_sinker/util"
)

type Ring struct {
	mux              sync.Mutex //protect ring*
	ringBuf          []model.MsgRow
	ringCap          int64 //message is allowed to insert into the ring if its offset in inside [ringGroundOff, ringGroundOff+ringCap)
	ringGroundOff    int64 //min message offset inside the ring
	ringCeilingOff   int64 //1 + max message offset inside the ring
	ringFilledOffset int64 //every message which's offset inside range [ringGroundOff, ringFilledOffset) is in the ring
	batchSizeShift   int   //the shift of desired batch size
	tid              goetty.Timeout
	idleCnt          int
	isIdle           bool
	partition        int
	batchSys         *model.BatchSys

	service *Service
}

func (ring *Ring) PutElem(msgRow model.MsgRow) {
	var err error
	msgOffset := msgRow.Msg.Offset
	ring.mux.Lock()
	defer ring.mux.Unlock()
	if msgOffset < ring.ringFilledOffset {
		return
	}
	// ring.mux is locked at this point
	if ring.isIdle {
		ring.idleCnt = 0
		ring.isIdle = false
		ring.ringBuf = make([]model.MsgRow, ring.ringCap)
		log.Infof("%s: topic %s partition %d quit idle", ring.service.taskCfg.Name, ring.service.taskCfg.Topic, ring.partition)
	}
	// assert(msgOffset < ring.ringGroundOff + ring.ringCap)
	if msgOffset >= ring.ringCeilingOff {
		ring.ringCeilingOff = msgOffset + 1
	}

	if ring.service.sharder != nil {
		if msgRow.Shard, err = ring.service.sharder.Calc(msgRow.Row); err != nil {
			log.Criticalf("%s: got error %+v", ring.service.taskCfg.Name, err)
		}
	}
	ring.ringBuf[msgOffset%ring.ringCap] = msgRow
	for ; ring.ringFilledOffset < ring.ringCeilingOff && ring.ringBuf[ring.ringFilledOffset%ring.ringCap].Msg != nil; ring.ringFilledOffset++ {
	}
	if (ring.ringFilledOffset >> ring.batchSizeShift) != (ring.ringGroundOff >> ring.batchSizeShift) {
		ring.genBatchOrShard(ring.ringFilledOffset)
		// reschedule the delayed ForceBatchOrShard
		ring.tid.Stop()
		if ring.tid, err = util.GlobalTimerWheel.Schedule(time.Duration(ring.service.taskCfg.FlushInterval)*time.Second, ring.ForceBatchOrShard, nil); err != nil {
			err = errors.Wrap(err, "")
			log.Criticalf("%s: got error %+v", ring.service.taskCfg.Name, err)
		}
	}
}

type OffsetRange struct {
	Begin int64 //inclusive
	End   int64 //exclusive
}

func (ring *Ring) ForceBatchOrShard(arg interface{}) {
	var newMsg *model.InputMessage
	select {
	case <-ring.service.ctx.Done():
		log.Errorf("%s: Ring.ForceBatchOrShard quit due to the context has been canceled", ring.service.taskCfg.Name)
		return
	default:
	}

	ring.mux.Lock()
	defer ring.mux.Unlock()
	if arg != nil {
		newMsg = arg.(*model.InputMessage)
		log.Warnf("%s: Ring.ForceBatchOrShard partition %d message range [%d, %d)", ring.service.taskCfg.Name, newMsg.Partition, ring.ringGroundOff, newMsg.Offset)
	}
	if !ring.isIdle {
		if newMsg == nil {
			if ring.ringFilledOffset > ring.ringGroundOff {
				ring.genBatchOrShard(ring.ringFilledOffset)
				ring.idleCnt = 0
			} else {
				ring.idleCnt++
				if ring.idleCnt >= 2 {
					ring.idleCnt = 0
					ring.isIdle = true
					ring.ringBuf = nil
					log.Infof("%s: topic %s partition %d enter idle", ring.service.taskCfg.Name, ring.service.taskCfg.Topic, ring.partition)
				}
			}
		} else {
			statistics.RingForceBatchAllTotal.WithLabelValues(ring.service.taskCfg.Name).Inc()
		LOOP:
			for {
				ring.genBatchOrShard(ring.ringCeilingOff)
				if ring.ringGroundOff == ring.ringCeilingOff {
					break LOOP
				}
			}
			ring.ringGroundOff = newMsg.Offset
			ring.ringFilledOffset = newMsg.Offset
			ring.ringCeilingOff = newMsg.Offset
			ring.idleCnt = 0
		}
	}

	// reschedule the delayed ForceBatchOrShard
	ring.tid.Stop()
	var err error
	if ring.tid, err = util.GlobalTimerWheel.Schedule(time.Duration(ring.service.taskCfg.FlushInterval)*time.Second, ring.ForceBatchOrShard, nil); err != nil {
		err = errors.Wrap(err, "")
		log.Criticalf("%s: got error %+v", ring.service.taskCfg.Name, err)
	}
}

// assume ring.mux is locked
func (ring *Ring) genBatchOrShard(expNewGroundOff int64) {
	if expNewGroundOff <= ring.ringGroundOff {
		return
	}
	var gaps []OffsetRange
	endOff := (ring.ringGroundOff | int64(1<<ring.batchSizeShift-1)) + 1
	if endOff > expNewGroundOff {
		endOff = expNewGroundOff
	}
	if endOff > ring.ringCeilingOff {
		endOff = ring.ringCeilingOff
	}
	if ring.service.sharder != nil {
		msgCnt := ring.service.sharder.PutElems(ring.partition, ring.ringBuf, ring.ringGroundOff, endOff, ring.ringCap)
		statistics.ParseMsgsBacklog.WithLabelValues(ring.service.taskCfg.Name).Sub(float64(msgCnt))
	} else {
		gapBegOff := int64(-1)
		batch := model.NewBatch()
		for i := ring.ringGroundOff; i < endOff; i++ {
			off := i & (ring.ringCap - 1)
			msg := ring.ringBuf[off].Msg
			if msg != nil {
				//assert msg.Offset==i
				if gapBegOff >= 0 {
					gaps = append(gaps, OffsetRange{Begin: gapBegOff, End: i})
					gapBegOff = -1
				}
				*batch.Rows = append(*batch.Rows, ring.ringBuf[off].Row)
			} else if gapBegOff < 0 {
				gapBegOff = i
			}
		}
		if gapBegOff >= 0 {
			gaps = append(gaps, OffsetRange{Begin: gapBegOff, End: endOff})
		}

		if batch.RealSize > 0 {
			log.Debugf("%s: going to flush a batch for topic %v patittion %d, offset %d, messages %d, gaps: %+v",
				ring.service.taskCfg.Name, ring.service.taskCfg.Topic, ring.partition, endOff-1,
				batch.RealSize, gaps)

			batch.BatchIdx = (endOff - 1) >> ring.batchSizeShift
			ring.batchSys.CreateBatchGroupSingle(batch, ring.partition, endOff-1)
			ring.service.batchChan <- batch
			statistics.ParseMsgsBacklog.WithLabelValues(ring.service.taskCfg.Name).Sub(float64(batch.RealSize))
			if gaps == nil {
				statistics.RingNormalBatchsTotal.WithLabelValues(ring.service.taskCfg.Name).Inc()
			} else {
				statistics.RingForceBatchAllGapTotal.WithLabelValues(ring.service.taskCfg.Name).Inc()
			}
		}
	}

	ring.ringGroundOff = endOff
	if ring.ringFilledOffset < ring.ringGroundOff {
		ring.ringFilledOffset = ring.ringGroundOff
	}
}
