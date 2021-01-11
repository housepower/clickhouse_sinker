package task

import (
	"sync"
	"time"

	"github.com/fagongzi/goetty"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"

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
	taskCfg := &ring.service.cfg.Task
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
		log.Infof("%s: topic %s partition %d quit idle", taskCfg.Name, taskCfg.Topic, ring.partition)
	}
	// assert(msgOffset < ring.ringGroundOff + ring.ringCap)
	if msgOffset >= ring.ringCeilingOff {
		ring.ringCeilingOff = msgOffset + 1
	}

	if ring.service.sharder != nil && msgRow.Row != nil {
		if msgRow.Shard, err = ring.service.sharder.Calc(msgRow.Row); err != nil {
			log.Fatalf("%s: got error %+v", taskCfg.Name, err)
		}
	}
	statistics.RingMsgs.WithLabelValues(taskCfg.Name).Inc()
	ring.ringBuf[msgOffset&(ring.ringCap-1)] = msgRow
	for ; ring.ringFilledOffset < ring.ringCeilingOff && ring.ringBuf[ring.ringFilledOffset&(ring.ringCap-1)].Msg != nil; ring.ringFilledOffset++ {
	}
	if (ring.ringFilledOffset >> ring.batchSizeShift) != (ring.ringGroundOff >> ring.batchSizeShift) {
		ring.genBatchOrShard(ring.ringFilledOffset)
		// reschedule the delayed ForceBatchOrShard
		ring.tid.Stop()
		if ring.tid, err = util.GlobalTimerWheel.Schedule(time.Duration(taskCfg.FlushInterval)*time.Second, ring.ForceBatchOrShard, nil); err != nil {
			err = errors.Wrap(err, "")
			log.Fatalf("%s: got error %+v", taskCfg.Name, err)
		}
	}
}

type OffsetRange struct {
	Begin int64 //inclusive
	End   int64 //exclusive
}

func (ring *Ring) ForceBatchOrShard(arg interface{}) {
	var newMsg *model.InputMessage
	taskCfg := &ring.service.cfg.Task
	select {
	case <-ring.service.ctx.Done():
		log.Errorf("%s: Ring.ForceBatchOrShard quit due to the context has been canceled", taskCfg.Name)
		return
	default:
	}

	ring.mux.Lock()
	defer ring.mux.Unlock()
	if arg != nil {
		newMsg = arg.(*model.InputMessage)
		log.Warnf("%s: Ring.ForceBatchOrShard partition %d message range [%d, %d)", taskCfg.Name, newMsg.Partition, ring.ringGroundOff, newMsg.Offset)
	}
	if !ring.isIdle {
		if newMsg == nil {
			if ring.ringFilledOffset > ring.ringGroundOff {
				ring.genBatchOrShard(ring.ringFilledOffset)
				ring.idleCnt = 0
			} else if ring.ringGroundOff == ring.ringCeilingOff {
				ring.idleCnt++
				if ring.idleCnt >= 2 {
					ring.idleCnt = 0
					ring.isIdle = true
					ring.ringBuf = nil
					log.Infof("%s: topic %s partition %d enter idle", taskCfg.Name, taskCfg.Topic, ring.partition)
				}
			}
		} else {
			statistics.RingForceBatchAllTotal.WithLabelValues(taskCfg.Name).Inc()
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
	if ring.tid, err = util.GlobalTimerWheel.Schedule(time.Duration(taskCfg.FlushInterval)*time.Second, ring.ForceBatchOrShard, nil); err != nil {
		err = errors.Wrap(err, "")
		log.Fatalf("%s: got error %+v", taskCfg.Name, err)
	}
}

// assume ring.mux is locked
func (ring *Ring) genBatchOrShard(expNewGroundOff int64) {
	if expNewGroundOff <= ring.ringGroundOff {
		return
	}
	taskCfg := &ring.service.cfg.Task
	var gaps []OffsetRange
	var msgCnt, parseErrs int
	endOff := (ring.ringGroundOff | int64(1<<ring.batchSizeShift-1)) + 1
	if endOff > expNewGroundOff {
		endOff = expNewGroundOff
	}
	if endOff > ring.ringCeilingOff {
		endOff = ring.ringCeilingOff
	}
	if ring.service.sharder != nil {
		msgCnt = ring.service.sharder.PutElems(ring.partition, ring.ringBuf, ring.ringGroundOff, endOff, ring.ringCap)
		statistics.RingMsgs.WithLabelValues(taskCfg.Name).Sub(float64(msgCnt))
	} else {
		gapBegOff := int64(-1)
		batch := model.NewBatch()
		for i := ring.ringGroundOff; i < endOff; i++ {
			msgRow := &ring.ringBuf[i&(ring.ringCap-1)]
			if msgRow.Msg != nil {
				msgCnt++
				//assert msg.Offset==i
				if gapBegOff >= 0 {
					gaps = append(gaps, OffsetRange{Begin: gapBegOff, End: i})
					gapBegOff = -1
				}
				if msgRow.Row != nil {
					*batch.Rows = append(*batch.Rows, msgRow.Row)
				} else {
					parseErrs++
				}
			} else if gapBegOff < 0 {
				gapBegOff = i
			}
			msgRow.Msg = nil
			msgRow.Row = nil
			msgRow.Shard = -1
		}
		batch.RealSize = len(*batch.Rows)
		if gapBegOff >= 0 {
			gaps = append(gaps, OffsetRange{Begin: gapBegOff, End: endOff})
		}

		if batch.RealSize > 0 {
			log.Debugf("%s: going to flush a batch for topic %v patittion %d, offset %d, messages %d, gaps: %+v, parse errors: %d",
				taskCfg.Name, taskCfg.Topic, ring.partition, endOff-1,
				batch.RealSize, gaps, parseErrs)

			batch.BatchIdx = (endOff - 1) >> ring.batchSizeShift
			ring.batchSys.CreateBatchGroupSingle(batch, ring.partition, endOff-1)
			ring.service.batchChan <- batch
			if gaps == nil {
				statistics.RingNormalBatchsTotal.WithLabelValues(taskCfg.Name).Inc()
			} else {
				statistics.RingForceBatchAllGapTotal.WithLabelValues(taskCfg.Name).Inc()
			}
		}
		statistics.RingMsgs.WithLabelValues(taskCfg.Name).Sub(float64(batch.RealSize))
	}

	ring.ringGroundOff = endOff
	if ring.ringFilledOffset < ring.ringGroundOff {
		ring.ringFilledOffset = ring.ringGroundOff
	}
}
