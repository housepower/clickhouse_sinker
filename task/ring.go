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
	ring.ringBuf[msgOffset%ring.ringCap] = msgRow
	if msgOffset >= ring.ringCeilingOff {
		ring.ringCeilingOff = msgOffset + 1
	}

	for ; ring.ringFilledOffset < ring.ringCeilingOff && ring.ringBuf[ring.ringFilledOffset%ring.ringCap].Msg != nil; ring.ringFilledOffset++ {
	}

	if (ring.ringFilledOffset >> ring.batchSizeShift) != (ring.ringGroundOff >> ring.batchSizeShift) {
		ring.genBatch(ring.ringFilledOffset)
		// reschedule the delayed ForceBatch
		ring.tid.Stop()
		if ring.tid, err = util.GlobalTimerWheel.Schedule(time.Duration(ring.service.taskCfg.FlushInterval)*time.Second, ring.ForceBatch, nil); err != nil {
			err = errors.Wrap(err, "")
			log.Criticalf("got error %+v", err)
		}
	}
}

type OffsetRange struct {
	Begin int64 //inclusive
	End   int64 //exclusive
}

func (ring *Ring) ForceBatch(arg interface{}) {
	var (
		err    error
		newMsg *model.InputMessage
		gaps   []OffsetRange
	)

	select {
	case <-ring.service.ctx.Done():
		log.Errorf("Ring.ForceBatch quit due to the context has been canceled")
		return
	default:
	}

	ring.mux.Lock()
	defer ring.mux.Unlock()
	if arg != nil {
		newMsg = arg.(*model.InputMessage)
		log.Warnf("Ring.ForceBatchAll partition %d message range [%d, %d)", newMsg.Partition, ring.ringGroundOff, newMsg.Offset)
	}
	if !ring.isIdle {
		if newMsg == nil {
			if ring.ringFilledOffset > ring.ringGroundOff {
				ring.genBatch(ring.ringFilledOffset)
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
				gaps = ring.genBatch(ring.ringCeilingOff)
				if gaps != nil {
					log.Warnf("Ring.ForceBatchAll noticed topic %v partition %d message offset gaps %v", newMsg.Topic, newMsg.Partition, gaps)
				}
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
	// reschedule myself
	ring.tid.Stop()
	if ring.tid, err = util.GlobalTimerWheel.Schedule(time.Duration(ring.service.taskCfg.FlushInterval)*time.Second, ring.ForceBatch, nil); err != nil {
		err = errors.Wrap(err, "")
		log.Criticalf("got error %+v", err)
	}
}

// assume ring.mux is locked
func (ring *Ring) genBatch(expNewGroundOff int64) (gaps []OffsetRange) {
	if expNewGroundOff <= ring.ringGroundOff {
		return
	}
	endOff := (ring.ringGroundOff | int64(1<<ring.batchSizeShift-1)) + 1
	if endOff > expNewGroundOff {
		endOff = expNewGroundOff
	}
	if endOff > ring.ringCeilingOff {
		endOff = ring.ringCeilingOff
	}
	batch := ring.batchSys.NewBatchGroup().NewBatch(0, ring.service.taskCfg.BufferSize)
	expOff := ring.ringGroundOff
	for i := ring.ringGroundOff; i < endOff; i++ {
		off := i & (ring.ringCap - 1)
		msg := ring.ringBuf[off].Msg
		if msg != nil {
			//assert msg.Offset==i
			if i != expOff {
				gaps = append(gaps, OffsetRange{Begin: expOff, End: i})
			}
			expOff = i + 1
			batch.MsgRows = append(batch.MsgRows, ring.ringBuf[off])
			batch.Group.UpdateOffset(msg.Partition, msg.Offset)
			if ring.ringBuf[off].Row != nil {
				batch.RealSize++
			}
			ring.ringBuf[off].Msg = nil
			ring.ringBuf[off].Row = nil
		}
	}

	if expOff != endOff {
		gaps = append(gaps, OffsetRange{Begin: expOff, End: endOff})
	}

	if batch.RealSize > 0 {
		log.Debugf("%s: going to flush a batch for topic %v patittion %d, size %d, offset %d-%d",
			ring.service.taskCfg.Name, batch.MsgRows[0].Msg.Topic, batch.MsgRows[0].Msg.Partition,
			len(batch.MsgRows), batch.MsgRows[0].Msg.Offset, batch.MsgRows[len(batch.MsgRows)-1].Msg.Offset)

		batch.BatchIdx = batch.MsgRows[0].Msg.Offset >> ring.batchSizeShift
		ring.service.batchChan <- batch
		statistics.ParseMsgsBacklog.WithLabelValues(ring.service.taskCfg.Name).Sub(float64(batch.RealSize))
		if gaps == nil {
			statistics.RingNormalBatchsTotal.WithLabelValues(ring.service.taskCfg.Name).Inc()
		} else {
			statistics.RingForceBatchAllGapTotal.WithLabelValues(ring.service.taskCfg.Name).Inc()
		}
	}

	ring.ringGroundOff = endOff
	if ring.ringFilledOffset < ring.ringGroundOff {
		ring.ringFilledOffset = ring.ringGroundOff
	}
	return
}
