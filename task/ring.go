package task

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fagongzi/goetty"
	"github.com/thanos-io/thanos/pkg/errors"
	"go.uber.org/zap"

	"github.com/viru-tech/clickhouse_sinker/model"
	"github.com/viru-tech/clickhouse_sinker/statistics"
	"github.com/viru-tech/clickhouse_sinker/util"
)

type Ring struct {
	mux              sync.Mutex //protect ring*
	available        *sync.Cond
	ringBuf          []model.MsgRow
	ringCap          int64 //message is allowed to insert into the ring if its offset in inside [ringGroundOff, ringGroundOff+ringCap)
	ringCapMask      int64
	ringGroundOff    int64 //min message offset inside the ring
	ringCeilingOff   int64 //1 + max message offset inside the ring
	ringFilledOffset int64 //every message which's offset inside range [ringGroundOff, ringFilledOffset) is in the ring
	batchSizeShift   uint  //the shift of desired batch size
	tid              goetty.Timeout
	idleCnt          int
	isIdle           bool
	partition        int
	batchSys         *model.BatchSys

	service *Service
}

// assumes ring.mux is locked
func (ring *Ring) QuitIdle() {
	if ring.isIdle {
		ring.idleCnt = 0
		ring.isIdle = false
		ring.ringBuf = make([]model.MsgRow, ring.ringCap)
		util.Logger.Info(fmt.Sprintf("topic %s partition %d quit idle", ring.service.taskCfg.Topic, ring.partition), zap.String("task", ring.service.taskCfg.Name))
		ring.scheduleForchBatchOrShard()
	}
}

// assumes ring.mux is locked, and msg.Offset is in range [ring.ringGroundOff, ring.ringGroundOff+ring.ringCap)
func (ring *Ring) PutMsgNolock(msg *model.InputMessage) {
	ring.QuitIdle()
	ring.ringBuf[msg.Offset&ring.ringCapMask].Msg = msg
	statistics.RingMsgs.WithLabelValues(ring.service.taskCfg.Name).Inc()
}

func (ring *Ring) PutElem(msgRow model.MsgRow) {
	var err error
	taskCfg := ring.service.taskCfg
	msgOffset := msgRow.Msg.Offset
	pMsgRow := &ring.ringBuf[msgOffset&ring.ringCapMask]
	ring.mux.Lock()
	defer ring.mux.Unlock()
	if msgOffset < ring.ringFilledOffset || pMsgRow.Msg != msgRow.Msg {
		return
	}
	// assert(msgOffset < ring.ringGroundOff + ring.ringCap)
	if msgOffset >= ring.ringCeilingOff {
		ring.ringCeilingOff = msgOffset + 1
	}

	pMsgRow.Row = msgRow.Row
	if ring.service.sharder != nil && msgRow.Row != &model.FakedRow {
		if msgRow.Shard, err = ring.service.sharder.Calc(msgRow.Row); err != nil {
			util.Logger.Fatal("shard number calculation failed", zap.String("task", taskCfg.Name), zap.Error(err))
		}
		pMsgRow.Shard = msgRow.Shard
	}
	for ; ring.ringFilledOffset < ring.ringCeilingOff && ring.ringBuf[ring.ringFilledOffset&(ring.ringCapMask)].Row != nil; ring.ringFilledOffset++ {
	}
	if (ring.ringFilledOffset >> ring.batchSizeShift) != (ring.ringGroundOff >> ring.batchSizeShift) {
		ring.genBatchOrShard()
		ring.scheduleForchBatchOrShard()
	}
}

func (ring *Ring) MakeRoom(newMsg *model.InputMessage) {
	// assert(!ring.isIdle)
	taskCfg := ring.service.taskCfg
	ring.mux.Lock()
	defer ring.mux.Unlock()
	statistics.RingForceBatchAllTotal.WithLabelValues(taskCfg.Name).Inc()
	ring.idleCnt = 0
	prevMsgOff := newMsg.Offset - 1
	if newMsg.Offset != ring.ringGroundOff+ring.ringCap ||
		ring.ringBuf[prevMsgOff&ring.ringCapMask].Msg == nil {
		var msgCnt int
		for off := ring.ringGroundOff; off < ring.ringGroundOff+ring.ringCap; off++ {
			msgRow := &ring.ringBuf[off&ring.ringCapMask]
			if msgRow.Msg != nil {
				msgCnt++
			}
			msgRow.Msg = nil
			msgRow.Row = nil
		}
		util.Logger.Info(fmt.Sprintf("Ring.MakeRoom discarded %d messages for topic %v patittion %d, offset [%d,%d)",
			msgCnt, taskCfg.Topic, ring.partition, ring.ringGroundOff, ring.ringGroundOff+ring.ringCap),
			zap.String("task", taskCfg.Name))
		ring.ringGroundOff = newMsg.Offset
		ring.ringFilledOffset = newMsg.Offset
		ring.ringCeilingOff = newMsg.Offset
	} else {
		for ; prevMsgOff > ring.ringGroundOff && ring.ringBuf[(prevMsgOff-1)&ring.ringCapMask].Msg != nil; prevMsgOff-- {
		}
		// assert(ring.ringFilledOffset < prevMsgOff)
		var msgCnt int
		for off := ring.ringGroundOff; off < prevMsgOff; off++ {
			msgRow := &ring.ringBuf[off&ring.ringCapMask]
			if msgRow.Msg != nil {
				msgCnt++
			}
			msgRow.Msg = nil
			msgRow.Row = nil
		}
		util.Logger.Info(fmt.Sprintf("Ring.MakeRoom discarded %d messages for topic %v patittion %d, offset [%d,%d)",
			msgCnt, taskCfg.Topic, ring.partition, ring.ringGroundOff, prevMsgOff),
			zap.String("task", taskCfg.Name))
		ring.ringGroundOff = prevMsgOff
		ring.ringFilledOffset = newMsg.Offset
		ring.ringCeilingOff = newMsg.Offset
	}
}

func (ring *Ring) ForceBatchOrShard(_ interface{}) {
	taskCfg := ring.service.taskCfg
	ring.mux.Lock()
	defer ring.mux.Unlock()
	if !ring.isIdle {
		if ring.ringFilledOffset > ring.ringGroundOff {
			ring.genBatchOrShard()
			ring.idleCnt = 0
		} else if ring.ringBuf[ring.ringGroundOff&ring.ringCapMask].Msg == nil {
			ring.idleCnt++
			if ring.idleCnt >= 2 {
				ring.idleCnt = 0
				ring.isIdle = true
				ring.ringBuf = nil
				util.Logger.Info(fmt.Sprintf("topic %s partition %d became idle", taskCfg.Topic, ring.partition), zap.String("task", taskCfg.Name))
				return
			}
		}
		ring.scheduleForchBatchOrShard()
	}
}

// schedule ForchBatchOrShard
// assume ring.mux is locked
func (ring *Ring) scheduleForchBatchOrShard() {
	var err error
	ring.tid.Stop()
	if ring.tid, err = util.GlobalTimerWheel.Schedule(time.Duration(ring.service.taskCfg.FlushInterval)*time.Second, ring.ForceBatchOrShard, nil); err != nil {
		if errors.Is(err, goetty.ErrSystemStopped) {
			util.Logger.Warn("Ring.ForceBatchOrShard scheduling timer to a stopped timer wheel", zap.String("task", ring.service.taskCfg.Name), zap.Error(err))
		} else {
			err = errors.Wrapf(err, "")
			util.Logger.Fatal("scheduling timer filed", zap.String("task", ring.service.taskCfg.Name), zap.Error(err))
		}
	}
}

// generate a batch for messages [ring.ringGroundOff, ring.ringFilledOffset), respect batchSize boundary
// assume ring.mux is locked
func (ring *Ring) genBatchOrShard() {
	taskCfg := ring.service.taskCfg
	var parseErrs int
	// Respect batchSize boundary
	endOff := ((ring.ringGroundOff >> ring.batchSizeShift) + 1) << ring.batchSizeShift
	if endOff > ring.ringFilledOffset {
		endOff = ring.ringFilledOffset
	}
	msgCnt := endOff - ring.ringGroundOff
	if atomic.LoadUint32(&ring.service.state) != util.StateRunning {
		util.Logger.Info(fmt.Sprintf("Ring.genBatchOrShard discarded a batch for topic %v patittion %d, offset [%d,%d), messages %d",
			taskCfg.Topic, ring.partition, ring.ringGroundOff, endOff, msgCnt),
			zap.String("task", taskCfg.Name))
		for i := ring.ringGroundOff; i < endOff; i++ {
			msgRow := &ring.ringBuf[i&(ring.ringCapMask)]
			msgRow.Msg = nil
			msgRow.Row = nil
			msgRow.Shard = -1
		}
	} else if ring.service.sharder != nil {
		ring.service.sharder.PutElems(ring.partition, ring.ringBuf, ring.ringGroundOff, endOff, ring.ringCapMask)
	} else {
		batch := model.NewBatch()
		for i := ring.ringGroundOff; i < endOff; i++ {
			msgRow := &ring.ringBuf[i&(ring.ringCapMask)]
			if msgRow.Row != &model.FakedRow {
				*batch.Rows = append(*batch.Rows, msgRow.Row)
			} else {
				parseErrs++
			}
			msgRow.Msg = nil
			msgRow.Row = nil
			msgRow.Shard = -1
		}
		batch.RealSize = len(*batch.Rows)

		if batch.RealSize > 0 {
			util.Logger.Info(fmt.Sprintf("created a batch for topic %v patittion %d, offset [%d,%d), messages %d, parse errors: %d",
				taskCfg.Topic, ring.partition, ring.ringGroundOff, endOff, batch.RealSize, parseErrs),
				zap.String("task", taskCfg.Name))

			batch.BatchIdx = ring.ringGroundOff >> ring.batchSizeShift
			ring.batchSys.CreateBatchGroupSingle(batch, ring.partition, endOff-1)
			ring.service.Flush(batch)
			statistics.RingNormalBatchsTotal.WithLabelValues(taskCfg.Name).Inc()
		}
	}
	statistics.RingMsgs.WithLabelValues(taskCfg.Name).Sub(float64(msgCnt))
	ring.ringGroundOff = endOff
	//util.Logger.Debug(fmt.Sprintf("genBatchOrShard changed ring %p ringGroundOff to %d", ring, ring.ringGroundOff))
	if ring.ringFilledOffset < ring.ringGroundOff {
		ring.ringFilledOffset = ring.ringGroundOff
	}
	ring.available.Broadcast()
}
