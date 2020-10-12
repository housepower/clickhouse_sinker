/*Copyright [2019] housepower

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package input

import (
	"context"
	"io"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/fagongzi/goetty"
	"github.com/pkg/errors"
	"github.com/segmentio/kafka-go"
	"github.com/sundy-li/go_commons/log"
	"golang.org/x/time/rate"

	"github.com/housepower/clickhouse_sinker/config"
	"github.com/housepower/clickhouse_sinker/model"
	"github.com/housepower/clickhouse_sinker/parser"
	"github.com/housepower/clickhouse_sinker/statistics"
	"github.com/housepower/clickhouse_sinker/util"
)

// Kafka reader configuration
type Kafka struct {
	taskCfg  *config.TaskConfig
	parser   parser.Parser
	dims     []*model.ColumnWithType
	r        *kafka.Reader
	mux      sync.Mutex
	rings    []*Ring
	batchCh  chan Batch
	stopped  chan struct{}
	ctx      context.Context
	limiter1 *rate.Limiter
	limiter2 *rate.Limiter
	limiter3 *rate.Limiter
}

type Ring struct {
	mux              sync.Mutex //protect ring*
	ringBuf          []MsgRow
	ringCap          int64 //message is allowed to insert into the ring if its offset in inside [ringGroundOff, ringGroundOff+ringCap)
	ringGroundOff    int64 //min message offset inside the ring
	ringCeilingOff   int64 //1 + max message offset inside the ring
	ringFilledOffset int64 //every message which's offset inside range [ringGroundOff, ringFilledOffset) is in the ring
	batchSizeShift   int   //the shift of desired batch size
	tid              goetty.Timeout
	idleCnt          int
	isIdle           bool
	partition        int
	kafka            *Kafka //point back to who hold this ring
}

type MsgRow struct {
	Msg *kafka.Message
	Row []interface{}
}

type Batch struct {
	MsgRows  []MsgRow
	RealSize int    //number of messages who's row!=nil
	BatchNum int64  //msg.Offset>>batchSizeShift is the same for all messages inside a batch
	kafka    *Kafka //point back to who hold this ring
}

func NewBatch(batchSize int, k *Kafka) (batch Batch) {
	return Batch{
		MsgRows: make([]MsgRow, batchSize),
		kafka:   k,
	}
}

func (batch Batch) Free() (err error) {
	numMsgs := len(batch.MsgRows)
	if numMsgs == 0 {
		return
	}
	msgs := make([]kafka.Message, numMsgs)
	for i, msgRow := range batch.MsgRows {
		msgs[i] = *msgRow.Msg
	}
	// "io: read/write on closed pipe" makes caller hard to tell what happened. context.Canceled is better.
	select {
	case <-batch.kafka.ctx.Done():
		err = errors.Wrap(context.Canceled, "")
		return
	default:
	}
	if err = batch.kafka.r.CommitMessages(batch.kafka.ctx, msgs...); err != nil {
		err = errors.Wrap(err, "")
		return
	}
	lastMsg := msgs[numMsgs-1]
	statistics.ConsumeOffsets.WithLabelValues(batch.kafka.taskCfg.Name, strconv.Itoa(lastMsg.Partition), lastMsg.Topic).Set(float64(lastMsg.Offset))
	return
}

// NewKafka get instance of kafka reader
func NewKafka(taskCfg *config.TaskConfig, parser parser.Parser) *Kafka {
	return &Kafka{taskCfg: taskCfg, parser: parser}
}

// Init Initialise the kafka instance with configuration
func (k *Kafka) Init(dims []*model.ColumnWithType) error {
	cfg := config.GetConfig()
	kfkCfg := cfg.Kafka[k.taskCfg.Kafka]
	k.stopped = make(chan struct{})
	k.dims = dims

	offset := kafka.LastOffset
	if k.taskCfg.Earliest {
		offset = kafka.FirstOffset
	}

	k.r = kafka.NewReader(kafka.ReaderConfig{
		Brokers:     strings.Split(kfkCfg.Brokers, ","),
		GroupID:     k.taskCfg.ConsumerGroup,
		Topic:       k.taskCfg.Topic,
		StartOffset: offset,
		MinBytes:    k.taskCfg.MinBufferSize * k.taskCfg.MsgSizeHint,
		MaxBytes:    k.taskCfg.BufferSize * k.taskCfg.MsgSizeHint,
		MaxWait:     time.Duration(k.taskCfg.FlushInterval) * time.Second,
	})
	k.rings = make([]*Ring, 0)
	k.batchCh = make(chan Batch, 32)
	k.limiter1 = rate.NewLimiter(rate.Every(10*time.Second), 1)
	k.limiter2 = rate.NewLimiter(rate.Every(10*time.Second), 1)
	k.limiter3 = rate.NewLimiter(rate.Every(10*time.Second), 1)
	return nil
}

func (k *Kafka) BatchCh() chan Batch {
	return k.batchCh
}

// kafka main loop
func (k *Kafka) Run(ctx context.Context) {
	k.ctx = ctx
	numRings := len(k.rings)
LOOP:
	for {
		var err error
		var msg kafka.Message
		if msg, err = k.r.FetchMessage(ctx); err != nil {
			switch errors.Cause(err) {
			case context.Canceled:
				log.Infof("%s Kafka.Run quit due to context has been canceled", k.taskCfg.Name)
				break LOOP
			case io.EOF:
				log.Infof("%s Kafka.Run quit due to reader has been closed", k.taskCfg.Name)
				break LOOP
			default:
				statistics.ConsumeMsgsErrorTotal.WithLabelValues(k.taskCfg.Name).Inc()
				err = errors.Wrap(err, "")
				log.Errorf("%s Kafka.Run got error %+v", k.taskCfg.Name, err)
				continue
			}
		}
		statistics.ConsumeMsgsTotal.WithLabelValues(k.taskCfg.Name).Inc()
		// ensure ring for this message exist
		k.mux.Lock()
		var ring *Ring
		if msg.Partition < numRings {
			ring = k.rings[msg.Partition]
		} else {
			for i := numRings; i < msg.Partition+1; i++ {
				k.rings = append(k.rings, nil)
			}
			numRings = msg.Partition + 1
		}
		if ring == nil {
			batchSizeShift := util.GetShift(k.taskCfg.BufferSize)
			ringCap := int64(1 << (batchSizeShift + 1))
			ring := &Ring{
				ringBuf:          make([]MsgRow, ringCap),
				ringCap:          ringCap,
				ringGroundOff:    msg.Offset,
				ringCeilingOff:   msg.Offset,
				ringFilledOffset: msg.Offset,
				batchSizeShift:   batchSizeShift,
				idleCnt:          0,
				isIdle:           false,
				partition:        msg.Partition,
				kafka:            k,
			}
			// schedule a delayed ForceBatch
			if ring.tid, err = util.GlobalTimerWheel.Schedule(time.Duration(k.taskCfg.FlushInterval)*time.Second, ring.ForceBatch, nil); err != nil {
				err = errors.Wrap(err, "")
				log.Criticalf("got error %+v", err)
			}
			k.rings[msg.Partition] = ring
			k.mux.Unlock()
		} else {
			k.mux.Unlock()
			var ringGroundOff int64
			ring.mux.Lock()
			ringGroundOff = ring.ringGroundOff
			ring.mux.Unlock()
			if msg.Offset < ringGroundOff {
				statistics.RingMsgsOffTooSmallErrorTotal.WithLabelValues(ring.kafka.taskCfg.Name).Inc()
				if ring.kafka.limiter2.Allow() {
					log.Warnf("got a message(topic %v, partition %d, offset %v) is left to the range [%v, %v)",
						msg.Topic, msg.Partition, msg.Offset, ring.ringGroundOff, ring.ringGroundOff+ring.ringCap)
				}
				continue LOOP
			}
			if msg.Offset >= ringGroundOff+ring.ringCap {
				statistics.RingMsgsOffTooLargeErrorTotal.WithLabelValues(ring.kafka.taskCfg.Name).Inc()
				if ring.kafka.limiter3.Allow() {
					log.Warnf("got a message(topic %v, partition %d, offset %v) is right to the range [%v, %v)",
						msg.Topic, msg.Partition, msg.Offset, ring.ringGroundOff, ring.ringGroundOff+ring.ringCap)
				}
				time.Sleep(1 * time.Second)
				ring.ForceBatch(&msg)
				// assert ring.ringGroundOff==ring.ringFilledOff==msg.Offset
			}
		}
		// submit message to a goroutine pool
		statistics.ParseMsgsBacklog.WithLabelValues(k.taskCfg.Name).Inc()
		_ = util.GlobalWorkerPool1.Submit(func() {
			var row []interface{}
			metric, err := k.parser.Parse(msg.Value)
			if err != nil {
				statistics.ParseMsgsErrorTotal.WithLabelValues(k.taskCfg.Name).Inc()
				if k.limiter1.Allow() {
					log.Errorf("failed to parse message(topic %v, partition %d, offset %v) %+v, string(value) <<<%+v>>>, got error %+v",
						msg.Topic, msg.Partition, msg.Offset, msg, string(msg.Value), err)
				}
			} else {
				row = MetricToRow(metric, msg, k.dims)
			}
			var ring *Ring
			k.mux.Lock()
			ring = k.rings[msg.Partition]
			k.mux.Unlock()
			ring.PutElem(MsgRow{Msg: &msg, Row: row})
		})
	}
}

// Stop kafka consumer and close all connections
func (k *Kafka) Stop() error {
	_ = k.r.Close()
	return nil
}

// Description of this kafka consumer, which topic it reads from
func (k *Kafka) Description() string {
	return "kafka consumer of topic " + k.taskCfg.Topic
}

func (ring *Ring) PutElem(msgRow MsgRow) {
	var err error
	msgOffset := msgRow.Msg.Offset
	ring.mux.Lock()
	defer ring.mux.Unlock()
	// ring.mux is locked at this point
	if ring.isIdle {
		ring.idleCnt = 0
		ring.isIdle = false
		ring.ringBuf = make([]MsgRow, ring.ringCap)
		log.Infof("%s: topic %s partition %d quit idle", ring.kafka.taskCfg.Name, ring.kafka.taskCfg.Topic, ring.partition)
	}
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
		if ring.tid, err = util.GlobalTimerWheel.Schedule(time.Duration(ring.kafka.taskCfg.FlushInterval)*time.Second, ring.ForceBatch, nil); err != nil {
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
		newMsg *kafka.Message
		gaps   []OffsetRange
	)

	select {
	case <-ring.kafka.ctx.Done():
		log.Errorf("Ring.ForceBatch quit due to the context has been canceled")
		return
	default:
	}

	ring.mux.Lock()
	defer ring.mux.Unlock()
	if arg != nil {
		newMsg = arg.(*kafka.Message)
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
					log.Infof("%s: topic %s partition %d enter idle", ring.kafka.taskCfg.Name, ring.kafka.taskCfg.Topic, ring.partition)
				}
			}
		} else {
			statistics.RingForceBatchAllTotal.WithLabelValues(ring.kafka.taskCfg.Name).Inc()
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
	if ring.tid, err = util.GlobalTimerWheel.Schedule(time.Duration(ring.kafka.taskCfg.FlushInterval)*time.Second, ring.ForceBatch, nil); err != nil {
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
	batch := NewBatch(0, ring.kafka)
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
			if ring.ringBuf[off].Row != nil {
				batch.RealSize++
			}
			ring.ringBuf[off] = MsgRow{Msg: nil, Row: nil}
		}
	}
	if expOff != endOff {
		gaps = append(gaps, OffsetRange{Begin: expOff, End: endOff})
	}
	if batch.RealSize > 0 {
		log.Debugf("%s: going to flush a batch for topic %v patittion %d, size %d, offset %d-%d",
			ring.kafka.taskCfg.Name, batch.MsgRows[0].Msg.Topic, batch.MsgRows[0].Msg.Partition,
			len(batch.MsgRows), batch.MsgRows[0].Msg.Offset, batch.MsgRows[len(batch.MsgRows)-1].Msg.Offset)

		batch.BatchNum = batch.MsgRows[0].Msg.Offset >> ring.batchSizeShift
		ring.kafka.batchCh <- batch
		statistics.ParseMsgsBacklog.WithLabelValues(ring.kafka.taskCfg.Name).Sub(float64(batch.RealSize))
		if gaps == nil {
			statistics.RingNormalBatchsTotal.WithLabelValues(ring.kafka.taskCfg.Name).Inc()
		} else {
			statistics.RingForceBatchAllGapTotal.WithLabelValues(ring.kafka.taskCfg.Name).Inc()
		}
	}
	ring.ringGroundOff = endOff
	if ring.ringFilledOffset < ring.ringGroundOff {
		ring.ringFilledOffset = ring.ringGroundOff
	}
	return
}

func MetricToRow(metric model.Metric, msg kafka.Message, dims []*model.ColumnWithType) (row []interface{}) {
	row = make([]interface{}, len(dims))
	for i, dim := range dims {
		if strings.HasPrefix(dim.Name, "__kafka") {
			if strings.HasSuffix(dim.Name, "_topic") {
				row[i] = msg.Topic
			} else if strings.HasSuffix(dim.Name, "_partition") {
				row[i] = msg.Partition
			} else {
				row[i] = msg.Offset
			}
		} else {
			row[i] = model.GetValueByType(metric, dim)
		}
	}
	return
}
