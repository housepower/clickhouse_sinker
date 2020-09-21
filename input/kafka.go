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
	"github.com/gammazero/workerpool"
	"github.com/pkg/errors"
	"github.com/segmentio/kafka-go"
	"github.com/sundy-li/go_commons/log"
	"golang.org/x/time/rate"

	"github.com/housepower/clickhouse_sinker/config"
	"github.com/housepower/clickhouse_sinker/model"
	"github.com/housepower/clickhouse_sinker/parser"
	"github.com/housepower/clickhouse_sinker/statistics"
)

// Kafka reader configuration
type Kafka struct {
	taskCfg *config.TaskConfig
	parser  parser.Parser
	dims    []*model.ColumnWithType
	r       *kafka.Reader
	mux     sync.Mutex
	rings   []*Ring
	batchCh chan Batch
	tw      *goetty.TimeoutWheel
	stopped chan struct{}
	ctx     context.Context
	limiter *rate.Limiter
	wp      *workerpool.WorkerPool
	tid     goetty.Timeout
}

type Ring struct {
	mux              sync.Mutex //protect ring*
	ringBuf          []MsgRow
	ringCap          int64 //message is allowed to insert into the ring if its offset in inside [ringGroundOff, ringGroundOff+ringCap)
	ringGroundOff    int64 //min message offset inside the ring
	ringCeilingOff   int64 //1 + max message offset inside the ring
	ringFilledOffset int64 //every message which's offset inside range [ringGroundOff, ringFilledOffset) is in the ring
	tid              goetty.Timeout
	idleCnt          int
	kafka            *Kafka //point back to who hold this ring
}

type MsgRow struct {
	Msg *kafka.Message
	Row []interface{}
}

type Batch struct {
	MsgRows  []MsgRow
	RealSize int    //number of messages who's row!=nil
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
	k.r = kafka.NewReader(kafka.ReaderConfig{
		Brokers:  strings.Split(kfkCfg.Brokers, ","),
		GroupID:  k.taskCfg.ConsumerGroup,
		Topic:    k.taskCfg.Topic,
		MinBytes: k.taskCfg.MinBufferSize * k.taskCfg.MsgSizeHint,
		MaxBytes: k.taskCfg.BufferSize * k.taskCfg.MsgSizeHint,
		MaxWait:  time.Duration(k.taskCfg.FlushInterval) * time.Second,
	})
	k.rings = make([]*Ring, 0)
	k.batchCh = make(chan Batch, 32)
	k.wp = workerpool.New(k.taskCfg.ConcurrentParsers)
	k.tw = goetty.NewTimeoutWheel(goetty.WithTickInterval(100 * time.Millisecond))
	k.limiter = rate.NewLimiter(rate.Every(30*time.Second), 1)
	return nil
}

func (k *Kafka) BatchCh() chan Batch {
	return k.batchCh
}

// kafka main loop
func (k *Kafka) Run(ctx context.Context) {
	var err error
	k.ctx = ctx
	if k.tid, err = k.tw.Schedule(time.Duration(k.taskCfg.FlushInterval*4)*time.Second, k.PurgeIdleRings, nil); err != nil {
		err = errors.Wrap(err, "")
		log.Criticalf("got error %+v", err)
		return
	}
	numRings := len(k.rings)
LOOP:
	for {
		var err error
		var msg kafka.Message
		if msg, err = k.r.FetchMessage(ctx); err != nil {
			if errors.Cause(err) == context.Canceled {
				log.Infof("%s Kafka.Run quit due to context has been canceled", k.taskCfg.Name)
				break LOOP
			} else if errors.Cause(err) == io.EOF {
				log.Infof("%s Kafka.Run quit due to reader has been closed", k.taskCfg.Name)
				break LOOP
			} else {
				statistics.ConsumeMsgsErrorTotal.WithLabelValues(k.taskCfg.Name).Inc()
				err = errors.Wrap(err, "")
				log.Errorf("%s Kafka.Run got error %+v", k.taskCfg.Name, err)
				continue
			}
		}
		statistics.ConsumeMsgsTotal.WithLabelValues(k.taskCfg.Name).Inc()
		statistics.ParseMsgsBacklog.WithLabelValues(k.taskCfg.Name).Inc()
		// ensure ring for this message exist
		if msg.Partition >= numRings {
			k.mux.Lock()
			for i := numRings; i < msg.Partition; i++ {
				k.rings = append(k.rings, nil)
			}
			cap := 2 ^ 10
			for ; cap < 2*k.taskCfg.BufferSize; cap *= 2 {
			}
			ring := &Ring{
				ringBuf:          make([]MsgRow, cap),
				ringCap:          int64(cap),
				ringGroundOff:    msg.Offset,
				ringCeilingOff:   msg.Offset,
				ringFilledOffset: msg.Offset,
				kafka:            k,
			}
			if ring.tid, err = ring.kafka.tw.Schedule(time.Duration(k.taskCfg.FlushInterval)*time.Second, ring.ForceBatch, nil); err != nil {
				err = errors.Wrap(err, "")
				log.Criticalf("got error %+v", err)
			}
			k.rings = append(k.rings, ring)
			numRings = len(k.rings)
			k.mux.Unlock()
		}

		// submit message to a goroutine pool
		k.wp.Submit(func() {
			var row []interface{}
			metric, err := k.parser.Parse(msg.Value)
			if err != nil {
				statistics.ParseMsgsErrorTotal.WithLabelValues(k.taskCfg.Name).Inc()
				if k.limiter.Allow() {
					log.Errorf("failed to parse %+v, string(value) <<<%+v>>>, got error %+v", msg, string(msg.Value), err)
				}
			} else {
				row = model.MetricToRow(metric, k.dims)
			}
			var ring *Ring
			k.mux.Lock()
			ring = k.rings[msg.Partition]
			k.mux.Unlock()
			ring.PutElem(MsgRow{Msg: &msg, Row: row})
		})
	}
	k.wp.StopWait()
	k.tw.Stop()
	return
}

// Description of this kafka consumre, which topic it reads from
func (k *Kafka) PurgeIdleRings(interface{}) {
	var err error
	var idleRings []int
	k.mux.Lock()
	for i, ring := range k.rings {
		if ring != nil && ring.idleCnt >= 2 {
			k.rings[i] = nil
			idleRings = append(idleRings, i)
		}
	}
	k.mux.Unlock()
	if idleRings != nil {
		log.Infof("purged idle rings %+v", idleRings)
	}

	if k.tid, err = k.tw.Schedule(time.Duration(k.taskCfg.FlushInterval*4)*time.Second, k.PurgeIdleRings, nil); err != nil {
		err = errors.Wrap(err, "")
		log.Criticalf("got error %+v", err)
		return
	}
}

// Stop kafka consumer and close all connections
func (k *Kafka) Stop() error {
	_ = k.r.Close()
	return nil
}

// Description of this kafka consumre, which topic it reads from
func (k *Kafka) Description() string {
	return "kafka consumer of topic " + k.taskCfg.Topic
}

func (ring *Ring) PutElem(msgRow MsgRow) {
	var err error
	msgOffset := msgRow.Msg.Offset
LOOP:
	for {
		ring.mux.Lock()
		if msgOffset < ring.ringGroundOff {
			statistics.RingMsgsOffTooSmallErrorTotal.WithLabelValues(ring.kafka.taskCfg.Name).Inc()
			log.Errorf("Ring.PutElem quit due to msgOffset %v is less than ring.ringGroundOff %v", msgOffset, ring.ringGroundOff)
			ring.mux.Unlock()
			return
		}
		if msgOffset < ring.ringGroundOff+ring.ringCap {
			break LOOP
		}
		ring.mux.Unlock()
		statistics.RingMsgsOffTooLargeErrorTotal.WithLabelValues(ring.kafka.taskCfg.Name).Inc()
		log.Warnf("Ring.PutElem sleep due to msgOffset %v exceeds %v (the max offest allowed by the ring)", msgOffset, ring.ringGroundOff+ring.ringCap)
		select {
		case <-ring.kafka.ctx.Done():
			log.Errorf("Ring.PutElem quit due to the context has been canceled")
			return
		default:
		}
		time.Sleep(1 * time.Second)
	}
	// ring.mux is locked at this point
	ring.ringBuf[msgOffset%ring.ringCap] = msgRow
	if msgOffset >= ring.ringCeilingOff {
		ring.ringCeilingOff = msgOffset + 1
	}
	for ; ring.ringFilledOffset < ring.ringCeilingOff && ring.ringBuf[ring.ringFilledOffset%ring.ringCap].Msg != nil; ring.ringFilledOffset++ {
	}
	if ring.ringFilledOffset-ring.ringGroundOff >= int64(ring.kafka.taskCfg.BufferSize) {
		batchSize := ring.kafka.taskCfg.BufferSize
		batch := NewBatch(batchSize, ring.kafka)
		for i := 0; i < batchSize; i++ {
			off := (ring.ringGroundOff + int64(i)) % ring.ringCap
			batch.MsgRows[i] = ring.ringBuf[off]
			if ring.ringBuf[off].Row != nil {
				batch.RealSize++
			}
			ring.ringBuf[off] = MsgRow{Msg: nil, Row: nil}
		}
		ring.ringGroundOff += int64(batchSize)
		ring.kafka.batchCh <- batch
		statistics.ParseMsgsBacklog.WithLabelValues(ring.kafka.taskCfg.Name).Sub(float64(batchSize))
		statistics.RingNormalBatchsTotal.WithLabelValues(ring.kafka.taskCfg.Name).Inc()
		ring.tid.Stop()
		if ring.tid, err = ring.kafka.tw.Schedule(time.Duration(ring.kafka.taskCfg.FlushInterval)*time.Second, ring.ForceBatch, nil); err != nil {
			err = errors.Wrap(err, "")
			log.Criticalf("got error %+v", err)
		}
	}
	ring.mux.Unlock()
	return
}

func (ring *Ring) ForceBatch(interface{}) {
	var err error
	select {
	case <-ring.kafka.ctx.Done():
		log.Errorf("Ring.ForceBatch quit due to the context has been canceled")
		return
	default:
	}
	ring.mux.Lock()
	defer ring.mux.Unlock()
	batchSize := int(ring.ringFilledOffset - ring.ringGroundOff)
	if batchSize == 0 {
		ring.idleCnt++
		return
	}
	if batchSize > ring.kafka.taskCfg.BufferSize {
		batchSize = ring.kafka.taskCfg.BufferSize
	}
	batch := NewBatch(batchSize, ring.kafka)
	for i := 0; i < batchSize; i++ {
		off := (ring.ringGroundOff + int64(i)) % ring.ringCap
		batch.MsgRows[i] = ring.ringBuf[off]
		if ring.ringBuf[off].Row != nil {
			batch.RealSize++
		}
		ring.ringBuf[off] = MsgRow{Msg: nil, Row: nil}
	}
	ring.ringGroundOff += int64(batchSize)
	ring.kafka.batchCh <- batch
	statistics.ParseMsgsBacklog.WithLabelValues(ring.kafka.taskCfg.Name).Sub(float64(batchSize))
	statistics.RingForceBatchsTotal.WithLabelValues(ring.kafka.taskCfg.Name).Inc()
	if ring.tid, err = ring.kafka.tw.Schedule(time.Duration(ring.kafka.taskCfg.FlushInterval)*time.Second, ring.ForceBatch, nil); err != nil {
		err = errors.Wrap(err, "")
		log.Criticalf("got error %+v", err)
	}
}
