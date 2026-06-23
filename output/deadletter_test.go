package output

import (
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/housepower/clickhouse_sinker/model"
)

type fakeProducer struct {
	sent     []map[string]string
	failNext bool
}

func (f *fakeProducer) Produce(topic string, key, value []byte, headers map[string]string) error {
	if f.failNext {
		return errors.New("boom")
	}
	f.sent = append(f.sent, headers)
	return nil
}
func (f *fakeProducer) Close() {}

func newTestSink(p dlqProducer) *DeadLetterSink {
	return &DeadLetterSink{taskName: "t1", table: "db.tb", topic: "dlq", prod: p}
}

func TestDeadLetterSendBatchPayload(t *testing.T) {
	fp := &fakeProducer{}
	s := newTestSink(fp)
	ts := time.Unix(1700000000, 0)
	b := &model.Batch{
		RealSize: 2,
		Msgs: []*model.InputMessage{
			{Topic: "in", Partition: 3, Offset: 100, Key: []byte("k0"), Value: []byte("v0"), Timestamp: &ts},
			{Topic: "in", Partition: 3, Offset: 101, Value: []byte("v1"), Timestamp: &ts},
		},
	}
	if err := s.SendBatch(b, "data", "code: 53"); err != nil {
		t.Fatalf("SendBatch err: %v", err)
	}
	if len(fp.sent) != 2 {
		t.Fatalf("produced %d, want 2", len(fp.sent))
	}
	h := fp.sent[0]
	if h["task"] != "t1" || h["table"] != "db.tb" || h["error_class"] != "data" ||
		h["topic"] != "in" || h["partition"] != "3" || h["offset"] != "100" ||
		h["error_msg"] != "code: 53" {
		t.Fatalf("bad headers: %#v", h)
	}
	wantTs := strconv.FormatInt(ts.UnixMilli(), 10)
	if h["ts"] != wantTs {
		t.Fatalf("bad ts header: got %q, want %q", h["ts"], wantTs)
	}
}

func TestDeadLetterSendBatchEmptyMsgs(t *testing.T) {
	s := newTestSink(&fakeProducer{})
	b := &model.Batch{RealSize: 0, Msgs: nil}
	if err := s.SendBatch(b, "data", "x"); err == nil {
		t.Fatal("expected error when batch has no raw msgs")
	}
}

func TestDeadLetterSendBatchFailurePropagates(t *testing.T) {
	s := newTestSink(&fakeProducer{failNext: true})
	b := &model.Batch{RealSize: 1, Msgs: []*model.InputMessage{{Value: []byte("v")}}}
	if err := s.SendBatch(b, "unknown", "x"); err == nil {
		t.Fatal("expected error so caller can fall back to drop")
	}
}
