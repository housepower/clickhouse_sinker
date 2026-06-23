package output

import (
	"context"
	"strconv"

	"github.com/housepower/clickhouse_sinker/config"
	"github.com/housepower/clickhouse_sinker/model"
	"github.com/housepower/clickhouse_sinker/util"
	"github.com/thanos-io/thanos/pkg/errors"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
)

// dlqProducer 抽象死信投递接口,便于测试时注入 fake 实现。
type dlqProducer interface {
	Produce(topic string, key, value []byte, headers map[string]string) error
	Close()
}

// kgoProducer 是 dlqProducer 的 franz-go 实现(同步投递)。
type kgoProducer struct {
	cl *kgo.Client
}

func (p *kgoProducer) Produce(topic string, key, value []byte, headers map[string]string) error {
	hs := make([]kgo.RecordHeader, 0, len(headers))
	for k, v := range headers {
		hs = append(hs, kgo.RecordHeader{Key: k, Value: []byte(v)})
	}
	rec := &kgo.Record{Topic: topic, Key: key, Value: value, Headers: hs}
	return p.cl.ProduceSync(context.Background(), rec).FirstErr()
}

func (p *kgoProducer) Close() { p.cl.Close() }

// DeadLetterSink 把写入失败批次的原始 Kafka 消息投递到死信 topic。
type DeadLetterSink struct {
	taskName string
	table    string
	topic    string
	prod     dlqProducer
}

// NewDeadLetterSink 根据任务配置构建 DeadLetterSink,并可选自动建 topic。
func NewDeadLetterSink(taskName, table string, cfg *config.WriteFailureConfig) (sink *DeadLetterSink, err error) {
	if len(cfg.BootstrapServers) == 0 || cfg.TopicName == "" {
		return nil, errors.Newf("dead-letter requires bootstrapServers and topicName for task %s", taskName)
	}
	cl, err := kgo.NewClient(kgo.SeedBrokers(cfg.BootstrapServers...))
	if err != nil {
		return nil, errors.Wrapf(err, "create dead-letter producer for task %s", taskName)
	}
	defer func() {
		if err != nil {
			cl.Close()
		}
	}()
	if cfg.AutoCreateTopic {
		parts := cfg.AutoCreateTopicPartitions
		if parts <= 0 {
			parts = 1
		}
		rf := cfg.AutoCreateTopicReplicationFactor
		if rf <= 0 {
			rf = 1
		}
		adm := kadm.NewClient(cl)
		// 已存在等情形不致命,仅告警。
		if _, err := adm.CreateTopic(context.Background(), parts, rf, nil, cfg.TopicName); err != nil {
			util.Logger.Warn("dead-letter auto-create topic returned error (may already exist)",
				zap.String("task", taskName), zap.String("topic", cfg.TopicName), zap.Error(err))
		}
	}
	sink = &DeadLetterSink{taskName: taskName, table: table, topic: cfg.TopicName, prod: &kgoProducer{cl: cl}}
	return sink, nil
}

// SendBatch 把整批原始消息逐条投递到死信 topic。任一条失败即返回错误,
// 由调用方兜底(降级为丢弃)。
func (s *DeadLetterSink) SendBatch(b *model.Batch, label, errMsg string) error {
	if len(b.Msgs) == 0 {
		// 没有贯通原始字节(理论上不应发生),无可旁路。
		return errors.Newf("dead-letter batch has no raw msgs (task %s)", s.taskName)
	}
	for _, m := range b.Msgs {
		if m == nil {
			continue
		}
		headers := map[string]string{
			"task":        s.taskName,
			"table":       s.table,
			"error_class": label,
			"error_msg":   errMsg,
			"topic":       m.Topic,
			"partition":   strconv.Itoa(m.Partition),
			"offset":      strconv.FormatInt(m.Offset, 10),
		}
		if m.Timestamp != nil {
			headers["ts"] = strconv.FormatInt(m.Timestamp.UnixMilli(), 10)
		}
		if err := s.prod.Produce(s.topic, m.Key, m.Value, headers); err != nil {
			return errors.Wrapf(err, "produce to dead-letter topic %s", s.topic)
		}
	}
	return nil
}

// Close 释放底层 Kafka 生产者资源。
func (s *DeadLetterSink) Close() {
	if s.prod != nil {
		s.prod.Close()
	}
}
