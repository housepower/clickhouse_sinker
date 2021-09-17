package input

import (
	"context"
	"encoding/binary"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/housepower/clickhouse_sinker/config"
	"github.com/housepower/clickhouse_sinker/model"
	"github.com/housepower/clickhouse_sinker/statistics"
	"github.com/housepower/clickhouse_sinker/util"
	"github.com/pkg/errors"
	"github.com/riferrei/srclient"
	"go.uber.org/zap"
	"io"
)

var _ Inputer = (*KafkaAvro)(nil)

// KafkaAvro implements input.Inputer
type KafkaAvro struct {
	consumer             *kafka.Consumer
	cfg                  *config.Config
	taskCfg              *config.TaskConfig
	stopped              chan struct{}
	putFn                func(msg model.InputMessage)
	schemaRegistryClient *srclient.SchemaRegistryClient
}

// NewKafkaAvro get instance of kafka reader
func NewKafkaAvro() *KafkaAvro {
	return &KafkaAvro{}
}

// Init Initialise the kafka instance with configuration
func (k *KafkaAvro) Init(cfg *config.Config, taskCfg *config.TaskConfig, putFn func(msg model.InputMessage), cleanupFn func()) (err error) {
	k.cfg = cfg
	k.taskCfg = taskCfg
	k.stopped = make(chan struct{})
	k.putFn = putFn
	kfkCfg := &cfg.Kafka

	offset := kafka.OffsetEnd
	if k.taskCfg.Earliest {
		offset = kafka.OffsetBeginning
	}

	k.consumer, err = kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  kfkCfg.Brokers,
		"security.protocol":  kfkCfg.Security["protocol"],
		"sasl.mechanism":     kfkCfg.Sasl.Mechanism,
		"sasl.username":      kfkCfg.Sasl.Username,
		"sasl.password":      kfkCfg.Sasl.Password,
		"group.id":           k.taskCfg.ConsumerGroup,
		"session.timeout.ms": 6000,
		"auto.offset.reset":  offset,
	})
	k.schemaRegistryClient = srclient.CreateSchemaRegistryClient(kfkCfg.SchemaRegistryURL)
	return k.consumer.SubscribeTopics([]string{k.taskCfg.Topic}, nil)
}

// kafka main loop
func (k *KafkaAvro) Run(_ context.Context) {
LOOP_ARMO:
	for {

		msg, err := k.consumer.ReadMessage(-1)

		if err != nil {
			if errors.Is(err, context.Canceled) {
				util.Logger.Info("KafkaAvro.Run quit due to context has been canceled", zap.String("task", k.taskCfg.Name))
				break LOOP_ARMO
			} else if errors.Is(err, io.EOF) {
				util.Logger.Info("KafkaAvro.Run quit due to reader has been closed", zap.String("task", k.taskCfg.Name))
				break LOOP_ARMO
			} else {
				statistics.ConsumeMsgsErrorTotal.WithLabelValues(k.taskCfg.Name).Inc()
				err = errors.Wrap(err, "")
				util.Logger.Error("kafka.Reader.FetchMessage failed", zap.String("task", k.taskCfg.Name), zap.Error(err))
				continue
			}
		}

		if len(msg.Value) < 5 {
			continue
		}

		schemaID := binary.BigEndian.Uint32(msg.Value[1:5])
		schema, err := k.schemaRegistryClient.GetSchema(int(schemaID))
		if err != nil {
			continue
		}

		native, _, _ := schema.Codec().NativeFromBinary(msg.Value[5:])
		value, _ := schema.Codec().TextualFromNative(nil, native)

		k.putFn(model.InputMessage{
			Topic:     *msg.TopicPartition.Topic,
			Partition: int(msg.TopicPartition.Partition),
			Key:       msg.Key,
			Value:     value,
			Timestamp: &msg.Timestamp,
			Offset:    int64(msg.TopicPartition.Offset),
		})
	}
	k.stopped <- struct{}{}
}

func (k *KafkaAvro) CommitMessages(_ context.Context, msg *model.InputMessage) error {
	if msg.Value == nil {
		return nil
	}

	message := kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &msg.Topic, Partition: int32(msg.Partition)},
		Key:            msg.Key,
		Timestamp:      *msg.Timestamp,
		Value:          msg.Value,
	}
	if _, err := k.consumer.CommitMessage(&message); err != nil {
		util.Logger.Error("CommitMessage failed", zap.String("task", k.taskCfg.Name), zap.Error(err))
		return err
	}
	return nil
}

func (k *KafkaAvro) Stop() error {
	if k.consumer != nil {
		k.consumer.Close()
		<-k.stopped
	}
	return nil
}

// Description of this kafka consumer, which topic it reads from
func (k *KafkaAvro) Description() string {
	return "kafka consumer of topic " + k.taskCfg.Topic
}
