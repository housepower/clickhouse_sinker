package input

import (
	"context"

	"github.com/housepower/clickhouse_sinker/config"
	"github.com/housepower/clickhouse_sinker/model"
)

const (
	TypeKafkaGo     = "kafka-go"
	TypeKafkaSarama = "kafka-sarama"
	TypePulsar      = "pulsar"
)

type Inputer interface {
	Init(taskCfg *config.TaskConfig, putFn func(msg model.InputMessage)) error
	Run(ctx context.Context)
	Stop() error
	CommitMessages(ctx context.Context, message *model.InputMessage) error
}

func NewInputer(typ string) Inputer {
	switch typ {
	case TypeKafkaGo:
		return NewKafka()
	default:
		return nil
	}
}
