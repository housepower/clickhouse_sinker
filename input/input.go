package input

import (
	"context"
	"log"

	"github.com/housepower/clickhouse_sinker/config"
	"github.com/housepower/clickhouse_sinker/model"
)

const (
	TypeKafkaGo     = "kafka-go"
	TypeKafkaSarama = "sarama"
	TypePulsar      = "pulsar"
)

type Inputer interface {
	Init(cfg *config.Config, taskName string, putFn func(msg model.InputMessage)) error
	Run(ctx context.Context)
	Stop() error
	CommitMessages(ctx context.Context, message *model.InputMessage) error
}

func NewInputer(typ string) Inputer {
	switch typ {
	case TypeKafkaGo:
		return NewKafkaGo()
	case TypeKafkaSarama:
		return NewKafkaSarama()
	default:
		log.Fatalf("%s is not a supported input type", typ)
		return nil
	}
}
