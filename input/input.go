package input

import (
	"fmt"

	"github.com/housepower/clickhouse_sinker/config"
	"github.com/housepower/clickhouse_sinker/model"
	"github.com/housepower/clickhouse_sinker/util"
)

const (
	TypeKafkaGo     = "kafka-go"
	TypeKafkaSarama = "sarama"
	TypePulsar      = "pulsar"
)

type Inputer interface {
	Init(cfg *config.Config, taskCfg *config.TaskConfig, putFn func(msg model.InputMessage), cleanupFn func()) error
	Run()
	Stop() error
	CommitMessages(message *model.InputMessage) error
}

func NewInputer(typ string) Inputer {
	switch typ {
	case TypeKafkaGo:
		return NewKafkaGo()
	case TypeKafkaSarama:
		return NewKafkaSarama()
	default:
		util.Logger.Fatal(fmt.Sprintf("BUG: %s is not a supported input type", typ))
		return nil
	}
}
