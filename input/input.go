package input

import (
	"fmt"

	"github.com/viru-tech/clickhouse_sinker/config"
	"github.com/viru-tech/clickhouse_sinker/model"
	"github.com/viru-tech/clickhouse_sinker/util"
)

const (
	TypeKafkaGo     = "kafka-go"
	TypeKafkaSarama = "sarama"
	TypeKafkaFranz  = "franz"
)

type Inputer interface {
	Init(cfg *config.Config, taskCfg *config.TaskConfig, putFn func(msg *model.InputMessage), cleanupFn func()) error
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
	case TypeKafkaFranz:
		return NewKafkaFranz()
	default:
		util.Logger.Fatal(fmt.Sprintf("BUG: %s is not a supported input type", typ))
		return nil
	}
}
