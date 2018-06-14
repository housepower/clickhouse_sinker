package parser

import (
	"github.com/housepower/clickhouse_sinker/model"
)

type Parser interface {
	Parse(bs []byte) model.LogKV
}

func NewParser(typ string) Parser {
	switch typ {
	case "json":
		return &JsonParser{}
	default:
		return &JsonParser{}
	}
}
