package parser

import (
	"github.com/housepower/clickhouse_sinker/model"
)

// Parse is the Parser interface
type Parser interface {
	Parse(bs []byte) model.Metric
}

// NewParser is a factory method to generate new parse
func NewParser(typ string, title []string, delimiter string) Parser {
	switch typ {
	case "json", "gjson":
		return &GjsonParser{}
	case "fastjson":
		return &FastjsonParser{}
	case "csv":
		return &CsvParser{title: title, delimiter: delimiter}
	case "gjson_extend": //extend gjson that could extract the map
		return &GjsonExtendParser{}
	default:
		return &GjsonParser{}
	}
}
