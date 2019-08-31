package parser

import (
	"github.com/tidwall/gjson"

	"github.com/housepower/clickhouse_sinker/model"
)

type GjsonParser struct {
}

func (c *GjsonParser) Parse(bs []byte) model.Metric {
	return &GjsonMetric{string(bs)}
}

type GjsonMetric struct {
	raw string
}

func (c *GjsonMetric) Get(key string) interface{} {
	return gjson.Get(c.raw, key).Value()
}

func (c *GjsonMetric) GetString(key string) string {
	return gjson.Get(c.raw, key).String()
}

func (c *GjsonMetric) GetArray(key string, t string) []interface{} {
	slice := gjson.Get(c.raw, key).Array()
	results := make([]interface{}, 0, len(slice))
	switch t {
	default:
		return []interface{}{}
	case "float":
		for i := range slice {
			results = append(results, slice[i].Float())
		}
		return results
	case "int":
		for i := range slice {
			results = append(results, slice[i].Int())
		}
		return results
	case "string":
		for i := range slice {
			results = append(results, slice[i].String())
		}
		return results
	}
}

func (c *GjsonMetric) GetFloat(key string) float64 {
	return gjson.Get(c.raw, key).Float()
}

func (c *GjsonMetric) GetInt(key string) int64 {
	return gjson.Get(c.raw, key).Int()
}
