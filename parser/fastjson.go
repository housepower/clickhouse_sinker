package parser

import (
	"github.com/valyala/fastjson"

	"github.com/housepower/clickhouse_sinker/model"
)

type FastjsonParser struct {
	parser fastjson.Parser
}

func (c *FastjsonParser) Parse(bs []byte) model.Metric {
	value, err := c.parser.Parse(string(bs))
	if err == nil {
		return &FastjsonMetric{value: value}
	}
	return &DummyMetric{}
}

type FastjsonMetric struct {
	value *fastjson.Value
}

func (c *FastjsonMetric) Get(key string) interface{} {
	return c.value.Get(key)
}

func (c *FastjsonMetric) GetString(key string) string {
	v := c.value.GetStringBytes(key)
	return string(v)
}

func (c *FastjsonMetric) GetFloat(key string) float64 {
	return c.value.GetFloat64(key)
}

func (c *FastjsonMetric) GetInt(key string) int64 {
	return int64(c.value.GetInt(key))
}

func (c *FastjsonMetric) GetArray(key string, t string) []interface{} {
	array, _ := c.value.Array()
	results := make([]interface{}, 0, len(array))

	switch t {
	default:
		return []interface{}{}
	case "float":
		for i := range array {
			results = append(results, array[i].GetFloat64())
		}
		return results
	case "int":
		for i := range array {
			results = append(results, array[i].GetInt())
		}
		return results
	case "string":
		for i := range array {
			results = append(results, string(array[i].GetStringBytes()))
		}
		return results
	}
}

func (c *FastjsonMetric) String() string {
	return c.value.String()
}
