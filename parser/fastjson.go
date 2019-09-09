package parser

import (
	"github.com/valyala/fastjson"

	"github.com/housepower/clickhouse_sinker/model"
)

type FastjsonParser struct {
}

func (c *FastjsonParser) Parse(bs []byte) model.Metric {
	// todo pool the parser
	var parser fastjson.Parser
	value, err := parser.Parse(string(bs))
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

func (c *FastjsonMetric) GetArray(key string, t string) interface{} {
	array, _ := c.value.Array()
	switch t {
	case "float":
		results := make([]float64, 0, len(array))
		for i := range array {
			results = append(results, array[i].GetFloat64())
		}
		return results
	case "int":
		results := make([]int, 0, len(array))
		for i := range array {
			results = append(results, array[i].GetInt())
		}
		return results
	case "string":
		results := make([]string, 0, len(array))
		for i := range array {
			results = append(results, string(array[i].GetStringBytes()))
		}
		return results
	default:
		panic("not supported array type " + t)
	}
	return nil
}

func (c *FastjsonMetric) String() string {
	return c.value.String()
}
