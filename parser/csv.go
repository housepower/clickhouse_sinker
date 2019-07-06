package parser

import (
	"strings"

	"github.com/housepower/clickhouse_sinker/model"
	"github.com/tidwall/gjson"
	"github.com/wswz/go_commons/log"
)

type CsvParser struct {
	title     []string
	delimiter string
}

func (c *CsvParser) Parse(bs []byte) model.Metric {
	msgData := string(bs)
	msgs := strings.Split(msgData, c.delimiter)
	v := make(map[string]interface{})
	for i, key := range c.title {
		v[key] = msgs[i]
	}
	b, err := json.Marshal(v)
	if err != nil {
		log.Infof("the csv value: %s , parse json failed", v)
		return nil
	}
	return &CsvMetric{string(b)}
}

type CsvMetric struct {
	raw string
}

func (c *CsvMetric) Get(key string) interface{} {
	return gjson.Get(c.raw, key).Value()
}

func (c *CsvMetric) GetString(key string) string {
	return gjson.Get(c.raw, key).String()
}

func (c *CsvMetric) GetFloat(key string) float64 {
	return gjson.Get(c.raw, key).Float()
}

func (c *CsvMetric) GetInt(key string) int64 {
	return gjson.Get(c.raw, key).Int()
}
