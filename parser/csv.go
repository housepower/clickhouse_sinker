package parser

import (
	"strconv"
	"strings"

	"github.com/housepower/clickhouse_sinker/model"
)

type CsvParser struct {
	title     []string
	delimiter string
}

func (c *CsvParser) Parse(bs []byte) model.Metric {
	msgData := string(bs)
	msgs := strings.Split(msgData, c.delimiter)
	v := make(map[string]string)
	for i, key := range c.title {
		v[key] = msgs[i]
	}
	return &CsvMetric{v}
}

type CsvMetric struct {
	mp map[string]string
}

func (c *CsvMetric) Get(key string) interface{} {
	return c.mp[key]
}

func (c *CsvMetric) GetString(key string) string {
	v, _ := c.mp[key]
	return v
}

func (c *CsvMetric) GetArray(key string, t string) []interface{} {
	panic("csv unsupport Array ")
	return nil
}

func (c *CsvMetric) GetFloat(key string) float64 {
	v, _ := c.mp[key]
	n, _ := strconv.ParseFloat(v, 64)
	return n
}

func (c *CsvMetric) GetInt(key string) int64 {
	v, _ := c.mp[key]
	n, _ := strconv.ParseInt(v, 10, 64)
	return n
}
