package parser
import (
	"github.com/housepower/clickhouse_sinker/model"
)
type CsvParser struct {
  title []string
  delimiter string
  mp map[string]interface{}
}

func (c *CsvParser) Parse(bs []byte) model.Metric {
  msgData := string(bs)
  msgs=strings.Split("msgData", c.delimiter)
	return &CsvMetric{}
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
