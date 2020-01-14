/*Copyright [2019] housepower

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package parser

import (
	"bytes"
	"encoding/csv"
	"strconv"
	"time"

	"github.com/housepower/clickhouse_sinker/model"
	"github.com/sundy-li/go_commons/log"
)

// CsvParser implementation to parse input from a CSV format
type CsvParser struct {
	title     []string
	delimiter string
}

// Parse extract comma separated values from the data
func (c *CsvParser) Parse(bs []byte) model.Metric {
	r := csv.NewReader(bytes.NewReader(bs))

	r.Comma = ','
	if len(c.delimiter) > 0 {
		r.Comma = rune(c.delimiter[0])
	}
	values, err := r.Read()
	if err != nil {
		log.Error("Parse csv error:" + err.Error())
		return &DummyMetric{}
	}
	return &CsvMetric{c.title, values}
}

// CsvMetic
type CsvMetric struct {
	titles []string
	values []string
}

// Get returns the value corresponding to a column expects called
// interpret the type
func (c *CsvMetric) Get(key string) interface{} {
	for i, k := range c.titles {
		if k == key && i < len(c.values) {
			return c.values[i]
		}
	}
	return nil
}

// GetString get the value as string
func (c *CsvMetric) GetString(key string) string {
	for i, k := range c.titles {
		if k == key && i < len(c.values) {
			return c.values[i]
		}
	}
	return ""
}

// GetFloat returns the value as float
func (c *CsvMetric) GetFloat(key string) float64 {
	for i, k := range c.titles {
		if k == key && i < len(c.values) {
			n, _ := strconv.ParseFloat(c.values[i], 64)
			return n
		}
	}
	return 0
}

// GetInt returns int
func (c *CsvMetric) GetInt(key string) int64 {
	for i, k := range c.titles {
		if k == key && i < len(c.values) {
			n, _ := strconv.ParseInt(c.values[i], 10, 64)
			return n
		}
	}
	return 0
}

// GetArray is Empty implemented for CsvMetric
func (c *CsvMetric) GetArray(key string, t string) interface{} {
	return []interface{}{}
}

func (c *CsvMetric) GetElasticDate(key string) int64 {
	val := c.GetString(key)
	t, _ := time.Parse(time.RFC3339, val)

	return t.Unix()
}
