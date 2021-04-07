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
	"strings"
	"sync"
	"time"

	"github.com/housepower/clickhouse_sinker/model"
	"github.com/pkg/errors"
)

var _ Parser = (*CsvParser)(nil)

// CsvParser implementation to parse input from a CSV format per RFC 4180
type CsvParser struct {
	pp *Pool
}

// Parse extract a list of comma-separated values from the data
func (p *CsvParser) Parse(bs []byte) (metric model.Metric, err error) {
	r := csv.NewReader(bytes.NewReader(bs))
	r.FieldsPerRecord = len(p.pp.csvFormat)
	if len(p.pp.delimiter) > 0 {
		r.Comma = rune(p.pp.delimiter[0])
	}
	var value []string
	if value, err = r.Read(); err != nil {
		err = errors.Wrap(err, "")
		return
	}
	metric = &CsvMetric{p.pp, value}
	return
}

// CsvMetic
type CsvMetric struct {
	pp     *Pool
	values []string
}

// Get returns the value corresponding to a column expects called
// interpret the type
func (c *CsvMetric) Get(key string) interface{} {
	for i, k := range c.pp.csvFormat {
		if k == key && i < len(c.values) {
			return c.values[i]
		}
	}
	return nil
}

// GetString get the value as string
func (c *CsvMetric) GetString(key string, nullable bool) interface{} {
	_ = nullable // nullable can not be supported with csv
	for i, k := range c.pp.csvFormat {
		if k == key && i < len(c.values) {
			return c.values[i]
		}
	}
	return ""
}

// GetFloat returns the value as float
func (c *CsvMetric) GetFloat(key string, nullable bool) interface{} {
	_ = nullable // nullable can not be supported with csv
	for i, k := range c.pp.csvFormat {
		if k == key && i < len(c.values) {
			n, _ := strconv.ParseFloat(c.values[i], 64)
			return n
		}
	}
	return float64(0)
}

// GetInt returns int
func (c *CsvMetric) GetInt(key string, nullable bool) interface{} {
	_ = nullable // nullable can not be supported with csv
	for i, k := range c.pp.csvFormat {
		if k == key && i < len(c.values) {
			n, _ := strconv.ParseInt(c.values[i], 10, 64)
			return n
		}
	}
	return int64(0)
}

// GetArray parse an CSV encoded array
func (c *CsvMetric) GetArray(key string, t string) interface{} {
	var err error
	var array []string
	var r *csv.Reader
	val, _ := c.GetString(key, false).(string)
	valLen := len(val)
	if val == "" || val[0] != '[' || val[valLen-1] != ']' {
		goto QUIT
	}
	r = csv.NewReader(strings.NewReader(val[1 : valLen-1]))
	if array, err = r.Read(); err != nil {
		goto QUIT
	}
	switch t {
	case "int":
		results := make([]int64, 0, len(array))
		for _, e := range array {
			v, _ := strconv.ParseInt(e, 10, 64)
			results = append(results, v)
		}
		return results
	case "float":
		results := make([]float64, 0, len(array))
		for _, e := range array {
			v, _ := strconv.ParseFloat(e, 64)
			results = append(results, v)
		}
		return results
	case "string":
		return array
	default:
		panic("not supported array type " + t)
	}
QUIT:
	switch t {
	case "int":
		return []int64{}
	case "float":
		return []float64{}
	case "string":
		return []string{}
	default:
		return nil
	}
}

func (c *CsvMetric) GetDate(key string, nullable bool) interface{} {
	_ = nullable // nullable can not be supported with csv

	val, _ := c.GetString(key, false).(string)
	t, _ := c.pp.ParseDateTime(key, val)
	return t
}

func (c *CsvMetric) GetDateTime(key string, nullable bool) interface{} {
	val, _ := c.GetString(key, false).(string)
	t, _ := c.pp.ParseDateTime(key, val)
	return t
}

func (c *CsvMetric) GetDateTime64(key string, nullable bool) interface{} {
	val, _ := c.GetString(key, false).(string)
	t, _ := c.pp.ParseDateTime(key, val)
	return t
}

func (c *CsvMetric) GetElasticDateTime(key string, nullable bool) interface{} {
	_ = nullable // nullable can not be supported with csv
	val, _ := c.GetString(key, false).(string)
	t, _ := time.Parse(time.RFC3339, val)

	return t.Unix()
}

func (c *CsvMetric) GetNewKeys(knownKeys *sync.Map, newKeys *sync.Map) bool {
	return false
}
