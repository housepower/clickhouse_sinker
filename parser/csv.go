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
	"io"
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

// GetString get the value as string
func (c *CsvMetric) GetString(key string, nullable bool) (val interface{}, err error) {
	var idx int
	var ok bool
	if idx, ok = c.pp.csvFormat[key]; !ok {
		if nullable {
			return
		}
		val = ""
		return
	}
	val = c.values[idx]
	return
}

// GetFloat returns the value as float
func (c *CsvMetric) GetFloat(key string, nullable bool) (val interface{}, err error) {
	var idx int
	var ok bool
	if idx, ok = c.pp.csvFormat[key]; !ok {
		if nullable {
			return
		}
		val = float64(0.0)
		return
	}
	val, err = strconv.ParseFloat(c.values[idx], 64)
	return
}

// GetInt returns int
func (c *CsvMetric) GetInt(key string, nullable bool) (val interface{}, err error) {
	var idx int
	var ok bool
	if idx, ok = c.pp.csvFormat[key]; !ok {
		if nullable {
			return
		}
		val = int64(0)
		return
	}
	val, err = strconv.ParseInt(c.values[idx], 10, 64)
	return
}

func (c *CsvMetric) GetDate(key string, nullable bool) (val interface{}, err error) {
	return c.GetDateTime(key, nullable)
}

func (c *CsvMetric) GetDateTime(key string, nullable bool) (val interface{}, err error) {
	var idx int
	var ok bool
	if idx, ok = c.pp.csvFormat[key]; !ok {
		if nullable {
			return
		}
		val = Epoch
		return
	}
	val, err = c.pp.ParseDateTime(key, c.values[idx])
	return
}

func (c *CsvMetric) GetDateTime64(key string, nullable bool) (val interface{}, err error) {
	return c.GetDateTime(key, nullable)
}

func (c *CsvMetric) GetElasticDateTime(key string, nullable bool) (val interface{}, err error) {
	var t interface{}
	if t, err = c.GetDateTime(key, nullable); err != nil {
		return
	}
	if t != nil {
		val = t.(time.Time).Unix()
	}
	return
}

// GetArray parse an CSV encoded array
func (c *CsvMetric) GetArray(key string, t string) (val interface{}, err error) {
	var s interface{}
	var array []string
	var r *csv.Reader
	if s, err = c.GetString(key, false); err != nil {
		return
	}
	str, _ := s.(string)
	strLen := len(str)
	if str == "" || str[0] != '[' || str[strLen-1] != ']' {
		err = errors.Errorf("GetArray %s got unexpected value %s", key, str)
		return
	}
	r = csv.NewReader(strings.NewReader(str[1 : strLen-1]))
	if array, err = r.Read(); err != nil {
		if errors.Is(err, io.EOF) {
			err = nil
			switch t {
			case "int":
				val = []int64{}
			case "float":
				val = []float64{}
			case "string":
				val = []string{}
			default:
				panic("LOGIC ERROR: not supported array type " + t)
			}
		}
		return
	}
	switch t {
	case "int":
		results := make([]int64, 0, len(array))
		var v int64
		for _, e := range array {
			if v, err = strconv.ParseInt(e, 10, 64); err != nil {
				return
			}
			results = append(results, v)
		}
		val = results
	case "float":
		results := make([]float64, 0, len(array))
		var v float64
		for _, e := range array {
			if v, err = strconv.ParseFloat(e, 64); err != nil {
				return
			}
			results = append(results, v)
		}
		val = results
	case "string":
		val = array
	default:
		panic("LOGIC ERROR: not supported array type " + t)
	}
	return
}

func (c *CsvMetric) GetNewKeys(knownKeys *sync.Map, newKeys *sync.Map) bool {
	return false
}
