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
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/housepower/clickhouse_sinker/model"
	"github.com/housepower/clickhouse_sinker/util"
	"github.com/pkg/errors"
	"github.com/valyala/fastjson/fastfloat"
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
	if len(value) != len(p.pp.csvFormat) {
		err = errors.Errorf("csv value doesn't match the format")
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
func (c *CsvMetric) GetString(key string, nullable bool) (val interface{}) {
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
func (c *CsvMetric) GetFloat(key string, nullable bool) (val interface{}) {
	var idx int
	var ok bool
	if idx, ok = c.pp.csvFormat[key]; !ok {
		if nullable {
			return
		}
		val = float64(0.0)
		return
	}
	val = fastfloat.ParseBestEffort(c.values[idx])
	return
}

// GetInt returns int
func (c *CsvMetric) GetInt(key string, nullable bool) (val interface{}) {
	var idx int
	var ok bool
	if idx, ok = c.pp.csvFormat[key]; !ok {
		if nullable {
			return
		}
		val = int64(0)
		return
	}
	val = fastfloat.ParseInt64BestEffort(c.values[idx])
	return
}

func (c *CsvMetric) GetDateTime(key string, nullable bool) (val interface{}) {
	var idx int
	var ok bool
	if idx, ok = c.pp.csvFormat[key]; !ok {
		if nullable {
			return
		}
		val = Epoch
		return
	}
	val = c.pp.ParseDateTime(key, c.values[idx])
	return
}

func (c *CsvMetric) GetElasticDateTime(key string, nullable bool) (val interface{}) {
	t := c.GetDateTime(key, nullable)
	if t != nil {
		val = t.(time.Time).Unix()
	}
	return
}

// GetArray parse an CSV encoded array
func (c *CsvMetric) GetArray(key string, typ int) (val interface{}) {
	var err error
	var array []string
	var r *csv.Reader
	s := c.GetString(key, false)
	str, _ := s.(string)
	strLen := len(str)
	if str == "" || str[0] != '[' || str[strLen-1] != ']' {
		val = makeArray(typ)
		return
	}
	r = csv.NewReader(strings.NewReader(str[1 : strLen-1]))
	if array, err = r.Read(); err != nil {
		val = makeArray(typ)
		return
	}
	switch typ {
	case model.Int:
		results := make([]int64, 0, len(array))
		for _, e := range array {
			v := fastfloat.ParseInt64BestEffort(e)
			results = append(results, v)
		}
		val = results
	case model.Float:
		results := make([]float64, 0, len(array))
		for _, e := range array {
			v := fastfloat.ParseBestEffort(e)
			results = append(results, v)
		}
		val = results
	case model.String:
		val = array
	case model.DateTime:
		results := make([]time.Time, 0, len(array))
		for _, e := range array {
			v := c.pp.ParseDateTime(key, e)
			results = append(results, v)
		}
		val = results
	default:
		util.Logger.Fatal(fmt.Sprintf("LOGIC ERROR: unsupported array type %v", typ))
	}
	return
}

func (c *CsvMetric) GetNewKeys(knownKeys *sync.Map, newKeys *sync.Map) bool {
	return false
}
