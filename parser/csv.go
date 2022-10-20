/*
Copyright [2019] housepower

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
	"math"
	"regexp"
	"strconv"
	"sync"
	"time"

	"github.com/shopspring/decimal"
	"github.com/thanos-io/thanos/pkg/errors"
	"github.com/tidwall/gjson"
	"github.com/valyala/fastjson/fastfloat"
	"golang.org/x/exp/constraints"

	"github.com/viru-tech/clickhouse_sinker/model"
	"github.com/viru-tech/clickhouse_sinker/util"
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
		err = errors.Wrapf(err, "")
		return
	}
	if len(value) != len(p.pp.csvFormat) {
		err = errors.Newf("csv value doesn't match the format")
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
	if idx, ok = c.pp.csvFormat[key]; !ok || c.values[idx] == "null" {
		if nullable {
			return
		}
		val = ""
		return
	}
	val = c.values[idx]
	return
}

func (c *CsvMetric) GetUUID(key string, nullable bool) interface{} {
	var idx int
	var ok bool
	if idx, ok = c.pp.csvFormat[key]; !ok || c.values[idx] == "null" {
		if nullable {
			return nil
		}
		return zeroUUID
	}

	if v := c.values[idx]; v != "" {
		return v
	}
	return zeroUUID
}

// GetDecimal returns the value as decimal
func (c *CsvMetric) GetDecimal(key string, nullable bool) (val interface{}) {
	var idx int
	var ok bool
	if idx, ok = c.pp.csvFormat[key]; !ok || c.values[idx] == "null" {
		if nullable {
			return
		}
		val = decimal.NewFromInt(0)
		return
	}
	val, _ = decimal.NewFromString(c.values[idx])
	return
}

func (c *CsvMetric) GetBool(key string, nullable bool) (val interface{}) {
	var idx int
	var ok bool
	if idx, ok = c.pp.csvFormat[key]; !ok || c.values[idx] == "" || c.values[idx] == "null" {
		if nullable {
			return
		}
		val = false
		return
	}
	val = (c.values[idx] == "true")
	return
}

func (c *CsvMetric) GetInt8(key string, nullable bool) (val interface{}) {
	return CsvGetInt[int8](c, key, nullable, math.MinInt8, math.MaxInt8)
}

func (c *CsvMetric) GetInt16(key string, nullable bool) (val interface{}) {
	return CsvGetInt[int16](c, key, nullable, math.MinInt16, math.MaxInt16)
}

func (c *CsvMetric) GetInt32(key string, nullable bool) (val interface{}) {
	return CsvGetInt[int32](c, key, nullable, math.MinInt32, math.MaxInt32)
}

func (c *CsvMetric) GetInt64(key string, nullable bool) (val interface{}) {
	return CsvGetInt[int64](c, key, nullable, math.MinInt64, math.MaxInt64)
}

func (c *CsvMetric) GetUint8(key string, nullable bool) (val interface{}) {
	return CsvGetUint[uint8](c, key, nullable, math.MaxUint8)
}

func (c *CsvMetric) GetUint16(key string, nullable bool) (val interface{}) {
	return CsvGetUint[uint16](c, key, nullable, math.MaxUint16)
}

func (c *CsvMetric) GetUint32(key string, nullable bool) (val interface{}) {
	return CsvGetUint[uint32](c, key, nullable, math.MaxUint32)
}

func (c *CsvMetric) GetUint64(key string, nullable bool) (val interface{}) {
	return CsvGetUint[uint64](c, key, nullable, math.MaxUint64)
}

func (c *CsvMetric) GetFloat32(key string, nullable bool) (val interface{}) {
	return CsvGetFloat[float32](c, key, nullable, math.MaxFloat32)
}

func (c *CsvMetric) GetFloat64(key string, nullable bool) (val interface{}) {
	return CsvGetFloat[float64](c, key, nullable, math.MaxFloat64)
}

func CsvGetInt[T constraints.Signed](c *CsvMetric, key string, nullable bool, min, max int64) (val interface{}) {
	var idx int
	var ok bool
	if idx, ok = c.pp.csvFormat[key]; !ok || c.values[idx] == "null" {
		if nullable {
			return
		}
		val = T(0)
		return
	}
	if s := c.values[idx]; s == "true" {
		val = T(1)
	} else {
		val2 := fastfloat.ParseInt64BestEffort(s)
		if val2 < min {
			val = T(min)
		} else if val2 > max {
			val = T(max)
		} else {
			val = T(val2)
		}
	}
	return
}

func CsvGetUint[T constraints.Unsigned](c *CsvMetric, key string, nullable bool, max uint64) (val interface{}) {
	var idx int
	var ok bool
	if idx, ok = c.pp.csvFormat[key]; !ok || c.values[idx] == "null" {
		if nullable {
			return
		}
		val = T(0)
		return
	}
	if s := c.values[idx]; s == "true" {
		val = T(1)
	} else {
		val2 := fastfloat.ParseUint64BestEffort(s)
		if val2 > max {
			val = T(max)
		} else {
			val = T(val2)
		}
	}
	return
}

// GetFloat returns the value as float
func CsvGetFloat[T constraints.Float](c *CsvMetric, key string, nullable bool, max float64) (val interface{}) {
	var idx int
	var ok bool
	if idx, ok = c.pp.csvFormat[key]; !ok || c.values[idx] == "null" {
		if nullable {
			return
		}
		val = float64(0.0)
		return
	}
	val2 := fastfloat.ParseBestEffort(c.values[idx])
	if val2 > max {
		val = T(max)
	} else {
		val = T(val2)
	}
	return
}

func (c *CsvMetric) GetDateTime(key string, nullable bool) (val interface{}) {
	var idx int
	var ok bool
	if idx, ok = c.pp.csvFormat[key]; !ok || c.values[idx] == "null" {
		if nullable {
			return
		}
		val = Epoch
		return
	}
	s := c.values[idx]
	if dd, err := strconv.ParseFloat(s, 64); err != nil {
		var err error
		if val, err = c.pp.ParseDateTime(key, s); err != nil {
			val = Epoch
		}
	} else {
		val = UnixFloat(dd, c.pp.timeUnit)
	}
	return
}

// GetArray parse an CSV encoded array
func (c *CsvMetric) GetArray(key string, typ int) (val interface{}) {
	s := c.GetString(key, false)
	str, _ := s.(string)
	var array []gjson.Result
	r := gjson.Parse(str)
	if r.IsArray() {
		array = r.Array()
	}
	switch typ {
	case model.Bool:
		results := make([]bool, 0, len(array))
		for _, e := range array {
			v := (e.Exists() && e.Type == gjson.True)
			results = append(results, v)
		}
		val = results
	case model.Int8:
		val = GjsonIntArray[int8](array, math.MinInt8, math.MaxInt8)
	case model.Int16:
		val = GjsonIntArray[int16](array, math.MinInt16, math.MaxInt16)
	case model.Int32:
		val = GjsonIntArray[int32](array, math.MinInt32, math.MaxInt32)
	case model.Int64:
		val = GjsonIntArray[int64](array, math.MinInt64, math.MaxInt64)
	case model.UInt8:
		val = GjsonUintArray[uint8](array, math.MaxUint8)
	case model.UInt16:
		val = GjsonUintArray[uint16](array, math.MaxUint16)
	case model.UInt32:
		val = GjsonUintArray[uint32](array, math.MaxUint32)
	case model.UInt64:
		val = GjsonUintArray[uint64](array, math.MaxUint64)
	case model.Float32:
		val = GjsonFloatArray[float32](array, math.MaxFloat32)
	case model.Float64:
		val = GjsonFloatArray[float64](array, math.MaxFloat64)
	case model.Decimal:
		results := make([]decimal.Decimal, 0, len(array))
		var f float64
		for _, e := range array {
			switch e.Type {
			case gjson.Number:
				f = e.Num
			default:
				f = float64(0.0)
			}
			results = append(results, decimal.NewFromFloat(f))
		}
		val = results
	case model.String:
		results := make([]string, 0, len(array))
		var s string
		for _, e := range array {
			switch e.Type {
			case gjson.Null:
				s = ""
			case gjson.String:
				s = e.Str
			default:
				s = e.Raw
			}
			results = append(results, s)
		}
		val = results
	case model.UUID:
		results := make([]string, 0, len(array))
		var s string
		for _, e := range array {
			switch e.Type {
			case gjson.Null:
				s = ""
			case gjson.String:
				s = e.Str
			default:
				s = e.Raw
			}
			if s == "" {
				s = zeroUUID
			}
			results = append(results, s)
		}
		val = results
	case model.DateTime:
		results := make([]time.Time, 0, len(array))
		var t time.Time
		for _, e := range array {
			switch e.Type {
			case gjson.Number:
				t = UnixFloat(e.Num, c.pp.timeUnit)
			case gjson.String:
				var err error
				if t, err = c.pp.ParseDateTime(key, e.Str); err != nil {
					t = Epoch
				}
			default:
				t = Epoch
			}
			results = append(results, t)
		}
		val = results
	default:
		util.Logger.Fatal(fmt.Sprintf("LOGIC ERROR: unsupported array type %v", typ))
	}
	return
}

func (c *CsvMetric) GetNewKeys(knownKeys, newKeys, warnKeys *sync.Map, white, black *regexp.Regexp, partition int, offset int64) bool {
	return false
}
