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
	"fmt"
	"regexp"
	"strconv"
	"sync"
	"time"

	"github.com/shopspring/decimal"
	"github.com/tidwall/gjson"
	"go.uber.org/zap"
	"golang.org/x/exp/constraints"

	"github.com/housepower/clickhouse_sinker/model"
	"github.com/housepower/clickhouse_sinker/util"
)

var _ Parser = (*GjsonParser)(nil)

type GjsonParser struct {
	pp *Pool
}

func (p *GjsonParser) Parse(bs []byte) (metric model.Metric, err error) {
	metric = &GjsonMetric{p.pp, string(bs)}
	return
}

type GjsonMetric struct {
	pp  *Pool
	raw string
}

func (c *GjsonMetric) GetString(key string, nullable bool) (val interface{}) {
	r := gjson.Get(c.raw, key)
	if !r.Exists() || r.Type == gjson.Null {
		if nullable {
			return
		}
		val = ""
		return
	}
	switch r.Type {
	case gjson.Null:
		val = ""
	case gjson.String:
		val = r.Str
	default:
		val = r.Raw
	}
	return
}

func (c *GjsonMetric) GetBool(key string, nullable bool) (val interface{}) {
	r := gjson.Get(c.raw, key)
	if !gjCompatibleBool(r) {
		val = getDefaultBool(nullable)
		return
	}
	val = (r.Type == gjson.True)
	return
}

func (c *GjsonMetric) GetDecimal(key string, nullable bool) (val interface{}) {
	r := gjson.Get(c.raw, key)
	if !gjCompatibleFloat(r) {
		val = getDefaultDecimal(nullable)
		return
	}
	switch r.Type {
	case gjson.Number:
		val = decimal.NewFromFloat(r.Num)
	default:
		val = getDefaultDecimal(nullable)
	}
	return
}

func (c *GjsonMetric) GetInt8(key string, nullable bool) (val interface{}) {
	return GjsonGetInt[int8](c, key, nullable)
}

func (c *GjsonMetric) GetInt16(key string, nullable bool) (val interface{}) {
	return GjsonGetInt[int16](c, key, nullable)
}

func (c *GjsonMetric) GetInt32(key string, nullable bool) (val interface{}) {
	return GjsonGetInt[int32](c, key, nullable)
}

func (c *GjsonMetric) GetInt64(key string, nullable bool) (val interface{}) {
	return GjsonGetInt[int64](c, key, nullable)
}

func (c *GjsonMetric) GetUint8(key string, nullable bool) (val interface{}) {
	return GjsonGetUint[uint8](c, key, nullable)
}

func (c *GjsonMetric) GetUint16(key string, nullable bool) (val interface{}) {
	return GjsonGetUint[uint16](c, key, nullable)
}

func (c *GjsonMetric) GetUint32(key string, nullable bool) (val interface{}) {
	return GjsonGetUint[uint32](c, key, nullable)
}

func (c *GjsonMetric) GetUint64(key string, nullable bool) (val interface{}) {
	return GjsonGetUint[uint64](c, key, nullable)
}

func (c *GjsonMetric) GetFloat32(key string, nullable bool) (val interface{}) {
	return GjsonGetFloat[float32](c, key, nullable)
}

func (c *GjsonMetric) GetFloat64(key string, nullable bool) (val interface{}) {
	return GjsonGetFloat[float64](c, key, nullable)
}

func GjsonGetInt[T constraints.Signed](c *GjsonMetric, key string, nullable bool) (val interface{}) {
	r := gjson.Get(c.raw, key)
	if !gjCompatibleInt(r) {
		val = getDefaultInt[T](nullable)
		return
	}
	switch r.Type {
	case gjson.True:
		val = T(1)
	case gjson.False:
		val = T(0)
	case gjson.Number:
		if v := r.Int(); float64(v) != r.Num {
			val = getDefaultInt[T](nullable)
		} else {
			val = T(v)
		}
	default:
		val = getDefaultInt[T](nullable)
	}
	return
}

func GjsonGetUint[T constraints.Unsigned](c *GjsonMetric, key string, nullable bool) (val interface{}) {
	r := gjson.Get(c.raw, key)
	if !gjCompatibleInt(r) {
		val = getDefaultInt[T](nullable)
		return
	}
	switch r.Type {
	case gjson.True:
		val = T(1)
	case gjson.False:
		val = T(0)
	case gjson.Number:
		if v := r.Uint(); float64(v) != r.Num {
			val = getDefaultInt[T](nullable)
		} else {
			val = T(v)
		}
	default:
		val = getDefaultInt[T](nullable)
	}
	return
}

func GjsonGetFloat[T constraints.Float](c *GjsonMetric, key string, nullable bool) (val interface{}) {
	r := gjson.Get(c.raw, key)
	if !gjCompatibleFloat(r) {
		val = getDefaultFloat[T](nullable)
		return
	}
	switch r.Type {
	case gjson.Number:
		val = T(r.Num)
	default:
		val = getDefaultFloat[T](nullable)
	}
	return
}

func (c *GjsonMetric) GetDateTime(key string, nullable bool) (val interface{}) {
	r := gjson.Get(c.raw, key)
	if !gjCompatibleDateTime(r) {
		val = getDefaultDateTime(nullable)
		return
	}
	switch r.Type {
	case gjson.Number:
		val = UnixFloat(r.Num, c.pp.timeUnit)
	case gjson.String:
		var err error
		if val, err = c.pp.ParseDateTime(key, r.Str); err != nil {
			val = getDefaultDateTime(nullable)
		}
	default:
		val = getDefaultDateTime(nullable)
	}
	return
}

func (c *GjsonMetric) GetArray(key string, typ int) (val interface{}) {
	var array []gjson.Result
	r := gjson.Get(c.raw, key)
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
		val = GjsonIntArray[int8](array)
	case model.Int16:
		val = GjsonIntArray[int16](array)
	case model.Int32:
		val = GjsonIntArray[int32](array)
	case model.Int64:
		val = GjsonIntArray[int64](array)
	case model.Uint8:
		val = GjsonUintArray[uint8](array)
	case model.Uint16:
		val = GjsonUintArray[uint16](array)
	case model.Uint32:
		val = GjsonUintArray[uint32](array)
	case model.Uint64:
		val = GjsonUintArray[uint64](array)
	case model.Float32:
		val = GjsonFloatArray[float32](array)
	case model.Float64:
		val = GjsonFloatArray[float64](array)
	case model.Decimal:
		results := make([]decimal.Decimal, 0, len(array))
		for _, e := range array {
			var f float64
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
		for _, e := range array {
			var s string
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
	case model.DateTime:
		results := make([]time.Time, 0, len(array))
		for _, e := range array {
			var t time.Time
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

func GjsonIntArray[T constraints.Signed](a []gjson.Result) (arr []T) {
	arr = make([]T, 0, len(a))
	for _, e := range a {
		var v T
		switch e.Type {
		case gjson.True:
			v = T(1)
		case gjson.Number:
			var tmpv int64
			if tmpv = e.Int(); float64(tmpv) != e.Num {
				v = T(0)
			} else {
				v = T(tmpv)
			}
		default:
			v = T(0)
		}
		arr = append(arr, v)
	}
	return
}

func GjsonUintArray[T constraints.Unsigned](a []gjson.Result) (arr []T) {
	arr = make([]T, 0, len(a))
	for _, e := range a {
		var v T
		switch e.Type {
		case gjson.True:
			v = T(1)
		case gjson.Number:
			var tmpv uint64
			if tmpv = e.Uint(); float64(tmpv) != e.Num {
				v = T(0)
			} else {
				v = T(tmpv)
			}
		default:
			v = T(0)
		}
		arr = append(arr, v)
	}
	return
}

func GjsonFloatArray[T constraints.Float](a []gjson.Result) (arr []T) {
	arr = make([]T, 0, len(a))
	for _, e := range a {
		var v T
		switch e.Type {
		case gjson.Number:
			v = T(e.Num)
		default:
			v = T(0.0)
		}
		arr = append(arr, v)
	}
	return
}

func (c *GjsonMetric) GetNewKeys(knownKeys, newKeys *sync.Map, white, black *regexp.Regexp) (foundNew bool) {
	gjson.Parse(c.raw).ForEach(func(k, v gjson.Result) bool {
		strKey := k.Str
		if _, loaded := knownKeys.LoadOrStore(strKey, nil); !loaded {
			if (white == nil || white.MatchString(strKey)) &&
				(black == nil || !black.MatchString(strKey)) {
				if typ, array := gjDetectType(v, 0); typ != model.Unknown && !array {
					newKeys.Store(strKey, typ)
					foundNew = true
				} else {
					util.Logger.Warn("GjsonMetric.GetNewKeys failed to detect field type", zap.String("key", strKey), zap.String("value", v.String()))
				}
			} else {
				util.Logger.Warn("GjsonMetric.GetNewKeys ignored new key due to white/black list setting", zap.String("key", strKey), zap.String("value", v.String()))
				knownKeys.Store(strKey, nil)
			}
		}
		return true
	})
	return
}

func gjCompatibleBool(r gjson.Result) (ok bool) {
	if !r.Exists() {
		return
	}
	switch r.Type {
	case gjson.True, gjson.False:
		ok = true
	default:
	}
	return
}

func gjCompatibleInt(r gjson.Result) (ok bool) {
	if !r.Exists() {
		return
	}
	switch r.Type {
	case gjson.True, gjson.False, gjson.Number:
		ok = true
	default:
	}
	return
}

func gjCompatibleFloat(r gjson.Result) (ok bool) {
	if !r.Exists() {
		return
	}
	switch r.Type {
	case gjson.Number:
		ok = true
	default:
	}
	return
}

func gjCompatibleDateTime(r gjson.Result) (ok bool) {
	if !r.Exists() {
		return
	}
	switch r.Type {
	case gjson.Number, gjson.String:
		ok = true
	default:
	}
	return
}

func gjDetectType(v gjson.Result, depth int) (typ int, array bool) {
	typ = model.Unknown
	if depth > 1 {
		return
	}
	switch v.Type {
	case gjson.True, gjson.False:
		typ = model.Bool
	case gjson.Number:
		if _, err := strconv.ParseInt(v.Raw, 10, 64); err == nil {
			typ = model.Int64
		} else {
			typ = model.Float64
		}
	case gjson.String:
		typ = model.String
		if _, layout := parseInLocation(v.Str, time.Local); layout != "" {
			typ = model.DateTime
		}
	case gjson.JSON:
		if v.IsArray() {
			if depth >= 1 {
				return
			}
			array = true
			if array := v.Array(); len(array) != 0 {
				typ, _ = gjDetectType(array[0], depth+1)
			}
		}
	default:
	}
	return
}
