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
	"math"
	"regexp"
	"strconv"
	"sync"
	"time"

	"golang.org/x/exp/constraints"

	"github.com/housepower/clickhouse_sinker/model"
	"github.com/housepower/clickhouse_sinker/util"
	"github.com/shopspring/decimal"
	"github.com/thanos-io/thanos/pkg/errors"
	"github.com/valyala/fastjson"
	"go.uber.org/zap"
)

var _ Parser = (*FastjsonParser)(nil)
var EmpytObject = make(map[string]interface{})

// FastjsonParser, parser for get data in json format
type FastjsonParser struct {
	pp  *Pool
	fjp fastjson.Parser
}

func (p *FastjsonParser) Parse(bs []byte) (metric model.Metric, err error) {
	var value *fastjson.Value
	if value, err = p.fjp.ParseBytes(bs); err != nil {
		err = errors.Wrapf(err, "")
		return
	}
	metric = &FastjsonMetric{pp: p.pp, value: value}
	return
}

type FastjsonMetric struct {
	pp    *Pool
	value *fastjson.Value
}

func (c *FastjsonMetric) GetString(key string, nullable bool) (val interface{}) {
	return getString(c.value.Get(key), nullable)
}

func (c *FastjsonMetric) GetBool(key string, nullable bool) interface{} {
	return getBool(c.value.Get(key), nullable)
}

func (c *FastjsonMetric) GetDecimal(key string, nullable bool) (val interface{}) {
	return getDecimal(c.value.Get(key), nullable)
}

func (c *FastjsonMetric) GetInt8(key string, nullable bool) (val interface{}) {
	return FastjsonGetInt[int8](c.value.Get(key), nullable, math.MinInt8, math.MaxInt8)
}

func (c *FastjsonMetric) GetInt16(key string, nullable bool) (val interface{}) {
	return FastjsonGetInt[int16](c.value.Get(key), nullable, math.MinInt16, math.MaxInt16)
}

func (c *FastjsonMetric) GetInt32(key string, nullable bool) (val interface{}) {
	return FastjsonGetInt[int32](c.value.Get(key), nullable, math.MinInt32, math.MaxInt32)
}

func (c *FastjsonMetric) GetInt64(key string, nullable bool) (val interface{}) {
	// 先按String 类型读取字符串
	strValue := c.value.Get(key).GetStringBytes()
	if strValue == nil {
		if nullable {
			return nil
		}
		return int64(0) // 默认值
	}
	// 将字符串转换为 int64
	intVal, err := strconv.ParseInt(string(strValue), 10, 64)
	if err != nil {
		if nullable {
			return nil
		}
		return int64(0) // 默认值
	}
	return intVal

	//return FastjsonGetInt[int64](c.value.Get(key), nullable, math.MinInt64, math.MaxInt64)
}

func (c *FastjsonMetric) GetUint8(key string, nullable bool) (val interface{}) {
	return FastjsonGetUint[uint8](c.value.Get(key), nullable, math.MaxUint8)
}

func (c *FastjsonMetric) GetUint16(key string, nullable bool) (val interface{}) {
	return FastjsonGetUint[uint16](c.value.Get(key), nullable, math.MaxUint16)
}

func (c *FastjsonMetric) GetUint32(key string, nullable bool) (val interface{}) {
	return FastjsonGetUint[uint32](c.value.Get(key), nullable, math.MaxUint32)
}

func (c *FastjsonMetric) GetUint64(key string, nullable bool) (val interface{}) {
	return FastjsonGetUint[uint64](c.value.Get(key), nullable, math.MaxUint64)
}

func (c *FastjsonMetric) GetFloat32(key string, nullable bool) (val interface{}) {
	return FastjsonGetFloat[float32](c.value.Get(key), nullable, math.MaxFloat32)
}

func (c *FastjsonMetric) GetFloat64(key string, nullable bool) (val interface{}) {
	// 先按String 类型读取字符串
	strValue := c.value.Get(key).GetStringBytes()
	if strValue == nil {
		if nullable {
			return nil
		}
		return 0.0 // 默认值
	}
	// 将字符串转换为 float64
	floatVal, err := strconv.ParseFloat(string(strValue), 64)
	if err != nil {
		if nullable {
			return nil
		}
		return 0.0 // 默认值
	}
	return floatVal
	//return FastjsonGetFloat[float64](c.value.Get(key), nullable, math.MaxFloat64)
}

func FastjsonGetInt[T constraints.Signed](v *fastjson.Value, nullable bool, min, max int64) (val interface{}) {
	if !fjCompatibleInt(v) {
		val = getDefaultInt[T](nullable)
		return
	}
	switch v.Type() {
	case fastjson.TypeTrue:
		val = T(1)
	case fastjson.TypeFalse:
		val = T(0)
	default:
		if val2, err := v.Int64(); err != nil {
			val = getDefaultInt[T](nullable)
		} else if val2 < min {
			val = T(min)
		} else if val2 > max {
			val = T(max)
		} else {
			val = T(val2)
		}
	}
	return
}

func FastjsonGetUint[T constraints.Unsigned](v *fastjson.Value, nullable bool, max uint64) (val interface{}) {
	if !fjCompatibleInt(v) {
		val = getDefaultInt[T](nullable)
		return
	}
	switch v.Type() {
	case fastjson.TypeTrue:
		val = T(1)
	case fastjson.TypeFalse:
		val = T(0)
	default:
		if val2, err := v.Uint64(); err != nil {
			val = getDefaultInt[T](nullable)
		} else if val2 > max {
			val = T(max)
		} else {
			val = T(val2)
		}
	}
	return
}

func FastjsonGetFloat[T constraints.Float](v *fastjson.Value, nullable bool, max float64) (val interface{}) {
	if !fjCompatibleFloat(v) {
		val = getDefaultFloat[T](nullable)
		return
	}
	if val2, err := v.Float64(); err != nil {
		val = getDefaultFloat[T](nullable)
	} else if val2 > max {
		val = T(max)
	} else {
		val = T(val2)
	}
	return
}

func (c *FastjsonMetric) GetDateTime(key string, nullable bool) (val interface{}) {
	return getDateTime(c, key, c.value.Get(key), nullable)
}

func (c *FastjsonMetric) GetObject(key string, nullable bool) (val interface{}) {
	v := c.value.Get(key)
	val = val2map(v)
	return
}

func (c *FastjsonMetric) GetArray(key string, typ int) (val interface{}) {
	return getArray(c, key, c.value.Get(key), typ)
}

func (c *FastjsonMetric) GetMap(key string, typeinfo *model.TypeInfo) (val interface{}) {
	return getMap(c, c.value.Get(key), typeinfo)
}

func (c *FastjsonMetric) val2OrderedMap(v *fastjson.Value, typeinfo *model.TypeInfo) (m *model.OrderedMap) {
	var err error
	var obj *fastjson.Object
	m = model.NewOrderedMap()
	if v == nil {
		return
	}
	if obj, err = v.Object(); err != nil {
		return
	}
	obj.Visit(func(key []byte, v *fastjson.Value) {
		rawKey := c.castMapKeyByType(key, typeinfo.MapKey)
		m.Put(rawKey, c.castMapValueByType(string(key), v, typeinfo.MapValue))
	})
	return
}

func val2map(v *fastjson.Value) (m map[string]interface{}) {
	var err error
	var obj *fastjson.Object
	m = EmpytObject
	if v == nil {
		return
	}
	if obj, err = v.Object(); err != nil {
		return
	}
	m = make(map[string]interface{}, obj.Len())
	obj.Visit(func(key []byte, v *fastjson.Value) {
		strKey := string(key)
		switch v.Type() {
		case fastjson.TypeString:
			var vb []byte
			if vb, err = v.StringBytes(); err != nil {
				return
			}
			m[strKey] = string(vb)
		case fastjson.TypeNumber:
			var f float64
			if f, err = v.Float64(); err != nil {
				return
			}
			m[strKey] = f
		}
	})
	return
}

func getString(v *fastjson.Value, nullable bool) (val interface{}) {
	if v == nil || v.Type() == fastjson.TypeNull {
		if nullable {
			return
		}
		val = ""
		return
	}
	switch v.Type() {
	case fastjson.TypeString:
		b, _ := v.StringBytes()
		val = string(b)
	default:
		val = v.String()
	}
	return
}

func getBool(v *fastjson.Value, nullable bool) (val interface{}) {
	if !fjCompatibleBool(v) {
		val = getDefaultBool(nullable)
		return
	}
	val = (v.Type() == fastjson.TypeTrue)
	return
}

func getDecimal(v *fastjson.Value, nullable bool) (val interface{}) {
	if !fjCompatibleFloat(v) {
		val = getDefaultDecimal(nullable)
		return
	}
	if val2, err := v.Float64(); err != nil {
		val = getDefaultDecimal(nullable)
	} else {
		val = decimal.NewFromFloat(val2)
	}
	return
}

func getDateTime(c *FastjsonMetric, sourcename string, v *fastjson.Value, nullable bool) (val interface{}) {
	if !fjCompatibleDateTime(v) {
		val = getDefaultDateTime(nullable)
		return
	}
	var err error
	switch v.Type() {
	case fastjson.TypeNumber:
		var f float64
		if f, err = v.Float64(); err != nil {
			val = getDefaultDateTime(nullable)
			return
		}
		val = UnixFloat(f, c.pp.timeUnit)
	case fastjson.TypeString:
		var b []byte
		if b, err = v.StringBytes(); err != nil || len(b) == 0 {
			val = getDefaultDateTime(nullable)
			return
		}
		if val, err = c.pp.ParseDateTime(sourcename, string(b)); err != nil {
			val = getDefaultDateTime(nullable)
		}
	default:
		val = getDefaultDateTime(nullable)
	}
	return
}

func getArray(c *FastjsonMetric, sourcename string, v *fastjson.Value, typ int) (val interface{}) {
	var array []*fastjson.Value
	if v != nil {
		array, _ = v.Array()
	}
	switch typ {
	case model.Bool:
		arr := make([]bool, 0)
		for _, e := range array {
			v := (e != nil && e.Type() == fastjson.TypeTrue)
			arr = append(arr, v)
		}
		val = arr
	case model.Int8:
		val = FastjsonIntArray[int8](array, math.MinInt8, math.MaxInt8)
	case model.Int16:
		val = FastjsonIntArray[int16](array, math.MinInt16, math.MaxInt16)
	case model.Int32:
		val = FastjsonIntArray[int32](array, math.MinInt32, math.MaxInt32)
	case model.Int64:
		val = FastjsonIntArray[int64](array, math.MinInt64, math.MaxInt64)
	case model.UInt8:
		val = FastjsonUintArray[uint8](array, math.MaxUint8)
	case model.UInt16:
		val = FastjsonUintArray[uint16](array, math.MaxUint16)
	case model.UInt32:
		val = FastjsonUintArray[uint32](array, math.MaxUint32)
	case model.UInt64:
		val = FastjsonUintArray[uint64](array, math.MaxUint64)
	case model.Float32:
		val = FastjsonFloatArray[float32](array, math.MaxFloat32)
	case model.Float64:
		val = FastjsonFloatArray[float64](array, math.MaxFloat64)
	case model.Decimal:
		arr := make([]decimal.Decimal, 0)
		for _, e := range array {
			v, _ := e.Float64()
			arr = append(arr, decimal.NewFromFloat(v))
		}
		val = arr
	case model.String:
		arr := make([]string, 0)
		var s string
		for _, e := range array {
			switch e.Type() {
			case fastjson.TypeNull:
				s = ""
			case fastjson.TypeString:
				b, _ := e.StringBytes()
				s = string(b)
			default:
				s = e.String()
			}
			arr = append(arr, s)
		}
		val = arr
	case model.DateTime:
		arr := make([]time.Time, 0)
		var t time.Time
		for _, e := range array {
			switch e.Type() {
			case fastjson.TypeNumber:
				if f, err := e.Float64(); err != nil {
					t = Epoch
				} else {
					t = UnixFloat(f, c.pp.timeUnit)
				}
			case fastjson.TypeString:
				if b, err := e.StringBytes(); err != nil || len(b) == 0 {
					t = Epoch
				} else {
					var err error
					if t, err = c.pp.ParseDateTime(sourcename, string(b)); err != nil {
						t = Epoch
					}
				}
			default:
				t = Epoch
			}
			arr = append(arr, t)
		}
		val = arr
	case model.Object:
		arr := make([]map[string]interface{}, 0)
		for _, e := range array {
			m := val2map(e)
			if m != nil {
				arr = append(arr, m)
			}
		}
		val = arr
	default:
		util.Logger.Fatal(fmt.Sprintf("LOGIC ERROR: unsupported array type %v", typ))
	}
	return
}

func getMap(c *FastjsonMetric, v *fastjson.Value, typeinfo *model.TypeInfo) (val interface{}) {
	if v != nil && v.Type() == fastjson.TypeObject {
		val = c.val2OrderedMap(v, typeinfo)
	}
	return
}

func (c *FastjsonMetric) castMapKeyByType(key []byte, typeinfo *model.TypeInfo) (val interface{}) {
	switch typeinfo.Type {
	case model.Int8:
		if res, err := strconv.ParseInt(string(key), 10, 8); err == nil {
			return int8(res)
		} else {
			util.Logger.Error("failed to parse map key", zap.Error(err))
		}
	case model.Int16:
		if res, err := strconv.ParseInt(string(key), 10, 16); err == nil {
			return int16(res)
		} else {
			util.Logger.Error("failed to parse map key", zap.Error(err))
		}
	case model.Int32:
		if res, err := strconv.ParseInt(string(key), 10, 32); err == nil {
			return int32(res)
		} else {
			util.Logger.Error("failed to parse map key", zap.Error(err))
		}
	case model.Int64:
		if res, err := strconv.ParseInt(string(key), 10, 64); err == nil {
			return int64(res)
		} else {
			util.Logger.Error("failed to parse map key", zap.Error(err))
		}
	case model.UInt8:
		if res, err := strconv.ParseUint(string(key), 10, 8); err == nil {
			return uint8(res)
		} else {
			util.Logger.Error("failed to parse map key", zap.Error(err))
		}
	case model.UInt16:
		if res, err := strconv.ParseUint(string(key), 10, 16); err == nil {
			return uint16(res)
		} else {
			util.Logger.Error("failed to parse map key", zap.Error(err))
		}
	case model.UInt32:
		if res, err := strconv.ParseUint(string(key), 10, 32); err == nil {
			return uint32(res)
		} else {
			util.Logger.Error("failed to parse map key", zap.Error(err))
		}
	case model.UInt64:
		if res, err := strconv.ParseUint(string(key), 10, 64); err == nil {
			return uint64(res)
		} else {
			util.Logger.Error("failed to parse map key", zap.Error(err))
		}
	case model.DateTime:
		if res, err := c.pp.ParseDateTime(string(key), string(key)); err == nil {
			return res
		} else {
			util.Logger.Error("failed to parse map key", zap.Error(err))
		}
		val = getDefaultDateTime(typeinfo.Nullable)
	case model.String:
		return string(key)
	default:
		util.Logger.Fatal("LOGIC ERROR: reached switch default condition")
	}

	return
}

func (c *FastjsonMetric) castMapValueByType(sourcename string, value *fastjson.Value, typeinfo *model.TypeInfo) (val interface{}) {
	if typeinfo.Array {
		val = getArray(c, sourcename, value, typeinfo.Type)
		return
	} else {
		switch typeinfo.Type {
		case model.Bool:
			val = getBool(value, typeinfo.Nullable)
		case model.Int8:
			val = FastjsonGetInt[int8](value, typeinfo.Nullable, math.MinInt8, math.MaxInt8)
		case model.Int16:
			val = FastjsonGetInt[int16](value, typeinfo.Nullable, math.MinInt16, math.MaxInt16)
		case model.Int32:
			val = FastjsonGetInt[int32](value, typeinfo.Nullable, math.MinInt32, math.MaxInt32)
		case model.Int64:
			val = FastjsonGetInt[int64](value, typeinfo.Nullable, math.MinInt64, math.MaxInt64)
		case model.UInt8:
			val = FastjsonGetUint[uint8](value, typeinfo.Nullable, math.MaxUint8)
		case model.UInt16:
			val = FastjsonGetUint[uint16](value, typeinfo.Nullable, math.MaxUint16)
		case model.UInt32:
			val = FastjsonGetUint[uint32](value, typeinfo.Nullable, math.MaxUint32)
		case model.UInt64:
			val = FastjsonGetUint[uint64](value, typeinfo.Nullable, math.MaxUint64)
		case model.Float32:
			val = FastjsonGetFloat[float32](value, typeinfo.Nullable, math.MaxFloat32)
		case model.Float64:
			val = FastjsonGetFloat[float64](value, typeinfo.Nullable, math.MaxFloat64)
		case model.Decimal:
			val = getDecimal(value, typeinfo.Nullable)
		case model.DateTime:
			val = getDateTime(c, sourcename, value, typeinfo.Nullable)
		case model.String:
			val = getString(value, typeinfo.Nullable)
		case model.Map:
			val = getMap(c, value, typeinfo)
		case model.Object:
			val = val2map(value)
		default:
			util.Logger.Fatal("LOGIC ERROR: reached switch default condition")
		}
	}
	return
}

func FastjsonIntArray[T constraints.Signed](a []*fastjson.Value, min, max int64) (arr []T) {
	arr = make([]T, 0)
	var val T
	for _, e := range a {
		if e.Type() == fastjson.TypeTrue {
			val = T(1)
		} else {
			val2, _ := e.Int64()
			if val2 < min {
				val = T(min)
			} else if val2 > max {
				val = T(max)
			} else {
				val = T(val2)
			}
		}
		arr = append(arr, val)
	}
	return
}

func FastjsonUintArray[T constraints.Unsigned](a []*fastjson.Value, max uint64) (arr []T) {
	arr = make([]T, 0)
	var val T
	for _, e := range a {
		if e.Type() == fastjson.TypeTrue {
			val = T(1)
		} else {
			val2, _ := e.Uint64()
			if val2 > max {
				val = T(max)
			} else {
				val = T(val2)
			}
		}
		arr = append(arr, val)
	}
	return
}

func FastjsonFloatArray[T constraints.Float](a []*fastjson.Value, max float64) (arr []T) {
	arr = make([]T, 0)
	var val T
	for _, e := range a {
		val2, _ := e.Float64()
		if val2 > max {
			val = T(max)
		} else {
			val = T(val2)
		}
		arr = append(arr, val)
	}
	return
}

func (c *FastjsonMetric) GetNewKeys(knownKeys, newKeys, warnKeys *sync.Map, white, black *regexp.Regexp, partition int, offset int64) (foundNew bool) {
	var obj *fastjson.Object
	var err error
	if obj, err = c.value.Object(); err != nil {
		return
	}
	obj.Visit(func(key []byte, v *fastjson.Value) {
		strKey := string(key)
		if _, loaded := knownKeys.LoadOrStore(strKey, nil); !loaded {
			if (white == nil || white.MatchString(strKey)) &&
				(black == nil || !black.MatchString(strKey)) {
				if typ, arr := fjDetectType(v, 0); typ != model.Unknown && typ != model.Object && !arr {
					newKeys.Store(strKey, typ)
					foundNew = true
				} else if _, loaded = warnKeys.LoadOrStore(strKey, nil); !loaded {
					util.Logger.Warn("FastjsonMetric.GetNewKeys ignored new key due to unsupported type of dynamic column", zap.Int("partition", partition), zap.Int64("offset", offset), zap.String("key", strKey), zap.String("value", v.String()))
				}
			} else if _, loaded = warnKeys.LoadOrStore(strKey, nil); !loaded {
				util.Logger.Warn("FastjsonMetric.GetNewKeys ignored new key due to white/black list setting", zap.Int("partition", partition), zap.Int64("offset", offset), zap.String("key", strKey), zap.String("value", v.String()))
				knownKeys.Store(strKey, nil)
			}
		}
	})
	return
}

func fjCompatibleBool(v *fastjson.Value) (ok bool) {
	if v == nil {
		return
	}
	switch v.Type() {
	case fastjson.TypeTrue, fastjson.TypeFalse:
		ok = true
	}
	return
}

func fjCompatibleInt(v *fastjson.Value) (ok bool) {
	if v == nil {
		return
	}
	switch v.Type() {
	case fastjson.TypeTrue, fastjson.TypeFalse, fastjson.TypeNumber:
		ok = true
	}
	return
}

func fjCompatibleFloat(v *fastjson.Value) (ok bool) {
	if v == nil {
		return
	}
	switch v.Type() {
	case fastjson.TypeNumber:
		ok = true
	}
	return
}

func fjCompatibleDateTime(v *fastjson.Value) (ok bool) {
	if v == nil {
		return
	}
	switch v.Type() {
	case fastjson.TypeNumber, fastjson.TypeString:
		ok = true
	}
	return
}

func getDefaultBool(nullable bool) (val interface{}) {
	if nullable {
		return
	}
	val = false
	return
}

func getDefaultInt[T constraints.Integer](nullable bool) (val interface{}) {
	if nullable {
		return
	}
	var zero T
	val = zero
	return
}

func getDefaultFloat[T constraints.Float](nullable bool) (val interface{}) {
	if nullable {
		return
	}
	val = T(0.0)
	return
}

func getDefaultDecimal(nullable bool) (val interface{}) {
	if nullable {
		return
	}
	val = decimal.NewFromInt(0)
	return
}

func getDefaultDateTime(nullable bool) (val interface{}) {
	if nullable {
		return
	}
	val = Epoch
	return
}

func fjDetectType(v *fastjson.Value, depth int) (typ int, array bool) {
	typ = model.Unknown
	if depth > 1 {
		return
	}
	switch v.Type() {
	case fastjson.TypeNull:
		typ = model.Unknown
	case fastjson.TypeTrue, fastjson.TypeFalse:
		typ = model.Bool
	case fastjson.TypeNumber:
		typ = model.Float64
		if _, err := v.Int64(); err == nil {
			typ = model.Int64
		}
	case fastjson.TypeString:
		typ = model.String
		if val, err := v.StringBytes(); err == nil {
			if _, layout := parseInLocation(string(val), time.Local); layout != "" {
				typ = model.DateTime
			}
		}
	case fastjson.TypeObject:
		typ = model.Object
	case fastjson.TypeArray:
		if depth >= 1 {
			return
		}
		array = true
		if arr, err := v.Array(); err == nil && len(arr) > 0 {
			typ, _ = fjDetectType(arr[0], depth+1)
		}
	default:
	}
	return
}
