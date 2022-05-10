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
	"sync"
	"time"

	"github.com/shopspring/decimal"
	"github.com/tidwall/gjson"
	"go.uber.org/zap"

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

func (c *GjsonMetric) GetFloat(key string, nullable bool) (val interface{}) {
	r := gjson.Get(c.raw, key)
	if !gjCompatibleFloat(r) {
		val = getDefaultFloat(nullable)
		return
	}
	switch r.Type {
	case gjson.Number:
		val = r.Num
	default:
		val = getDefaultFloat(nullable)
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

func (c *GjsonMetric) GetInt(key string, nullable bool) (val interface{}) {
	r := gjson.Get(c.raw, key)
	if !gjCompatibleInt(r) {
		val = getDefaultInt(nullable)
		return
	}
	switch r.Type {
	case gjson.True:
		val = int64(1)
	case gjson.False:
		val = int64(0)
	case gjson.Number:
		if v := r.Int(); float64(v) != r.Num {
			val = getDefaultInt(nullable)
		} else {
			val = v
		}
	default:
		val = getDefaultInt(nullable)
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

func (c *GjsonMetric) GetElasticDateTime(key string, nullable bool) (val interface{}) {
	t := c.GetDateTime(key, nullable)
	if t != nil {
		val = t.(time.Time).Unix()
	}
	return
}

func (c *GjsonMetric) GetArray(key string, typ int) (val interface{}) {
	r := gjson.Get(c.raw, key)
	if !r.IsArray() {
		val = makeArray(typ)
		return
	}
	array := r.Array()
	switch typ {
	case model.Bool:
		results := make([]bool, 0, len(array))
		for _, e := range array {
			v := (e.Exists() && e.Type == gjson.True)
			results = append(results, v)
		}
		val = results
	case model.Int:
		results := make([]int64, 0, len(array))
		for _, e := range array {
			var v int64
			switch e.Type {
			case gjson.True:
				v = int64(1)
			case gjson.Number:
				if v = e.Int(); float64(v) != e.Num {
					v = int64(0)
				}
			default:
				v = int64(0)
			}
			results = append(results, v)
		}
		val = results
	case model.Float:
		results := make([]float64, 0, len(array))
		for _, e := range array {
			var f float64
			switch e.Type {
			case gjson.Number:
				f = e.Num
			default:
				f = float64(0.0)
			}
			results = append(results, f)
		}
		val = results
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

func (c *GjsonMetric) GetNewKeys(knownKeys, newKeys *sync.Map, white, black *regexp.Regexp) (foundNew bool) {
	gjson.Parse(c.raw).ForEach(func(k, v gjson.Result) bool {
		strKey := k.Str
		if _, loaded := knownKeys.LoadOrStore(strKey, nil); !loaded {
			if (white == nil || white.MatchString(strKey)) &&
				(black == nil || !black.MatchString(strKey)) {
				if typ := gjDetectType(v); typ != model.Unknown {
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

func gjDetectType(v gjson.Result) (typ int) {
	typ = model.Unknown
	switch v.Type {
	case gjson.True, gjson.False:
		typ = model.Bool
	case gjson.Number:
		typ = model.Float
		if float64(v.Int()) == v.Num {
			typ = model.Int
		}
	case gjson.String:
		typ = model.String
		if _, layout := parseInLocation(v.Str, time.Local); layout != "" {
			typ = model.DateTime
		}
	case gjson.JSON:
		if v.IsObject() {
			typ = model.String
		} else if v.IsArray() {
			if array := v.Array(); len(array) != 0 {
				switch array[0].Type {
				case gjson.True, gjson.False:
					typ = model.BoolArray
				case gjson.Number:
					typ = model.FloatArray
					if float64(array[0].Int()) == array[0].Num {
						typ = model.IntArray
					}
				case gjson.String:
					typ = model.StringArray
					if _, layout := parseInLocation(array[0].Str, time.Local); layout != "" {
						typ = model.DateTimeArray
					}
				case gjson.JSON:
					typ = model.StringArray
				default:
				}
			}
		}
	default:
	}
	return
}
