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
	"sync"
	"time"

	"github.com/housepower/clickhouse_sinker/model"
	"github.com/housepower/clickhouse_sinker/util"
	"github.com/pkg/errors"
	"github.com/valyala/fastjson"
	"go.uber.org/zap"
)

var _ Parser = (*FastjsonParser)(nil)

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
	v := c.value.Get(key)
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

func (c *FastjsonMetric) GetFloat(key string, nullable bool) (val interface{}) {
	v := c.value.Get(key)
	if v == nil || v.Type() == fastjson.TypeNull {
		if nullable {
			return
		}
		val = float64(0.0)
		return
	}
	val, _ = v.Float64()
	return
}

func (c *FastjsonMetric) GetInt(key string, nullable bool) (val interface{}) {
	v := c.value.Get(key)
	if v == nil || v.Type() == fastjson.TypeNull {
		if nullable {
			return
		}
		val = int64(0)
		return
	}
	switch v.Type() {
	case fastjson.TypeTrue:
		val = int64(1)
	case fastjson.TypeFalse:
		val = int64(0)
	default:
		val, _ = v.Int64()
	}
	return
}

func (c *FastjsonMetric) GetDateTime(key string, nullable bool) (val interface{}) {
	v := c.value.Get(key)
	if v == nil || v.Type() == fastjson.TypeNull {
		if nullable {
			return
		}
		val = Epoch
		return
	}
	var err error
	switch v.Type() {
	case fastjson.TypeNumber:
		var f float64
		if f, err = v.Float64(); err != nil {
			val = Epoch
			return
		}
		val = time.Unix(int64(f), int64(f*1e9)%1e9).In(time.UTC)
	case fastjson.TypeString:
		var b []byte
		if b, err = v.StringBytes(); err != nil {
			val = Epoch
			return
		}
		val = c.pp.ParseDateTime(key, string(b))
	default:
		val = Epoch
	}
	return
}

func (c *FastjsonMetric) GetElasticDateTime(key string, nullable bool) (val interface{}) {
	t := c.GetDateTime(key, nullable)
	if t != nil {
		val = t.(time.Time).Unix()
	}
	return
}

func (c *FastjsonMetric) GetArray(key string, typ int) (val interface{}) {
	v := c.value.Get(key)
	if v == nil || v.Type() != fastjson.TypeArray {
		val = makeArray(typ)
		return
	}
	array, _ := v.Array()
	switch typ {
	case model.Int:
		results := make([]int64, 0, len(array))
		for _, e := range array {
			var v int64
			if e.Type() == fastjson.TypeTrue {
				v = 1
			} else {
				v, _ = e.Int64()
			}
			results = append(results, v)
		}
		val = results
	case model.Float:
		results := make([]float64, 0, len(array))
		for _, e := range array {
			v, _ := e.Float64()
			results = append(results, v)
		}
		val = results
	case model.String:
		results := make([]string, 0, len(array))
		for _, e := range array {
			v, _ := e.StringBytes()
			results = append(results, string(v))
		}
		val = results
	case model.DateTime:
		results := make([]time.Time, 0, len(array))
		for _, e := range array {
			v, _ := e.StringBytes()
			t := c.pp.ParseDateTime(key, string(v))
			results = append(results, t)
		}
		val = results
	default:
		util.Logger.Fatal(fmt.Sprintf("LOGIC ERROR: unsupported array type %v", typ))
	}
	return
}

func (c *FastjsonMetric) GetNewKeys(knownKeys *sync.Map, newKeys *sync.Map) (foundNew bool) {
	var obj *fastjson.Object
	var err error
	if obj, err = c.value.Object(); err != nil {
		return
	}
	obj.Visit(func(key []byte, v *fastjson.Value) {
		strKey := string(key)
		if _, loaded := knownKeys.LoadOrStore(strKey, nil); !loaded {
			if typ := fjDetectType(v); typ != model.TypeUnknown {
				newKeys.Store(strKey, typ)
				foundNew = true
			} else {
				util.Logger.Warn("FastjsonMetric.GetNewKeys failed to detect field type", zap.String("key", strKey), zap.String("value", v.String()))
			}
		}
	})
	return
}

func fjDetectType(v *fastjson.Value) (typ int) {
	if vt := v.Type(); vt == fastjson.TypeNull {
	} else if vt == fastjson.TypeTrue || vt == fastjson.TypeFalse {
		typ = model.Int
	} else if _, err := v.Int64(); err == nil {
		typ = model.Int
	} else if _, err := v.Float64(); err == nil {
		typ = model.Float
	} else if val, err := v.StringBytes(); err == nil {
		if _, layout := parseInLocation(string(val), time.Local); layout != "" {
			typ = model.DateTime
		} else {
			typ = model.String
		}
	} else if arr, err := v.Array(); err == nil && len(arr) > 0 {
		typ2 := fjDetectType(arr[0])
		switch typ2 {
		case model.Int:
			typ = model.IntArray
		case model.Float:
			typ = model.FloatArray
		case model.String:
			typ = model.StringArray
		case model.DateTime:
			typ = model.DateTimeArray
		}
	}
	return
}
