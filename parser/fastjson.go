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
	"sync"
	"time"

	"github.com/housepower/clickhouse_sinker/model"
	"github.com/housepower/clickhouse_sinker/util"
	"github.com/pkg/errors"
	"github.com/valyala/fastjson"
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

func (c *FastjsonMetric) Get(key string) interface{} {
	return c.value.Get(key)
}

func (c *FastjsonMetric) GetString(key string, nullable bool) interface{} {
	v := c.value.GetStringBytes(key)
	if nullable && v == nil {
		return nil
	}
	return string(v)
}

func (c *FastjsonMetric) GetFloat(key string, nullable bool) interface{} {
	if nullable && !c.value.Exists(key) {
		return nil
	}
	return c.value.GetFloat64(key)
}

func (c *FastjsonMetric) GetInt(key string, nullable bool) interface{} {
	v := c.value.Get(key)
	if v == nil {
		if nullable {
			return nil
		}
		return int64(0)
	}
	if v.Type() == fastjson.TypeTrue {
		return int64(1)
	}
	return c.value.GetInt64(key)
}

func (c *FastjsonMetric) GetArray(key string, t string) interface{} {
	array := c.value.GetArray(key)
	if array == nil {
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
	switch t {
	case "int":
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
		return results
	case "float":
		results := make([]float64, 0, len(array))
		for _, e := range array {
			v, _ := e.Float64()
			results = append(results, v)
		}
		return results
	case "string":
		results := make([]string, 0, len(array))
		for _, e := range array {
			v, _ := e.StringBytes()
			results = append(results, string(v))
		}
		return results
	default:
		panic("not supported array type " + t)
	}
}

func (c *FastjsonMetric) String() string {
	return c.value.String()
}

func (c *FastjsonMetric) GetDate(key string, nullable bool) interface{} {
	if nullable && !c.value.Exists(key) {
		return nil
	}

	val, _ := c.GetString(key, false).(string)
	t, _ := c.pp.ParseDateTime(key, val)
	return t
}

func (c *FastjsonMetric) GetDateTime(key string, nullable bool) interface{} {
	if nullable && !c.value.Exists(key) {
		return nil
	}

	if v, _ := c.GetFloat(key, false).(float64); v != 0 {
		return time.Unix(int64(v), int64(v*1e9)%1e9)
	}

	val, _ := c.GetString(key, false).(string)
	t, _ := c.pp.ParseDateTime(key, val)
	return t
}

func (c *FastjsonMetric) GetDateTime64(key string, nullable bool) interface{} {
	if nullable && !c.value.Exists(key) {
		return nil
	}

	if v, _ := c.GetFloat(key, false).(float64); v != 0 {
		return time.Unix(int64(v), int64(v*1e9)%1e9)
	}

	val, _ := c.GetString(key, false).(string)
	t, _ := c.pp.ParseDateTime(key, val)
	return t
}

func (c *FastjsonMetric) GetElasticDateTime(key string, nullable bool) interface{} {
	val := c.GetString(key, nullable)
	if val == nil {
		return nil
	}
	t, _ := time.Parse(time.RFC3339, val.(string))

	return t.Unix()
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
			if _, err = v.Int64(); err == nil {
				newKeys.Store(strKey, "int")
				foundNew = true
			} else if _, err = v.Float64(); err == nil {
				newKeys.Store(strKey, "float")
				foundNew = true
			} else if _, err = v.StringBytes(); err == nil {
				newKeys.Store(strKey, "string")
				foundNew = true
			} else {
				util.Logger.Warnf("FastjsonMetric.GetNewKeys found a kv not be int/float/string, key: %s, value: %s", strKey, v.String())
			}
		}
	})
	return
}
