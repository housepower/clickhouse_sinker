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

	"github.com/tidwall/gjson"

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
	if !r.Exists() || r.Type == gjson.Null {
		if nullable {
			return
		}
		val = float64(0.0)
		return
	}
	switch r.Type {
	case gjson.Number:
		val = r.Num
	default:
		val = float64(0.0)
	}
	return
}

func (c *GjsonMetric) GetInt(key string, nullable bool) (val interface{}) {
	r := gjson.Get(c.raw, key)
	if !r.Exists() || r.Type == gjson.Null {
		if nullable {
			return
		}
		val = int64(0)
		return
	}
	switch r.Type {
	case gjson.True:
		val = int64(1)
	case gjson.Number:
		if v := r.Int(); float64(v) != r.Num {
			val = int64(0)
		} else {
			val = v
		}
	default:
		val = int64(0)
	}
	return
}

func (c *GjsonMetric) GetDateTime(key string, nullable bool) (val interface{}) {
	r := gjson.Get(c.raw, key)
	if !r.Exists() || r.Type == gjson.Null {
		if nullable {
			return
		}
		val = Epoch
		return
	}
	switch r.Type {
	case gjson.Number:
		val = UnixFloat(r.Num)
	case gjson.String:
		val = c.pp.ParseDateTime(key, r.Str)
	default:
		val = Epoch
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
	if !r.Exists() || r.Type != gjson.JSON {
		val = makeArray(typ)
		return
	}
	array := r.Array()
	switch typ {
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
				t = UnixFloat(e.Num)
			case gjson.String:
				t = c.pp.ParseDateTime(key, e.Str)
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

func (c *GjsonMetric) GetNewKeys(knownKeys *sync.Map, newKeys *sync.Map) bool {
	return false
}
