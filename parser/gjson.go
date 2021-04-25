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

	"github.com/pkg/errors"
	"github.com/tidwall/gjson"

	"github.com/housepower/clickhouse_sinker/model"
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

func (c *GjsonMetric) GetString(key string, nullable bool) (val interface{}, err error) {
	r := gjson.Get(c.raw, key)
	if !r.Exists() || r.Type == gjson.Null {
		if nullable {
			return
		}
		val = ""
		return
	}
	switch r.Type {
	case gjson.String:
		val = r.Str
	default:
		err = errors.Errorf("value doesn't contain string, it contains %s", r.Type.String())
	}
	return
}

func (c *GjsonMetric) GetFloat(key string, nullable bool) (val interface{}, err error) {
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
		err = errors.Errorf("value doesn't contain number, it contains %s", r.Type.String())
	}
	return
}

func (c *GjsonMetric) GetInt(key string, nullable bool) (val interface{}, err error) {
	r := gjson.Get(c.raw, key)
	if !r.Exists() || r.Type == gjson.Null {
		if nullable {
			return
		}
		val = int64(0)
		return
	}
	switch r.Type {
	case gjson.Number:
		val = int64(r.Num)
	default:
		err = errors.Errorf("value doesn't contain number, it contains %s", r.Type.String())
	}
	return
}

func (c *GjsonMetric) GetDate(key string, nullable bool) (val interface{}, err error) {
	return c.GetDateTime(key, nullable)
}

func (c *GjsonMetric) GetDateTime(key string, nullable bool) (val interface{}, err error) {
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
		val = time.Unix(int64(r.Num), int64(r.Num*1e9)%1e9)
	case gjson.String:
		val, err = c.pp.ParseDateTime(key, r.Str)
	default:
		err = errors.Errorf("value doesn't contain string, it contains %s", r.Type.String())
	}
	return
}

func (c *GjsonMetric) GetDateTime64(key string, nullable bool) (val interface{}, err error) {
	return c.GetDateTime(key, nullable)
}

func (c *GjsonMetric) GetElasticDateTime(key string, nullable bool) (val interface{}, err error) {
	var t interface{}
	if t, err = c.GetDateTime(key, nullable); err != nil {
		return
	}
	if t != nil {
		val = t.(time.Time).Unix()
	}
	return
}

func (c *GjsonMetric) GetArray(key string, t string) (val interface{}, err error) {
	r := gjson.Get(c.raw, key)
	if !r.Exists() || r.Type == gjson.Null {
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
		return
	}
	if r.Type != gjson.JSON {
		err = errors.Errorf("value doesn't contain json, it contains %s", r.Type.String())
		return
	}
	array := r.Array()
	switch t {
	case "string":
		results := make([]string, 0, len(array))
		for _, s := range array {
			results = append(results, s.String())
		}
		val = results
	case "float":
		results := make([]float64, 0, len(array))

		for _, s := range array {
			results = append(results, s.Float())
		}
		val = results
	case "int":
		results := make([]int64, 0, len(array))
		for _, s := range array {
			results = append(results, s.Int())
		}
		val = results
	default:
		panic("LOGIC ERROR: not supported array type " + t)
	}
	return
}

func (c *GjsonMetric) GetNewKeys(knownKeys *sync.Map, newKeys *sync.Map) bool {
	return false
}
