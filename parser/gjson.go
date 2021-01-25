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

	"github.com/tidwall/gjson"

	"github.com/housepower/clickhouse_sinker/model"
)

var _ Parser = (*GjsonParser)(nil)

type GjsonParser struct {
}

func (p *GjsonParser) Parse(bs []byte) (metric model.Metric, err error) {
	metric = &GjsonMetric{string(bs)}
	return
}

type GjsonMetric struct {
	raw string
}

func (c *GjsonMetric) Get(key string) interface{} {
	return gjson.Get(c.raw, key).Value()
}

func (c *GjsonMetric) GetString(key string, nullable bool) interface{} {
	r := gjson.Get(c.raw, key)
	if nullable && !r.Exists() {
		return nil
	}

	return r.String()
}

func (c *GjsonMetric) GetArray(key string, t string) interface{} {
	slice := gjson.Get(c.raw, key).Array()
	switch t {
	case "string":
		results := make([]string, 0, len(slice))
		for _, s := range slice {
			results = append(results, s.String())
		}
		return results

	case "float":
		results := make([]float64, 0, len(slice))

		for _, s := range slice {
			results = append(results, s.Float())
		}
		return results

	case "int":
		results := make([]int64, 0, len(slice))
		for _, s := range slice {
			results = append(results, s.Int())
		}
		return results

	default:
		panic("not supported array type " + t)
	}
}

func (c *GjsonMetric) GetFloat(key string, nullable bool) interface{} {
	r := gjson.Get(c.raw, key)
	if nullable && !r.Exists() {
		return nil
	}
	return r.Float()
}

func (c *GjsonMetric) GetInt(key string, nullable bool) interface{} {
	r := gjson.Get(c.raw, key)
	if nullable && !r.Exists() {
		return nil
	}
	return r.Int()
}

func (c *GjsonMetric) GetDate(key string, nullable bool) interface{} {
	r := gjson.Get(c.raw, key)
	if nullable && !r.Exists() {
		return nil
	}

	val := r.String()
	t, _ := time.ParseInLocation(TSLayout[0], val, TimeZone)
	return t
}

func (c *GjsonMetric) GetDateTime(key string, nullable bool) interface{} {
	r := gjson.Get(c.raw, key)
	if nullable && !r.Exists() {
		return nil
	}

	if v := r.Float(); v != 0 {
		return time.Unix(int64(v), int64(v*1e9)%1e9)
	}

	val := r.String()
	t, _ := time.ParseInLocation(TSLayout[1], val, TimeZone)
	return t
}

func (c *GjsonMetric) GetDateTime64(key string, nullable bool) interface{} {
	r := gjson.Get(c.raw, key)
	if nullable && !r.Exists() {
		return nil
	}

	if v := r.Float(); v != 0 {
		return time.Unix(int64(v), int64(v*1e9)%1e9)
	}

	val := r.String()
	t, _ := time.ParseInLocation(TSLayout[2], val, TimeZone)
	return t
}

func (c *GjsonMetric) GetElasticDateTime(key string, nullable bool) interface{} {
	r := gjson.Get(c.raw, key)
	if nullable && !r.Exists() {
		return nil
	}

	t, _ := time.Parse(time.RFC3339, r.String())
	return t.Unix()
}

func (c *GjsonMetric) GetNewKeys(knownKeys *sync.Map, newKeys *sync.Map) bool {
	return false
}
