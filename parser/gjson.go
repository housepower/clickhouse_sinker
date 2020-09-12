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
	"github.com/tidwall/gjson"
)

type GjsonParser struct {
}

func (c *GjsonParser) Parse(bs []byte) model.Metric {
	jsonMetric := &GjsonMetric{}
	jsonMetric.init(string(bs))

	return jsonMetric
}

type GjsonMetric struct {
	raw     string
	objPool sync.Pool
}

func (c *GjsonMetric) Get(key string) interface{} {
	return c.getObj(key).Value()
}

func (c *GjsonMetric) GetString(key string) string {
	return c.getObj(key).String()
}

func (c *GjsonMetric) GetArray(key string, t string) interface{} {
	slice := c.getObj(key).Array()
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

func (c *GjsonMetric) GetFloat(key string) float64 {
	return c.getObj(key).Float()
}

func (c *GjsonMetric) GetInt(key string) int64 {
	return c.getObj(key).Int()
}

func (c *GjsonMetric) GetElasticDateTime(key string) int64 {
	val := c.GetString(key)
	t, _ := time.Parse(time.RFC3339, val)
	return t.Unix()
}

func (c *GjsonMetric) getObj(key string) gjson.Result {
	mapObj := c.objPool.Get().(map[string]gjson.Result)
	c.objPool.Put(mapObj)
	if val, ok := mapObj[key]; ok {
		return val
	}
	return gjson.Result{}
}

func (c *GjsonMetric) init(raw string) {
	c.raw = raw
	c.objPool = sync.Pool{
		New: func() interface{} {
			mapObj := gjson.Parse(c.raw).Map()
			//println(MD5(c.raw), "  :  ", time.Now().UnixNano())
			return mapObj
		},
	}
}
