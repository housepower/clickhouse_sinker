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
	"time"

	"github.com/housepower/clickhouse_sinker/model"
	"github.com/pkg/errors"
	"github.com/valyala/fastjson"
)

// FastjsonParser, parser for get data in json format
// uses
type FastjsonParser struct {
	tsLayout []string
}

func (p *FastjsonParser) Parse(bs []byte) (metric model.Metric, err error) {
	// todo pool the parser
	var parser fastjson.Parser
	var value *fastjson.Value
	if value, err = parser.Parse(string(bs)); err != nil {
		err = errors.Wrapf(err, "")
		return
	}
	metric = &FastjsonMetric{value: value, tsLayout: p.tsLayout}
	return
}

type FastjsonMetric struct {
	value *fastjson.Value
	tsLayout []string
}

func (c *FastjsonMetric) Get(key string) interface{} {
	return c.value.Get(key)
}

func (c *FastjsonMetric) GetString(key string) string {
	v := c.value.GetStringBytes(key)
	return string(v)
}

func (c *FastjsonMetric) GetFloat(key string) float64 {
	return c.value.GetFloat64(key)
}

func (c *FastjsonMetric) GetInt(key string) int64 {
	return int64(c.value.GetInt(key))
}

func (c *FastjsonMetric) GetArray(key string, t string) interface{} {
	array, _ := c.value.Array()
	switch t {
	case "float":
		results := make([]float64, 0, len(array))
		for i := range array {
			results = append(results, array[i].GetFloat64())
		}
		return results
	case "int":
		results := make([]int, 0, len(array))
		for i := range array {
			results = append(results, array[i].GetInt())
		}
		return results
	case "string":
		results := make([]string, 0, len(array))
		for i := range array {
			results = append(results, string(array[i].GetStringBytes()))
		}
		return results
	default:
		panic("not supported array type " + t)
	}
}

func (c *FastjsonMetric) String() string {
	return c.value.String()
}

func (c *FastjsonMetric) GetDate(key string) uint16 {
	val := c.GetString(key)
	if t, err := time.Parse(c.tsLayout[0], val); err==nil {
		return uint16(t.Sub(Epoch).Hours() /24.0)
	}
	return 0
}

func (c *FastjsonMetric) GetDateTime(key string) uint32 {
	val := c.GetString(key)
	if t, err := time.Parse(c.tsLayout[1], val); err==nil {
		return uint32(t.Unix())
	}
	return 0
}

func (c *FastjsonMetric) GetDateTime64(key string) int64 {
	val := c.GetString(key)
	if t, err := time.Parse(c.tsLayout[2], val); err==nil {
		return t.UnixNano()/int64(1000000)
	}
	return 0
}

func (c *FastjsonMetric) GetElasticDateTime(key string) int64 {
	val := c.GetString(key)
	t, _ := time.Parse(time.RFC3339, val)

	return t.Unix()
}
