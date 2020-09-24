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

	"github.com/tidwall/gjson"

	"github.com/housepower/clickhouse_sinker/model"
)

type GjsonParser struct {
	tsLayout []string
}

func (p *GjsonParser) Parse(bs []byte) (metric model.Metric, err error) {
	metric = &GjsonMetric{string(bs), p.tsLayout}
	return
}

type GjsonMetric struct {
	raw      string
	tsLayout []string
}

func (c *GjsonMetric) Get(key string) interface{} {
	return gjson.Get(c.raw, key).Value()
}

func (c *GjsonMetric) GetString(key string) string {
	return gjson.Get(c.raw, key).String()
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

func (c *GjsonMetric) GetFloat(key string) float64 {
	return gjson.Get(c.raw, key).Float()
}

func (c *GjsonMetric) GetInt(key string) int64 {
	return gjson.Get(c.raw, key).Int()
}

func (c *GjsonMetric) GetDate(key string) (t time.Time) {
	val := c.GetString(key)
	t, _ = time.Parse(c.tsLayout[0], val)
	return
}

func (c *GjsonMetric) GetDateTime(key string) (t time.Time) {
	val := c.GetString(key)
	t, _ = time.Parse(c.tsLayout[1], val)
	return
}

func (c *GjsonMetric) GetDateTime64(key string) (t time.Time) {
	val := c.GetString(key)
	t, _ = time.Parse(c.tsLayout[2], val)
	return
}

func (c *GjsonMetric) GetElasticDateTime(key string) int64 {
	val := c.GetString(key)
	t, _ := time.Parse(time.RFC3339, val)

	return t.Unix()
}
