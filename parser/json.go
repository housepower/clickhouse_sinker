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
	"encoding/json"

	"github.com/housepower/clickhouse_sinker/model"
)

type Parser interface {
	Parse(bs []byte) model.Metric
}

func NewParser(typ string, title []string, delimiter string) Parser {
	switch typ {
	case "json", "gjson":
		return &GjsonParser{}
	case "fastjson":
		return &FastjsonParser{}
	case "csv":
		return &CsvParser{title: title, delimiter: delimiter}
	case "gjson_extend": //extend gjson that could extract the map
		return &GjsonExtendParser{}
	default:
		return &GjsonParser{}
	}
}

// JsonParser is replaced by GjsonParser
type JsonParser struct {
}

func (c *JsonParser) Parse(bs []byte) model.Metric {
	v := make(map[string]interface{})
	json.Unmarshal(bs, &v)
	return &JsonMetric{v}
}

type JsonMetric struct {
	mp map[string]interface{}
}

func (c *JsonMetric) Get(key string) interface{} {
	return c.mp[key]
}

func (c *JsonMetric) GetString(key string) string {
	//判断object
	val, _ := c.mp[key]
	if val == nil {
		return ""
	}
	switch val.(type) {
	case map[string]interface{}:
		return GetJsonShortStr(val.(map[string]interface{}))

	case string:
		return val.(string)
	}
	return ""
}

func (c *JsonMetric) GetArray(key string, t string) interface{} {
	//判断object
	val, _ := c.mp[key]
	switch t {
	case "string":
		switch val.(type) {
		case []string:
			return val.([]string)

		default:
			return []string{}
		}

	case "float":
		switch val.(type) {
		case []float64:
			return val.([]float64)

		default:
			return []float64{}
		}

	case "int":
		switch val.(type) {
		case []float64:
			results := make([]int64, 0, len(val.([]float64)))
			for i := range val.([]float64) {
				results = append(results, int64(val.([]float64)[i]))
			}
			return results
		default:
			return []int64{}
		}
	default:
		panic("not supported array type " + t)
	}
	return nil
}

func (c *JsonMetric) GetFloat(key string) float64 {
	val, _ := c.mp[key]
	if val == nil {
		return 0
	}
	switch val.(type) {
	case float64:
		return val.(float64)
	}
	return 0
}

func (c *JsonMetric) GetInt(key string) int64 {
	val, _ := c.mp[key]
	if val == nil {
		return 0
	}
	switch val.(type) {
	case float64:
		return int64(val.(float64))
	}
	return 0
}

func GetJsonShortStr(v interface{}) string {
	bs, _ := json.Marshal(v)
	return string(bs)
}
