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

type GjsonExtendParser struct {
	tsLayout []string
}

type GjsonExtendMetric struct {
	mp       map[string]interface{}
	tsLayout []string
}

// {
// 	"aa" : "1",
// 	"bb" : {
// 		"cc" : 3,
// 		"dd" : [ "33", "44"]
// 	}
// }

// will be

// {
// 	"aa" : "1",
// 	"bb_cc" : 3,
// 	"bb_dd" : ["33", "44"]
// }
func (p *GjsonExtendParser) Parse(bs []byte) (metric model.Metric, err error) {
	var mp = make(map[string]interface{})

	jsonResults := gjson.ParseBytes(bs)
	jsonResults.ForEach(func(key gjson.Result, value gjson.Result) bool {
		if key.String() != "" {
			if value.Type != gjson.JSON {
				mp[key.String()] = value.Value()
			} else {
				injectObject(key.String(), mp, value)
			}
		}
		return true
	})

	metric = &GjsonExtendMetric{mp, p.tsLayout}
	return
}

func injectObject(prefix string, result map[string]interface{}, t gjson.Result) {
	if t.IsArray() {
		result[prefix] = t
		return
	}
	t.ForEach(func(key gjson.Result, value gjson.Result) bool {
		switch value.Type {
		case gjson.JSON:
			injectObject(prefix+"."+key.String(), result, value)
		default:
			result[prefix+"."+key.String()] = value.Value()
		}
		return true
	})
}

func (c *GjsonExtendMetric) Get(key string) interface{} {
	return c.mp[key]
}

func (c *GjsonExtendMetric) GetString(key string) string {
	//判断object
	val := c.mp[key]
	if val == nil {
		return ""
	}
	switch v := val.(type) {
	case map[string]interface{}:
		return GetJSONShortStr(v)

	case string:
		return v

	default:
		return ""
	}
}

func (c *GjsonExtendMetric) GetArray(key string, t string) interface{} {
	v, ok := c.mp[key].(gjson.Result)

	slice := v.Array()
	switch t {
	case "string":
		results := make([]string, 0, len(slice))
		if !ok {
			return results
		}
		for _, s := range slice {
			results = append(results, s.String())
		}
		return results

	case "float":
		results := make([]float64, 0, len(slice))

		if !ok {
			return results
		}

		for _, s := range slice {
			results = append(results, s.Float())
		}
		return results

	case "int":
		results := make([]int64, 0, len(slice))
		if !ok {
			return results
		}
		for _, s := range slice {
			results = append(results, s.Int())
		}
		return results

	default:
		panic("not supported array type " + t)
	}
}

func (c *GjsonExtendMetric) GetFloat(key string) float64 {
	val := c.mp[key]
	if val == nil {
		return 0
	}
	switch v := val.(type) {
	case float64:
		return v
	default:
		return 0
	}
}

func (c *GjsonExtendMetric) GetInt(key string) int64 {
	val := c.mp[key]
	if val == nil {
		return 0
	}
	switch v := val.(type) {
	case float64:
		return int64(v)
	default:
		return 0
	}
}

func (c *GjsonExtendMetric) GetDate(key string) (t time.Time) {
	val := c.GetString(key)
	t, _ = time.Parse(c.tsLayout[0], val)
	return
}

func (c *GjsonExtendMetric) GetDateTime(key string) (t time.Time) {
	if v := c.GetFloat(key); v != 0 {
		return time.Unix(int64(v), int64(v*1e9)%1e9)
	}

	val := c.GetString(key)
	t, _ = time.Parse(c.tsLayout[1], val)
	return
}

func (c *GjsonExtendMetric) GetDateTime64(key string) (t time.Time) {
	if v := c.GetFloat(key); v != 0 {
		return time.Unix(int64(v), int64(v*1e9)%1e9)
	}

	val := c.GetString(key)
	t, _ = time.Parse(c.tsLayout[2], val)
	return
}

func (c *GjsonExtendMetric) GetElasticDateTime(key string) int64 {
	val := c.GetString(key)
	t, _ := time.Parse(time.RFC3339, val)

	return t.Unix()
}
