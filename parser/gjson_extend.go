package parser

import (
	"github.com/tidwall/gjson"

	"github.com/housepower/clickhouse_sinker/model"
)

type GjsonExtendParser struct {
}

type GjsonExtendMetric struct {
	mp map[string]interface{}
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
func (c *GjsonExtendParser) Parse(bs []byte) model.Metric {
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

	return &GjsonExtendMetric{mp}
}

func injectObject(prefix string, result map[string]interface{}, t gjson.Result) {
	if t.IsArray() {
		result[prefix] = t
		return
	}
	t.ForEach(func(key gjson.Result, value gjson.Result) bool {
		switch value.Type {
		case gjson.JSON:
			injectObject(prefix+"_"+key.String(), result, value)
		default:
			result[prefix+"_"+key.String()] = value.Value()
		}
		return true
	})
}

func copyMap(dst, src map[string]interface{}) {
	for k, v := range src {
		dst[k] = v
	}
}
func (c *GjsonExtendMetric) Get(key string) interface{} {
	return c.mp[key]
}

func (c *GjsonExtendMetric) GetString(key string) string {
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
	return nil
}

func (c *GjsonExtendMetric) GetFloat(key string) float64 {
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

func (c *GjsonExtendMetric) GetInt(key string) int64 {
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
