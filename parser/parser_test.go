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
	"log"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
	"github.com/valyala/fastjson"
)

var jsonSample = []byte(`{
	"its":1536813227,
	"_ip":"112.96.65.228",
	"cgi":"/commui/queryhttpdns",
	"channel":"ws",
	"platform":"adr",
	"experiment":"default",
	"ip":"36.248.20.69",
	"version":"5.8.3",
	"success":0,
	"percent":0.11,
	"mp": {"a" : [1,2,3]},
	"mpf": {"a" : [1.1,2.2,3.3]},
	"mps": {"a" : ["aa","bb","cc"]},
	"date": "2019-12-16T12:10:30Z"
}`)

var jsonSample2 = []byte(`{"time":"2006-01-02 15:04:05","timestamp":"2006-01-02T15:04:05.123+08:00","item_guid":"bus070_ins062","metric_name":"CPU繁忙率","alg_name":"Ripple","value":60,"upper":100,"lower":60,"yhat_upper":100,"yhat_lower":60,"yhat_flag":23655,"total_anomaly":61357,"anomaly":0.3,"abnormal_type":22,"abnormality":913,"container_id":39929,"hard_upper":100,"hard_lower":60,"hard_anomaly":39371,"shift_tag":38292,"season_tag":56340,"spike_tag":13231,"is_missing":0,"str_array":["tag3","tag5"],"int_array":[123,456]}`)

func BenchmarkUnmarshalljson(b *testing.B) {
	mp := map[string]interface{}{}
	for i := 0; i < b.N; i++ {
		_ = json.Unmarshal(jsonSample, &mp)
	}
}

func BenchmarkUnmarshallFastJson(b *testing.B) {
	// mp := map[string]interface{}{}
	// var p fastjson.Parser
	str := string(jsonSample)
	var p fastjson.Parser
	for i := 0; i < b.N; i++ {
		v, err := p.Parse(str)
		if err != nil {
			log.Fatal(err)
		}
		v.GetInt("its")
		v.GetStringBytes("_ip")
		v.GetStringBytes("cgi")
		v.GetStringBytes("channel")
		v.GetStringBytes("platform")
		v.GetStringBytes("experiment")
		v.GetStringBytes("ip")
		v.GetStringBytes("version")
		v.GetInt("success")
		v.GetInt("trycount")
	}
}

// 字段个数较少的情况下，直接Get性能更好
func BenchmarkUnmarshallGjson(b *testing.B) {
	// mp := map[string]interface{}{}
	// var p fastjson.Parser
	str := string(jsonSample)
	for i := 0; i < b.N; i++ {
		_ = gjson.Get(str, "its").Int()
		_ = gjson.Get(str, "_ip").String()
		_ = gjson.Get(str, "cgi").String()
		_ = gjson.Get(str, "channel").String()
		_ = gjson.Get(str, "platform").String()
		_ = gjson.Get(str, "experiment").String()
		_ = gjson.Get(str, "ip").String()
		_ = gjson.Get(str, "version").String()
		_ = gjson.Get(str, "success").Int()
		_ = gjson.Get(str, "trycount").Int()
	}
}

func BenchmarkUnmarshalGabon2(b *testing.B) {
	// mp := map[string]interface{}{}
	// var p fastjson.Parser
	str := string(jsonSample)
	for i := 0; i < b.N; i++ {
		result := gjson.Parse(str).Map()
		_ = result["its"].Int()
		_ = result["_ip"].String()
		_ = result["cgi"].String()
		_ = result["channel"].String()
		_ = result["platform"].String()
		_ = result["experiment"].String()
		_ = result["ip"].String()
		_ = result["version"].String()
		_ = result["success"].Int()
		_ = result["trycount"].Int()
	}
}

func TestGjsonExtend(t *testing.T) {
	pp := NewParserPool("gjson_extend", nil, "", DefaultTSLayout)
	parser := pp.Get()
	defer pp.Put(parser)
	metric, _ := parser.Parse(jsonSample)

	arr := metric.GetArray("mp.a", "int").([]int64)
	expected := []int64{1, 2, 3}
	for i := range arr {
		require.Equal(t, arr[i], expected[i])
	}

	metric, _ = parser.Parse(jsonSample2)
	arr2 := metric.GetArray("str_array", "string").([]string)
	exp2 := []string{"tag3", "tag5"}
	require.Equal(t, exp2, arr2)
}

func TestFastJson(t *testing.T) {
	pp := NewParserPool("fastjson", nil, "", []string{DefaultTSLayout[0], "2006-01-02 15:04:05", time.RFC3339})
	parser := pp.Get()
	defer pp.Put(parser)
	metric, _ := parser.Parse(jsonSample2)

	ts1 := metric.GetDateTime("time", false)
	exp1, _ := time.Parse("2006-01-02 15:04:05", "2006-01-02 15:04:05")
	require.Equal(t, exp1, ts1)

	ts2 := metric.GetDateTime64("timestamp", false)
	exp2, _ := time.Parse(time.RFC3339, "2006-01-02T15:04:05.123+08:00")
	require.Equal(t, exp2, ts2)

	arr := metric.GetArray("str_array", "string").([]string)
	exp3 := []string{"tag3", "tag5"}
	require.Equal(t, exp3, arr)

	arr2 := metric.GetArray("int_array", "int").([]int)
	exp4 := []int{123, 456}
	require.Equal(t, exp4, arr2)
}
