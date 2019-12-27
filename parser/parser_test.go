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

	"github.com/tidwall/gjson"
	"github.com/valyala/fastjson"
)

var bs = []byte(`{"its":1536813227,"_ip":"112.96.65.228","cgi":"/commui/queryhttpdns","channel":"ws","platform":"adr","experiment":"default","ip":"36.248.20.69","version":"5.8.3","success":0, "mp": {"a" : [1,2,3]} }`)

func BenchmarkUnmarshalljson(b *testing.B) {
	mp := map[string]interface{}{}
	for i := 0; i < b.N; i++ {
		json.Unmarshal(bs, &mp)
	}

}

func BenchmarkUnmarshallFastJson(b *testing.B) {
	// mp := map[string]interface{}{}
	// var p fastjson.Parser
	str := string(bs)
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
	str := string(bs)
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

func BenchmarkUnmarshallGjson2(b *testing.B) {
	// mp := map[string]interface{}{}
	// var p fastjson.Parser
	str := string(bs)
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
	// mp := map[string]interface{}{}
	// var p fastjson.Parser
	parser := NewParser("gjson_extend", nil, "")
	metric := parser.Parse(bs)

	arr := metric.GetArray("mp_a", "int").([]int64)
	expected := []int64{1, 2, 3}
	for i := range arr {
		assertEqual(t, arr[i], expected[i])
	}
}

func assertEqual(t *testing.T, a, b interface{}) {
	if a != b {
		t.Errorf("Not Equal. %d %d", a, b)
	}
}
