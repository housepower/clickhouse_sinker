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
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/housepower/clickhouse_sinker/model"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
	"github.com/valyala/fastjson"
)

var jsonSample = []byte(`{
	"its":1536813227,
	"_ip":"112.96.65.228",
	"cgi":"/commui/queryhttpdns",
	"channel":"escaped_\"ws",
	"platform":"adr",
	"experiment":"default",
	"ip":"36.248.20.69",
	"version":"5.8.3",
	"success":0,
	"percent":0.11,
	"mp": {"i":[1,2,3],"f":[1.1,2.2,3.3],"s":["aa","bb","cc"],"e":[]},
	"date1": "2019-12-16",
	"time_sec_rfc3339_1":    "2019-12-16T12:10:30Z",
	"time_sec_rfc3339_2":    "2019-12-16T12:10:30+08:00",
	"time_sec_clickhouse_1": "2019-12-16 12:10:30",
	"time_ms_rfc3339_1":     "2019-12-16T12:10:30.123Z",
	"time_ms_rfc3339_2":     "2019-12-16T12:10:30.123+08:00",
	"time_ms_clickhouse_1":  "2019-12-16 12:10:30.123",
	"array_int": [1,2,3],
	"array_float": [1.1,2.2,3.3],
	"array_string": ["aa","bb","cc"],
	"array_date": ["2000-01-01","2000-01-02","2000-01-03"],
	"array_object": [{"i":[1,2,3],"f":[1.1,2.2,3.3]},{"s":["aa","bb","cc"],"e":[]}],
	"array_empty": [],
	"bool_true": true,
	"bool_false": false
}`)

var jsonSchema = map[string]string{
	"its":                   "number",
	"_ip":                   "string",
	"cgi":                   "string",
	"channel":               "string",
	"platform":              "string",
	"experiment":            "string",
	"ip":                    "string",
	"version":               "string",
	"success":               "number",
	"percent":               "number",
	"mp":                    "object",
	"date1":                 "string",
	"time_sec_rfc3339_1":    "string",
	"time_sec_rfc3339_2":    "string",
	"time_sec_clickhouse_1": "string",
	"time_ms_rfc3339_1":     "string",
	"time_ms_rfc3339_2":     "string",
	"time_ms_clickhouse_1":  "string",
	"array_int":             "array",
	"array_float":           "array",
	"array_string":          "array",
	"array_date":            "array",
	"array_object":          "array",
	"array_empty":           "array",
	"bool_true":             "true",
	"bool_false":            "false",
}

var csvSample = []byte(`1536813227,"0.11","escaped_""ws","{""i"":[1,2,3],""f"":[1.1,2.2,3.3],""s"":[""aa"",""bb"",""cc""],""e"":[]}",2019-12-16,2019-12-16T12:10:30Z,2019-12-16T12:10:30+08:00,2019-12-16 12:10:30,2019-12-16T12:10:30.123Z,2019-12-16T12:10:30.123+08:00,2019-12-16 12:10:30.123,"[1,2,3]","[1.1,2.2,3.3]","[""aa"",""bb"",""cc""]","[""2000-01-01"",""2000-01-02"",""2000-01-03""]","[{""i"":[1,2,3],""f"":[1.1,2.2,3.3]},{""s"":[""aa"",""bb"",""cc""],""e"":[]}]","[]","true","false"`)

var csvSchema = []string{
	"its",
	"percent",
	"channel",
	"mp",
	"date1",
	"time_sec_rfc3339_1",
	"time_sec_rfc3339_2",
	"time_sec_clickhouse_1",
	"time_ms_rfc3339_1",
	"time_ms_rfc3339_2",
	"time_ms_clickhouse_1",
	"array_int",
	"array_float",
	"array_string",
	"array_date",
	"array_object",
	"array_empty",
	"bool_true",
	"bool_false",
}

var initialize sync.Once
var initErr error
var names = []string{"csv", "fastjson", "gjson"}
var pools map[string]*Pool
var parsers map[string]Parser
var metrics map[string]model.Metric

type SimpleCase struct {
	Field    string
	Nullable bool
	ExpVal   interface{}
}

type ArrayCase struct {
	Field  string
	Type   int
	ExpVal interface{}
}

func Bool2Str(b bool) string {
	if b {
		return "true"
	}
	return "false"
}

func initMetrics() {
	var pp *Pool
	var parser Parser
	var metric model.Metric
	var sample []byte
	pools = make(map[string]*Pool)
	parsers = make(map[string]Parser)
	metrics = make(map[string]model.Metric)
	for _, name := range names {
		switch name {
		case "csv":
			pp, _ = NewParserPool("csv", csvSchema, "", "")
			sample = csvSample
		case "fastjson":
			pp, _ = NewParserPool("fastjson", nil, "", "")
			sample = jsonSample
		case "gjson":
			pp, _ = NewParserPool("gjson", nil, "", "")
			sample = jsonSample
		}
		parser = pp.Get()
		if metric, initErr = parser.Parse(sample); initErr != nil {
			msg := fmt.Sprintf("%+v", initErr)
			panic(msg)
		}
		pools[name] = pp
		parsers[name] = parser
		metrics[name] = metric
	}
}

func doTestSimple(t *testing.T, method string, testCases []SimpleCase) {
	t.Helper()
	initialize.Do(initMetrics)
	require.Nil(t, initErr)
	for i := range names {
		name := names[i]
		metric := metrics[name]
		for j := range testCases {
			var v interface{}
			desc := fmt.Sprintf(`%s %s("%s", %s)`, name, method, testCases[j].Field, Bool2Str(testCases[j].Nullable))
			switch method {
			case "GetInt":
				v = metric.GetInt(testCases[j].Field, testCases[j].Nullable)
			case "GetFloat":
				v = metric.GetFloat(testCases[j].Field, testCases[j].Nullable)
			case "GetString":
				v = metric.GetString(testCases[j].Field, testCases[j].Nullable)
			case "GetDateTime":
				v = metric.GetDateTime(testCases[j].Field, testCases[j].Nullable)
			case "GetElasticDateTime":
				v = metric.GetElasticDateTime(testCases[j].Field, testCases[j].Nullable)
			default:
				panic("error!")
			}
			require.Equal(t, testCases[j].ExpVal, v, desc)
		}
	}
}

func TestParserInt(t *testing.T) {
	testCases := []SimpleCase{
		{"its", false, int64(1536813227)},
		{"not_exist", false, int64(0)},
		{"not_exist", true, nil},
		{"channel", false, int64(0)},
	}
	doTestSimple(t, "GetInt", testCases)
}

func TestParserFloat(t *testing.T) {
	testCases := []SimpleCase{
		{"percent", false, 0.11},
		{"not_exist", false, 0.0},
		{"not_exist", true, nil},
		{"channel", false, 0.0},
	}
	doTestSimple(t, "GetFloat", testCases)
}

func TestParserString(t *testing.T) {
	testCases := []SimpleCase{
		{"channel", false, "escaped_\"ws"},
		{"not_exist", false, ""},
		{"not_exist", true, nil},
		{"its", false, "1536813227"},
		{"array_int", false, "[1,2,3]"},
		{"array_string", false, `["aa","bb","cc"]`},
		{"mp", false, `{"i":[1,2,3],"f":[1.1,2.2,3.3],"s":["aa","bb","cc"],"e":[]}`},
	}
	doTestSimple(t, "GetString", testCases)
}

func TestParserDateTime(t *testing.T) {
	testCases := []SimpleCase{
		{"date1", false, time.Date(2019, 12, 16, 0, 0, 0, 0, time.Local).In(time.UTC)},
		{"time_sec_rfc3339_1", false, time.Date(2019, 12, 16, 12, 10, 30, 0, time.UTC)},
		{"time_sec_rfc3339_2", false, time.Date(2019, 12, 16, 12, 10, 30, 0, time.FixedZone("CST", 8*60*60)).In(time.UTC)},
		{"time_sec_clickhouse_1", false, time.Date(2019, 12, 16, 12, 10, 30, 0, time.Local).In(time.UTC)},
		{"time_ms_rfc3339_1", false, time.Date(2019, 12, 16, 12, 10, 30, 123000000, time.UTC)},
		{"time_ms_rfc3339_2", false, time.Date(2019, 12, 16, 12, 10, 30, 123000000, time.FixedZone("CST", 8*60*60)).In(time.UTC)},
		{"time_ms_clickhouse_1", false, time.Date(2019, 12, 16, 12, 10, 30, 123000000, time.Local).In(time.UTC)},
		{"not_exist", false, Epoch},
		{"array_int", false, Epoch},
	}
	doTestSimple(t, "GetDateTime", testCases)
}

func TestParserElasticDateTime(t *testing.T) {
	testCases := []SimpleCase{
		{"time_ms_rfc3339_1", false, time.Date(2019, 12, 16, 12, 10, 30, 0, time.UTC).Unix()},
		{"not_exist", false, int64(0)},
		{"not_exist", true, nil},
	}
	doTestSimple(t, "GetElasticDateTime", testCases)
}

func TestParserArray(t *testing.T) {
	initialize.Do(initMetrics)
	require.Nil(t, initErr)
	var ts []time.Time
	for _, e := range []string{"2000-01-01", "2000-01-02", "2000-01-03"} {
		t, _ := parseInLocation(e, time.Local)
		ts = append(ts, t)
	}
	testCases := []ArrayCase{
		{"array_int", model.Int, []int64{1, 2, 3}},
		{"array_float", model.Float, []float64{1.1, 2.2, 3.3}},
		{"array_string", model.String, []string{"aa", "bb", "cc"}},
		{"array_date", model.DateTime, ts},
		{"array_object", model.String, []string{`{"i":[1,2,3],"f":[1.1,2.2,3.3]}`, `{"s":["aa","bb","cc"],"e":[]}`}},
		{"array_empty", model.Int, []int64{}},
		{"array_empty", model.Float, []float64{}},
		{"array_empty", model.String, []string{}},
		{"array_empty", model.DateTime, []time.Time{}},
	}

	for i := range names {
		name := names[i]
		metric := metrics[name]
		for j := range testCases {
			if name == "csv" && testCases[j].Field == "array_object" {
				// csv parser doesn't support object array yet.
				continue
			}
			var v interface{}
			desc := fmt.Sprintf(`%s GetArray("%s", %d)`, name, testCases[j].Field, testCases[j].Type)
			v = metric.GetArray(testCases[j].Field, testCases[j].Type)
			require.Equal(t, testCases[j].ExpVal, v, desc)
		}
	}
}

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
			panic(err)
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
