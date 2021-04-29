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
	"array_empty":           "array",
	"bool_true":             "true",
	"bool_false":            "false",
}

var csvSample = []byte(`1536813227,"0.11","escaped_""ws","{""i"":[1,2,3],""f"":[1.1,2.2,3.3],""s"":[""aa"",""bb"",""cc""],""e"":[]}",2019-12-16,2019-12-16T12:10:30Z,2019-12-16T12:10:30+08:00,2019-12-16 12:10:30,2019-12-16T12:10:30.123Z,2019-12-16T12:10:30.123+08:00,2019-12-16 12:10:30.123,"[1,2,3]","[1.1,2.2,3.3]","[""aa"",""bb"",""cc""]","[]","true","false"`)

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
	ExpErr   error
}

type ArrayCase struct {
	Field  string
	Type   string
	ExpVal interface{}
	ExpErr error
}

var ErrParse = fmt.Errorf("generic parsing error")

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
			return
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
			var err error
			var v interface{}
			desc := fmt.Sprintf(`%s %s("%s", %s)`, name, method, testCases[j].Field, Bool2Str(testCases[j].Nullable))
			switch method {
			case "GetInt":
				v, err = metric.GetInt(testCases[j].Field, testCases[j].Nullable)
			case "GetFloat":
				v, err = metric.GetFloat(testCases[j].Field, testCases[j].Nullable)
			case "GetString":
				v, err = metric.GetString(testCases[j].Field, testCases[j].Nullable)
			case "GetDate":
				v, err = metric.GetDate(testCases[j].Field, testCases[j].Nullable)
			case "GetDateTime":
				v, err = metric.GetDateTime(testCases[j].Field, testCases[j].Nullable)
			case "GetDateTime64":
				v, err = metric.GetDateTime64(testCases[j].Field, testCases[j].Nullable)
			case "GetElasticDateTime":
				v, err = metric.GetElasticDateTime(testCases[j].Field, testCases[j].Nullable)
			default:
				panic("error!")
			}
			if testCases[j].ExpErr == nil {
				require.Nil(t, err, desc)
				require.Equal(t, testCases[j].ExpVal, v, desc)
			} else {
				require.NotNil(t, err, desc)
			}
		}
	}
}

func TestParserInt(t *testing.T) {
	testCases := []SimpleCase{
		{"its", false, int64(1536813227), nil},
		{"not_exist", false, int64(0), nil},
		{"not_exist", true, nil, nil},
		{"channel", false, nil, ErrParse},
	}
	doTestSimple(t, "GetInt", testCases)
}

func TestParserFloat(t *testing.T) {
	testCases := []SimpleCase{
		{"percent", false, 0.11, nil},
		{"not_exist", false, 0.0, nil},
		{"not_exist", true, nil, nil},
		{"channel", false, nil, ErrParse},
	}
	doTestSimple(t, "GetFloat", testCases)
}

func TestParserString(t *testing.T) {
	testCases := []SimpleCase{
		{"channel", false, "escaped_\"ws", nil},
		{"not_exist", false, "", nil},
		{"not_exist", true, nil, nil},
		{"its", false, "1536813227", nil},
		{"array_int", false, "[1,2,3]", nil},
		{"array_string", false, `["aa","bb","cc"]`, nil},
		{"mp", false, `{"i":[1,2,3],"f":[1.1,2.2,3.3],"s":["aa","bb","cc"],"e":[]}`, nil},
	}
	doTestSimple(t, "GetString", testCases)
}

func TestParserDateTime(t *testing.T) {
	testCases := []SimpleCase{
		{"date1", false, time.Date(2019, 12, 16, 0, 0, 0, 0, time.Local).In(time.UTC), nil},
		{"time_sec_rfc3339_1", false, time.Date(2019, 12, 16, 12, 10, 30, 0, time.UTC), nil},
		{"time_sec_rfc3339_2", false, time.Date(2019, 12, 16, 12, 10, 30, 0, time.FixedZone("CST", 8*60*60)).In(time.UTC), nil},
		{"time_sec_clickhouse_1", false, time.Date(2019, 12, 16, 12, 10, 30, 0, time.Local).In(time.UTC), nil},
		{"not_exist", false, Epoch, nil},
		{"array_int", false, Epoch, nil},
	}
	doTestSimple(t, "GetDateTime", testCases)
}

func TestParserDateTime64(t *testing.T) {
	testCases := []SimpleCase{
		{"time_ms_rfc3339_1", false, time.Date(2019, 12, 16, 12, 10, 30, 123000000, time.UTC), nil},
		{"time_ms_rfc3339_2", false, time.Date(2019, 12, 16, 12, 10, 30, 123000000, time.FixedZone("CST", 8*60*60)).In(time.UTC), nil},
		{"time_ms_clickhouse_1", false, time.Date(2019, 12, 16, 12, 10, 30, 123000000, time.Local).In(time.UTC), nil},
		{"not_exist", false, Epoch, nil},
		{"array_int", false, Epoch, nil},
	}
	doTestSimple(t, "GetDateTime64", testCases)
}

func TestParserElasticDateTime(t *testing.T) {
	testCases := []SimpleCase{
		{"time_ms_rfc3339_1", false, time.Date(2019, 12, 16, 12, 10, 30, 0, time.UTC).Unix(), nil},
		{"not_exist", false, int64(0), nil},
		{"not_exist", true, nil, nil},
	}
	doTestSimple(t, "GetElasticDateTime", testCases)
}

func TestParserArray(t *testing.T) {
	initialize.Do(initMetrics)
	require.Nil(t, initErr)
	testCases := []ArrayCase{
		{"array_int", "int", []int64{1, 2, 3}, nil},
		{"array_float", "float", []float64{1.1, 2.2, 3.3}, nil},
		{"array_string", "string", []string{"aa", "bb", "cc"}, nil},
		{"array_empty", "int", []int64{}, nil},
		{"array_empty", "float", []float64{}, nil},
	}

	for i := range names {
		name := names[i]
		metric := metrics[name]
		for j := range testCases {
			var err error
			var v interface{}
			desc := fmt.Sprintf(`%s GetArray("%s", "%s")`, name, testCases[j].Field, testCases[j].Type)
			v, err = metric.GetArray(testCases[j].Field, testCases[j].Type)
			if testCases[j].ExpErr == nil {
				require.Nil(t, err, desc)
				require.Equal(t, testCases[j].ExpVal, v, desc)
			} else {
				require.NotNil(t, err, desc)
				require.Nil(t, v, desc)
			}
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
