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
	"mp": {"i" : [1,2,3], "f": [1.1,2.2,3.3], "s": ["aa","bb","cc"], "e": []},
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

var csvSample = []byte(`1536813227,"0.11","escaped_""ws",{""i"" : [1,2,3], ""f"": [1.1,2.2,3.3], ""s"": [""aa"",""bb"",""cc""], ""e"": []},2019-12-16,2019-12-16T12:10:30Z,2019-12-16T12:10:30+08:00,2019-12-16 12:10:30,2019-12-16T12:10:30.123Z,2019-12-16T12:10:30.123+08:00,2019-12-16 12:10:30.123,"[1,2,3]","[1.1,2.2,3.3]","[aa,bb,cc]","[]", "true", "false"`)

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

func TestParserInt(t *testing.T) {
	initialize.Do(initMetrics)
	require.Nil(t, initErr)

	var err error
	var v interface{}
	var exp, act int64
	var desc string
	for i := range names {
		name := names[i]
		metric := metrics[name]

		desc = name + ` GetInt("its", false)`
		exp = 1536813227
		v, err = metric.GetInt("its", false)
		require.Nil(t, err, desc)
		require.IsType(t, exp, v, desc)
		act, _ = v.(int64)
		require.Equal(t, exp, act, desc)

		desc = name + ` GetInt("not_exist", false)`
		exp = int64(0)
		v, err = metric.GetInt("not_exist", false)
		require.Nil(t, err, desc)
		require.IsType(t, exp, v, desc)
		act, _ = v.(int64)
		require.Equal(t, exp, act, desc)

		desc = name + ` GetInt("not_exist", true)`
		v, err = metric.GetInt("not_exist", true)
		require.Nil(t, err, desc)
		require.Nil(t, v, desc)

		// Verify parsers treat type mismatch as error.
		desc = name + ` GetInt("channel", false)`
		_, err = metric.GetInt("channel", false)
		require.NotNil(t, err, desc)
	}
}

func TestParserFloat(t *testing.T) {
	initialize.Do(initMetrics)
	require.Nil(t, initErr)

	var err error
	var v interface{}
	var exp, act float64
	var desc string
	for i := range names {
		name := names[i]
		metric := metrics[name]

		desc = name + ` GetFloat("percent", false)`
		exp = 0.11
		v, err = metric.GetFloat("percent", false)
		require.Nil(t, err, desc)
		require.IsType(t, exp, v, desc)
		act, _ = v.(float64)
		require.Equal(t, exp, act, desc)

		desc = name + ` GetFloat("not_exist", false)`
		exp = 0.0
		v, err = metric.GetFloat("not_exist", false)
		require.Nil(t, err, desc)
		require.IsType(t, exp, v, desc)
		act, _ = v.(float64)
		require.Equal(t, exp, act, desc)

		desc = name + ` GetFloat("not_exist", true)`
		v, err = metric.GetFloat("not_exist", true)
		require.Nil(t, err, desc)
		require.Nil(t, v, desc)

		// Verify parsers treat type mismatch as error.
		desc = name + ` GetFloat("channel", false)`
		_, err = metric.GetFloat("channel", false)
		require.NotNil(t, err, desc)
	}
}

func TestParserString(t *testing.T) {
	initialize.Do(initMetrics)
	require.Nil(t, initErr)

	var err error
	var v interface{}
	var exp, act string
	var desc string
	for i := range names {
		name := names[i]
		metric := metrics[name]

		desc = name + ` GetString("channel", false)`
		exp = "escaped_\"ws"
		v, err = metric.GetString("channel", false)
		require.Nil(t, err, desc)
		require.IsType(t, exp, v, desc)
		act, _ = v.(string)
		require.Equal(t, exp, act, desc)

		desc = name + ` GetString("not_exist", false)`
		exp = ""
		v, err = metric.GetString("not_exist", false)
		require.Nil(t, err, desc)
		require.IsType(t, exp, v, desc)
		act, _ = v.(string)
		require.Equal(t, exp, act, desc)

		desc = name + ` GetString("not_exist", true)`
		v, err = metric.GetString("not_exist", true)
		require.Nil(t, err, desc)
		require.Nil(t, v, desc)

		// Verify everything can be converted to string.
		desc = name + ` GetString("its", false)`
		exp = "1536813227"
		v, err = metric.GetString("its", false)
		require.Nil(t, err, desc)
		require.IsType(t, exp, v, desc)
		act, _ = v.(string)
		require.Equal(t, exp, act, desc)
	}
}

func TestParserDateTime(t *testing.T) {
	initialize.Do(initMetrics)
	require.Nil(t, initErr)

	var err error
	var v interface{}
	var exp, act time.Time
	var desc string
	for i := range names {
		name := names[i]
		metric := metrics[name]

		desc = name + ` GetDate("date1", false)`
		exp = time.Date(2019, 12, 16, 0, 0, 0, 0, time.Local)
		v, err = metric.GetDate("date1", false)
		require.Nil(t, err, desc)
		require.IsType(t, exp, v, desc)
		act = v.(time.Time).In(time.Local)
		require.Equal(t, exp, act, desc)

		desc = name + ` GetDateTime("time_sec_rfc3339_1", false)`
		exp = time.Date(2019, 12, 16, 12, 10, 30, 0, time.UTC)
		v, err = metric.GetDateTime("time_sec_rfc3339_1", false)
		require.Nil(t, err, desc)
		require.IsType(t, exp, v, desc)
		act = v.(time.Time).In(time.UTC)
		require.Equal(t, exp, act, desc)

		desc = name + ` GetDateTime("time_sec_rfc3339_2", false)`
		exp = time.Date(2019, 12, 16, 12, 10, 30, 0, time.FixedZone("CST", 8*60*60)).In(time.UTC)
		v, err = metric.GetDateTime("time_sec_rfc3339_2", false)
		require.Nil(t, err, desc)
		require.IsType(t, exp, v, desc)
		act = v.(time.Time).In(time.UTC)
		require.Equal(t, exp, act, desc)

		desc = name + ` GetDateTime("time_sec_clickhouse_1", false)`
		exp = time.Date(2019, 12, 16, 12, 10, 30, 0, time.Local).In(time.UTC)
		v, err = metric.GetDateTime("time_sec_clickhouse_1", false)
		require.Nil(t, err, desc)
		require.IsType(t, exp, v, desc)
		act = v.(time.Time).In(time.UTC)
		require.Equal(t, exp, act, desc)

		desc = name + ` GetDateTime("not_exist", false)`
		exp = Epoch
		v, err = metric.GetDateTime("not_exist", false)
		require.Nil(t, err, desc)
		require.IsType(t, exp, v, desc)
		act = v.(time.Time).In(time.UTC)
		require.Equal(t, exp, act, desc)

		// Verify parsers treat type mismatch as error.
		desc = name + ` GetDateTime("array_int", false)`
		_, err = metric.GetDateTime("array_int", false)
		require.NotNil(t, err, desc)
	}
}

func TestParserDateTime64(t *testing.T) {
	initialize.Do(initMetrics)
	require.Nil(t, initErr)

	var err error
	var v interface{}
	var exp, act time.Time
	var desc string
	for i := range names {
		name := names[i]
		metric := metrics[name]

		desc = name + ` GetDateTime64("time_ms_rfc3339_1", false)`
		exp = time.Date(2019, 12, 16, 12, 10, 30, 123000000, time.UTC)
		v, err = metric.GetDateTime64("time_ms_rfc3339_1", false)
		require.Nil(t, err, desc)
		require.IsType(t, exp, v, desc)
		act = v.(time.Time).In(time.UTC)
		require.Equal(t, exp, act, desc)

		desc = name + ` GetDateTime64("time_ms_rfc3339_2", false)`
		exp = time.Date(2019, 12, 16, 12, 10, 30, 123000000, time.FixedZone("CST", 8*60*60)).In(time.UTC)
		v, err = metric.GetDateTime64("time_ms_rfc3339_2", false)
		require.Nil(t, err, desc)
		require.IsType(t, exp, v, desc)
		act = v.(time.Time).In(time.UTC)
		require.Equal(t, exp, act, desc)

		desc = name + ` GetDateTime64("time_ms_clickhouse_1", false)`
		exp = time.Date(2019, 12, 16, 12, 10, 30, 123000000, time.Local).In(time.UTC)
		v, err = metric.GetDateTime64("time_ms_clickhouse_1", false)
		require.Nil(t, err, desc)
		require.IsType(t, exp, v, desc)
		act = v.(time.Time).In(time.UTC)
		require.Equal(t, exp, act, desc)

		desc = name + ` GetDateTime64("not_exist", false)`
		exp = Epoch
		v, err = metric.GetDateTime64("not_exist", false)
		require.Nil(t, err, desc)
		require.IsType(t, exp, v, desc)
		act = v.(time.Time).In(time.UTC)
		require.Equal(t, exp, act, desc)
	}
}

func TestParserElasticDateTime(t *testing.T) {
	initialize.Do(initMetrics)
	require.Nil(t, initErr)

	var err error
	var v interface{}
	var exp, act int64
	var desc string
	for i := range names {
		name := names[i]
		metric := metrics[name]

		desc = name + ` GetElasticDateTime("time_sec_rfc3339_1", false)`
		exp = time.Date(2019, 12, 16, 12, 10, 30, 0, time.UTC).Unix()
		v, err = metric.GetElasticDateTime("time_sec_rfc3339_1", false)
		require.Nil(t, err, desc)
		require.IsType(t, exp, v, desc)
		act, _ = v.(int64)
		require.Equal(t, exp, act, desc)

		desc = name + ` GetElasticDateTime("not_exist", false)`
		exp = 0
		v, err = metric.GetElasticDateTime("not_exist", false)
		require.Nil(t, err, desc)
		require.IsType(t, exp, v, desc)
		act, _ = v.(int64)
		require.Equal(t, exp, act, desc)

		desc = name + ` GetElasticDateTime("not_exist", true)`
		v, err = metric.GetElasticDateTime("not_exist", true)
		require.Nil(t, err, desc)
		require.Nil(t, v, desc)
	}
}

func TestParserArray(t *testing.T) {
	initialize.Do(initMetrics)
	require.Nil(t, initErr)

	var err error
	var v interface{}
	var desc string
	for i := range names {
		name := names[i]
		metric := metrics[name]

		desc = name + ` GetArray("array_int", "int")`
		expI := []int64{1, 2, 3}
		v, err = metric.GetArray("array_int", "int")
		require.Nil(t, err, desc)
		require.IsType(t, expI, v, desc)
		actI, _ := v.([]int64)
		require.Equal(t, expI, actI, desc)

		desc = name + ` GetArray("array_float", "float")`
		expF := []float64{1.1, 2.2, 3.3}
		v, err = metric.GetArray("array_float", "float")
		require.Nil(t, err, desc)
		require.IsType(t, expF, v, desc)
		actF, _ := v.([]float64)
		require.Equal(t, expF, actF, desc)

		desc = name + ` GetArray("array_string", "string")`
		expS := []string{"aa", "bb", "cc"}
		v, err = metric.GetArray("array_string", "string")
		require.Nil(t, err, desc)
		require.IsType(t, expS, v, desc)
		actS, _ := v.([]string)
		require.Equal(t, expS, actS, desc)

		desc = name + ` GetArray("array_empty", "int")`
		expIE := []int64{}
		v, err = metric.GetArray("array_empty", "int")
		require.Nil(t, err, desc)
		require.IsType(t, expIE, v, desc)
		actIE, _ := v.([]int64)
		require.Equal(t, expIE, actIE, desc)

		desc = name + ` GetArray("array_empty", "float")`
		expFE := []float64{}
		v, err = metric.GetArray("array_empty", "float")
		require.Nil(t, err, desc)
		require.IsType(t, expFE, v, desc)
		actFE, _ := v.([]float64)
		require.Equal(t, expFE, actFE, desc)

		desc = name + ` GetArray("array_empty", "string")`
		expSE := []string{}
		v, err = metric.GetArray("array_empty", "string")
		require.Nil(t, err, desc)
		require.IsType(t, expSE, v, desc)
		actSE, _ := v.([]string)
		require.Equal(t, expSE, actSE, desc)
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
