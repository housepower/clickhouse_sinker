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
	"log"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/housepower/clickhouse_sinker/model"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
	"github.com/valyala/fastjson"
)

// https://golang.org/pkg/math/, Mathematical constants
var jsonSample = []byte(`{
	"null": null,
	"bool_true": true,
	"bool_false": false,
	"num_int": 123,
	"num_float": 123.321,
	"str": "escaped_\"ws",
	"str_int": "123",
	"str_float": "123.321",
	"str_date_1": "2009-07-13",
	"str_date_2": "13/07/2009",
	"str_time_rfc3339_1": "2009-07-13T09:07:13Z",
	"str_time_rfc3339_2": "2009-07-13T09:07:13.123+08:00",
	"str_time_clickhouse_1": "2009-07-13 09:07:13",
	"str_time_clickhouse_2": "2009-07-13 09:07:13.123",
	"obj": {"i":[1,2,3],"f":[1.1,2.2,3.3],"s":["aa","bb","cc"],"e":[]},
	"array_empty": [],
	"array_null": [null],
	"array_bool": [true,false],
	"array_num_int_1": [0, 255, 256, 65535, 65536, 4294967295, 4294967296, 18446744073709551615, 18446744073709551616],
	"array_num_int_2": [-9223372036854775808, -2147483649, -2147483648, -32769, -32768, -129, -128, 0, 127, 128, 32767, 32768, 2147483647, 2147483648, 9223372036854775807],
	"array_num_float": [4.940656458412465441765687928682213723651e-324, 1.401298464324817070923729583289916131280e-45, 0.0, 3.40282346638528859811704183484516925440e+38, 1.797693134862315708145274237317043567981e+308],
	"array_str": ["aa","bb","cc"],
	"array_str_int_1": ["0", "255", "256", "65535", "65536", "4294967295", "4294967296", "18446744073709551615", "18446744073709551616"],
	"array_str_int_2": ["-9223372036854775808", "-2147483649", "-2147483648", "-32769", "-32768", "-129", "-128", "0", "127", "128", "32767", "32768", "2147483647", "2147483648", "9223372036854775807"],
	"array_str_float": ["4.940656458412465441765687928682213723651e-324", "1.401298464324817070923729583289916131280e-45", "0.0", "3.40282346638528859811704183484516925440e+38", "1.797693134862315708145274237317043567981e+308"],
	"array_str_date_1": ["2009-07-13","2009-07-14","2009-07-15"],
	"array_str_date_2": ["13/07/2009","14/07/2009","15/07/2009"],
	"array_str_time_rfc3339": ["2009-07-13T09:07:13Z", "2009-07-13T09:07:13+08:00", "2009-07-13T09:07:13.123Z", "2009-07-13T09:07:13.123+08:00"],
	"array_str_time_clickhouse": ["2009-07-13 09:07:13", "2009-07-13 09:07:13.123"],
	"array_obj": [{"i":[1,2,3],"f":[1.1,2.2,3.3]},{"s":["aa","bb","cc"],"e":[]}]
}`)

var jsonSchema = map[string]string{
	"null":                      "null",
	"bool_true":                 "true",
	"bool_false":                "false",
	"num_int":                   "number",
	"num_float":                 "number",
	"str":                       "string",
	"str_int":                   "string",
	"str_float":                 "string",
	"str_date_1":                "string",
	"str_date_2":                "string",
	"str_time_rfc3339_1":        "string",
	"str_time_rfc3339_2":        "string",
	"str_time_clickhouse_1":     "string",
	"str_time_clickhouse_2":     "string",
	"obj":                       "object",
	"array_empty":               "array",
	"array_null":                "array",
	"array_bool":                "array",
	"array_num_int_1":           "array",
	"array_num_int_2":           "array",
	"array_num_float":           "array",
	"array_str":                 "array",
	"array_str_int_1":           "array",
	"array_str_int_2":           "array",
	"array_str_float":           "array",
	"array_str_date_1":          "array",
	"array_str_date_2":          "array",
	"array_str_time_rfc3339":    "array",
	"array_str_time_clickhouse": "array",
	"array_obj":                 "array",
}

var csvSample = []byte(`null,true,false,123,123.321,"escaped_""ws",123,123.321,2009-07-13,13/07/2009,2009-07-13T09:07:13Z,2009-07-13T09:07:13.123+08:00,2009-07-13 09:07:13,2009-07-13 09:07:13.123,"{""i"":[1,2,3],""f"":[1.1,2.2,3.3],""s"":[""aa"",""bb"",""cc""],""e"":[]}",[],[null],"[true,false]","[0,255,256,65535,65536,4294967295,4294967296,18446744073709551615,18446744073709551616]","[-9223372036854775808,-2147483649,-2147483648,-32769,-32768,-129,-128,0,127,128,32767,32768,2147483647,2147483648,9223372036854775807]","[4.940656458412465441765687928682213723651e-324,1.401298464324817070923729583289916131280e-45,0.0,3.40282346638528859811704183484516925440e+38,1.797693134862315708145274237317043567981e+308]","[""aa"",""bb"",""cc""]","[""0"",""255"",""256"",""65535"",""65536"",""4294967295"",""4294967296"",""18446744073709551615"",""18446744073709551616""]","[""-9223372036854775808"",""-2147483649"",""-2147483648"",""-32769"",""-32768"",""-129"",""-128"",""0"",""127"",""128"",""32767"",""32768"",""2147483647"",""2147483648"",""9223372036854775807""]","[""4.940656458412465441765687928682213723651e-324"",""1.401298464324817070923729583289916131280e-45"",""0.0"",""3.40282346638528859811704183484516925440e+38"",""1.797693134862315708145274237317043567981e+308""]","[""2009-07-13"",""2009-07-14"",""2009-07-15""]","[""13/07/2009"",""14/07/2009"",""15/07/2009""]","[""2009-07-13T09:07:13Z"",""2009-07-13T09:07:13+08:00"",""2009-07-13T09:07:13.123Z"",""2009-07-13T09:07:13.123+08:00""]","[""2009-07-13 09:07:13"",""2009-07-13 09:07:13.123""]","[{""i"":[1,2,3],""f"":[1.1,2.2,3.3]},{""s"":[""aa"",""bb"",""cc""],""e"":[]}]"`)

var csvSchema = []string{
	"null",
	"bool_true",
	"bool_false",
	"num_int",
	"num_float",
	"str",
	"str_int",
	"str_float",
	"str_date_1",
	"str_date_2",
	"str_time_rfc3339_1",
	"str_time_rfc3339_2",
	"str_time_clickhouse_1",
	"str_time_clickhouse_2",
	"obj",
	"array_empty",
	"array_null",
	"array_bool",
	"array_num_int_1",
	"array_num_int_2",
	"array_num_float",
	"array_str",
	"array_str_int_1",
	"array_str_int_2",
	"array_str_float",
	"array_str_date_1",
	"array_str_date_2",
	"array_str_time_rfc3339",
	"array_str_time_clickhouse",
	"array_obj",
}

var (
	bdUtcNs       = time.Date(2009, 7, 13, 9, 7, 13, 123000000, time.UTC)
	bdUtcSec      = bdUtcNs.Truncate(1 * time.Second)
	bdShNsOrig    = time.Date(2009, 7, 13, 9, 7, 13, 123000000, time.FixedZone("CST", 8*60*60))
	bdShNs        = bdShNsOrig.UTC()
	bdShSec       = bdShNsOrig.Truncate(1 * time.Second).UTC()
	bdShMin       = bdShNsOrig.Truncate(1 * time.Minute).UTC()
	bdLocalNsOrig = time.Date(2009, 7, 13, 9, 7, 13, 123000000, time.Local)
	bdLocalNs     = bdLocalNsOrig.UTC()
	bdLocalSec    = bdLocalNsOrig.Truncate(1 * time.Second).UTC()
	bdLocalDate   = time.Date(2009, 7, 13, 0, 0, 0, 0, time.Local).UTC()
)

var initialize sync.Once
var initErr error
var names = []string{"fastjson", "gjson", "csv"}
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

type DateTimeCase struct {
	TS     string
	ExpVal time.Time
}

func initMetrics() {
	var pp *Pool
	var parser Parser
	var metric model.Metric
	var sample []byte
	metrics = make(map[string]model.Metric)
	for _, name := range names {
		switch name {
		case "csv":
			pp, _ = NewParserPool("csv", csvSchema, ",", "")
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
			msg := fmt.Sprintf("parser.Parse failed: %+v\n", initErr)
			panic(msg)
		}
		metrics[name] = metric
	}
}

func sliceContains(list []string, target string) bool {
	for _, s := range list {
		if s == target {
			return true
		}
	}
	return false
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
			desc := fmt.Sprintf(`%s %s("%s", %s)`, name, method, testCases[j].Field, strconv.FormatBool(testCases[j].Nullable))
			if name == "csv" && sliceContains([]string{"GetInt", "GetFloat", "GetDateTime", "GetElasticDateTime"}, method) && sliceContains([]string{"str_int", "str_float"}, testCases[j].Field) {
				log.Printf("%s is known to not compatible with fastjson parser, skipping", desc)
				continue
			}
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
		// nullable: false
		{"not_exist", false, int64(0)},
		{"null", false, int64(0)},
		{"bool_true", false, int64(1)},
		{"bool_false", false, int64(0)},
		{"num_int", false, int64(123)},
		{"num_float", false, int64(0)},
		{"str", false, int64(0)},
		{"str_int", false, int64(0)},
		{"str_float", false, int64(0)},
		{"str_date_1", false, int64(0)},
		{"obj", false, int64(0)},
		{"array_empty", false, int64(0)},
		// nullable: true
		{"not_exist", true, nil},
		{"null", true, nil},
		{"bool_true", true, int64(1)},
		{"bool_false", true, int64(0)},
		{"num_int", true, int64(123)},
		{"num_float", true, int64(0)},
		{"str", true, int64(0)},
		{"str_int", true, int64(0)},
		{"str_float", true, int64(0)},
		{"str_date_1", true, int64(0)},
		{"obj", true, int64(0)},
		{"array_empty", true, int64(0)},
	}
	doTestSimple(t, "GetInt", testCases)
}

func TestParserFloat(t *testing.T) {
	testCases := []SimpleCase{
		// nullable: false
		{"not_exist", false, 0.0},
		{"null", false, 0.0},
		{"bool_true", false, 0.0},
		{"bool_false", false, 0.0},
		{"num_int", false, 123.0},
		{"num_float", false, 123.321},
		{"str", false, 0.0},
		{"str_int", false, 0.0},
		{"str_float", false, 0.0},
		{"str_date_1", false, 0.0},
		{"obj", false, 0.0},
		{"array_empty", false, 0.0},
		// nullable: true
		{"not_exist", true, nil},
		{"null", true, nil},
		{"bool_true", true, 0.0},
		{"bool_false", true, 0.0},
		{"num_int", true, 123.0},
		{"num_float", true, 123.321},
		{"str", true, 0.0},
		{"str_int", true, 0.0},
		{"str_float", true, 0.0},
		{"str_date_1", true, 0.0},
		{"obj", true, 0.0},
		{"array_empty", true, 0.0},
	}
	doTestSimple(t, "GetFloat", testCases)
}

func TestParserString(t *testing.T) {
	testCases := []SimpleCase{
		// nullable: false
		{"not_exist", false, ""},
		{"null", false, ""},
		{"bool_true", false, "true"},
		{"bool_false", false, "false"},
		{"num_int", false, "123"},
		{"num_float", false, "123.321"},
		{"str", false, `escaped_"ws`},
		{"str_int", false, "123"},
		{"str_float", false, "123.321"},
		{"str_date_1", false, "2009-07-13"},
		{"obj", false, `{"i":[1,2,3],"f":[1.1,2.2,3.3],"s":["aa","bb","cc"],"e":[]}`},
		{"array_empty", false, "[]"},
		{"array_null", false, "[null]"},
		{"array_bool", false, "[true,false]"},
		{"array_str", false, `["aa","bb","cc"]`},
		// nullable: true
		{"not_exist", true, nil},
		{"null", true, nil},
		{"bool_true", true, "true"},
		{"bool_false", true, "false"},
		{"num_int", true, "123"},
		{"num_float", true, "123.321"},
		{"str", true, `escaped_"ws`},
		{"str_int", true, "123"},
		{"str_float", true, "123.321"},
		{"str_date_1", true, "2009-07-13"},
		{"obj", true, `{"i":[1,2,3],"f":[1.1,2.2,3.3],"s":["aa","bb","cc"],"e":[]}`},
		{"array_empty", true, "[]"},
		{"array_null", true, "[null]"},
		{"array_bool", true, "[true,false]"},
		{"array_str", true, `["aa","bb","cc"]`},
	}
	doTestSimple(t, "GetString", testCases)
}

func TestParserDateTime(t *testing.T) {
	testCases := []SimpleCase{
		// nullable: false
		{"not_exist", false, Epoch},
		{"null", false, Epoch},
		{"bool_true", false, Epoch},
		{"bool_false", false, Epoch},
		{"num_int", false, UnixInt(123)},
		{"num_float", false, UnixFloat(123.321)},
		{"str", false, Epoch},
		{"str_int", false, Epoch},
		{"str_float", false, Epoch},
		{"str_date_1", false, bdLocalDate},
		{"str_time_rfc3339_1", false, bdUtcSec},
		{"str_time_rfc3339_2", false, bdShNs},
		{"str_time_clickhouse_1", false, bdLocalSec},
		{"str_time_clickhouse_2", false, bdLocalNs},
		{"obj", false, Epoch},
		{"array_empty", false, Epoch},
		// nullable: true
		{"not_exist", true, nil},
		{"null", true, nil},
		{"bool_true", true, Epoch},
		{"bool_false", true, Epoch},
		{"num_int", true, UnixInt(123)},
		{"num_float", true, UnixFloat(123.321)},
		{"str", true, Epoch},
		{"str_int", true, Epoch},
		{"str_float", true, Epoch},
		{"str_date_1", true, bdLocalDate},
		{"str_time_rfc3339_1", true, bdUtcSec},
		{"str_time_rfc3339_2", true, bdShNs},
		{"str_time_clickhouse_1", true, bdLocalSec},
		{"str_time_clickhouse_2", true, bdLocalNs},
		{"obj", true, Epoch},
		{"array_empty", true, Epoch},
	}
	doTestSimple(t, "GetDateTime", testCases)
}

func TestParserElasticDateTime(t *testing.T) {
	testCases := []SimpleCase{
		// nullable: false
		{"not_exist", false, Epoch.Unix()},
		{"null", false, Epoch.Unix()},
		{"bool_true", false, Epoch.Unix()},
		{"bool_false", false, Epoch.Unix()},
		{"num_int", false, UnixInt(123).Unix()},
		{"num_float", false, UnixFloat(123.321).Unix()},
		{"str", false, Epoch.Unix()},
		{"str_int", false, Epoch.Unix()},
		{"str_float", false, Epoch.Unix()},
		{"str_date_1", false, bdLocalDate.Unix()},
		{"str_time_rfc3339_1", false, bdUtcSec.Unix()},
		{"str_time_rfc3339_2", false, bdShNs.Unix()},
		{"str_time_clickhouse_1", false, bdLocalSec.Unix()},
		{"str_time_clickhouse_2", false, bdLocalNs.Unix()},
		{"obj", false, Epoch.Unix()},
		{"array_empty", false, Epoch.Unix()},
		// nullable: true
		{"not_exist", true, nil},
		{"null", true, nil},
		{"bool_true", true, Epoch.Unix()},
		{"bool_false", true, Epoch.Unix()},
		{"num_int", true, UnixInt(123).Unix()},
		{"num_float", true, UnixFloat(123.321).Unix()},
		{"str", true, Epoch.Unix()},
		{"str_int", true, Epoch.Unix()},
		{"str_float", true, Epoch.Unix()},
		{"str_date_1", true, bdLocalDate.Unix()},
		{"str_time_rfc3339_1", true, bdUtcSec.Unix()},
		{"str_time_rfc3339_2", true, bdShNs.Unix()},
		{"str_time_clickhouse_1", true, bdLocalSec.Unix()},
		{"str_time_clickhouse_2", true, bdLocalNs.Unix()},
		{"obj", true, Epoch.Unix()},
		{"array_empty", true, Epoch.Unix()},
	}
	doTestSimple(t, "GetElasticDateTime", testCases)
}

func TestParserArray(t *testing.T) {
	initialize.Do(initMetrics)
	require.Nil(t, initErr)

	testCases := []ArrayCase{
		{"not_exist", model.Float, []float64{}},
		{"null", model.Float, []float64{}},
		{"num_int", model.Int, []int64{}},
		{"num_float", model.Float, []float64{}},
		{"str", model.String, []string{}},
		{"str_int", model.String, []string{}},
		{"str_date_1", model.DateTime, []time.Time{}},
		{"obj", model.String, []string{}},

		{"array_empty", model.Int, []int64{}},
		{"array_empty", model.Float, []float64{}},
		{"array_empty", model.String, []string{}},
		{"array_empty", model.DateTime, []time.Time{}},

		{"array_null", model.Int, []int64{0}},
		{"array_null", model.Float, []float64{0.0}},
		{"array_null", model.String, []string{""}},
		{"array_null", model.DateTime, []time.Time{Epoch}},

		{"array_bool", model.Int, []int64{1, 0}},
		{"array_bool", model.Float, []float64{0.0, 0.0}},
		{"array_bool", model.String, []string{"true", "false"}},
		{"array_bool", model.DateTime, []time.Time{Epoch, Epoch}},

		{"array_num_int_1", model.Int, []int64{0, 255, 256, 65535, 65536, 4294967295, 4294967296, 0, 0}},
		{"array_num_int_1", model.Float, []float64{0, 255, 256, 65535, 65536, 4294967295, 4294967296, 18446744073709551615, 18446744073709551616}},
		{"array_num_int_1", model.String, []string{"0", "255", "256", "65535", "65536", "4294967295", "4294967296", "18446744073709551615", "18446744073709551616"}},
		{"array_num_int_1", model.DateTime, []time.Time{Epoch, UnixInt(255), UnixInt(256), UnixInt(65535), UnixInt(65536), UnixInt(4294967295), UnixInt(4294967296), Epoch, Epoch}},

		{"array_num_int_2", model.Int, []int64{-9223372036854775808, -2147483649, -2147483648, -32769, -32768, -129, -128, 0, 127, 128, 32767, 32768, 2147483647, 2147483648, 9223372036854775807}},
		{"array_num_int_2", model.Float, []float64{-9223372036854775808, -2147483649, -2147483648, -32769, -32768, -129, -128, 0, 127, 128, 32767, 32768, 2147483647, 2147483648, 9223372036854775807}},
		{"array_num_int_2", model.String, []string{"-9223372036854775808", "-2147483649", "-2147483648", "-32769", "-32768", "-129", "-128", "0", "127", "128", "32767", "32768", "2147483647", "2147483648", "9223372036854775807"}},
		{"array_num_int_2", model.DateTime, []time.Time{Epoch, Epoch, Epoch, Epoch, Epoch, Epoch, Epoch, Epoch, UnixInt(127), UnixInt(128), UnixInt(32767), UnixInt(32768), UnixInt(2147483647), UnixInt(2147483648), UnixInt(9223372036854775807)}},

		{"array_num_float", model.Int, []int64{0, 0, 0, 0, 0}},
		{"array_num_float", model.Float, []float64{4.940656458412465441765687928682213723651e-324, 1.401298464324817070923729583289916131280e-45, 0.0, 3.40282346638528859811704183484516925440e+38, 1.797693134862315708145274237317043567981e+308}},
		{"array_num_float", model.String, []string{"4.940656458412465441765687928682213723651e-324", "1.401298464324817070923729583289916131280e-45", "0.0", "3.40282346638528859811704183484516925440e+38", "1.797693134862315708145274237317043567981e+308"}},
		{"array_num_float", model.DateTime, []time.Time{Epoch, Epoch, Epoch, UnixFloat(3.40282346638528859811704183484516925440e+38), UnixFloat(1.797693134862315708145274237317043567981e+308)}},

		{"array_str", model.Int, []int64{0, 0, 0}},
		{"array_str", model.Float, []float64{0.0, 0.0, 0.0}},
		{"array_str", model.String, []string{"aa", "bb", "cc"}},
		{"array_str", model.DateTime, []time.Time{Epoch, Epoch, Epoch}},

		{"array_str_int_1", model.Int, []int64{0, 0, 0, 0, 0, 0, 0, 0, 0}},
		{"array_str_int_1", model.Float, []float64{0, 0, 0, 0, 0, 0, 0, 0, 0}},
		{"array_str_int_1", model.String, []string{"0", "255", "256", "65535", "65536", "4294967295", "4294967296", "18446744073709551615", "18446744073709551616"}},
		{"array_str_int_1", model.DateTime, []time.Time{Epoch, Epoch, Epoch, Epoch, Epoch, Epoch, Epoch, Epoch, Epoch}},

		{"array_str_int_2", model.Int, []int64{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}},
		{"array_str_int_2", model.Float, []float64{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}},
		{"array_str_int_2", model.String, []string{"-9223372036854775808", "-2147483649", "-2147483648", "-32769", "-32768", "-129", "-128", "0", "127", "128", "32767", "32768", "2147483647", "2147483648", "9223372036854775807"}},
		{"array_str_int_2", model.DateTime, []time.Time{Epoch, Epoch, Epoch, Epoch, Epoch, Epoch, Epoch, Epoch, Epoch, Epoch, Epoch, Epoch, Epoch, Epoch, Epoch}},

		{"array_str_float", model.Int, []int64{0, 0, 0, 0, 0}},
		{"array_str_float", model.Float, []float64{0, 0, 0, 0, 0}},
		{"array_str_float", model.String, []string{"4.940656458412465441765687928682213723651e-324", "1.401298464324817070923729583289916131280e-45", "0.0", "3.40282346638528859811704183484516925440e+38", "1.797693134862315708145274237317043567981e+308"}},
		{"array_str_float", model.DateTime, []time.Time{Epoch, Epoch, Epoch, Epoch, Epoch}},

		{"array_str_date_1", model.DateTime, []time.Time{bdLocalDate, bdLocalDate.Add(24 * time.Hour), bdLocalDate.Add(48 * time.Hour)}},
		{"array_str_date_2", model.DateTime, []time.Time{bdLocalDate, bdLocalDate.Add(24 * time.Hour), bdLocalDate.Add(48 * time.Hour)}},
		{"array_str_time_rfc3339", model.DateTime, []time.Time{bdUtcSec, bdShSec, bdUtcNs, bdShNs}},
		{"array_str_time_clickhouse", model.DateTime, []time.Time{bdLocalSec, bdLocalNs}},
	}

	for i := range names {
		name := names[i]
		metric := metrics[name]
		for j := range testCases {
			if name == "csv" && testCases[j].Field == "array_obj" {
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

func TestParseDateTime(t *testing.T) {
	// https://en.wikipedia.org/wiki/List_of_tz_database_time_zones
	// https://en.wikipedia.org/wiki/List_of_time_zone_abbreviations, "not part of the international time and date standard  ISO 8601 and their use as sole designator for a time zone is discouraged."
	savedLocal := time.Local
	defer func() {
		time.Local = savedLocal
	}()
	locations := []string{"UTC", "Asia/Shanghai", "Europe/Moscow", "America/Los_Angeles"}
	for _, location := range locations {
		// change timezone settings programmatically
		loc, err := time.LoadLocation(location)
		require.Nil(t, err, fmt.Sprintf("time.LoadLocation(%s)", location))
		time.Local = loc

		bdLocalNsOrig = time.Date(2009, 7, 13, 9, 7, 13, 123000000, time.Local)
		bdLocalNs = bdLocalNsOrig.UTC()
		bdLocalSec = bdLocalNsOrig.Truncate(1 * time.Second).UTC()
		bdLocalDate = time.Date(2009, 7, 13, 0, 0, 0, 0, time.Local).UTC()

		testCases := []DateTimeCase{
			//DateTime, RFC3339
			{"2009-07-13T09:07:13.123+08:00", bdShNs},
			{"2009-07-13T09:07:13.123+0800", bdShNs},
			{"2009-07-13T09:07:13+08:00", bdShSec},
			{"2009-07-13T09:07:13+0800", bdShSec},
			{"2009-07-13T09:07:13.123Z", bdUtcNs},
			{"2009-07-13T09:07:13Z", bdUtcSec},
			{"2009-07-13T09:07:13.123", bdLocalNs},
			{"2009-07-13T09:07:13", bdLocalSec},
			//DateTime, ISO8601
			{"2009-07-13 09:07:13.123+08:00", bdShNs},
			{"2009-07-13 09:07:13.123+0800", bdShNs},
			{"2009-07-13 09:07:13+08:00", bdShSec},
			{"2009-07-13 09:07:13+0800", bdShSec},
			{"2009-07-13 09:07:13.123Z", bdUtcNs},
			{"2009-07-13 09:07:13Z", bdUtcSec},
			{"2009-07-13 09:07:13.123", bdLocalNs},
			{"2009-07-13 09:07:13", bdLocalSec},
			//DateTime, other layouts supported by golang
			{"Mon Jul 13 09:07:13 2009", bdLocalSec},
			{"Mon Jul 13 09:07:13 CST 2009", bdShSec},
			{"Mon Jul 13 09:07:13 +0800 2009", bdShSec},
			{"13 Jul 09 09:07 CST", bdShMin},
			{"13 Jul 09 09:07 +0800", bdShMin},
			{"Monday, 13-Jul-09 09:07:13 CST", bdShSec},
			{"Mon, 13 Jul 2009 09:07:13 CST", bdShSec},
			{"Mon, 13 Jul 2009 09:07:13 +0800", bdShSec},
			//DateTime, linux utils
			{"Mon 13 Jul 2009 09:07:13 AM CST", bdShSec},
			{"Mon Jul 13 09:07:13 CST 2009", bdShSec},
			//DateTime, home-brewed
			{"Jul 13, 2009 09:07:13.123+08:00", bdShNs},
			{"Jul 13, 2009 09:07:13.123+0800", bdShNs},
			{"Jul 13, 2009 09:07:13+08:00", bdShSec},
			{"Jul 13, 2009 09:07:13+0800", bdShSec},
			{"Jul 13, 2009 09:07:13.123Z", bdUtcNs},
			{"Jul 13, 2009 09:07:13Z", bdUtcSec},
			{"Jul 13, 2009 09:07:13.123", bdLocalNs},
			{"Jul 13, 2009 09:07:13", bdLocalSec},
			{"13/Jul/2009 09:07:13.123 +08:00", bdShNs},
			{"13/Jul/2009 09:07:13.123 +0800", bdShNs},
			{"13/Jul/2009 09:07:13 +08:00", bdShSec},
			{"13/Jul/2009 09:07:13 +0800", bdShSec},
			{"13/Jul/2009 09:07:13.123 Z", bdUtcNs},
			{"13/Jul/2009 09:07:13 Z", bdUtcSec},
			{"13/Jul/2009 09:07:13.123", bdLocalNs},
			{"13/Jul/2009 09:07:13", bdLocalSec},
			//Date
			{"2009-07-13", bdLocalDate},
			{"13/07/2009", bdLocalDate},
			{"13/Jul/2009", bdLocalDate},
			{"Jul 13, 2009", bdLocalDate},
			{"Mon Jul 13, 2009", bdLocalDate},
		}

		for _, tc := range testCases {
			v, layout := parseInLocation(tc.TS, time.Local)
			desc := fmt.Sprintf(`parseInLocation("%s", "%s") = %s(layout: %s), expect %s`, tc.TS, location, v.Format(time.RFC3339Nano), layout, tc.ExpVal.Format(time.RFC3339Nano))
			if strings.Contains(tc.TS, "CST") && v != tc.ExpVal {
				log.Printf(desc + "(CST is ambiguous)")
			} else {
				require.Equal(t, tc.ExpVal, v, desc)
			}
		}
	}
}

func BenchmarkUnmarshalljson(b *testing.B) {
	object := map[string]interface{}{}
	for i := 0; i < b.N; i++ {
		_ = json.Unmarshal(jsonSample, &object)
	}
}

func BenchmarkUnmarshallFastJson(b *testing.B) {
	str := string(jsonSample)
	var p fastjson.Parser
	for i := 0; i < b.N; i++ {
		v, err := p.Parse(str)
		if err != nil {
			panic(err)
		}
		v.GetInt("null")
		v.GetInt("bool_true")
		v.GetInt("num_int")
		v.GetFloat64("num_float")
		v.GetStringBytes("str")
		v.GetStringBytes("str_float")
	}
}

// 字段个数较少的情况下，直接Get性能更好
func BenchmarkUnmarshallGjson(b *testing.B) {
	str := string(jsonSample)
	for i := 0; i < b.N; i++ {
		_ = gjson.Get(str, "null").Int()
		_ = gjson.Get(str, "bool_true").Int()
		_ = gjson.Get(str, "num_int").Int()
		_ = gjson.Get(str, "num_float").Float()
		_ = gjson.Get(str, "str").String()
		_ = gjson.Get(str, "str_float").String()
	}
}

func BenchmarkUnmarshalGabon2(b *testing.B) {
	str := string(jsonSample)
	for i := 0; i < b.N; i++ {
		result := gjson.Parse(str).Map()
		_ = result["null"].Int()
		_ = result["bool_true"].Int()
		_ = result["num_int"].Int()
		_ = result["num_float"].Float()
		_ = result["str"].String()
		_ = result["str_float"].String()
	}
}
