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
	"errors"
	"fmt"
	"log"
	"math"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/housepower/clickhouse_sinker/model"
	"github.com/housepower/clickhouse_sinker/util"
	"github.com/stretchr/testify/assert"
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
	"array_num_float": [4.940656458412465441765687928682213723651e-324, 1.401298464324817070923729583289916131280e-45, 0.0, 3.40282346638528859811704183484516925440e+38, 1.797693134862315708145274237317043567981e+308, -inf, +inf],
	"array_str": ["aa","bb","cc"],
	"array_str_int_1": ["0", "255", "256", "65535", "65536", "4294967295", "4294967296", "18446744073709551615", "18446744073709551616"],
	"array_str_int_2": ["-9223372036854775808", "-2147483649", "-2147483648", "-32769", "-32768", "-129", "-128", "0", "127", "128", "32767", "32768", "2147483647", "2147483648", "9223372036854775807"],
	"array_str_float": ["4.940656458412465441765687928682213723651e-324", "1.401298464324817070923729583289916131280e-45", "0.0", "3.40282346638528859811704183484516925440e+38", "1.797693134862315708145274237317043567981e+308", "-inf", "+inf"],
	"array_str_date_1": ["2009-07-13","2009-07-14","2009-07-15"],
	"array_str_date_2": ["13/07/2009","14/07/2009","15/07/2009"],
	"array_str_time_rfc3339": ["2009-07-13T09:07:13Z", "2009-07-13T09:07:13+08:00", "2009-07-13T09:07:13.123Z", "2009-07-13T09:07:13.123+08:00"],
	"array_str_time_clickhouse": ["2009-07-13 09:07:13", "2009-07-13 09:07:13.123"],
	"array_obj": [{"i":[1,2,3],"f":[1.1,2.2,3.3]},{"s":["aa","bb","cc"],"e":[]}]
}`)

var jsonSchema = map[string]string{
	"null":                      "Unknown",
	"bool_true":                 "Bool",
	"bool_false":                "Bool",
	"num_int":                   "Int64",
	"num_float":                 "Float64",
	"str":                       "String",
	"str_int":                   "String",
	"str_float":                 "String",
	"str_date_1":                "DateTime",
	"str_date_2":                "DateTime",
	"str_time_rfc3339_1":        "DateTime",
	"str_time_rfc3339_2":        "DateTime",
	"str_time_clickhouse_1":     "DateTime",
	"str_time_clickhouse_2":     "DateTime",
	"obj":                       "Unknown",
	"array_empty":               "Unknown",
	"array_null":                "Unknown",
	"array_bool":                "BoolArray",
	"array_num_int_1":           "Int64Array",
	"array_num_int_2":           "Int64Array",
	"array_num_float":           "Float64Array",
	"array_str":                 "StringArray",
	"array_str_int_1":           "StringArray",
	"array_str_int_2":           "StringArray",
	"array_str_float":           "StringArray",
	"array_str_date_1":          "DateTimeArray",
	"array_str_date_2":          "DateTimeArray",
	"array_str_time_rfc3339":    "DateTimeArray",
	"array_str_time_clickhouse": "DateTimeArray",
	"array_obj":                 "Unknown",
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
	timeUnit      = float64(0.000001)
)

var initialize sync.Once
var errInit error
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
			pp, _ = NewParserPool("csv", csvSchema, ",", "", timeUnit)
			sample = csvSample
		case "fastjson":
			pp, _ = NewParserPool("fastjson", nil, "", "", timeUnit)
			sample = jsonSample
		case "gjson":
			pp, _ = NewParserPool("gjson", nil, "", "", timeUnit)
			sample = jsonSample
		}
		parser = pp.Get()
		if metric, errInit = parser.Parse(sample); errInit != nil {
			msg := fmt.Sprintf("parser.Parse failed: %+v\n", errInit)
			panic(msg)
		}
		metrics[name] = metric
	}
	util.InitLogger([]string{"stdout"})
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
	require.Nil(t, errInit)
	for i := range names {
		name := names[i]
		metric := metrics[name]
		var skipped []string
		for j := range testCases {
			var v interface{}
			desc := fmt.Sprintf(`%s.%s("%s", %s)`, name, method, testCases[j].Field, strconv.FormatBool(testCases[j].Nullable))
			if name == "csv" && (sliceContains([]string{"GetBool", "GetInt64", "GetFloat64", "GetDateTime"}, method) && sliceContains([]string{"str_int", "str_float"}, testCases[j].Field) || testCases[j].Nullable) {
				skipped = append(skipped, desc)
				continue
			}
			switch method {
			case "GetBool":
				v = metric.GetBool(testCases[j].Field, testCases[j].Nullable)
			case "GetInt8":
				v = metric.GetInt8(testCases[j].Field, testCases[j].Nullable)
			case "GetInt16":
				v = metric.GetInt16(testCases[j].Field, testCases[j].Nullable)
			case "GetInt32":
				v = metric.GetInt32(testCases[j].Field, testCases[j].Nullable)
			case "GetInt64":
				v = metric.GetInt64(testCases[j].Field, testCases[j].Nullable)
			case "GetUint8":
				v = metric.GetUint8(testCases[j].Field, testCases[j].Nullable)
			case "GetUint16":
				v = metric.GetUint16(testCases[j].Field, testCases[j].Nullable)
			case "GetUint32":
				v = metric.GetUint32(testCases[j].Field, testCases[j].Nullable)
			case "GetUint64":
				v = metric.GetUint64(testCases[j].Field, testCases[j].Nullable)
			case "GetFloat32":
				v = metric.GetFloat32(testCases[j].Field, testCases[j].Nullable)
			case "GetFloat64":
				v = metric.GetFloat64(testCases[j].Field, testCases[j].Nullable)
			case "GetDateTime":
				v = metric.GetDateTime(testCases[j].Field, testCases[j].Nullable)
			case "GetString":
				v = metric.GetString(testCases[j].Field, testCases[j].Nullable)
			default:
				panic("error!")
			}
			require.Equal(t, testCases[j].ExpVal, v, desc)
		}
		if skipped != nil {
			log.Printf("Skipped %d cases incompatible with fastjson parser: %v\n", len(skipped), strings.Join(skipped, ", "))
		}
	}
}

func TestParserBool(t *testing.T) {
	testCases := []SimpleCase{
		// nullable: false
		{"not_exist", false, false},
		{"null", false, false},
		{"bool_true", false, true},
		{"bool_false", false, false},
		{"num_int", false, false},
		{"num_float", false, false},
		{"str", false, false},
		{"str_int", false, false},
		{"str_float", false, false},
		{"str_date_1", false, false},
		{"obj", false, false},
		{"array_empty", false, false},
		// nullable: true
		{"not_exist", true, nil},
		{"null", true, nil},
		{"bool_true", true, true},
		{"bool_false", true, false},
		{"num_int", true, nil},
		{"num_float", true, nil},
		{"str", true, nil},
		{"str_int", true, nil},
		{"str_float", true, nil},
		{"str_date_1", true, nil},
		{"obj", true, nil},
		{"array_empty", true, nil},
	}
	doTestSimple(t, "GetBool", testCases)
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
		{"num_float", true, nil},
		{"str", true, nil},
		{"str_int", true, nil},
		{"str_float", true, nil},
		{"str_date_1", true, nil},
		{"obj", true, nil},
		{"array_empty", true, nil},
	}
	doTestSimple(t, "GetInt64", testCases)
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
		{"bool_true", true, nil},
		{"bool_false", true, nil},
		{"num_int", true, 123.0},
		{"num_float", true, 123.321},
		{"str", true, nil},
		{"str_int", true, nil},
		{"str_float", true, nil},
		{"str_date_1", true, nil},
		{"obj", true, nil},
		{"array_empty", true, nil},
	}
	doTestSimple(t, "GetFloat64", testCases)
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
		{"num_int", false, UnixFloat(123, timeUnit)},
		{"num_float", false, UnixFloat(123.321, timeUnit)},
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
		{"bool_true", true, nil},
		{"bool_false", true, nil},
		{"num_int", true, UnixFloat(123, timeUnit)},
		{"num_float", true, UnixFloat(123.321, timeUnit)},
		{"str", true, nil},
		{"str_int", true, nil},
		{"str_float", true, nil},
		{"str_date_1", true, bdLocalDate},
		{"str_time_rfc3339_1", true, bdUtcSec},
		{"str_time_rfc3339_2", true, bdShNs},
		{"str_time_clickhouse_1", true, bdLocalSec},
		{"str_time_clickhouse_2", true, bdLocalNs},
		{"obj", true, nil},
		{"array_empty", true, nil},
	}
	doTestSimple(t, "GetDateTime", testCases)
}

func TestParserArray(t *testing.T) {
	initialize.Do(initMetrics)
	require.Nil(t, errInit)

	testCases := []ArrayCase{
		{"not_exist", model.Float64, []float64{}},
		{"null", model.Float64, []float64{}},
		{"num_int", model.Int64, []int64{}},
		{"num_float", model.Float64, []float64{}},
		{"str", model.String, []string{}},
		{"str_int", model.String, []string{}},
		{"str_date_1", model.DateTime, []time.Time{}},
		{"obj", model.String, []string{}},

		{"array_empty", model.Bool, []bool{}},
		{"array_empty", model.Int64, []int64{}},
		{"array_empty", model.Float64, []float64{}},
		{"array_empty", model.String, []string{}},
		{"array_empty", model.DateTime, []time.Time{}},

		{"array_null", model.Bool, []bool{false}},
		{"array_null", model.Int64, []int64{0}},
		{"array_null", model.Float64, []float64{0.0}},
		{"array_null", model.String, []string{""}},
		{"array_null", model.DateTime, []time.Time{Epoch}},

		{"array_bool", model.Bool, []bool{true, false}},
		{"array_bool", model.Int64, []int64{1, 0}},
		{"array_bool", model.Float64, []float64{0.0, 0.0}},
		{"array_bool", model.String, []string{"true", "false"}},
		{"array_bool", model.DateTime, []time.Time{Epoch, Epoch}},

		{"array_num_int_1", model.Bool, []bool{false, false, false, false, false, false, false, false, false}},
		{"array_num_int_1", model.Int64, []int64{0, 255, 256, 65535, 65536, 4294967295, 4294967296, 0, 0}},
		{"array_num_int_1", model.Float64, []float64{0, 255, 256, 65535, 65536, 4294967295, 4294967296, 18446744073709551615, 18446744073709551616}},
		{"array_num_int_1", model.String, []string{"0", "255", "256", "65535", "65536", "4294967295", "4294967296", "18446744073709551615", "18446744073709551616"}},
		{"array_num_int_1", model.DateTime, []time.Time{Epoch, UnixFloat(255, timeUnit), UnixFloat(256, timeUnit), UnixFloat(65535, timeUnit), UnixFloat(65536, timeUnit), UnixFloat(4294967295, timeUnit), UnixFloat(4294967296, timeUnit), Epoch, Epoch}},

		{"array_num_int_2", model.Bool, []bool{false, false, false, false, false, false, false, false, false, false, false, false, false, false, false}},
		{"array_num_int_2", model.Int64, []int64{-9223372036854775808, -2147483649, -2147483648, -32769, -32768, -129, -128, 0, 127, 128, 32767, 32768, 2147483647, 2147483648, 9223372036854775807}},
		{"array_num_int_2", model.Float64, []float64{-9223372036854775808, -2147483649, -2147483648, -32769, -32768, -129, -128, 0, 127, 128, 32767, 32768, 2147483647, 2147483648, 9223372036854775807}},
		{"array_num_int_2", model.String, []string{"-9223372036854775808", "-2147483649", "-2147483648", "-32769", "-32768", "-129", "-128", "0", "127", "128", "32767", "32768", "2147483647", "2147483648", "9223372036854775807"}},
		{"array_num_int_2", model.DateTime, []time.Time{Epoch, Epoch, Epoch, Epoch, Epoch, Epoch, Epoch, Epoch, UnixFloat(127, timeUnit), UnixFloat(128, timeUnit), UnixFloat(32767, timeUnit), UnixFloat(32768, timeUnit), UnixFloat(2147483647, timeUnit), UnixFloat(2147483648, timeUnit), UnixFloat(9223372036854775807, timeUnit)}},

		{"array_num_float", model.Bool, []bool{false, false, false, false, false, false, false}},
		{"array_num_float", model.Int64, []int64{0, 0, 0, 0, 0, 0, 0}},
		{"array_num_float", model.Float64, []float64{4.940656458412465441765687928682213723651e-324, 1.401298464324817070923729583289916131280e-45, 0.0, 3.40282346638528859811704183484516925440e+38, 1.797693134862315708145274237317043567981e+308, math.Inf(-1), math.Inf(1)}},
		{"array_num_float", model.String, []string{"4.940656458412465441765687928682213723651e-324", "1.401298464324817070923729583289916131280e-45", "0.0", "3.40282346638528859811704183484516925440e+38", "1.797693134862315708145274237317043567981e+308", "-inf", "+inf"}},
		{"array_num_float", model.DateTime, []time.Time{Epoch, Epoch, Epoch, UnixFloat(3.40282346638528859811704183484516925440e+38, timeUnit), UnixFloat(1.797693134862315708145274237317043567981e+308, timeUnit), UnixFloat(math.Inf(-1), timeUnit), UnixFloat(math.Inf(1), timeUnit)}},

		{"array_str", model.Bool, []bool{false, false, false}},
		{"array_str", model.Int64, []int64{0, 0, 0}},
		{"array_str", model.Float64, []float64{0.0, 0.0, 0.0}},
		{"array_str", model.String, []string{"aa", "bb", "cc"}},
		{"array_str", model.DateTime, []time.Time{Epoch, Epoch, Epoch}},

		{"array_str_int_1", model.Bool, []bool{false, false, false, false, false, false, false, false, false}},
		{"array_str_int_1", model.Int64, []int64{0, 0, 0, 0, 0, 0, 0, 0, 0}},
		{"array_str_int_1", model.Float64, []float64{0, 0, 0, 0, 0, 0, 0, 0, 0}},
		{"array_str_int_1", model.String, []string{"0", "255", "256", "65535", "65536", "4294967295", "4294967296", "18446744073709551615", "18446744073709551616"}},
		{"array_str_int_1", model.DateTime, []time.Time{Epoch, Epoch, Epoch, Epoch, Epoch, Epoch, Epoch, Epoch, Epoch}},

		{"array_str_int_2", model.Bool, []bool{false, false, false, false, false, false, false, false, false, false, false, false, false, false, false}},
		{"array_str_int_2", model.Int64, []int64{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}},
		{"array_str_int_2", model.Float64, []float64{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}},
		{"array_str_int_2", model.String, []string{"-9223372036854775808", "-2147483649", "-2147483648", "-32769", "-32768", "-129", "-128", "0", "127", "128", "32767", "32768", "2147483647", "2147483648", "9223372036854775807"}},
		{"array_str_int_2", model.DateTime, []time.Time{Epoch, Epoch, Epoch, Epoch, Epoch, Epoch, Epoch, Epoch, Epoch, Epoch, Epoch, Epoch, Epoch, Epoch, Epoch}},

		{"array_str_float", model.Bool, []bool{false, false, false, false, false, false, false}},
		{"array_str_float", model.Int64, []int64{0, 0, 0, 0, 0, 0, 0}},
		{"array_str_float", model.Float64, []float64{0, 0, 0, 0, 0, 0, 0}},
		{"array_str_float", model.String, []string{"4.940656458412465441765687928682213723651e-324", "1.401298464324817070923729583289916131280e-45", "0.0", "3.40282346638528859811704183484516925440e+38", "1.797693134862315708145274237317043567981e+308", "-inf", "+inf"}},
		{"array_str_float", model.DateTime, []time.Time{Epoch, Epoch, Epoch, Epoch, Epoch, Epoch, Epoch}},

		{"array_str_date_1", model.DateTime, []time.Time{bdLocalDate, bdLocalDate.Add(24 * time.Hour), bdLocalDate.Add(48 * time.Hour)}},
		{"array_str_date_2", model.DateTime, []time.Time{bdLocalDate, bdLocalDate.Add(24 * time.Hour), bdLocalDate.Add(48 * time.Hour)}},
		{"array_str_time_rfc3339", model.DateTime, []time.Time{bdUtcSec, bdShSec, bdUtcNs, bdShNs}},
		{"array_str_time_clickhouse", model.DateTime, []time.Time{bdLocalSec, bdLocalNs}},
	}

	for i := range names {
		name := names[i]
		metric := metrics[name]
		var skipped []string
		for j := range testCases {
			var v interface{}
			desc := fmt.Sprintf(`%s.GetArray("%s", %s)`, name, testCases[j].Field, model.GetTypeName(testCases[j].Type))
			if (name == "gjson" && testCases[j].Field == "array_num_float") ||
				(name == "csv" && sliceContains([]string{"array_num_float", "array_str_float"}, testCases[j].Field)) {
				skipped = append(skipped, desc)
				continue
			}
			v = metric.GetArray(testCases[j].Field, testCases[j].Type)
			assert.Equal(t, testCases[j].ExpVal, v, desc)
		}
		if skipped != nil {
			log.Printf("Skipped %d cases incompatible with fastjson parser: %v\n", len(skipped), strings.Join(skipped, ", "))
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

func TestParseInt(t *testing.T) {
	arrayStrInt := []string{"invalid", "-9223372036854775809", "-9223372036854775808", "-2147483649", "-2147483648", "-32769", "-32768", "-129", "-128", "0", "127", "128", "255", "256", "32767", "32768", "65535", "65536", "2147483647", "2147483648", "4294967295", "4294967296", "9223372036854775807", "18446744073709551615", "18446744073709551616"}
	i8Exp := []int8{0, -128, -128, -128, -128, -128, -128, -128, -128, 0, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127}
	i16Exp := []int16{0, -32768, -32768, -32768, -32768, -32768, -32768, -129, -128, 0, 127, 128, 255, 256, 32767, 32767, 32767, 32767, 32767, 32767, 32767, 32767, 32767, 32767, 32767}
	i32Exp := []int32{0, -2147483648, -2147483648, -2147483648, -2147483648, -32769, -32768, -129, -128, 0, 127, 128, 255, 256, 32767, 32768, 65535, 65536, 2147483647, 2147483647, 2147483647, 2147483647, 2147483647, 2147483647, 2147483647}
	i64Exp := []int64{0, -9223372036854775808, -9223372036854775808, -2147483649, -2147483648, -32769, -32768, -129, -128, 0, 127, 128, 255, 256, 32767, 32768, 65535, 65536, 2147483647, 2147483648, 4294967295, 4294967296, 9223372036854775807, 9223372036854775807, 9223372036854775807}
	u8Exp := []uint8{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 127, 128, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255}
	u16Exp := []uint16{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 127, 128, 255, 256, 32767, 32768, 65535, 65535, 65535, 65535, 65535, 65535, 65535, 65535, 65535}
	u32Exp := []uint32{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 127, 128, 255, 256, 32767, 32768, 65535, 65536, 2147483647, 2147483648, 4294967295, 4294967295, 4294967295, 4294967295, 4294967295}
	u64Exp := []uint64{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 127, 128, 255, 256, 32767, 32768, 65535, 65536, 2147483647, 2147483648, 4294967295, 4294967296, 9223372036854775807, 18446744073709551615, 18446744073709551615}

	for _, bitSize := range []int{8, 16, 32, 64} {
		for i, s := range arrayStrInt {
			var iv int64
			var uv uint64
			var ivErr, uvErr error
			var desc string
			iv, ivErr = strconv.ParseInt(s, 10, bitSize)
			uv, uvErr = strconv.ParseUint(s, 10, bitSize)
			var ivExp, uvExp, ivAct, uvAct interface{}
			switch bitSize {
			case 8:
				ivExp = i8Exp[i]
				ivAct = int8(iv)
				uvExp = u8Exp[i]
				uvAct = uint8(uv)
			case 16:
				ivExp = i16Exp[i]
				ivAct = int16(iv)
				uvExp = u16Exp[i]
				uvAct = uint16(uv)
			case 32:
				ivExp = i32Exp[i]
				ivAct = int32(iv)
				uvExp = u32Exp[i]
				uvAct = uint32(uv)
			case 64:
				ivExp = i64Exp[i]
				ivAct = iv
				uvExp = u64Exp[i]
				uvAct = uv
			}
			desc = fmt.Sprintf(`ParseInt("%s", 10, %d)=%d(%v)`, s, bitSize, iv, errors.Unwrap(ivErr))
			require.Equal(t, ivExp, ivAct, desc)
			desc = fmt.Sprintf(`ParseUint("%s", 10, %d)=%d(%v)`, s, bitSize, uv, errors.Unwrap(uvErr))
			require.Equal(t, uvExp, uvAct, desc)
			if strings.Contains(s, "invalid") {
				require.True(t, errors.Is(ivErr, strconv.ErrSyntax))
				require.True(t, errors.Is(uvErr, strconv.ErrSyntax))
			} else if strings.Contains(s, "-") {
				require.True(t, errors.Is(uvErr, strconv.ErrSyntax))
			}
		}
	}
}

func TestFastjsonDetectSchema(t *testing.T) {
	pp, _ := NewParserPool("fastjson", nil, "", "", timeUnit)
	parser := pp.Get()
	defer pp.Put(parser)
	metric, _ := parser.Parse(jsonSample)

	act := make(map[string]string)
	c, _ := metric.(*FastjsonMetric)
	var obj *fastjson.Object
	var err error
	if obj, err = c.value.Object(); err != nil {
		return
	}
	obj.Visit(func(k []byte, v *fastjson.Value) {
		typ, array := fjDetectType(v, 0)
		tn := model.GetTypeName(typ)
		if typ != model.Unknown && array {
			tn += "Array"
		}
		act[string(k)] = tn
	})
	require.Equal(t, jsonSchema, act)
}

func TestGjsonDetectSchema(t *testing.T) {
	pp, _ := NewParserPool("gjson", nil, "", "", timeUnit)
	parser := pp.Get()
	defer pp.Put(parser)
	metric, _ := parser.Parse(jsonSample)

	act := make(map[string]string)
	c, _ := metric.(*GjsonMetric)
	obj := gjson.Parse(c.raw)
	obj.ForEach(func(k, v gjson.Result) bool {
		typ, array := gjDetectType(v, 0)
		tn := model.GetTypeName(typ)
		if typ != model.Unknown && array {
			tn += "Array"
		}
		act[k.Str] = tn
		return true
	})
	require.Equal(t, jsonSchema, act)
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
