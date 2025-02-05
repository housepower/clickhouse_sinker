/*
Copyright [2019] housepower

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
	"github.com/confluentinc/confluent-kafka-go/schemaregistry"
	"log"
	"math"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
	"github.com/valyala/fastjson"
	"github.com/viru-tech/clickhouse_sinker/model"
	"github.com/viru-tech/clickhouse_sinker/util"
	"golang.org/x/exp/constraints"
)

var jsonSchema = map[string]string{
	"null":                      "Unknown",
	"bool_true":                 "Bool",
	"bool_false":                "Bool",
	"num_int":                   "Int64",
	"num_float":                 "Float64",
	"app.kubernetes.io/name":    "String",
	"str":                       "String",
	"str_int":                   "String",
	"str_float":                 "String",
	"str_date_1":                "DateTime",
	"str_date_2":                "DateTime",
	"str_time_rfc3339_1":        "DateTime",
	"str_time_rfc3339_2":        "DateTime",
	"str_time_clickhouse_1":     "DateTime",
	"str_time_clickhouse_2":     "DateTime",
	"obj":                       "Object('json')",
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
	"array_obj":                 "Object('json')Array",
	"map_str_str":               "Object('json')",
	"map_str_uint":              "Object('json')",
	"map_str_int":               "Object('json')",
	"map_str_float":             "Object('json')",
	"map_str_bool":              "Object('json')",
	"map_str_date":              "Object('json')",
	"map_str_array":             "Object('json')",
	"map_str_map":               "Object('json')",
	"map_uint_uint":             "Object('json')",
	"map_int_string":            "Object('json')",
	"uuid":                      "String",
	"ipv4":                      "String",
	"ipv6":                      "String",
}
var jsonFields = `{
	"fnull": null,
	"fbool_true": true,
	"fbool_false": false,
	"fnum_int": 123,
	"fnum_float": 123.321,
	"fapp.kubernetes.io/name": "kube-state-metrics",
	"fstr": "escaped_\"ws",
	"fstr_int": "123",
	"fstr_float": "123.321",
	"fstr_date_1": "2009-07-13",
	"fstr_date_2": "13/07/2009",
	"fstr_time_rfc3339_1": "2009-07-13T09:07:13Z",
	"fstr_time_rfc3339_2": "2009-07-13T09:07:13.123+08:00",
	"fstr_time_clickhouse_1": "2009-07-13 09:07:13",
	"fstr_time_clickhouse_2": "2009-07-13 09:07:13.123",
	"fobj": {"i":[1,2,3],"f":[1.1,2.2,3.3],"s":["aa","bb","cc"],"e":[]},
	"farray_empty": [],
	"farray_null": [null],
	"farray_bool": [true,false],
	"farray_num_int_1": [0, 255, 256, 65535, 65536, 4294967295, 4294967296, 18446744073709551615, 18446744073709551616],
	"farray_num_int_2": [-9223372036854775808, -2147483649, -2147483648, -32769, -32768, -129, -128, 0, 127, 128, 32767, 32768, 2147483647, 2147483648, 9223372036854775807],
	"farray_num_float": [4.940656458412465441765687928682213723651e-324, 1.401298464324817070923729583289916131280e-45, 0.0, 3.40282346638528859811704183484516925440e+38, 1.797693134862315708145274237317043567981e+308, -inf, +inf],
	"farray_str": ["aa","bb","cc"],
	"farray_str_int_1": ["0", "255", "256", "65535", "65536", "4294967295", "4294967296", "18446744073709551615", "18446744073709551616"],
	"farray_str_int_2": ["-9223372036854775808", "-2147483649", "-2147483648", "-32769", "-32768", "-129", "-128", "0", "127", "128", "32767", "32768", "2147483647", "2147483648", "9223372036854775807"],
	"farray_str_float": ["4.940656458412465441765687928682213723651e-324", "1.401298464324817070923729583289916131280e-45", "0.0", "3.40282346638528859811704183484516925440e+38", "1.797693134862315708145274237317043567981e+308", "-inf", "+inf"],
	"farray_str_date_1": ["2009-07-13","2009-07-14","2009-07-15"],
	"farray_str_date_2": ["13/07/2009","14/07/2009","15/07/2009"],
	"farray_str_time_rfc3339": ["2009-07-13T09:07:13Z", "2009-07-13T09:07:13+08:00", "2009-07-13T09:07:13.123Z", "2009-07-13T09:07:13.123+08:00"],
	"farray_str_time_clickhouse": ["2009-07-13 09:07:13", "2009-07-13 09:07:13.123"],
	"farray_obj": [{"i":[1,2,3],"f":[1.1,2.2,3.3]},{"s":["aa","bb","cc"],"e":[]}],
	"fmap_str_str": {"i":"first", "j":"second"},
	"fmap_str_uint": {"i":1, "j":2},
	"fmap_str_int": {"i":-1, "j":-2},
	"fmap_str_float": {"i":3.1415, "j":9.876},
	"fmap_str_bool": {"i":true, "j":false},
	"fmap_str_date": {"i":"2008-08-08", "j":"2022-01-01"},
	"fmap_str_array": {"i":[1,2,3],"j":[4,5,6]},
	"fmap_str_map": {"i":{"i":1, "j":2}, "j":{"i":3, "j":4}},
	"fmap_uint_uint": {"1":1, "2":2},
	"fmap_int_string": {"1":"3.1415", "2":"9.876"}
}`

var csvSchema = []string{
	"null",
	"bool_true",
	"bool_false",
	"num_int",
	"num_float",
	"app.kubernetes.io/name",
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
	"uuid",
	"ipv4",
	"ipv6",
}

var csvSpecificOp = []string{"GetBool", "GetInt8", "GetInt16", "GetInt32", "GetInt64",
	"GetUint8", "GetUint16", "GetUint32", "GetUint64", "GetFloat32", "GetFloat64", "GetDateTime", "GetDecimal"}
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

	jsonSample []byte
	csvSample  []byte
)

var (
	names   = []string{fastJsonName, gjsonName, csvName}
	metrics = make(map[string]model.Metric)
)

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

type MapCase struct {
	field  string
	typ    *model.TypeInfo
	expVal interface{}
}

type DateTimeCase struct {
	TS     string
	ExpVal time.Time
}

func TestMain(m *testing.M) {
	_, currFile, _, ok := runtime.Caller(0)
	if !ok {
		log.Fatal("failed to get current file location")
	}
	testDataPath := filepath.Join(currFile, "..", "testdata")

	data, err := os.ReadFile(filepath.Join(testDataPath, "test.proto"))
	if err != nil {
		log.Fatalf("failed to read .proto file: %v", err)
	}
	jsonSample, err = os.ReadFile(filepath.Join(testDataPath, "test.json"))
	if err != nil {
		log.Fatalf("failed to read .json file: %v", err)
	}
	csvSample, err = os.ReadFile(filepath.Join(testDataPath, "test.csv"))
	if err != nil {
		log.Fatalf("failed to read .csv file: %v", err)
	}

	schemaInfo = schemaregistry.SchemaInfo{
		Schema:     string(data),
		SchemaType: "PROTOBUF",
		References: []schemaregistry.Reference{},
	}

	if err := initMetrics(); err != nil {
		log.Fatalf("failed to init metrics: %v", err)
	}

	os.Exit(m.Run())
}

func initMetrics() error {
	for _, name := range names {
		var (
			pp     *Pool
			sample []byte
		)

		switch name {
		case csvName:
			pp, _ = NewParserPool(csvName, csvSchema, ",", "", timeUnit, "", nil, "")
			sample = csvSample
		case fastJsonName:
			pp, _ = NewParserPool(fastJsonName, nil, "", "", timeUnit, "", nil, jsonFields)
			sample = jsonSample
		case gjsonName:
			pp, _ = NewParserPool(gjsonName, nil, "", "", timeUnit, "", nil, jsonFields)
			sample = jsonSample
		}

		parser, err := pp.Get()
		if err != nil {
			return fmt.Errorf("failed to initialize parser: %w", err)
		}
		metric, err := parser.Parse(sample)
		if err != nil {
			return fmt.Errorf("failed to parse %s metrics: %w", name, err)
		}

		metrics[name] = metric
	}
	util.InitLogger([]string{"stdout"})
	return nil
}

func sliceContains(list []string, target string) bool {
	for _, s := range list {
		if s == target {
			return true
		}
	}
	return false
}

func testCaseDescription(parserName, method, field string, nullable bool) string {
	return fmt.Sprintf(`%s.%s("%s", %s)`, parserName, method, field, strconv.FormatBool(nullable))
}

func doTestSimple(t *testing.T, method string, testCases []SimpleCase) {
	t.Helper()

	for i := range names {
		name := names[i]
		metric := metrics[name]
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			doTestSimpleForParser(t, name, method, testCases, metric)
		})
	}
}

func doTestSimpleForParser(t *testing.T, parserName, method string, tt []SimpleCase, metric model.Metric) {
	for i := range tt {
		tc := tt[i]
		t.Run(tc.Field, func(t *testing.T) {
			t.Parallel()

			desc := testCaseDescription(parserName, method, tc.Field, tc.Nullable)
			if parserName == "csv" && (sliceContains(csvSpecificOp, method) &&
				sliceContains([]string{"str_int", "str_float"}, tc.Field) ||
				tc.Nullable) ||
				(parserName == gjsonName && strings.Contains(tc.Field, ".")) {
				t.Skipf("incompatible with %s parser: %v", parserName, desc)
			}

			var v interface{}
			switch method {
			case "GetBool":
				v = metric.GetBool(tc.Field, tc.Nullable)
			case "GetInt8":
				v = metric.GetInt8(tc.Field, tc.Nullable)
			case "GetInt16":
				v = metric.GetInt16(tc.Field, tc.Nullable)
			case "GetInt32":
				v = metric.GetInt32(tc.Field, tc.Nullable)
			case "GetInt64":
				v = metric.GetInt64(tc.Field, tc.Nullable)
			case "GetUint8":
				v = metric.GetUint8(tc.Field, tc.Nullable)
			case "GetUint16":
				v = metric.GetUint16(tc.Field, tc.Nullable)
			case "GetUint32":
				v = metric.GetUint32(tc.Field, tc.Nullable)
			case "GetUint64":
				v = metric.GetUint64(tc.Field, tc.Nullable)
			case "GetFloat32":
				v = metric.GetFloat32(tc.Field, tc.Nullable)
			case "GetFloat64":
				v = metric.GetFloat64(tc.Field, tc.Nullable)
			case "GetDecimal":
				v = metric.GetDecimal(tc.Field, tc.Nullable)
			case "GetDateTime":
				v = metric.GetDateTime(tc.Field, tc.Nullable)
			case "GetString":
				v = metric.GetString(tc.Field, tc.Nullable)
			case "GetUUID":
				v = metric.GetUUID(tc.Field, tc.Nullable)
			case "GetIPv4":
				v = metric.GetIPv4(tc.Field, tc.Nullable)
			case "GetIPv6":
				v = metric.GetIPv6(tc.Field, tc.Nullable)
			default:
				t.Fatal("unknown method")
			}
			require.Equal(t, tc.ExpVal, v, desc)
		})
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
	testFloatType[float32](t, "GetFloat32")
	testFloatType[float64](t, "GetFloat64")
}

func testFloatType[T constraints.Float](t *testing.T, method string) {
	defaultVal := T(0.0)
	testCases := []SimpleCase{
		// nullable: false
		{"not_exist", false, defaultVal},
		{"null", false, defaultVal},
		{"bool_true", false, defaultVal},
		{"bool_false", false, defaultVal},
		{"num_int", false, T(123.0)},
		{"num_float", false, T(123.321)},
		{"str", false, defaultVal},
		{"str_int", false, defaultVal},
		{"str_float", false, defaultVal},
		{"str_date_1", false, defaultVal},
		{"obj", false, defaultVal},
		{"array_empty", false, defaultVal},
		// nullable: true
		{"not_exist", true, nil},
		{"null", true, nil},
		{"bool_true", true, nil},
		{"bool_false", true, nil},
		{"num_int", true, T(123.0)},
		{"num_float", true, T(123.321)},
		{"str", true, nil},
		{"str_int", true, nil},
		{"str_float", true, nil},
		{"str_date_1", true, nil},
		{"obj", true, nil},
		{"array_empty", true, nil},
	}
	doTestSimple(t, method, testCases)
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
		{"app.kubernetes.io/name", false, `kube-state-metrics`},
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
		{"app.kubernetes.io/name", true, `kube-state-metrics`},
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

func TestParserGetUUID(t *testing.T) {
	t.Parallel()

	testCases := []SimpleCase{
		// nullable: false
		{"not_exist", false, zeroUUID},
		{"uuid", false, "2211a6ec-3799-41c1-ac41-4ab02f8e3cf2"},
		{"array_empty", false, "[]"},
		// nullable: true
		{"not_exist", true, nil},
		{"uuid", true, "2211a6ec-3799-41c1-ac41-4ab02f8e3cf2"},
		{"array_empty", true, "[]"},
	}

	doTestSimple(t, "GetUUID", testCases)
}

func TestParserGetIPv4(t *testing.T) {
	t.Parallel()

	testCases := []SimpleCase{
		// nullable: false
		{"not_exist", false, net.IPv4zero.String()},
		{"ipv4", false, "1.2.3.4"},
		// nullable: true
		{"not_exist", true, nil},
		{"ipv4", true, "1.2.3.4"},
	}

	doTestSimple(t, "GetIPv4", testCases)
}

func TestParserGetIPv6(t *testing.T) {
	t.Parallel()

	testCases := []SimpleCase{
		// nullable: false
		{"not_exist", false, net.IPv6zero.String()},
		{"ipv6", false, "fe80::74e6:b5f3:fe92:830e"},
		// nullable: true
		{"not_exist", true, nil},
		{"ipv6", true, "fe80::74e6:b5f3:fe92:830e"},
	}

	doTestSimple(t, "GetIPv6", testCases)
}

func TestParserArray(t *testing.T) {
	tt := []ArrayCase{
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
		{"array_num_float", model.Float64, []float64{4.940656458412465441765687928682213723651e-324, 1.401298464324817070923729583289916131280e-45, 0.0, 3.40282346638528859811704183484516925440e+38, 1.797693134862315708145274237317043567981e+308, math.Inf(-1), math.MaxFloat64}},
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
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			for i := range tt {
				tc := tt[i]
				t.Run(tc.Field, func(t *testing.T) {
					t.Parallel()

					var v interface{}
					desc := fmt.Sprintf(`%s.GetArray("%s", %s)`, name, tc.Field, model.GetTypeName(tc.Type))
					if (name == gjsonName && tc.Field == "array_num_float") ||
						(name == csvName && sliceContains([]string{"array_num_float", "array_str_float"}, tc.Field)) {
						t.Skipf("incompatible with fastjson parser: %v", desc)
					}
					v = metric.GetArray(tc.Field, tc.Type)
					require.Equal(t, tc.ExpVal, v, desc)
				})
			}
		})
	}
}

func TestParserMap(t *testing.T) {
	testCases := []MapCase{
		{"map_str_str", &model.TypeInfo{Type: model.Map, MapKey: &model.TypeInfo{Type: model.String}, MapValue: &model.TypeInfo{Type: model.String}}, map[string]string{"i": "first", "j": "second"}},
		{"map_str_uint", &model.TypeInfo{Type: model.Map, MapKey: &model.TypeInfo{Type: model.String}, MapValue: &model.TypeInfo{Type: model.UInt64}}, map[string]uint64{"i": 1, "j": 2}},
		{"map_str_int", &model.TypeInfo{Type: model.Map, MapKey: &model.TypeInfo{Type: model.String}, MapValue: &model.TypeInfo{Type: model.Int64}}, map[string]int64{"i": -1, "j": -2}},
		{"map_str_float", &model.TypeInfo{Type: model.Map, MapKey: &model.TypeInfo{Type: model.String}, MapValue: &model.TypeInfo{Type: model.Float64}}, map[string]float64{"i": 3.1415, "j": 9.876}},
		{"map_str_bool", &model.TypeInfo{Type: model.Map, MapKey: &model.TypeInfo{Type: model.String}, MapValue: &model.TypeInfo{Type: model.Bool}}, map[string]bool{"i": true, "j": false}},
		{"map_str_date", &model.TypeInfo{Type: model.Map, MapKey: &model.TypeInfo{Type: model.String}, MapValue: &model.TypeInfo{Type: model.DateTime}}, map[string]time.Time{"i": time.Date(2008, 8, 8, 0, 0, 0, 0, time.Local).UTC(), "j": time.Date(2022, 1, 1, 0, 0, 0, 0, time.Local).UTC()}},
		{"map_str_array", &model.TypeInfo{Type: model.Map, MapKey: &model.TypeInfo{Type: model.String}, MapValue: &model.TypeInfo{Type: model.UInt64, Array: true}}, map[string][]uint64{"i": {1, 2, 3}, "j": {4, 5, 6}}},
		{"map_str_map", &model.TypeInfo{Type: model.Map, MapKey: &model.TypeInfo{Type: model.String}, MapValue: &model.TypeInfo{Type: model.Map, MapKey: &model.TypeInfo{Type: model.String}, MapValue: &model.TypeInfo{Type: model.UInt64}}}, map[string]map[string]uint64{"i": {"i": 1, "j": 2}, "j": {"i": 3, "j": 4}}},
	}
	for _, name := range []string{fastJsonName, gjsonName} {
		metric := metrics[name]
		for _, it := range testCases {
			desc := fmt.Sprintf(`%s.GetMap("%s", %s)`, name, it.field, model.GetTypeName(it.typ.Type))

			orderMap := metric.GetMap(it.field, it.typ)
			compareMap(t, orderMap, it.expVal, desc)
		}
	}
	// GetMap is not supported for csv format
	require.Equal(t, nil, metrics["csv"].GetMap("whatever", &model.TypeInfo{Type: model.String}))

	// map key as int type, only supported by fastjson parser
	testCases = []MapCase{
		{"map_uint_uint", &model.TypeInfo{Type: model.Map, MapKey: &model.TypeInfo{Type: model.UInt64}, MapValue: &model.TypeInfo{Type: model.UInt64}}, map[uint64]uint64{1: 1, 2: 2}},
		{"map_int_string", &model.TypeInfo{Type: model.Map, MapKey: &model.TypeInfo{Type: model.Int64}, MapValue: &model.TypeInfo{Type: model.String}}, map[int64]string{1: "3.1415", 2: "9.876"}},
	}
	for _, it := range testCases {
		desc := fmt.Sprintf(`%s.GetMap("%s", %s)`, fastJsonName, it.field, model.GetTypeName(it.typ.Type))
		orderMap := metrics[fastJsonName].GetMap(it.field, it.typ)
		compareMap(t, orderMap, it.expVal, desc)
	}
}

func compareMap(t *testing.T, map1 interface{}, map2 interface{}, desc string) {
	oMap, ok := map1.(*model.OrderedMap)
	assert.True(t, ok, desc)
	map1 = oMap.GetValues()

	value1 := reflect.ValueOf(map1)
	value2 := reflect.ValueOf(map2)
	assert.Equal(t, value1.IsNil(), value2.IsNil())
	assert.Equal(t, value1.Len(), value2.Len())

	if value1.Kind() == reflect.Ptr {
		value1 = value1.Elem()
		value2 = value2.Elem()
	}
	assert.Equal(t, value1.Kind(), reflect.Map, fmt.Sprintf("ToMap only accepts struct or struct pointer; got %T", value1))
	assert.Equal(t, value2.Kind(), reflect.Map, fmt.Sprintf("ToMap only accepts struct or struct pointer; got %T", value2))

	// v1 - map[interface{}]interface{}, v2 could be any map type
	var compareValueFunc func(v1, v2 reflect.Value) = func(v1, v2 reflect.Value) {
		switch v2.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			assert.Equal(t, v1.Interface().(int64), v2.Int())
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
			assert.Equal(t, v1.Interface().(uint64), v2.Uint())
		case reflect.String:
			assert.Equal(t, v1.Interface().(string), v2.String())
		case reflect.Bool:
			assert.Equal(t, v1.Interface().(bool), v2.Bool())
		case reflect.Float32, reflect.Float64:
			assert.Equal(t, v1.Interface().(float64), v2.Float())
		case reflect.Map:
			compareMap(t, v1.Interface(), v2.Interface(), desc)
		case reflect.Array:
			fallthrough
		case reflect.Slice:
			if v2.Len() == 0 {
				return
			}
			// didn't find a good way to convert interface{} to []Type
			switch v2.Index(0).Kind() {
			case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
				array1 := v1.Interface().([]uint64)
				assert.Equal(t, len(array1), v2.Len())
				for i := 0; i < v2.Len(); i++ {
					assert.Equal(t, array1[i], v2.Index(i).Uint())
				}
			default:
				assert.Fail(t, "uncovered array type comparison, update compareValueFunc accordingly")
			}
		default:
			// Normal equality suffices
			assert.Equal(t, v1.Interface(), v2.Interface())
		}
	}
	for _, key := range value2.MapKeys() {
		compareValueFunc(value1.MapIndex(key), value2.MapIndex(key))
	}
}

func TestParseObject(t *testing.T) {
	compareObj := func(t *testing.T, map1 interface{}, map2 interface{}, desc string) {
		value1 := reflect.ValueOf(map1)
		value2 := reflect.ValueOf(map2)
		assert.Equal(t, value1.Len(), value2.Len())

		// v1 - map[interface{}]interface{}, v2 could be map[string][string] or map[string][float64]
		var compareValueFunc func(v1, v2 reflect.Value) = func(v1, v2 reflect.Value) {
			switch v2.Kind() {
			case reflect.String:
				assert.Equal(t, v1.Interface().(string), v2.String())
			case reflect.Float32, reflect.Float64:
				assert.Equal(t, v1.Interface().(float64), v2.Float())
			default:
				// Normal equality suffices
				assert.Equal(t, v1.Interface(), v2.Interface())
			}
		}
		for _, key := range value2.MapKeys() {
			compareValueFunc(value1.MapIndex(key), value2.MapIndex(key))
		}
	}

	testCases := []struct {
		name   string
		expVal interface{}
	}{
		// ParseObject only result a map[string][string] or map[string][float64]
		{"map_str_str", map[string]string{"i": "first", "j": "second"}},
		{"map_str_uint", map[string]float64{"i": float64(1), "j": float64(2)}},
		{"map_str_int", map[string]float64{"i": float64(-1), "j": float64(-2)}},
		{"map_str_float", map[string]float64{"i": float64(3.1415), "j": float64(9.876)}},
		{"map_str_bool", map[string]interface{}{}}, // type with non fastjson.TypeNumber will be discarded
		{"map_str_date", map[string]string{"i": "2008-08-08", "j": "2022-01-01"}},
	}

	for _, it := range testCases {
		desc := fmt.Sprintf(`fastjson.GetObject("%s", false)`, it.name)
		result := metrics[fastJsonName].GetObject(it.name, false)
		compareObj(t, result, it.expVal, desc)
	}

	// GetObject is not supported for csv ang gjson parser
	require.Equal(t, nil, metrics["csv"].GetObject("whatever", false))
	require.Equal(t, nil, metrics[gjsonName].GetObject("whatever", false))
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
			// DateTime, RFC3339
			{"2009-07-13T09:07:13.123+08:00", bdShNs},
			{"2009-07-13T09:07:13.123+0800", bdShNs},
			{"2009-07-13T09:07:13+08:00", bdShSec},
			{"2009-07-13T09:07:13+0800", bdShSec},
			{"2009-07-13T09:07:13.123Z", bdUtcNs},
			{"2009-07-13T09:07:13Z", bdUtcSec},
			{"2009-07-13T09:07:13.123", bdLocalNs},
			{"2009-07-13T09:07:13", bdLocalSec},
			// DateTime, ISO8601
			{"2009-07-13 09:07:13.123+08:00", bdShNs},
			{"2009-07-13 09:07:13.123+0800", bdShNs},
			{"2009-07-13 09:07:13+08:00", bdShSec},
			{"2009-07-13 09:07:13+0800", bdShSec},
			{"2009-07-13 09:07:13.123Z", bdUtcNs},
			{"2009-07-13 09:07:13Z", bdUtcSec},
			{"2009-07-13 09:07:13.123", bdLocalNs},
			{"2009-07-13 09:07:13", bdLocalSec},
			// DateTime, other layouts supported by golang
			{"Mon Jul 13 09:07:13 2009", bdLocalSec},
			{"Mon Jul 13 09:07:13 CST 2009", bdShSec},
			{"Mon Jul 13 09:07:13 +0800 2009", bdShSec},
			{"13 Jul 09 09:07 CST", bdShMin},
			{"13 Jul 09 09:07 +0800", bdShMin},
			{"Monday, 13-Jul-09 09:07:13 CST", bdShSec},
			{"Mon, 13 Jul 2009 09:07:13 CST", bdShSec},
			{"Mon, 13 Jul 2009 09:07:13 +0800", bdShSec},
			// DateTime, linux utils
			{"Mon 13 Jul 2009 09:07:13 AM CST", bdShSec},
			{"Mon Jul 13 09:07:13 CST 2009", bdShSec},
			// DateTime, home-brewed
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
			// Date
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
				log.Print(desc + "(CST is ambiguous)")
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

	testIntType[int8](t, "GetInt8")
	testIntType[int16](t, "GetInt16")
	testIntType[int32](t, "GetInt32")
	testIntType[int64](t, "GetInt64")
	testIntType[uint8](t, "GetUint8")
	testIntType[uint16](t, "GetUint16")
	testIntType[uint32](t, "GetUint32")
	testIntType[uint64](t, "GetUint64")
}

func testIntType[T constraints.Integer](t *testing.T, method string) {
	defaultVal := T(0)
	testCases := []SimpleCase{
		// nullable: false
		{"not_exist", false, defaultVal},
		{"null", false, defaultVal},
		{"bool_true", false, T(1)},
		{"bool_false", false, defaultVal},
		{"num_int", false, T(123)},
		{"num_float", false, defaultVal},
		{"str", false, defaultVal},
		{"str_int", false, defaultVal},
		{"str_float", false, defaultVal},
		{"str_date_1", false, defaultVal},
		{"obj", false, defaultVal},
		{"array_empty", false, defaultVal},
		// nullable: true
		{"not_exist", true, nil},
		{"null", true, nil},
		{"bool_true", true, T(1)},
		{"bool_false", true, defaultVal},
		{"num_int", true, T(123)},
		{"num_float", true, nil},
		{"str", true, nil},
		{"str_int", true, nil},
		{"str_float", true, nil},
		{"str_date_1", true, nil},
		{"obj", true, nil},
		{"array_empty", true, nil},
	}
	doTestSimple(t, method, testCases)
}

func TestParseDecimal(t *testing.T) {
	expvar := decimal.NewFromInt(0)
	testCases := []SimpleCase{
		// nullable: false
		{"not_exist", false, expvar},
		{"null", false, expvar},
		{"bool_true", false, expvar},
		{"bool_false", false, expvar},
		{"num_int", false, decimal.NewFromInt(123)},
		{"num_float", false, decimal.NewFromFloat(123.321)},
		{"str", false, expvar},
		{"str_int", false, expvar},
		{"str_float", false, expvar},
		{"str_date_1", false, expvar},
		{"obj", false, expvar},
		{"array_empty", false, expvar},
		// // nullable: true
		{"not_exist", true, nil},
		{"null", true, nil},
		{"bool_true", true, nil},
		{"bool_false", true, nil},
		{"num_int", true, decimal.NewFromInt(123)},
		{"num_float", true, decimal.NewFromFloat(123.321)},
		{"str", true, nil},
		{"str_int", true, nil},
		{"str_float", true, nil},
		{"str_date_1", true, nil},
		{"obj", true, nil},
		{"array_empty", true, nil},
	}
	doTestSimple(t, "GetDecimal", testCases)
}

func TestFields(t *testing.T) {
	testFunc := func(metric model.Metric, kind string) {
		v := metric.GetBool("fbool_true", false)
		require.Equal(t, true, v, "error calling GetBool")
		v = metric.GetInt8("fnum_int", false)
		require.Equal(t, int8(123), v, "error calling GetInt8")
		v = metric.GetString("fstr_time_rfc3339_1", false)
		require.Equal(t, "2009-07-13T09:07:13Z", v, "error calling GetString")
		v = metric.GetDecimal("fnum_float", false)
		require.Equal(t, decimal.NewFromFloat(123.321), v, "error calling GetDecimal")
		if kind == fastJsonName {
			v = metric.GetObject("fmap_str_str", false)
			require.Equal(t, map[string]interface{}{"i": "first", "j": "second"}, v, "error calling GetObject")
		}
	}

	pp, _ := NewParserPool(fastJsonName, nil, "", "", timeUnit, "", nil, jsonFields)
	fparser, _ := pp.Get()
	defer pp.Put(fparser)
	fmetric, _ := fparser.Parse(jsonSample)
	testFunc(fmetric, fastJsonName)

	pp, _ = NewParserPool(gjsonName, nil, "", "", timeUnit, "", nil, jsonFields)
	gparser, _ := pp.Get()
	defer pp.Put(gparser)
	gmetric, _ := gparser.Parse(jsonSample)
	testFunc(gmetric, gjsonName)
}

func TestFastjsonDetectSchema(t *testing.T) {
	pp, _ := NewParserPool(fastJsonName, nil, "", "", timeUnit, "", nil, jsonFields)
	parser, _ := pp.Get()
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

	fastjsonSchema := map[string]string{
		"fnull":                      "Unknown",
		"fbool_true":                 "Bool",
		"fbool_false":                "Bool",
		"fnum_int":                   "Int64",
		"fnum_float":                 "Float64",
		"fapp.kubernetes.io/name":    "String",
		"fstr":                       "String",
		"fstr_int":                   "String",
		"fstr_float":                 "String",
		"fstr_date_1":                "DateTime",
		"fstr_date_2":                "DateTime",
		"fstr_time_rfc3339_1":        "DateTime",
		"fstr_time_rfc3339_2":        "DateTime",
		"fstr_time_clickhouse_1":     "DateTime",
		"fstr_time_clickhouse_2":     "DateTime",
		"fobj":                       "Object('json')",
		"farray_empty":               "Unknown",
		"farray_null":                "Unknown",
		"farray_bool":                "BoolArray",
		"farray_num_int_1":           "Int64Array",
		"farray_num_int_2":           "Int64Array",
		"farray_num_float":           "Float64Array",
		"farray_str":                 "StringArray",
		"farray_str_int_1":           "StringArray",
		"farray_str_int_2":           "StringArray",
		"farray_str_float":           "StringArray",
		"farray_str_date_1":          "DateTimeArray",
		"farray_str_date_2":          "DateTimeArray",
		"farray_str_time_rfc3339":    "DateTimeArray",
		"farray_str_time_clickhouse": "DateTimeArray",
		"farray_obj":                 "Object('json')Array",
		"fmap_str_str":               "Object('json')",
		"fmap_str_uint":              "Object('json')",
		"fmap_str_int":               "Object('json')",
		"fmap_str_float":             "Object('json')",
		"fmap_str_bool":              "Object('json')",
		"fmap_str_date":              "Object('json')",
		"fmap_str_array":             "Object('json')",
		"fmap_str_map":               "Object('json')",
		"fmap_uint_uint":             "Object('json')",
		"fmap_int_string":            "Object('json')"}

	for k, v := range jsonSchema {
		fastjsonSchema[k] = v
	}
	require.Equal(t, fastjsonSchema, act)
}

func TestGjsonDetectSchema(t *testing.T) {
	pp, _ := NewParserPool(gjsonName, nil, "", "", timeUnit, "", nil, "")
	parser, _ := pp.Get()
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
