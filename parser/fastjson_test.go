package parser

import (
	"fmt"
	"strings"
	"testing"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"github.com/valyala/fastjson"
)

func TestFastjsonInt(t *testing.T) {
	pp, _ := NewParserPool("fastjson", nil, "", "")
	parser := pp.Get()
	defer pp.Put(parser)
	metric, _ := parser.Parse(jsonSample)

	var exp, act int64
	exp = 1536813227
	act, _ = metric.GetInt("its", false).(int64)
	require.Equal(t, exp, act)

	exp = 0
	act, _ = metric.GetInt("not_exist", false).(int64)
	require.Equal(t, exp, act)

	actual := metric.GetInt("not_exist", true)
	require.Nil(t, actual, "err should be nothing")

	exp = 0
	act, _ = metric.GetInt("bool_false", false).(int64)
	require.Equal(t, exp, act)

	exp = 1
	act, _ = metric.GetInt("bool_true", false).(int64)
	require.Equal(t, exp, act)
}

func TestFastjsonFloat(t *testing.T) {
	pp, _ := NewParserPool("fastjson", nil, "", "")
	parser := pp.Get()
	defer pp.Put(parser)
	metric, _ := parser.Parse(jsonSample)

	var exp, act float64
	exp = 0.11
	act, _ = metric.GetFloat("percent", false).(float64)
	require.Equal(t, exp, act)

	exp = 0.0
	act, _ = metric.GetFloat("not_exist", false).(float64)
	require.Equal(t, exp, act)

	actual := metric.GetFloat("not_exist", true)
	require.Nil(t, actual, "err should be nothing")
}

func TestFastjsonString(t *testing.T) {
	pp, _ := NewParserPool("fastjson", nil, "", "")
	parser := pp.Get()
	defer pp.Put(parser)
	metric, _ := parser.Parse(jsonSample)

	var exp, act string
	exp = "ws"
	act, _ = metric.GetString("channel", false).(string)
	require.Equal(t, exp, act)

	exp = ""
	act, _ = metric.GetString("not_exist", false).(string)
	require.Equal(t, exp, act)

	actual := metric.GetString("not_exist", true)
	require.Nil(t, actual, "err should be nothing")
}

func TestFastjsonDate(t *testing.T) {
	pp, _ := NewParserPool("fastjson", nil, "", "")
	parser := pp.Get()
	defer pp.Put(parser)
	metric, _ := parser.Parse(jsonSample)

	var exp, act time.Time
	exp = time.Date(2019, 12, 16, 0, 0, 0, 0, time.Local)
	act, _ = metric.GetDate("date1", false).(time.Time)
	require.Equal(t, exp, act)

	exp = time.Time{}
	act, _ = metric.GetDate("not_exist", false).(time.Time)
	require.Equal(t, exp, act)

	actual := metric.GetDate("not_exist", true)
	require.Nil(t, actual, "err should be nothing")
}

func TestFastjsonDateTime(t *testing.T) {
	pp, _ := NewParserPool("fastjson", nil, "", "")
	parser := pp.Get()
	defer pp.Put(parser)
	metric, _ := parser.Parse(jsonSample)

	var exp, act time.Time
	exp = time.Date(2019, 12, 16, 12, 10, 30, 0, time.UTC)
	act, _ = metric.GetDateTime("time_sec_rfc3339_1", false).(time.Time)
	require.Equal(t, exp, act)

	exp = time.Date(2019, 12, 16, 12, 10, 30, 0, time.FixedZone("CST", 8*60*60)).In(time.UTC)
	act = metric.GetDateTime("time_sec_rfc3339_2", false).(time.Time).In(time.UTC)
	require.Equal(t, exp, act)

	exp = time.Date(2019, 12, 16, 12, 10, 30, 0, time.Local).In(time.UTC)
	act = metric.GetDateTime("time_sec_clickhouse_1", false).(time.Time).In(time.UTC)
	require.Equal(t, exp, act)

	exp = time.Time{}
	act, _ = metric.GetDateTime("not_exist", false).(time.Time)
	require.Equal(t, exp, act)

	actual := metric.GetDateTime("not_exist", true)
	require.Nil(t, actual, "err should be nothing")
}

func TestFastjsonDateTime64(t *testing.T) {
	pp, _ := NewParserPool("fastjson", nil, "", "")
	parser := pp.Get()
	defer pp.Put(parser)
	metric, _ := parser.Parse(jsonSample)

	var exp, act time.Time
	exp = time.Date(2019, 12, 16, 12, 10, 30, 123000000, time.UTC)
	act, _ = metric.GetDateTime64("time_ms_rfc3339_1", false).(time.Time)
	require.Equal(t, exp, act)

	exp = time.Date(2019, 12, 16, 12, 10, 30, 123000000, time.FixedZone("CST", 8*60*60)).In(time.UTC)
	act = metric.GetDateTime64("time_ms_rfc3339_2", false).(time.Time).In(time.UTC)
	require.EqualValues(t, exp, act)

	exp = time.Date(2019, 12, 16, 12, 10, 30, 123000000, time.Local).In(time.UTC)
	act = metric.GetDateTime64("time_ms_clickhouse_1", false).(time.Time).In(time.UTC)
	require.Equal(t, exp, act)

	exp = time.Time{}
	act, _ = metric.GetDateTime64("not_exist", false).(time.Time)
	require.Equal(t, exp, act)

	actual := metric.GetDateTime64("not_exist", true)
	require.Nil(t, actual, "err should be nothing")
}

func TestFastjsonElasticDateTime(t *testing.T) {
	pp, _ := NewParserPool("fastjson", nil, "", "")
	parser := pp.Get()
	defer pp.Put(parser)
	metric, _ := parser.Parse(jsonSample)

	var exp, act int64
	// {"date": "2019-12-16T12:10:30Z"}
	// TZ=UTC date -d @1576498230 => Mon 16 Dec 2019 12:10:30 PM UTC
	exp = 1576498230
	act, _ = metric.GetElasticDateTime("time_sec_rfc3339_1", false).(int64)
	require.Equal(t, exp, act)

	exp = -62135596800
	act, _ = metric.GetElasticDateTime("not_exist", false).(int64)
	require.Equal(t, exp, act)

	actual := metric.GetElasticDateTime("not_exist", true)
	require.Nil(t, actual, "err should be nothing")
}

func TestFastjsonArray(t *testing.T) {
	pp, _ := NewParserPool("fastjson", nil, "", "")
	parser := pp.Get()
	defer pp.Put(parser)
	metric, _ := parser.Parse(jsonSample)

	actI, _ := metric.GetArray("array_int", "int").([]int64)
	expI := []int64{1, 2, 3}
	require.Equal(t, expI, actI)

	actF, _ := metric.GetArray("array_float", "float").([]float64)
	expF := []float64{1.1, 2.2, 3.3}
	require.Equal(t, expF, actF)

	actS, _ := metric.GetArray("array_string", "string").([]string)
	expS := []string{"aa", "bb", "cc"}
	require.Equal(t, expS, actS)

	actIE, _ := metric.GetArray("array_empty", "int").([]int64)
	expIE := []int64{}
	require.Equal(t, expIE, actIE)

	actFE, _ := metric.GetArray("array_empty", "float").([]float64)
	expFE := []float64{}
	require.Equal(t, expFE, actFE)

	actSE, _ := metric.GetArray("array_empty", "string").([]string)
	expSE := []string{}
	require.Equal(t, expSE, actSE)

	metric, _ = parser.Parse(jsonSample2)
	actS, _ = metric.GetArray("listvalue", "string").([]string)
	expS = []string{"aaa", "bbb", "ccc"}
	require.Equal(t, expS, actS)
}

func TestFastjsonDetectSchema(t *testing.T) {
	pp, _ := NewParserPool("fastjson", nil, "", "")
	parser := pp.Get()
	defer pp.Put(parser)
	metric, _ := parser.Parse(jsonSample)

	var nameAndTypes []string
	c, _ := metric.(*FastjsonMetric)
	var obj *fastjson.Object
	var err error
	if obj, err = c.value.Object(); err != nil {
		return
	}
	obj.Visit(func(key []byte, v *fastjson.Value) {
		nameAndTypes = append(nameAndTypes, fmt.Sprintf("%s: %s", string(key), v.Type().String()))
	})
	log.Infof("detected json schema: {%s}", strings.Join(nameAndTypes, ", "))
}
