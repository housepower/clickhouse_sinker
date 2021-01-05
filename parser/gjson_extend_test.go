package parser

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGjsonExtendInt(t *testing.T) {
	pp := NewParserPool("gjson_extend", nil, "", DefaultTSLayout)
	parser := pp.Get()
	defer pp.Put(parser)
	metric, _ := parser.Parse(jsonSample)

	var expected int64 = 1536813227
	result := metric.GetInt("its", false).(int64)
	require.Equal(t, result, expected)
}

func TestGjsonExtendIntNullableFalse(t *testing.T) {
	pp := NewParserPool("gjson_extend", nil, "", DefaultTSLayout)
	parser := pp.Get()
	defer pp.Put(parser)
	metric, _ := parser.Parse(jsonSample)

	var expected int = 0
	result := metric.GetInt("its_not_exist", false).(int)
	require.Equal(t, result, expected)
}

func TestGjsonExtendIntNullableTrue(t *testing.T) {
	pp := NewParserPool("gjson_extend", nil, "", DefaultTSLayout)
	parser := pp.Get()
	defer pp.Put(parser)
	metric, _ := parser.Parse(jsonSample)

	result := metric.GetInt("its_not_exist", true)
	require.Nil(t, result, "err should be nothing")
}

func TestGjsonExtendStr(t *testing.T) {
	pp := NewParserPool("gjson_extend", nil, "", DefaultTSLayout)
	parser := pp.Get()
	defer pp.Put(parser)
	metric, _ := parser.Parse(jsonSample)

	var expected string = "ws"
	result := metric.GetString("channel", false).(string)
	require.Equal(t, result, expected)
}

func TestGjsonExtendStrNullableFalse(t *testing.T) {
	pp := NewParserPool("gjson_extend", nil, "", DefaultTSLayout)
	parser := pp.Get()
	defer pp.Put(parser)
	metric, _ := parser.Parse(jsonSample)

	var expected string = ""
	result := metric.GetString("channel_not_exist", false).(string)
	require.Equal(t, result, expected)
}

func TestGjsonExtendStrNullableTrue(t *testing.T) {
	pp := NewParserPool("gjson_extend", nil, "", DefaultTSLayout)
	parser := pp.Get()
	defer pp.Put(parser)
	metric, _ := parser.Parse(jsonSample)

	result := metric.GetString("channel_not_exist", true)
	require.Nil(t, result, "err should be nothing")
}

func TestGjsonExtendFloat(t *testing.T) {
	pp := NewParserPool("gjson_extend", nil, "", DefaultTSLayout)
	parser := pp.Get()
	defer pp.Put(parser)
	metric, _ := parser.Parse(jsonSample)

	var expected float64 = 0.11
	result := metric.GetFloat("percent", false).(float64)
	require.Equal(t, result, expected)
}

func TestGjsonExtendFloatNullableFalse(t *testing.T) {
	pp := NewParserPool("gjson_extend", nil, "", DefaultTSLayout)
	parser := pp.Get()
	defer pp.Put(parser)
	metric, _ := parser.Parse(jsonSample)

	var expected int = 0
	result := metric.GetFloat("percent_not_exist", false).(int)
	require.Equal(t, result, expected)
}

func TestGjsonExtendFloatNullableTrue(t *testing.T) {
	pp := NewParserPool("gjson_extend", nil, "", DefaultTSLayout)
	parser := pp.Get()
	defer pp.Put(parser)
	metric, _ := parser.Parse(jsonSample)

	result := metric.GetFloat("percent_not_exist", true)
	require.Nil(t, result, "err should be nothing")
}

func TestGjsonExtendArray(t *testing.T) {
	pp := NewParserPool("gjson_extend", nil, "", DefaultTSLayout)
	parser := pp.Get()
	defer pp.Put(parser)
	metric, _ := parser.Parse(jsonSample)

	actI := metric.GetArray("mp.i", "int").([]int64)
	expI := []int64{1, 2, 3}
	require.Equal(t, actI, expI)

	actF := metric.GetArray("mp.f", "float").([]float64)
	expF := []float64{1.1, 2.2, 3.3}
	require.Equal(t, expF, actF)

	actS := metric.GetArray("mp.s", "string").([]string)
	expS := []string{"aa", "bb", "cc"}
	require.Equal(t, expS, actS)

	actIE := metric.GetArray("mp.e", "int").([]int64)
	expIE := []int64{}
	require.Equal(t, expIE, actIE)

	actFE := metric.GetArray("mp.e", "float").([]float64)
	expFE := []float64{}
	require.Equal(t, expFE, actFE)

	actSE := metric.GetArray("mp.e", "string").([]string)
	expSE := []string{}
	require.Equal(t, expSE, actSE)
}

func TestGjsonExtendElasticDateTime(t *testing.T) {
	pp := NewParserPool("gjson_extend", nil, "", DefaultTSLayout)
	parser := pp.Get()
	defer pp.Put(parser)
	metric, _ := parser.Parse(jsonSample)

	// {"date": "2019-12-16T12:10:30Z"}
	// TZ=UTC date -d @1576498230 => Mon 16 Dec 2019 12:10:30 PM UTC
	var expected int64 = 1576498230
	result := metric.GetElasticDateTime("date", false).(int64)
	require.Equal(t, result, expected)
}

func TestGjsonExtendElasticDateTimeNullableFalse(t *testing.T) {
	pp := NewParserPool("gjson_extend", nil, "", DefaultTSLayout)
	parser := pp.Get()
	defer pp.Put(parser)
	metric, _ := parser.Parse(jsonSample)

	var expected int64 = -62135596800
	result := metric.GetElasticDateTime("date_not_exist", false).(int64)
	require.Equal(t, result, expected)
}

func TestGjsonExtendElasticDateTimeNullableTrue(t *testing.T) {
	pp := NewParserPool("gjson_extend", nil, "", DefaultTSLayout)
	parser := pp.Get()
	defer pp.Put(parser)
	metric, _ := parser.Parse(jsonSample)

	result := metric.GetElasticDateTime("date_not_exist", true)
	require.Nil(t, result, "err should be nothing")
}
