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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCsvInt(t *testing.T) {
	pp, _ := NewParserPool("csv", csvSampleSchema, "", "")
	parser := pp.Get()
	defer pp.Put(parser)
	metric, err := parser.Parse(csvSample)
	require.Nil(t, err)

	var exp, act int64
	exp = 1536813227
	act = metric.GetInt("its", false).(int64)
	require.Equal(t, exp, act)

	exp = 0
	act = metric.GetInt("not_exist", false).(int64)
	require.Equal(t, exp, act)

	act = metric.GetInt("not_exist", true).(int64)
	require.Equal(t, exp, act)
}

func TestCsvFloat(t *testing.T) {
	pp, _ := NewParserPool("csv", csvSampleSchema, "", "")
	parser := pp.Get()
	defer pp.Put(parser)
	metric, err := parser.Parse(csvSample)
	require.Nil(t, err)

	var exp, act float64
	exp = 0.11
	act = metric.GetFloat("percent", false).(float64)
	require.Equal(t, exp, act)

	exp = 0.0
	act = metric.GetFloat("not_exist", false).(float64)
	require.Equal(t, exp, act)

	act = metric.GetFloat("not_exist", true).(float64)
	require.Equal(t, exp, act)
}

func TestCsvString(t *testing.T) {
	pp, _ := NewParserPool("csv", csvSampleSchema, "", "")
	parser := pp.Get()
	defer pp.Put(parser)
	metric, err := parser.Parse(csvSample)
	require.Nil(t, err)

	var exp, act string
	exp = `escaped_"ws`
	act = metric.GetString("channel", false).(string)
	require.Equal(t, exp, act)

	exp = ""
	act = metric.GetString("not_exist", false).(string)
	require.Equal(t, exp, act)

	act = metric.GetString("not_exist", true).(string)
	require.Equal(t, exp, act)
}

func TestCsvDate(t *testing.T) {
	pp, _ := NewParserPool("csv", csvSampleSchema, "", "")
	parser := pp.Get()
	defer pp.Put(parser)
	metric, err := parser.Parse(csvSample)
	require.Nil(t, err)

	var exp, act time.Time
	exp = time.Date(2019, 12, 16, 0, 0, 0, 0, time.Local)
	act = metric.GetDate("date1", false).(time.Time)
	require.Equal(t, exp, act)
}

func TestCsvDateTime(t *testing.T) {
	pp, _ := NewParserPool("csv", csvSampleSchema, "", "")
	parser := pp.Get()
	defer pp.Put(parser)
	metric, err := parser.Parse(csvSample)
	require.Nil(t, err)

	var exp, act time.Time
	exp = time.Date(2019, 12, 16, 12, 10, 30, 0, time.UTC)
	act = metric.GetDateTime("time_sec_rfc3339_1", false).(time.Time)
	require.Equal(t, exp, act)

	exp = time.Date(2019, 12, 16, 12, 10, 30, 0, time.FixedZone("CST", 8*60*60)).In(time.UTC)
	act = metric.GetDateTime("time_sec_rfc3339_2", false).(time.Time).In(time.UTC)
	require.Equal(t, exp, act)

	exp = time.Date(2019, 12, 16, 12, 10, 30, 0, time.Local).In(time.UTC)
	act = metric.GetDateTime("time_sec_clickhouse_1", false).(time.Time).In(time.UTC)
	require.Equal(t, exp, act)

	exp = time.Time{}
	act = metric.GetDateTime("not_exist", false).(time.Time)
	require.Equal(t, exp, act)
}

func TestCsvDateTime64(t *testing.T) {
	pp, _ := NewParserPool("csv", csvSampleSchema, "", "")
	parser := pp.Get()
	defer pp.Put(parser)
	metric, err := parser.Parse(csvSample)
	require.Nil(t, err)

	var exp, act time.Time
	exp = time.Date(2019, 12, 16, 12, 10, 30, 123000000, time.UTC)
	act = metric.GetDateTime64("time_ms_rfc3339_1", false).(time.Time)
	require.Equal(t, exp, act)

	exp = time.Date(2019, 12, 16, 12, 10, 30, 123000000, time.FixedZone("CST", 8*60*60)).In(time.UTC)
	act = metric.GetDateTime64("time_ms_rfc3339_2", false).(time.Time).In(time.UTC)
	require.EqualValues(t, exp, act)

	exp = time.Date(2019, 12, 16, 12, 10, 30, 123000000, time.Local).In(time.UTC)
	act = metric.GetDateTime64("time_ms_clickhouse_1", false).(time.Time).In(time.UTC)
	require.Equal(t, exp, act)

	exp = time.Time{}
	act = metric.GetDateTime64("not_exist", false).(time.Time)
	require.Equal(t, exp, act)
}

func TestCsvElasticDateTime(t *testing.T) {
	pp, _ := NewParserPool("csv", csvSampleSchema, "", "")
	parser := pp.Get()
	defer pp.Put(parser)
	metric, err := parser.Parse(csvSample)
	require.Nil(t, err)

	var exp, act int64
	// {"date": "2019-12-16T12:10:30Z"}
	// TZ=UTC date -d @1576498230 => Mon 16 Dec 2019 12:10:30 PM UTC
	exp = 1576498230
	act = metric.GetElasticDateTime("time_sec_rfc3339_1", false).(int64)
	require.Equal(t, exp, act)

	exp = -62135596800
	act = metric.GetElasticDateTime("not_exist", false).(int64)
	require.Equal(t, exp, act)

	act = metric.GetElasticDateTime("not_exist", true).(int64)
	require.Equal(t, exp, act)
}

func TestCsvArray(t *testing.T) {
	pp, _ := NewParserPool("csv", csvSampleSchema, "", "")
	parser := pp.Get()
	defer pp.Put(parser)
	metric, err := parser.Parse(csvSample)
	require.Nil(t, err)

	actI := metric.GetArray("array_int", "int").([]int64)
	expI := []int64{1, 2, 3}
	require.Equal(t, expI, actI)

	actF := metric.GetArray("array_float", "float").([]float64)
	expF := []float64{1.1, 2.2, 3.3}
	require.Equal(t, expF, actF)

	actS := metric.GetArray("array_string", "string").([]string)
	expS := []string{"aa", "bb", "cc"}
	require.Equal(t, expS, actS)

	actIE := metric.GetArray("array_empty", "int").([]int64)
	expIE := []int64{}
	require.Equal(t, expIE, actIE)

	actFE := metric.GetArray("array_empty", "float").([]float64)
	expFE := []float64{}
	require.Equal(t, expFE, actFE)

	actSE := metric.GetArray("array_empty", "string").([]string)
	expSE := []string{}
	require.Equal(t, expSE, actSE)
}
