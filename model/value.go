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
package model

import (
	"strings"

	"github.com/ClickHouse/clickhouse-go"
)

const (
	Int = iota
	Float
	String
	IntArray
	FloatArray
	StringArray
	Date
	DateTime
	DateTime64
	ElasticDateTime
)

type TypeInfo struct {
	Type     int
	Nullable bool
}

var (
	typeInfo map[string]TypeInfo
)

// There are only three cases for the value type of metric, (float64, string, map [string] interface {})
func GetValueByType(metric Metric, cwt *ColumnWithType) interface{} {
	name := cwt.SourceName
	switch cwt.Type {
	case Int:
		return metric.GetInt(name, cwt.Nullable)
	case Float:
		return metric.GetFloat(name, cwt.Nullable)
	case String:
		return metric.GetString(name, cwt.Nullable)
	case IntArray:
		return clickhouse.Array(metric.GetArray(name, "int"))
	case FloatArray:
		return clickhouse.Array(metric.GetArray(name, "float"))
	case StringArray:
		return clickhouse.Array(metric.GetArray(name, "string"))
	case Date:
		return metric.GetDate(name, cwt.Nullable)
	case DateTime:
		return metric.GetDateTime(name, cwt.Nullable)
	case DateTime64:
		return metric.GetDateTime64(name, cwt.Nullable)
	case ElasticDateTime:
		return metric.GetElasticDateTime(name, cwt.Nullable)
	default:
		panic("BUG: reached switch default condition")
	}
}

func WhichType(typ string) (dataType int, nullable bool) {
	ti, ok := typeInfo[typ]
	if ok {
		dataType, nullable = ti.Type, ti.Nullable
		return
	}
	nullable = strings.HasPrefix(typ, "Nullable(")
	if nullable {
		typ = typ[len("Nullable(") : len(typ)-1]
	}
	if strings.HasPrefix(typ, "DateTime64") {
		dataType = DateTime64
	} else if strings.HasPrefix(typ, "DateTime") {
		dataType = DateTime
	} else {
		panic("unsupported ClickHouse data type " + typ)
	}
	typeInfo[typ] = TypeInfo{Type: dataType, Nullable: nullable}
	return
}

func init() {
	typeInfo = make(map[string]TypeInfo)
	for _, t := range []string{"UInt8", "UInt16", "UInt32", "UInt64", "Int8",
		"Int16", "Int32", "Int64"} {
		typeInfo[t] = TypeInfo{Type: Int, Nullable: false}
	}
	for _, t := range []string{"Nullable(UInt8)", "Nullable(UInt16)", "Nullable(UInt32)", "Nullable(UInt64)",
		"Nullable(Int8)", "Nullable(Int16)", "Nullable(Int32)", "Nullable(Int64)"} {
		typeInfo[t] = TypeInfo{Type: Int, Nullable: true}
	}
	for _, t := range []string{"Float32", "Float64"} {
		typeInfo[t] = TypeInfo{Type: Float, Nullable: false}
	}
	for _, t := range []string{"Nullable(Float32)", "Nullable(Float64)"} {
		typeInfo[t] = TypeInfo{Type: Float, Nullable: true}
	}
	for _, t := range []string{"String", "FixedString"} {
		typeInfo[t] = TypeInfo{Type: String, Nullable: false}
	}
	for _, t := range []string{"Nullable(String)", "Nullable(FixedString)"} {
		typeInfo[t] = TypeInfo{Type: String, Nullable: true}
	}
	for _, t := range []string{"Array(UInt8)", "Array(UInt16)", "Array(UInt32)",
		"Array(UInt64)", "Array(Int8)", "Array(Int16)", "Array(Int32)", "Array(Int64)"} {
		typeInfo[t] = TypeInfo{Type: IntArray, Nullable: false}
	}
	for _, t := range []string{"Array(Float32)", "Array(Float64)"} {
		typeInfo[t] = TypeInfo{Type: FloatArray, Nullable: false}
	}
	for _, t := range []string{"Array(String)", "Array(FixedString)"} {
		typeInfo[t] = TypeInfo{Type: StringArray, Nullable: false}
	}
	typeInfo["Date"] = TypeInfo{Type: Date, Nullable: false}
	typeInfo["Nullable(Date)"] = TypeInfo{Type: Date, Nullable: true}
	typeInfo["DateTime"] = TypeInfo{Type: DateTime, Nullable: false}
	typeInfo["Nullable(DateTime)"] = TypeInfo{Type: DateTime, Nullable: true}
	typeInfo["DateTime64"] = TypeInfo{Type: DateTime64, Nullable: false}
	typeInfo["Nullable(DateTime64)"] = TypeInfo{Type: DateTime64, Nullable: true}
	typeInfo["ElasticDateTime"] = TypeInfo{Type: ElasticDateTime, Nullable: false}
	typeInfo["Nullable(ElasticDateTime)"] = TypeInfo{Type: ElasticDateTime, Nullable: true}
}
