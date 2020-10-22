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

// There are only three cases for the value type of metric, (float64, string, map [string] interface {})
func GetValueByType(metric Metric, cwt *ColumnWithType) interface{} {
	swType, nullable := switchType(cwt.Type)
	name := cwt.SourceName
	switch swType {
	case "int":
		return metric.GetInt(name, nullable)
	case "float":
		return metric.GetFloat(name, nullable)
	case "string":
		return metric.GetString(name, nullable)
	case "stringArray":
		return clickhouse.Array(metric.GetArray(name, "string"))
	case "intArray":
		return clickhouse.Array(metric.GetArray(name, "int"))
	case "floatArray":
		return clickhouse.Array(metric.GetArray(name, "float"))
	case "Date":
		return metric.GetDate(name, nullable)
	case "DateTime":
		return metric.GetDateTime(name, nullable)
	case "DateTime64":
		return metric.GetDateTime64(name, nullable)
	case "ElasticDateTime":
		return metric.GetElasticDateTime(name, nullable)

	//never happen
	default:
		return ""
	}
}

func switchType(typ string) (dataType string, nullable bool) {
	nullable = strings.HasPrefix(typ, "Nullable")

	switch typ {
	case "UInt8", "UInt16", "UInt32", "UInt64", "Int8",
		"Int16", "Int32", "Int64",
		"Nullable(UInt8)", "Nullable(UInt16)", "Nullable(UInt32)", "Nullable(UInt64)",
		"Nullable(Int8)", "Nullable(Int16)", "Nullable(Int32)", "Nullable(Int64)":
		return "int", nullable
	case "Array(UInt8)", "Array(UInt16)", "Array(UInt32)",
		"Array(UInt64)", "Array(Int8)", "Array(Int16)", "Array(Int32)", "Array(Int64)":
		return "intArray", false
	case "String", "FixedString", "Nullable(String)":
		return "string", nullable
	case "Array(String)", "Array(FixedString)":
		return "stringArray", false
	case "Float32", "Float64", "Nullable(Float32)", "Nullable(Float64)":
		return "float", nullable
	case "Array(Float32)", "Array(Float64)":
		return "floatArray", false
	case "Date", "Nullable(Date)":
		return "Date", nullable
	case "DateTime", "Nullable(DateTime)":
		return "DateTime", nullable
	case "DateTime64", "Nullable(DateTime64)":
		return "DateTime64", nullable
	case "ElasticDateTime", "Nullable(ElasticDateTime)":
		return "ElasticDateTime", nullable
	default:
	}
	panic("unsupported type " + typ)
}
