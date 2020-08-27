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
package util

import (
	"strings"

	"github.com/housepower/clickhouse_sinker/model"
	"github.com/kshvakov/clickhouse"
)

// There are only three cases for the value type of metric, (float64, string, map [string] interface {})
func GetValueByType(metric model.Metric, cwt *model.ColumnWithType) interface{} {
	swType := switchType(cwt.Type)
	var name string
	if name = cwt.SourceName; name == "" {
		name = strings.Replace(cwt.Name, ".", "\\.", -1)
	}
	switch swType {
	case "int":
		return metric.GetInt(name)
	case "float":
		return metric.GetFloat(name)
	case "string":
		return metric.GetString(name)
	case "stringArray":
		return clickhouse.Array(metric.GetArray(name, "string"))
	case "intArray":
		return clickhouse.Array(metric.GetArray(name, "int"))
	case "floatArray":
		return clickhouse.Array(metric.GetArray(name, "float"))
	case "ElasticDateTime":
		return metric.GetElasticDateTime(name)

	//never happen
	default:
		return ""
	}
}

func switchType(typ string) string {
	switch typ {
	case "Date", "DateTime", "UInt8", "UInt16", "UInt32", "UInt64", "Int8",
		"Int16", "Int32", "Int64", "Nullable(Date)", "Nullable(DateTime)",
		"Nullable(UInt8)", "Nullable(UInt16)", "Nullable(UInt32)", "Nullable(UInt64)",
		"Nullable(Int8)", "Nullable(Int16)", "Nullable(Int32)", "Nullable(Int64)":
		return "int"
	case "Array(Date)", "Array(DateTime)", "Array(UInt8)", "Array(UInt16)", "Array(UInt32)",
		"Array(UInt64)", "Array(Int8)", "Array(Int16)", "Array(Int32)", "Array(Int64)":
		return "intArray"
	case "String", "FixedString", "Nullable(String)":
		return "string"
	case "Array(String)", "Array(FixedString)":
		return "stringArray"
	case "Float32", "Float64", "Nullable(Float32)", "Nullable(Float64)":
		return "float"
	case "Array(Float32)", "Array(Float64)":
		return "floatArray"
	case "ElasticDateTime":
		return "ElasticDateTime"
	default:
		panic("unsupport type " + typ)
	}
}
