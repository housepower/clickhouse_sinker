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
	"fmt"
	"strings"

	"github.com/housepower/clickhouse_sinker/util"
)

const (
	Unknown = iota
	Bool
	Int
	Float
	Decimal
	String
	DateTime
	ElasticDateTime
	BoolArray
	IntArray
	FloatArray
	DecimalArray
	StringArray
	DateTimeArray
)

type TypeInfo struct {
	Type     int
	Nullable bool
}

var (
	typeInfo map[string]TypeInfo
)

func GetTypeName(typ int) (name string) {
	switch typ {
	case Bool:
		name = "Bool"
	case Int:
		name = "Int"
	case Float:
		name = "Float"
	case Decimal:
		name = "Decimal"
	case String:
		name = "String"
	case DateTime:
		name = "DateTime"
	case ElasticDateTime:
		name = "ElasticDateTime"
	case BoolArray:
		name = "BoolArray"
	case IntArray:
		name = "IntArray"
	case FloatArray:
		name = "FloatArray"
	case StringArray:
		name = "StringArray"
	case DateTimeArray:
		name = "DateTimeArray"
	default:
		name = "Unknown"
	}
	return
}

// There are only three cases for the value type of metric, (float64, string, map [string] interface {})
func GetValueByType(metric Metric, cwt *ColumnWithType) (val interface{}) {
	name := cwt.SourceName
	switch cwt.Type {
	case Bool:
		val = metric.GetBool(name, cwt.Nullable)
	case Int:
		val = metric.GetInt64(name, cwt.Nullable)
	case Float:
		val = metric.GetFloat64(name, cwt.Nullable)
	case Decimal:
		val = metric.GetDecimal(name, cwt.Nullable)
	case String:
		val = metric.GetString(name, cwt.Nullable)
	case DateTime:
		val = metric.GetDateTime(name, cwt.Nullable)
	case ElasticDateTime:
		val = metric.GetElasticDateTime(name, cwt.Nullable)
	case BoolArray:
		val = metric.GetArray(name, Bool)
	case IntArray:
		val = metric.GetArray(name, Int)
	case FloatArray:
		val = metric.GetArray(name, Float)
	case DecimalArray:
		val = metric.GetArray(name, Decimal)
	case StringArray:
		val = metric.GetArray(name, String)
	case DateTimeArray:
		val = metric.GetArray(name, DateTime)
	default:
		util.Logger.Fatal("LOGIC ERROR: reached switch default condition")
	}
	return
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
		dataType = DateTime
	} else if strings.HasPrefix(typ, "Array(DateTime64") {
		dataType = DateTimeArray
		nullable = false
	} else if strings.HasPrefix(typ, "Decimal") {
		dataType = Decimal
	} else if strings.HasPrefix(typ, "Array(Decimal") {
		dataType = DecimalArray
		nullable = false
	} else if strings.HasPrefix(typ, "FixedString") {
		dataType = String
	} else if strings.HasPrefix(typ, "Array(FixedString") {
		dataType = StringArray
		nullable = false
	} else if strings.HasPrefix(typ, "Enum8(") {
		dataType = String
	} else if strings.HasPrefix(typ, "Enum16(") {
		dataType = String
	} else {
		util.Logger.Fatal(fmt.Sprintf("LOGIC ERROR: unsupported ClickHouse data type %v", typ))
	}
	typeInfo[typ] = TypeInfo{Type: dataType, Nullable: nullable}
	return
}

func init() {
	primTypeInfo := make(map[string]TypeInfo)
	typeInfo = make(map[string]TypeInfo)
	primTypeInfo["Bool"] = TypeInfo{Type: Bool, Nullable: false}
	for _, t := range []string{"UInt8", "UInt16", "UInt32", "UInt64", "Int8",
		"Int16", "Int32", "Int64"} {
		primTypeInfo[t] = TypeInfo{Type: Int, Nullable: false}
	}
	for _, t := range []string{"Float32", "Float64"} {
		primTypeInfo[t] = TypeInfo{Type: Float, Nullable: false}
	}
	for _, t := range []string{"String", "UUID"} {
		primTypeInfo[t] = TypeInfo{Type: String, Nullable: false}
	}
	for _, t := range []string{"Date", "DateTime"} {
		primTypeInfo[t] = TypeInfo{Type: DateTime, Nullable: false}
	}
	primTypeInfo["ElasticDateTime"] = TypeInfo{Type: ElasticDateTime, Nullable: false}
	for k, v := range primTypeInfo {
		typeInfo[k] = v
		nullK := fmt.Sprintf("Nullable(%s)", k)
		typeInfo[nullK] = TypeInfo{Type: v.Type, Nullable: true}
		arrK := fmt.Sprintf("Array(%s)", k)
		switch v.Type {
		case Bool:
			typeInfo[arrK] = TypeInfo{Type: BoolArray, Nullable: false}
		case Int:
			typeInfo[arrK] = TypeInfo{Type: IntArray, Nullable: false}
		case Float:
			typeInfo[arrK] = TypeInfo{Type: FloatArray, Nullable: false}
		case String:
			typeInfo[arrK] = TypeInfo{Type: StringArray, Nullable: false}
		case DateTime:
			typeInfo[arrK] = TypeInfo{Type: DateTimeArray, Nullable: false}
		}
	}
}
