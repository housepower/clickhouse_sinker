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
	Int8
	Int16
	Int32
	Int64
	Uint8
	Uint16
	Uint32
	Uint64
	Float32
	Float64
	Decimal
	DateTime
	String
)

type TypeInfo struct {
	Type     int
	Nullable bool
	Array    bool
}

var (
	typeInfo map[string]TypeInfo
)

func GetTypeName(typ int) (name string) {
	switch typ {
	case Bool:
		name = "Bool"
	case Int8:
		name = "Int8"
	case Int16:
		name = "Int16"
	case Int32:
		name = "Int32"
	case Int64:
		name = "Int64"
	case Uint8:
		name = "Uint8"
	case Uint16:
		name = "Uint16"
	case Uint32:
		name = "Uint32"
	case Uint64:
		name = "Uint64"
	case Float32:
		name = "Float32"
	case Float64:
		name = "Float64"
	case Decimal:
		name = "Decimal"
	case DateTime:
		name = "DateTime"
	case String:
		name = "String"
	default:
		name = "Unknown"
	}
	return
}

func GetValueByType(metric Metric, cwt *ColumnWithType) (val interface{}) {
	name := cwt.SourceName
	if cwt.Array {
		val = metric.GetArray(name, cwt.Type)
	} else {
		switch cwt.Type {
		case Bool:
			val = metric.GetBool(name, cwt.Nullable)
		case Int8:
			val = metric.GetInt8(name, cwt.Nullable)
		case Int16:
			val = metric.GetInt16(name, cwt.Nullable)
		case Int32:
			val = metric.GetInt32(name, cwt.Nullable)
		case Int64:
			val = metric.GetInt64(name, cwt.Nullable)
		case Uint8:
			val = metric.GetUint8(name, cwt.Nullable)
		case Uint16:
			val = metric.GetUint16(name, cwt.Nullable)
		case Uint32:
			val = metric.GetUint32(name, cwt.Nullable)
		case Uint64:
			val = metric.GetUint64(name, cwt.Nullable)
		case Float32:
			val = metric.GetFloat32(name, cwt.Nullable)
		case Float64:
			val = metric.GetFloat64(name, cwt.Nullable)
		case Decimal:
			val = metric.GetDecimal(name, cwt.Nullable)
		case DateTime:
			val = metric.GetDateTime(name, cwt.Nullable)
		case String:
			val = metric.GetString(name, cwt.Nullable)
		default:
			util.Logger.Fatal("LOGIC ERROR: reached switch default condition")
		}
	}
	return
}

func WhichType(typ string) (dataType int, nullable bool, array bool) {
	ti, ok := typeInfo[typ]
	if ok {
		dataType, nullable, array = ti.Type, ti.Nullable, ti.Array
		return
	}
	origTyp := typ
	nullable = strings.HasPrefix(typ, "Nullable(")
	array = strings.HasPrefix(typ, "Array(")
	if nullable {
		typ = typ[len("Nullable(") : len(typ)-1]
	} else if array {
		typ = typ[len("Array(") : len(typ)-1]
	}
	if strings.HasPrefix(typ, "DateTime64") {
		dataType = DateTime
	} else if strings.HasPrefix(typ, "FixedString") {
		dataType = String
	} else if strings.HasPrefix(typ, "Enum8(") {
		dataType = String
	} else if strings.HasPrefix(typ, "Enum16(") {
		dataType = String
	} else {
		util.Logger.Fatal(fmt.Sprintf("ClickHouse column type %v is not inside supported ones: %v", origTyp, typeInfo))
	}
	typeInfo[origTyp] = TypeInfo{Type: dataType, Nullable: nullable, Array: array}
	return
}

func init() {
	primTypeInfo := make(map[string]TypeInfo)
	typeInfo = make(map[string]TypeInfo)
	for _, t := range []int{Bool, Int8, Int16, Int32, Int64, Uint8, Uint16, Uint32, Uint64, Float32, Float64, DateTime, String} {
		tn := GetTypeName(t)
		primTypeInfo[tn] = TypeInfo{Type: t}
		nullTn := fmt.Sprintf("Nullable(%s)", tn)
		primTypeInfo[nullTn] = TypeInfo{Type: t, Nullable: true}
		arrTn := fmt.Sprintf("Array(%s)", tn)
		primTypeInfo[arrTn] = TypeInfo{Type: t, Array: true}
	}
	primTypeInfo["UUID"] = TypeInfo{Type: String}
	primTypeInfo["Nullable(UUID)"] = TypeInfo{Type: String, Nullable: true}
	primTypeInfo["Array(UUID)"] = TypeInfo{Type: String, Array: true}
	primTypeInfo["Date"] = TypeInfo{Type: DateTime}
	primTypeInfo["Nullable(Date)"] = TypeInfo{Type: DateTime, Nullable: true}
	primTypeInfo["Array(Date)"] = TypeInfo{Type: DateTime, Array: true}
}
