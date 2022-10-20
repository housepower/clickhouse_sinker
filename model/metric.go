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
	"regexp"
	"sync"
)

// Metric interface for metric collection
type Metric interface {
	GetBool(key string, nullable bool) (val interface{})
	GetInt8(key string, nullable bool) (val interface{})
	GetInt16(key string, nullable bool) (val interface{})
	GetInt32(key string, nullable bool) (val interface{})
	GetInt64(key string, nullable bool) (val interface{})
	GetUint8(key string, nullable bool) (val interface{})
	GetUint16(key string, nullable bool) (val interface{})
	GetUint32(key string, nullable bool) (val interface{})
	GetUint64(key string, nullable bool) (val interface{})
	GetFloat32(key string, nullable bool) (val interface{})
	GetFloat64(key string, nullable bool) (val interface{})
	GetDecimal(key string, nullable bool) (val interface{})
	GetDateTime(key string, nullable bool) (val interface{})
	GetString(key string, nullable bool) (val interface{})
	GetUUID(key string, nullable bool) (val interface{})
	GetArray(key string, t int) (val interface{})
	GetNewKeys(knownKeys, newKeys, warnKeys *sync.Map, white, black *regexp.Regexp, partition int, offset int64) bool
}

// DimMetrics
type DimMetrics struct {
	Dims   []*ColumnWithType
	Fields []*ColumnWithType
}

// ColumnWithType
type ColumnWithType struct {
	Name       string
	Type       int
	Nullable   bool
	Array      bool
	SourceName string
	// Const is used to set column value to some constant from config.
	Const string
}
