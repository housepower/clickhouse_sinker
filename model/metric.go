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
	"time"
)

// Metric interface for metric collection
type Metric interface {
	GetBool(key string, nullable bool) interface{}
	GetInt8(key string, nullable bool) interface{}
	GetInt16(key string, nullable bool) interface{}
	GetInt32(key string, nullable bool) interface{}
	GetInt64(key string, nullable bool) interface{}
	GetUint8(key string, nullable bool) interface{}
	GetUint16(key string, nullable bool) interface{}
	GetUint32(key string, nullable bool) interface{}
	GetUint64(key string, nullable bool) interface{}
	GetFloat32(key string, nullable bool) interface{}
	GetFloat64(key string, nullable bool) interface{}
	GetDecimal(key string, nullable bool) interface{}
	GetDateTime(key string, nullable bool) interface{}
	GetString(key string, nullable bool) interface{}
	GetUUID(key string, nullable bool) interface{}
	GetObject(key string, nullable bool) interface{}
	GetMap(key string, typeinfo *TypeInfo) interface{}
	GetArray(key string, t int) interface{}
	GetIPv4(key string, nullable bool) interface{}
	GetIPv6(key string, nullable bool) interface{}
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
	Type       *TypeInfo
	SourceName string
	NotNullable bool

	// Const is used to set column value to some constant from config.
	Const string
}

// struct for ingesting a clickhouse Map type value
type OrderedMap struct {
	keys   []interface{}
	values map[interface{}]interface{}
}

func (om *OrderedMap) Get(key interface{}) (interface{}, bool) {
	if value, present := om.values[key]; present {
		return value, present
	}
	return nil, false
}

func (om *OrderedMap) Put(key interface{}, value interface{}) {
	if _, present := om.values[key]; present {
		om.values[key] = value
		return
	}
	om.keys = append(om.keys, key)
	om.values[key] = value
}

func (om *OrderedMap) Keys() <-chan interface{} {
	ch := make(chan interface{})
	go func() {
		defer close(ch)
		for _, key := range om.keys {
			ch <- key
		}
	}()
	return ch
}

func (om *OrderedMap) GetValues() map[interface{}]interface{} {
	return om.values
}

func NewOrderedMap() *OrderedMap {
	om := OrderedMap{}
	om.keys = []interface{}{}
	om.values = map[interface{}]interface{}{}
	return &om
}

type SeriesQuota struct {
	sync.RWMutex   `json:"-"`
	NextResetQuota time.Time
	BmSeries       map[int64]int64
	WrSeries       int
	Birth          time.Time
}
