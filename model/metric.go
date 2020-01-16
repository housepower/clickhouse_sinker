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

// Metric interface for metric collection
type Metric interface {
	Get(key string) interface{}
	GetString(key string) string
	GetArray(key string, t string) interface{}
	GetFloat(key string) float64
	GetInt(key string) int64
	GetElasticDateTime(key string) int64
}

// DimMetrics
type DimMetrics struct {
	Dims   []*ColumnWithType
	Fields []*ColumnWithType
}

// ColumnWithType
type ColumnWithType struct {
	Name string
	Type string
}
