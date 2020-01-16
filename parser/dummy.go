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

type DummyMetric struct {
}

func (c *DummyMetric) Get(key string) interface{} {
	return ""
}

func (c *DummyMetric) GetString(key string) string {
	return ""
}

func (c *DummyMetric) GetFloat(key string) float64 {
	return 0
}

func (c *DummyMetric) GetInt(key string) int64 {
	return 0
}

// GetArray is Empty implemented for DummyMetric
func (c *DummyMetric) GetArray(key string, t string) interface{} {
	return []string{}
}

func (c *DummyMetric) String() string {
	return "_dummy"
}
func (c *DummyMetric) GetElasticDateTime(key string) int64 {
	return 0
}
