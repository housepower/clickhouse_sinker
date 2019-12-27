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
	"strconv"
	"strings"

	"github.com/housepower/clickhouse_sinker/model"
)

type CsvParser struct {
	title     []string
	delimiter string
}

func (c *CsvParser) Parse(bs []byte) model.Metric {
	msgData := string(bs)
	msgs := strings.Split(msgData, c.delimiter)
	v := make(map[string]string)
	msLen := len(msgs)
	for i, key := range c.title {
		if i >= msLen {
			continue
		}
		v[key] = msgs[i]
	}
	return &CsvMetric{v}
}

type CsvMetric struct {
	mp map[string]string
}

func (c *CsvMetric) Get(key string) interface{} {
	return c.mp[key]
}

func (c *CsvMetric) GetString(key string) string {
	v, _ := c.mp[key]
	return v
}

func (c *CsvMetric) GetFloat(key string) float64 {
	v, _ := c.mp[key]
	n, _ := strconv.ParseFloat(v, 64)
	return n
}

func (c *CsvMetric) GetInt(key string) int64 {
	v, _ := c.mp[key]
	n, _ := strconv.ParseInt(v, 10, 64)
	return n
}

// GetArray is Empty implemented for CsvMetric
func (c *CsvMetric) GetArray(key string, t string) interface{} {
	return []interface{}{}
}
