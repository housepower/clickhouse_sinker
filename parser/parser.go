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
	"encoding/json"

	"github.com/housepower/clickhouse_sinker/model"
)

// Parse is the Parser interface
type Parser interface {
	Parse(bs []byte) (metric model.Metric, err error)
}

// NewParser is a factory method to generate new parse
func NewParser(name string, csvFormat []string, delimiter string) Parser {
	switch name {
	case "json", "gjson":
		return &GjsonParser{}
	case "fastjson":
		return &FastjsonParser{}
	case "csv":
		return &CsvParser{title: csvFormat, delimiter: delimiter}
	//extend gjson that could extract the map
	case "gjson_extend":
		return &GjsonExtendParser{}
	default:
		return &GjsonParser{}
	}
}

func GetJSONShortStr(v interface{}) string {
	bs, _ := json.Marshal(v)
	return string(bs)
}
