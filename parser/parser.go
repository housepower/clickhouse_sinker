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
	Parse(bs []byte) model.Metric
}

// NewParser is a factory method to generate new parse
func NewParser(typ string, title []string, delimiter string) Parser {
	switch typ {
	case "json", "gjson":
		return &GjsonParser{}
	case "fastjson":
		return &FastjsonParser{}
	case "csv":
		return &CsvParser{title: title, delimiter: delimiter}
	case "gjson_extend": //extend gjson that could extract the map
		return &GjsonExtendParser{}
	default:
		return &GjsonParser{}
	}
}

func GetJSONShortStr(v interface{}) string {
	bs, _ := json.Marshal(v)
	return string(bs)
}
