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
	"sync"
	"time"

	"github.com/housepower/clickhouse_sinker/model"
)

var (
	DefaultTSLayout = []string{"2006-01-02", time.RFC3339Nano, time.RFC3339Nano}
	Epoch           = time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
)

// Parse is the Parser interface
type Parser interface {
	Parse(bs []byte) (metric model.Metric, err error)
}

// Pool may be used for pooling Parsers for similarly typed JSONs.
type Pool struct {
	name      string
	csvFormat []string
	delimiter string
	tsLayout  []string
	pool      sync.Pool
}

// NewParserPool create a parser pool
func NewParserPool(name string, csvFormat []string, delimiter string, tsLayout []string) *Pool {
	return &Pool{
		name:      name,
		csvFormat: csvFormat,
		delimiter: delimiter,
		tsLayout:  tsLayout,
	}
}

// Get returns a Parser from pp.
//
// The Parser must be Put to pp after use.
func (pp *Pool) Get() Parser {
	v := pp.pool.Get()
	if v == nil {
		switch pp.name {
		case "json", "gjson":
			return &GjsonParser{pp.tsLayout}
		case "fastjson":
			return &FastjsonParser{tsLayout: pp.tsLayout}
		case "csv":
			return &CsvParser{pp.csvFormat, pp.delimiter, pp.tsLayout}
		//extend gjson that could extract the map
		case "gjson_extend":
			return &GjsonExtendParser{pp.tsLayout}
		default:
			return &GjsonParser{pp.tsLayout}
		}
	}
	return v.(Parser)
}

// Put returns p to pp.
//
// p and objects recursively returned from p cannot be used after p
// is put into pp.
func (pp *Pool) Put(p Parser) {
	pp.pool.Put(p)
}

func GetJSONShortStr(v interface{}) string {
	bs, _ := json.Marshal(v)
	return string(bs)
}
