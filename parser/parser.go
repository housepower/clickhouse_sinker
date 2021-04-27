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
	"strconv"
	"sync"
	"time"

	"github.com/housepower/clickhouse_sinker/model"
	"github.com/pkg/errors"
)

var (
	Layouts = []string{
		//DateTime
		"2006-01-02T15:04:05.999999999Z07:00", //time.RFC3339Nano, `date --iso-8601=ns` output format
		"2006-01-02T15:04:05Z07:00",           //time.RFC3339, `date --iso-8601=s` on Ubuntu 20.04
		"2006-01-02T15:04:05-0700",            //`date --iso-8601=s` on CentOS 7.6
		"2006-01-02T15:04:05.999999999",
		"2006-01-02T15:04:05",
		"2006-01-02 15:04:05.999999999Z07:00", //`date --rfc-3339=ns` output format
		"2006-01-02 15:04:05Z07:00",           //`date --rfc-3339=s` output format
		"2006-01-02 15:04:05.999999999",
		"2006-01-02 15:04:05",
		"Jan 02, 2006 15:04:05.999999999Z07:00",
		"Jan 02, 2006 15:04:05.999999999",
		"Jan 02, 2006 15:04:05",
		"Jan 02, 2006 03:04:05 PM",
		"02/01/2006 15:04:05.999999999",
		"02/01/06 15:04:05.999999999",
		"02/Jan/2006 15:04:05 Z07:00",
		"02/Jan/2006 15:04:05 -0700",
		"Mon Jan _2 15:04:05 2006",        //time.ANSIC
		"Mon Jan _2 15:04:05 MST 2006",    //time.UnixDate
		"Mon Jan 02 15:04:05 -0700 2006",  //time.RubyDate
		"02 Jan 06 15:04 MST",             //time.RFC822
		"02 Jan 06 15:04 -0700",           //time.RFC822Z
		"Monday, 02-Jan-06 15:04:05 MST",  //time.RFC850
		"Mon, 02 Jan 2006 15:04:05 MST",   //time.RFC1123
		"Mon, 02 Jan 2006 15:04:05 -0700", //time.RFC1123Z
		"Mon Jan 02 15:04:05 MST 2006",
		"Mon 02 Jan 2006 03:04:05 PM MST", // `date` default output format
		//Date
		"2006-01-02",
		"02/01/2006",
		"02/Jan/2006",
		"Jan 02, 2006",
		"Mon Jan 02, 2006",
	}
	Epoch = time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
)

// Parse is the Parser interface
type Parser interface {
	Parse(bs []byte) (metric model.Metric, err error)
}

// Pool may be used for pooling Parsers for similarly typed JSONs.
type Pool struct {
	name         string
	csvFormat    map[string]int
	delimiter    string
	timeZone     *time.Location
	knownLayouts sync.Map
	pool         sync.Pool
}

// NewParserPool creates a parser pool
func NewParserPool(name string, csvFormat []string, delimiter string, timezone string) (pp *Pool, err error) {
	var tz *time.Location
	if timezone == "" {
		tz = time.Local
	} else if tz, err = time.LoadLocation(timezone); err != nil {
		err = errors.Wrapf(err, "")
		return
	}
	pp = &Pool{
		name:      name,
		delimiter: delimiter,
		timeZone:  tz,
	}
	if csvFormat != nil {
		pp.csvFormat = make(map[string]int)
		for i, title := range csvFormat {
			pp.csvFormat[title] = i
		}
	}
	return
}

// Get returns a Parser from pp.
//
// The Parser must be Put to pp after use.
func (pp *Pool) Get() Parser {
	v := pp.pool.Get()
	if v == nil {
		switch pp.name {
		case "gjson":
			return &GjsonParser{pp: pp}
		case "fastjson":
			return &FastjsonParser{pp: pp}
		case "csv":
			return &CsvParser{pp: pp}
		default:
			return &FastjsonParser{pp: pp}
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

func (pp *Pool) ParseDateTime(key string, val string) (t time.Time, err error) {
	var layout string
	var lay interface{}
	var ok bool
	if lay, ok = pp.knownLayouts.Load(key); !ok {
		for _, layout = range Layouts {
			if t, err = time.ParseInLocation(layout, val, pp.timeZone); err == nil {
				t = t.In(time.UTC)
				pp.knownLayouts.Store(key, layout)
				return
			}
		}
		pp.knownLayouts.Store(key, nil)
	}
	if lay == nil {
		err = errors.Errorf("cannot parse time %s at field %s", strconv.Quote(val), key)
		return
	}
	layout, _ = lay.(string)
	if t, err = time.ParseInLocation(layout, val, pp.timeZone); err != nil {
		return
	}
	t = t.In(time.UTC)
	return
}

func GetJSONShortStr(v interface{}) string {
	bs, _ := json.Marshal(v)
	return string(bs)
}
