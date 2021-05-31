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
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/housepower/clickhouse_sinker/model"
	"github.com/housepower/clickhouse_sinker/util"
	"github.com/pkg/errors"
)

var (
	Layouts = []string{
		//DateTime, RFC3339
		"2006-01-02T15:04:05Z07:00", //time.RFC3339, `date --iso-8601=s` on Ubuntu 20.04
		"2006-01-02T15:04:05Z0700",  //`date --iso-8601=s` on CentOS 7.6
		"2006-01-02T15:04:05",
		//DateTime, ISO8601
		"2006-01-02 15:04:05Z07:00", //`date --rfc-3339=s` output format
		"2006-01-02 15:04:05Z0700",
		"2006-01-02 15:04:05",
		//DateTime, other layouts supported by golang
		"Mon Jan _2 15:04:05 2006",        //time.ANSIC
		"Mon Jan _2 15:04:05 MST 2006",    //time.UnixDate
		"Mon Jan 02 15:04:05 -0700 2006",  //time.RubyDate
		"02 Jan 06 15:04 MST",             //time.RFC822
		"02 Jan 06 15:04 -0700",           //time.RFC822Z
		"Monday, 02-Jan-06 15:04:05 MST",  //time.RFC850
		"Mon, 02 Jan 2006 15:04:05 MST",   //time.RFC1123
		"Mon, 02 Jan 2006 15:04:05 -0700", //time.RFC1123Z
		//DateTime, linux utils
		"Mon Jan 02 15:04:05 MST 2006",    // `date` on CentOS 7.6 default output format
		"Mon 02 Jan 2006 03:04:05 PM MST", // `date` on Ubuntu 20.4 default output format
		//DateTime, home-brewed
		"Jan 02, 2006 15:04:05Z07:00",
		"Jan 02, 2006 15:04:05Z0700",
		"Jan 02, 2006 15:04:05",
		"02/Jan/2006 15:04:05 Z07:00",
		"02/Jan/2006 15:04:05 Z0700",
		"02/Jan/2006 15:04:05",
		//Date
		"2006-01-02",
		"02/01/2006",
		"02/Jan/2006",
		"Jan 02, 2006",
		"Mon Jan 02, 2006",
	}
	Epoch            = time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
	ErrParseDateTime = errors.Errorf("value doesn't contain DateTime")
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

// Assuming that all values of a field of kafka message has the same layout, and layouts of each field are unrelated.
// Automatically detect the layout from till the first successful detection and reuse that layout forever.
// Return time in UTC.
func (pp *Pool) ParseDateTime(key string, val string) (t time.Time, err error) {
	var layout string
	var lay interface{}
	var ok bool
	var t2 time.Time
	if val == "" {
		err = ErrParseDateTime
		return
	}
	if lay, ok = pp.knownLayouts.Load(key); !ok {
		t2, layout = parseInLocation(val, pp.timeZone)
		if layout == "" {
			err = ErrParseDateTime
			return
		}
		t = t2
		pp.knownLayouts.Store(key, layout)
		return
	}
	if layout, ok = lay.(string); !ok {
		err = ErrParseDateTime
		return
	}
	if t2, err = time.ParseInLocation(layout, val, pp.timeZone); err != nil {
		err = ErrParseDateTime
		return
	}
	t = t2.UTC()
	return
}

func makeArray(typ int) (val interface{}) {
	switch typ {
	case model.Int:
		val = []int64{}
	case model.Float:
		val = []float64{}
	case model.String:
		val = []string{}
	case model.DateTime:
		val = []time.Time{}
	default:
		util.Logger.Fatal(fmt.Sprintf("LOGIC ERROR: unsupported array type %v", typ))
	}
	return
}

func parseInLocation(val string, loc *time.Location) (t time.Time, layout string) {
	var err error
	var lay string
	for _, lay = range Layouts {
		if t, err = time.ParseInLocation(lay, val, loc); err == nil {
			t = t.UTC()
			layout = lay
			return
		}
	}
	return
}

func UnixInt(sec int64) (t time.Time) {
	//2^32 seconds since epoch: 2106-02-07T06:28:16Z
	if sec < 0 || sec >= 4294967296 {
		return Epoch
	}
	return time.Unix(sec, 0).UTC()
}

func UnixFloat(sec float64) (t time.Time) {
	//2^32 seconds since epoch: 2106-02-07T06:28:16Z
	if sec < 0 || sec >= 4294967296.0 {
		return Epoch
	}
	i, f := math.Modf(sec)
	return time.Unix(int64(i), int64(f*1e9)).UTC()
}
