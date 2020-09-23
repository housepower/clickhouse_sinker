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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestParseCsv(t *testing.T) {
	testCases := []struct {
		msg    string
		values []string
	}{
		{
			`1,"DO,NOT,SPLIT",42`,
			[]string{"1", "DO,NOT,SPLIT", "42"},
		},

		{
			`2,Daniel,26`,
			[]string{"2", "Daniel", "26"},
		},

		{
			`2,Daniel,`,
			[]string{"2", "Daniel", ""},
		},

		{
			`2,,Daniel`,
			[]string{"2", "", "Daniel"},
		},
	}

	csvParser := NewParser("csv", nil, ",", []string{"2006-01-02", time.RFC3339, time.RFC3339})
	for _, c := range testCases {
		metric, _ := csvParser.Parse([]byte(c.msg))
		csvMetric, ok := metric.(*CsvMetric)
		assert.Equal(t, ok, true)
		assert.Equal(t, c.values, csvMetric.values)
	}
}
