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

package util

import (
	"github.com/stretchr/testify/assert"

	"testing"
)

// TestBind
func TestBind(t *testing.T) {
	config := map[string]interface{}{
		"a": "A",
		"b": 1,
		"c": 3.0,
		"x": 1,
		"z": "zz",
	}

	type Foo struct {
		X int
		Z string
	}
	type Entity struct {
		A string
		B int
		C float32
		Foo
	}
	var entity = &Entity{}
	IngestConfig(config, entity)

	assert.Equal(t, &Entity{A: "A", B: 1, C: 3.0, Foo: Foo{1, "zz"}}, entity, "must be equal")
}
