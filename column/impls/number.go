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
package impls

import (
	"fmt"
)

// IntColumn
type IntColumn struct {
	name string
}

// NewIntColumn get an instance of Int column
func NewIntColumn(bits int, isUint bool) *IntColumn {
	name := fmt.Sprintf("Int%d", bits)
	if isUint {
		name = "U" + name
	}
	return &IntColumn{name: name}
}

// Name this column name
func (c *IntColumn) Name() string {
	return c.name
}

// DefaultValue for int column 0
func (c *IntColumn) DefaultValue() interface{} {
	return int64(0)
}

// only judge int and float64
func (c *IntColumn) GetValue(val interface{}) interface{} {
	switch v := val.(type) {
	case int:
		return int64(v)
	case float64:
		return int64(v)
	default:
		return int64(0)
	}
}

// FloatColumn
type FloatColumn struct {
	name string
	bits int
}

// NewFloatColumn new instance of Float column
func NewFloatColumn(bits int) *FloatColumn {
	name := fmt.Sprintf("Float%d", bits)
	return &FloatColumn{name: name, bits: bits}
}

// Name return the column name
func (c *FloatColumn) Name() string {
	return c.name
}

// DefaultValue of float column 0
func (c *FloatColumn) DefaultValue() interface{} {
	if c.bits == 32 {
		return float32(0)
	}
	return float64(0)
}

// only judge int and float64
func (c *FloatColumn) GetValue(val interface{}) interface{} {
	switch v := val.(type) {
	case int:
		if c.bits == 32 {
			return float32(v)
		}
		return float64(v)
	case float64:
		if c.bits == 32 {
			return float32(v)
		}
		return v
	}
	if c.bits == 32 {
		return float32(0)
	}
	return float64(0)
}
