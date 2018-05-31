package util

import (
	"github.com/stretchr/testify/assert"

	"testing"
)

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
