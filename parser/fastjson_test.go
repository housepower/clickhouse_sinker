package parser

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/valyala/fastjson"
)

func TestFastjsonDetectSchema(t *testing.T) {
	pp, _ := NewParserPool("fastjson", nil, "", "")
	parser := pp.Get()
	defer pp.Put(parser)
	metric, _ := parser.Parse(jsonSample)

	act := make(map[string]string)
	c, _ := metric.(*FastjsonMetric)
	var obj *fastjson.Object
	var err error
	if obj, err = c.value.Object(); err != nil {
		return
	}
	obj.Visit(func(key []byte, v *fastjson.Value) {
		act[string(key)] = v.Type().String()
	})
	require.Equal(t, jsonSchema, act)
}
