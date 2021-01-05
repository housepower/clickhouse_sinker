package parser

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFastjsonArray(t *testing.T) {
	pp := NewParserPool("fastjson", nil, "", DefaultTSLayout)
	parser := pp.Get()
	defer pp.Put(parser)
	metric, _ := parser.Parse(jsonSample)

	actI := metric.GetArray("array_int", "int").([]int64)
	expI := []int64{1, 2, 3}
	require.Equal(t, expI, actI)

	actF := metric.GetArray("array_float", "float").([]float64)
	expF := []float64{1.1, 2.2, 3.3}
	require.Equal(t, expF, actF)

	actS := metric.GetArray("array_string", "string").([]string)
	expS := []string{"aa", "bb", "cc"}
	require.Equal(t, expS, actS)

	actIE := metric.GetArray("array_empty", "int").([]int64)
	expIE := []int64{}
	require.Equal(t, expIE, actIE)

	actFE := metric.GetArray("array_empty", "float").([]float64)
	expFE := []float64{}
	require.Equal(t, expFE, actFE)

	actSE := metric.GetArray("array_empty", "string").([]string)
	expSE := []string{}
	require.Equal(t, expSE, actSE)
}
