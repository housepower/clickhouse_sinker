package model

type Metric interface {
	Get(key string) interface{}
	GetString(key string) string
	GetArray(key string, t string) interface{}
	GetFloat(key string) float64
	GetInt(key string) int64
}

type DimMetrics struct {
	Dims   []*ColumnWithType
	Fields []*ColumnWithType
}

type ColumnWithType struct {
	Name string
	Type string
}
