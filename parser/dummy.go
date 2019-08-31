package parser

type DummyMetric struct {
}

func (c *DummyMetric) Get(key string) interface{} {
	return ""
}

func (c *DummyMetric) GetString(key string) string {
	return ""
}

func (c *DummyMetric) GetFloat(key string) float64 {
	return 0
}

func (c *DummyMetric) GetInt(key string) int64 {
	return 0
}

// GetArray is Empty implemented for DummyMetric
func (c *DummyMetric) GetArray(key string, t string) []interface{} {
	return []interface{}{}
}

func (c *DummyMetric) String() string {
	return "_dummy"
}
