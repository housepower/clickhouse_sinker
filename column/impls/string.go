package impls

type StringColumn struct {
}

func (c *StringColumn) Name() string {
	return "String"
}

func (c *StringColumn) DefaultValue() interface{} {
	return ""
}

func NewStringColumn() *StringColumn {
	return &StringColumn{}
}

// only judge string column
func (c *StringColumn) GetValue(val interface{}) interface{} {
	switch val.(type) {
	case string:
		return val.(string)
	}
	return ""
}
