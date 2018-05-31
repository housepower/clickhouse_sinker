package column

// IColumn is the Column interface for clickhouse
type IColumn interface {
	Name() string
	DefaultValue() interface{}
	GetValue(val interface{}) interface{}
}
