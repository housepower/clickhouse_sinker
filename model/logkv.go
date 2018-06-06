package model

import (
	"github.com/houseflys/clickhouse_sinker/column"
)

type LogKV map[string]interface{}

func (logkv LogKV) GetValueByType(key string, typ string) interface{} {
	val := logkv[key]
	col := column.GetColumnByName(typ)
	if val == nil {
		return col.DefaultValue()
	}
	return col.GetValue(val)
}
