package column

import (
	"github.com/houseflys/clickhouse_sinker/column/impls"
)

var (
	columns = map[string]IColumn{}
)

type creator func() IColumn

func regist(name string, creator creator) {
	columns[name] = creator()
}

// GetColumnByName get the IColumn by the name of type
func GetColumnByName(name string) IColumn {
	return columns[name]
}

func init() {
	regist("UInt8", func() IColumn {
		return impls.NewIntColumn(8, false)
	})
	regist("UInt16", func() IColumn {
		return impls.NewIntColumn(16, false)
	})
	regist("UInt32", func() IColumn {
		return impls.NewIntColumn(32, false)
	})
	regist("UInt64", func() IColumn {
		return impls.NewIntColumn(64, false)
	})

	regist("Int8", func() IColumn {
		return impls.NewIntColumn(8, false)
	})
	regist("Int16", func() IColumn {
		return impls.NewIntColumn(16, false)
	})
	regist("Int32", func() IColumn {
		return impls.NewIntColumn(32, false)
	})
	regist("Int64", func() IColumn {
		return impls.NewIntColumn(64, false)
	})

	regist("Float32", func() IColumn {
		return impls.NewFloatColumn(32)
	})
	regist("Float64", func() IColumn {
		return impls.NewFloatColumn(64)
	})

	regist("String", func() IColumn {
		return impls.NewStringColumn()
	})

	regist("FixedString", func() IColumn {
		return impls.NewStringColumn()
	})
}
