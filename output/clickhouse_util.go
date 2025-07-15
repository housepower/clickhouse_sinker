package output

import (
	"fmt"
	"strings"

	"github.com/thanos-io/thanos/pkg/errors"
	"github.com/viru-tech/clickhouse_sinker/model"
	"github.com/viru-tech/clickhouse_sinker/pool"
	"github.com/viru-tech/clickhouse_sinker/util"
)

func writeRows(prepareSQL string, rows model.Rows, idxBegin, idxEnd int, conn *pool.Conn) (numBad int, err error) {
	return conn.Write(prepareSQL, rows, idxBegin, idxEnd)
}

func getDims(database, table string, excludedColumns []string, parser string, conn *pool.Conn) (dims []*model.ColumnWithType, err error) {
	var rs *pool.Rows
	notNullable := make(map[string]bool)
	if rs, err = conn.Query(fmt.Sprintf(referedSQLTemplate, database, table)); err != nil {
		err = errors.Wrapf(err, "")
		return
	}
	var default_expression, referenced_col_type, col_name, ori_type string
	for rs.Next() {
		if err = rs.Scan(&default_expression, &referenced_col_type, &col_name, &ori_type); err != nil {
			err = errors.Wrapf(err, "")
			return
		}
		if strings.HasPrefix(referenced_col_type, "Nullable(") && !strings.HasSuffix(ori_type, "Nullable(") {
			notNullable[default_expression] = true
		}
	}

	rs.Close()
	if rs, err = conn.Query(fmt.Sprintf(selectSQLTemplate, database, table)); err != nil {
		err = errors.Wrapf(err, "")
		return
	}
	defer rs.Close()

	dims = make([]*model.ColumnWithType, 0, 10)
	var name, typ, defaultKind string
	for rs.Next() {
		if err = rs.Scan(&name, &typ, &defaultKind); err != nil {
			err = errors.Wrapf(err, "")
			return
		}
		if !util.StringContains(excludedColumns, name) && defaultKind != "MATERIALIZED" {
			nnull, ok := notNullable[name]
			if !ok {
				nnull = false
			}
			dims = append(dims, &model.ColumnWithType{
				Name:        name,
				Type:        model.WhichType(typ),
				SourceName:  util.GetSourceName(parser, name),
				NotNullable: nnull,
			})
		}
	}
	if len(dims) == 0 {
		err = errors.Wrapf(ErrTblNotExist, "%s.%s", database, table)
		return
	}
	return
}
