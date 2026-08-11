package task

import (
	"regexp"
	"strconv"
	"strings"
)

// labelPair 是拼 labels JSON 前收集到的一个候选键值对。
type labelPair struct {
	key string
	val string
}

// labelFilter 决定一个 label key 是否应当出现在 labels JSON 中。零值不过滤任何 key。
type labelFilter struct {
	nameKey string         // metric 名所在的列，它是指标标识而非 label
	blkList *regexp.Regexp // promLabelsBlackList，可为 nil
}

func (f labelFilter) accept(key string) bool {
	if f.nameKey != "" && key == f.nameKey {
		return false
	}
	// "labels" JSON excludes "le", so that "labels" can be used as group key for histogram queries.
	if key == "le" {
		return false
	}
	return f.blkList == nil || !f.blkList.MatchString(key)
}

// pairLabelArrays 把平行的 key/value 数组按下标配对。长度不等时取较短者，
// dropped 返回被丢弃的元素个数，交由调用方观测——这是上游 ETL 的数据错误，不能静默截断。
func pairLabelArrays(keys, vals []string) (pairs []labelPair, dropped int) {
	n := len(keys)
	if len(vals) < n {
		n = len(vals)
	}
	if d := len(keys) - n; d > 0 {
		dropped += d
	}
	if d := len(vals) - n; d > 0 {
		dropped += d
	}
	pairs = make([]labelPair, 0, n)
	for i := 0; i < n; i++ {
		pairs = append(pairs, labelPair{key: keys[i], val: vals[i]})
	}
	return
}

// buildLabelsJSON 过滤、去重并拼出 series 表 labels 列的 JSON 文本。重复 key 只保留
// 第一次出现的，输出顺序即 pairs 的顺序。格式必须与历史实现逐字节一致，下游查询依赖它。
func buildLabelsJSON(pairs []labelPair, f labelFilter) string {
	var sb strings.Builder
	seen := make(map[string]struct{}, len(pairs))
	sb.WriteByte('{')
	for _, p := range pairs {
		if !f.accept(p.key) {
			continue
		}
		if _, dup := seen[p.key]; dup {
			continue
		}
		seen[p.key] = struct{}{}
		if len(seen) > 1 {
			sb.WriteString(", ")
		}
		sb.WriteString(strconv.Quote(p.key))
		sb.WriteString(": ")
		sb.WriteString(strconv.Quote(p.val))
	}
	sb.WriteByte('}')
	return sb.String()
}
