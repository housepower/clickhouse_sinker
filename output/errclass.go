package output

import (
	"context"
	"errors"
	"io"
	"net"
	"regexp"
	"strconv"

	chproto "github.com/ClickHouse/clickhouse-go/v2/lib/proto"
	"github.com/housepower/clickhouse_sinker/pool"
)

// ErrorClass 表示写入错误的可重试级别。
type ErrorClass int

const (
	// ClassRetryable 表示瞬时错误，可安全重试。
	ClassRetryable ErrorClass = iota
	// ClassFatal 表示永久性错误，重试无意义。
	ClassFatal
)

// 内置可重试白名单(梳理过的常见瞬时错误码)。
var builtinRetryableCodes = map[int32]bool{
	202:  true, // TOO_MANY_SIMULTANEOUS_QUERIES
	225:  true, // NO_ZOOKEEPER
	242:  true, // TABLE_IS_READ_ONLY
	252:  true, // TOO_MANY_PARTS
	319:  true, // UNKNOWN_STATUS_OF_INSERT
	999:  true, // KEEPER_EXCEPTION
	1000: true, // POCO_EXCEPTION
}

// 仅用于观测 label —— 结构级(列/表/库)错误码。
var structuralCodes = map[int32]bool{
	7: true, 8: true, 10: true, 15: true, 16: true, 47: true, 60: true, 81: true, 352: true,
}

// 仅用于观测 label —— 数据级(类型/解析/越界)错误码。
var dataCodes = map[int32]bool{
	6: true, 26: true, 27: true, 41: true, 53: true, 69: true, 70: true, 72: true, 117: true, 131: true,
}

var codeRe = regexp.MustCompile(`code:\s*(\d+)`)

// buildCodeSet 将 []int32 配置转换为 map 以便 O(1) 查询。
func buildCodeSet(codes []int32) map[int32]bool {
	if len(codes) == 0 {
		return nil
	}
	m := make(map[int32]bool, len(codes))
	for _, c := range codes {
		m[c] = true
	}
	return m
}

// chErrorCode 从(可能被包装的)错误里提取 ClickHouse 错误码。
// 先尝试结构化 *proto.Exception，失败则正则匹配错误串里的 "code: NNN"。
func chErrorCode(err error) (int32, bool) {
	var ex *chproto.Exception
	if errors.As(err, &ex) {
		return ex.Code, true
	}
	if m := codeRe.FindStringSubmatch(err.Error()); m != nil {
		if n, e := strconv.Atoi(m[1]); e == nil {
			return int32(n), true
		}
	}
	return 0, false
}

// isConnLevel 判断错误是否属于连接/网络层瞬时错误。
// context.Canceled 不算连接级(主动取消，视为 fatal)。
func isConnLevel(err error) bool {
	if errors.Is(err, context.Canceled) {
		return false
	}
	if errors.Is(err, pool.ErrAllReplicasDown) ||
		errors.Is(err, io.EOF) ||
		errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var ne net.Error
	return errors.As(err, &ne)
}

// labelForCode 根据错误码返回观测用 label。
func labelForCode(code int32, known bool) string {
	switch {
	case known && structuralCodes[code]:
		return "structural"
	case known && dataCodes[code]:
		return "data"
	default:
		return "unknown"
	}
}

// classifyError 判定错误可重试性，并返回观测 label。
// 判定顺序：fatal 覆盖 → 连接级 → 白名单/内置可重试 → 默认不可重试。
func classifyError(err error, extraRetryable, fatalOverride map[int32]bool) (ErrorClass, string) {
	code, known := chErrorCode(err)
	if known && fatalOverride[code] {
		return ClassFatal, labelForCode(code, known)
	}
	if isConnLevel(err) {
		return ClassRetryable, "transient"
	}
	if known && (builtinRetryableCodes[code] || extraRetryable[code]) {
		return ClassRetryable, "transient"
	}
	return ClassFatal, labelForCode(code, known)
}
