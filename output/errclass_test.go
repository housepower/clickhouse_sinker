package output

import (
	"context"
	"fmt"
	"io"
	"testing"

	chproto "github.com/ClickHouse/clickhouse-go/v2/lib/proto"
	"github.com/housepower/clickhouse_sinker/pool"
	"github.com/thanos-io/thanos/pkg/errors"
)

func TestClassifyError(t *testing.T) {
	cases := []struct {
		name  string
		err   error
		class ErrorClass
		label string
	}{
		{"too_many_parts", &chproto.Exception{Code: 252, Message: "too many parts"}, ClassRetryable, "transient"},
		{"keeper", &chproto.Exception{Code: 999, Message: "zk"}, ClassRetryable, "transient"},
		{"type_mismatch", &chproto.Exception{Code: 53, Message: "type"}, ClassFatal, "data"},
		{"no_such_column", &chproto.Exception{Code: 16, Message: "col"}, ClassFatal, "structural"},
		{"unknown_code", &chproto.Exception{Code: 12345, Message: "?"}, ClassFatal, "unknown"},
		{"regex_only_code", fmt.Errorf("code: 252, message: too many parts"), ClassRetryable, "transient"},
		{"wrapped_str_fallback", errors.Wrapf(io.EOF, "code: 252, message: too many parts"), ClassRetryable, "transient"},
		{"all_replicas_down", errors.Wrapf(pool.ErrAllReplicasDown, "x"), ClassRetryable, "transient"},
		{"eof", io.EOF, ClassRetryable, "transient"},
		{"ctx_canceled", context.Canceled, ClassFatal, "unknown"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cls, lbl := classifyError(tc.err, nil, nil)
			if cls != tc.class || lbl != tc.label {
				t.Fatalf("got (%v,%q), want (%v,%q)", cls, lbl, tc.class, tc.label)
			}
		})
	}
}

func TestClassifyOverrides(t *testing.T) {
	// FatalErrorCodes 把 252 强制改判为不可重试；252 不在 structural/data 码表，label 为 "unknown"
	cls, lbl := classifyError(&chproto.Exception{Code: 252}, nil, map[int32]bool{252: true})
	if cls != ClassFatal {
		t.Fatalf("fatal override failed: got %v", cls)
	}
	if lbl != "unknown" {
		t.Fatalf("fatal override label: got %q, want %q", lbl, "unknown")
	}
	// RetryableErrorCodes 把未知码 12345 改判为可重试；白名单可重试路径返回 "transient"
	cls, lbl = classifyError(&chproto.Exception{Code: 12345}, map[int32]bool{12345: true}, nil)
	if cls != ClassRetryable {
		t.Fatalf("retryable override failed: got %v", cls)
	}
	if lbl != "transient" {
		t.Fatalf("retryable override label: got %q, want %q", lbl, "transient")
	}
}

func TestBuildCodeSet(t *testing.T) {
	if buildCodeSet(nil) != nil {
		t.Fatal("nil input should return nil")
	}
	m := buildCodeSet([]int32{252, 999})
	if !m[252] || !m[999] || len(m) != 2 {
		t.Fatalf("unexpected result: %#v", m)
	}
}
