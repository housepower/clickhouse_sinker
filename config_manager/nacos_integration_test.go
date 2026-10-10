//go:build integration

package rcm

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nacos-group/nacos-sdk-go/v2/vo"
)

// Run: SINKER_IT_NACOS_ADDR=127.0.0.1:58848 go test -tags integration ./config_manager/ -run TestNacos -v
//
// The CAS tests additionally need SINKER_IT_NACOS_CAS=1 and a nacos backed by
// an external MySQL datasource: embedded storage (standalone Derby) ignores
// casMd5, so a stale md5 is silently accepted there.
func newITNacos(t *testing.T) *Nacos {
	addr := os.Getenv("SINKER_IT_NACOS_ADDR")
	if addr == "" {
		t.Skip("SINKER_IT_NACOS_ADDR not set")
	}
	n, err := NewNacos(NacosOptions{
		Addr:     addr,
		Username: "nacos",
		Password: "nacos",
		DataID:   fmt.Sprintf("clickhouse_sinker_it_%d", time.Now().UnixNano()),
		CacheDir: t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = n.client.DeleteConfig(voConfigParam(n))
		_ = n.Close()
	})
	return n
}

func requireNacosCAS(t *testing.T) {
	t.Helper()
	if os.Getenv("SINKER_IT_NACOS_CAS") != "1" {
		t.Skip("requires nacos backed by an external MySQL datasource (embedded Derby ignores casMd5)")
	}
}

func TestNacosCreateAndUpdate(t *testing.T) {
	n := newITNacos(t)
	ctx := context.Background()
	if err := n.Update(ctx, func(old []byte) ([]byte, error) {
		if len(old) != 0 {
			t.Errorf("new dataId must read as empty, got %q", old)
		}
		return []byte("created"), nil
	}); err != nil {
		t.Fatal(err)
	}
	waitNacosContent(t, n, "created")
	if err := n.Update(ctx, func(old []byte) ([]byte, error) {
		return append(bytes.Clone(old), "+updated"...), nil
	}); err != nil {
		t.Fatal(err)
	}
	waitNacosContent(t, n, "created+updated")
}

func TestNacosCASConflictDetected(t *testing.T) {
	requireNacosCAS(t)
	n := newITNacos(t)
	ctx := context.Background()
	if err := n.Update(ctx, func([]byte) ([]byte, error) { return []byte("v1"), nil }); err != nil {
		t.Fatal(err)
	}
	waitNacosContent(t, n, "v1")
	conflict, err := n.write(ctx, []byte("v2"), "00000000000000000000000000000000")
	if err != nil {
		t.Fatalf("a stale casMd5 must be reported as conflict, got error: %v", err)
	}
	if !conflict {
		t.Fatal("a stale casMd5 must be reported as conflict")
	}
}

func TestNacosConcurrentUpdate(t *testing.T) {
	requireNacosCAS(t)
	n := newITNacos(t)
	ctx := context.Background()
	if err := n.Update(ctx, func([]byte) ([]byte, error) { return []byte("start\n"), nil }); err != nil {
		t.Fatal(err)
	}
	waitNacosContent(t, n, "start\n")

	var mu sync.Mutex
	var committed []string
	var wg sync.WaitGroup
	for _, writer := range []string{"a", "b"} {
		wg.Add(1)
		go func(writer string) {
			defer wg.Done()
			for i := 0; i < 5; i++ {
				line := fmt.Sprintf("%s-%d\n", writer, i)
				err := n.Update(ctx, func(old []byte) ([]byte, error) {
					return append(bytes.Clone(old), line...), nil
				})
				if err == nil {
					mu.Lock()
					committed = append(committed, line)
					mu.Unlock()
				}
			}
		}(writer)
	}
	wg.Wait()
	if len(committed) == 0 {
		t.Fatal("no update succeeded")
	}
	// GetConfig is served from nacos' dumped copy, which lags the database
	// briefly, so poll until every committed line shows up.
	deadline := time.Now().Add(10 * time.Second)
	for {
		final, err := n.Fetch(ctx)
		if err != nil {
			t.Fatal(err)
		}
		missing := ""
		for _, line := range committed {
			if !strings.Contains(string(final), line) {
				missing = line
				break
			}
		}
		if missing == "" {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("committed line %q lost; final:\n%s", missing, final)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func waitNacosContent(t *testing.T, n *Nacos, want string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		got, err := n.Fetch(context.Background())
		if err == nil && string(got) == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("content never became %q, last %q, %v", want, got, err)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func voConfigParam(n *Nacos) vo.ConfigParam {
	return vo.ConfigParam{DataId: n.dataID, Group: n.group}
}
