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
)

// TestEtcdConcurrentUpdate: every Update that returned nil must be present in
// the final value, i.e. CAS never loses a write.
// Run: SINKER_IT_ETCD_ENDPOINTS=127.0.0.1:52379 go test -tags integration ./config_manager/ -run TestEtcd
func TestEtcdConcurrentUpdate(t *testing.T) {
	eps := os.Getenv("SINKER_IT_ETCD_ENDPOINTS")
	if eps == "" {
		t.Skip("SINKER_IT_ETCD_ENDPOINTS not set")
	}
	key := fmt.Sprintf("/clickhouse_sinker_it/%s/%d", t.Name(), time.Now().UnixNano())
	e, err := NewEtcd(EtcdOptions{Endpoints: eps, Key: key})
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	ctx := context.Background()
	if _, err = e.cli.Put(ctx, key, "start\n"); err != nil {
		t.Fatal(err)
	}
	defer e.cli.Delete(ctx, key)

	var mu sync.Mutex
	var committed []string
	var wg sync.WaitGroup
	for _, writer := range []string{"a", "b"} {
		wg.Add(1)
		go func(writer string) {
			defer wg.Done()
			for i := 0; i < 10; i++ {
				line := fmt.Sprintf("%s-%d\n", writer, i)
				err := e.Update(ctx, func(old []byte) ([]byte, error) {
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

	final, err := e.Fetch(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(committed) == 0 {
		t.Fatal("no update succeeded")
	}
	for _, line := range committed {
		if !strings.Contains(string(final), line) {
			t.Fatalf("committed line %q lost; final:\n%s", line, final)
		}
	}
}

func TestEtcdFetchMissingKey(t *testing.T) {
	eps := os.Getenv("SINKER_IT_ETCD_ENDPOINTS")
	if eps == "" {
		t.Skip("SINKER_IT_ETCD_ENDPOINTS not set")
	}
	e, err := NewEtcd(EtcdOptions{Endpoints: eps, Key: fmt.Sprintf("/clickhouse_sinker_it/missing/%d", time.Now().UnixNano())})
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	if _, err = e.Fetch(context.Background()); err == nil || !strings.Contains(err.Error(), "does not exist") {
		t.Fatalf("want does-not-exist error, got %v", err)
	}
}
