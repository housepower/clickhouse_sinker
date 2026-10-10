package rcm

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/housepower/clickhouse_sinker/config"
	"github.com/housepower/clickhouse_sinker/util"
)

func TestMain(m *testing.M) {
	util.InitLogger([]string{"stdout"})
	os.Exit(m.Run())
}

const twoTasks = `{
	clickhouse: {hosts: [["ch1"]], db: "default"}
	kafka: {brokers: "k:9092"}
	tasks: [{name: "t1"}, {name: "t2"}]
}`

// memBackend is an in-memory Source + Writer.
type memBackend struct {
	content  []byte
	fetchErr error
	pending  bool
	writes   int
}

func (b *memBackend) Fetch(context.Context) ([]byte, error) { return b.content, b.fetchErr }
func (b *memBackend) Close() error                          { return nil }
func (b *memBackend) Update(_ context.Context, fn UpdateFunc) error {
	if b.pending {
		return ErrPendingChanges
	}
	neu, err := fn(b.content)
	if err != nil {
		return err
	}
	b.content = neu
	b.writes++
	return nil
}

func TestManagerGetConfig(t *testing.T) {
	m := NewManager("mem", &memBackend{content: []byte(twoTasks)}, nil)
	cfg, err := m.GetConfig()
	if err != nil {
		t.Fatalf("GetConfig: %v", err)
	}
	if cfg.Kafka.Brokers != "k:9092" || len(cfg.Tasks) != 2 || cfg.Groups == nil {
		t.Fatalf("unexpected config: %+v", cfg)
	}
}

func TestManagerGetConfigEmpty(t *testing.T) {
	m := NewManager("mem", &memBackend{content: []byte(" \n ")}, nil)
	_, err := m.GetConfig()
	if err == nil || !strings.Contains(err.Error(), "mem") || !strings.Contains(err.Error(), "empty") {
		t.Fatalf("want empty-config error naming the backend, got %v", err)
	}
}

func TestManagerGetConfigInvalid(t *testing.T) {
	m := NewManager("mem", &memBackend{content: []byte("{")}, nil)
	if _, err := m.GetConfig(); err == nil || !strings.Contains(err.Error(), "mem") {
		t.Fatalf("want parse error naming the backend, got %v", err)
	}
}

func TestManagerGetConfigFetchError(t *testing.T) {
	m := NewManager("mem", &memBackend{fetchErr: errors.New("boom")}, nil)
	if _, err := m.GetConfig(); err == nil || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("want fetch error, got %v", err)
	}
}

func TestManagerUpdateConfigAppliesOnLatest(t *testing.T) {
	b := &memBackend{content: []byte(twoTasks)}
	m := NewManager("mem", b, b)
	err := m.UpdateConfig(func(c *config.Config) bool {
		c.Clickhouse.Hosts = [][]string{{"ch1"}, {"ch2"}}
		return true
	})
	if err != nil {
		t.Fatalf("UpdateConfig: %v", err)
	}
	got, err := DecodeConfig(b.content)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Clickhouse.Hosts) != 2 || len(got.Tasks) != 2 {
		t.Fatalf("hosts=%v tasks=%d, want 2 shards and both tasks kept", got.Clickhouse.Hosts, len(got.Tasks))
	}
}

func TestManagerUpdateConfigNoChange(t *testing.T) {
	b := &memBackend{content: []byte(twoTasks)}
	m := NewManager("mem", b, b)
	if err := m.UpdateConfig(func(*config.Config) bool { return false }); err != nil {
		t.Fatalf("UpdateConfig: %v", err)
	}
	if b.writes != 0 {
		t.Fatalf("writes=%d, want 0", b.writes)
	}
}

func TestManagerUpdateConfigPending(t *testing.T) {
	b := &memBackend{content: []byte(twoTasks), pending: true}
	m := NewManager("mem", b, b)
	err := m.UpdateConfig(func(*config.Config) bool { return true })
	if !errors.Is(err, ErrPendingChanges) {
		t.Fatalf("want ErrPendingChanges, got %v", err)
	}
}

func TestManagerReadOnly(t *testing.T) {
	m := NewManager("mem", &memBackend{content: []byte(twoTasks)}, nil)
	if m.Writable() {
		t.Fatal("manager without writer must not be writable")
	}
	if err := m.UpdateConfig(func(*config.Config) bool { return true }); err == nil {
		t.Fatal("UpdateConfig on read-only backend must fail")
	}
	var nilManager *Manager
	if nilManager.Writable() {
		t.Fatal("nil manager must not be writable")
	}
}

func TestUpdateWithCASGivesUpAfterRetries(t *testing.T) {
	reads, calls := 0, 0
	var seen []string
	read := func(context.Context) ([]byte, string, error) {
		reads++
		return []byte(fmt.Sprintf("c%d", reads)), fmt.Sprint(reads), nil
	}
	write := func(context.Context, []byte, string) (bool, error) { return true, nil }
	fn := func(old []byte) ([]byte, error) {
		calls++
		seen = append(seen, string(old))
		return append(old, '!'), nil
	}
	err := updateWithCAS(context.Background(), fn, read, write)
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("want ErrConflict, got %v", err)
	}
	if calls != casMaxRetries+1 {
		t.Fatalf("fn called %d times, want %d", calls, casMaxRetries+1)
	}
	if strings.Join(seen, ",") != "c1,c2,c3,c4" {
		t.Fatalf("each retry must re-read, saw %v", seen)
	}
}

func TestUpdateWithCASSucceedsAfterConflict(t *testing.T) {
	writes := 0
	read := func(context.Context) ([]byte, string, error) { return []byte("old"), "v", nil }
	write := func(context.Context, []byte, string) (bool, error) {
		writes++
		return writes == 1, nil
	}
	err := updateWithCAS(context.Background(), func([]byte) ([]byte, error) { return []byte("new"), nil }, read, write)
	if err != nil || writes != 2 {
		t.Fatalf("err=%v writes=%d, want nil and 2", err, writes)
	}
}

func TestUpdateWithCASSkipsIdenticalContent(t *testing.T) {
	read := func(context.Context) ([]byte, string, error) { return []byte("same"), "v", nil }
	write := func(context.Context, []byte, string) (bool, error) {
		t.Fatal("write must not be called when content is unchanged")
		return false, nil
	}
	if err := updateWithCAS(context.Background(), func(old []byte) ([]byte, error) { return old, nil }, read, write); err != nil {
		t.Fatal(err)
	}
}

func TestUpdateWithCASStopsOnCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	writes := 0
	read := func(context.Context) ([]byte, string, error) { return []byte("old"), "v", nil }
	write := func(context.Context, []byte, string) (bool, error) {
		writes++
		cancel()
		return true, nil
	}
	err := updateWithCAS(ctx, func([]byte) ([]byte, error) { return []byte("new"), nil }, read, write)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("want context.Canceled, got %v", err)
	}
	if writes != 1 {
		t.Fatalf("write called %d times, want 1", writes)
	}
}
