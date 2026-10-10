package discovery

import (
	"context"
	"os"
	"testing"

	"github.com/housepower/clickhouse_sinker/config"
	cm "github.com/housepower/clickhouse_sinker/config_manager"
	"github.com/housepower/clickhouse_sinker/util"
	"github.com/stretchr/testify/assert"
)

func TestConvert(t *testing.T) {
	var hosts = [][]string{
		{"127.0.0.1:8080", "127.0.0.1:8081"},
		{"127.0.0.1:8082", "127.0.0.1:8083"},
	}

	shards := hosts2shards(hosts)
	target := Shards{
		Replicas{"127.0.0.1:8080", "127.0.0.1:8081"},
		Replicas{"127.0.0.1:8082", "127.0.0.1:8083"},
	}
	assert.Equal(t, target, shards)
}

func TestDiffShards(t *testing.T) {
	var old1 = Shards{
		Replicas{"127.0.0.1:8080", "127.0.0.1:8081"},
		Replicas{"127.0.0.1:8082", "127.0.0.1:8083"},
	}
	var new1 = Shards{
		Replicas{"127.0.0.1:8080", "127.0.0.1:8081"},
		Replicas{"127.0.0.1:8082", "127.0.0.1:8083"},
		Replicas{"127.0.0.1:8084", "127.0.0.1:8085"},
	}

	assert.Equal(t, true, diffShards(old1, new1))

	var new2 = Shards{
		Replicas{"127.0.0.1:8082", "127.0.0.1:8083"},
		Replicas{"127.0.0.1:8081", "127.0.0.1:8080"},
	}
	assert.Equal(t, true, diffShards(old1, new2))

	var new3 = Shards{
		Replicas{"127.0.0.1:8082", "127.0.0.1:8083"},
		Replicas{"127.0.0.2:8081", "127.0.0.1:8080"},
	}
	assert.Equal(t, true, diffShards(old1, new3))

	var new4 = Shards{
		Replicas{"127.0.0.1:8081", "127.0.0.1:8080"},
		Replicas{"127.0.0.1:8083", "127.0.0.1:8082"},
	}
	assert.Equal(t, false, diffShards(old1, new4))
}

func TestMain(m *testing.M) {
	util.InitLogger([]string{"stdout"})
	os.Exit(m.Run())
}

// memBackend is an in-memory config center for discovery tests.
type memBackend struct {
	content []byte
	pending bool
	writes  int
}

func (b *memBackend) Fetch(context.Context) ([]byte, error) { return b.content, nil }
func (b *memBackend) Close() error                          { return nil }
func (b *memBackend) Update(_ context.Context, fn cm.UpdateFunc) error {
	if b.pending {
		return cm.ErrPendingChanges
	}
	neu, err := fn(b.content)
	if err != nil {
		return err
	}
	b.content = neu
	b.writes++
	return nil
}

func staleConfig() *config.Config {
	return &config.Config{
		Clickhouse: config.ClickHouseConfig{Hosts: [][]string{{"a"}}},
		Tasks:      []*config.TaskConfig{{Name: "t1"}},
	}
}

func TestApplyShardsPatchesOnlyHostsOnLatestConfig(t *testing.T) {
	// Someone added t2 after discovery loaded its (stale) copy.
	b := &memBackend{content: []byte(`{clickhouse: {hosts: [["a"]]}, kafka: {brokers: "k"}, tasks: [{name: "t1"}, {name: "t2"}]}`)}
	d := NewDiscovery(staleConfig(), cm.NewManager("mem", b, b))
	if err := d.applyShards(Shards{{"a"}, {"b"}}); err != nil {
		t.Fatal(err)
	}
	got, err := cm.DecodeConfig(b.content)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Clickhouse.Hosts) != 2 {
		t.Fatalf("hosts=%v, want 2 shards", got.Clickhouse.Hosts)
	}
	if len(got.Tasks) != 2 {
		t.Fatalf("tasks=%d, the concurrently added t2 must survive", len(got.Tasks))
	}
	if got.Discovery.UpdatedBy != "discovery-changed" {
		t.Fatalf("UpdatedBy=%q", got.Discovery.UpdatedBy)
	}
}

func TestApplyShardsSkipsWriteWhenLatestAlreadyMatches(t *testing.T) {
	// Another sinker instance already wrote the new shards.
	b := &memBackend{content: []byte(`{clickhouse: {hosts: [["a"], ["b"]]}, kafka: {brokers: "k"}}`)}
	d := NewDiscovery(staleConfig(), cm.NewManager("mem", b, b))
	if err := d.applyShards(Shards{{"a"}, {"b"}}); err != nil {
		t.Fatal(err)
	}
	if b.writes != 0 {
		t.Fatalf("writes=%d, want 0", b.writes)
	}
}

func TestApplyShardsPendingChangesIsNotAnError(t *testing.T) {
	b := &memBackend{content: []byte(`{clickhouse: {hosts: [["a"]]}, kafka: {brokers: "k"}}`), pending: true}
	d := NewDiscovery(staleConfig(), cm.NewManager("mem", b, b))
	if err := d.applyShards(Shards{{"a"}, {"b"}}); err != nil {
		t.Fatalf("pending changes must only warn, got %v", err)
	}
}

func TestIsEnabledNeedsWritableBackend(t *testing.T) {
	enabled := config.Config{Discovery: config.Discovery{Enabled: true}}
	ro := NewDiscovery(nil, cm.NewManager("ro", &memBackend{}, nil))
	ro.SetConfig(enabled)
	if ro.IsEnabled() {
		t.Fatal("read-only backend must disable discovery")
	}
	local := NewDiscovery(nil, nil)
	local.SetConfig(enabled)
	if local.IsEnabled() {
		t.Fatal("local file mode must disable discovery")
	}
	b := &memBackend{}
	rw := NewDiscovery(nil, cm.NewManager("rw", b, b))
	rw.SetConfig(enabled)
	if !rw.IsEnabled() {
		t.Fatal("writable backend with discovery enabled must be enabled")
	}
}
