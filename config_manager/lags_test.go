package rcm

import (
	"sync"
	"testing"

	"github.com/housepower/clickhouse_sinker/config"
	"github.com/housepower/clickhouse_sinker/util"
)

// kgo clients connect lazily, so these tests need no Kafka.
func testKafkaConfig(t *testing.T, brokers string) config.KafkaConfig {
	t.Helper()
	cfg := &config.Config{
		Clickhouse: config.ClickHouseConfig{Hosts: [][]string{{"127.0.0.1"}}, Port: 9000, DB: "default"},
		Kafka:      config.KafkaConfig{Brokers: brokers},
	}
	if err := cfg.Normallize(false, util.Credentials{}); err != nil {
		t.Fatal(err)
	}
	return cfg.Kafka
}

// After Manager.Close tears the client down, the /state and /live handlers
// still call in; they must get a fresh client, not a nil one.
func TestAdminClientRecreatedAfterCleanup(t *testing.T) {
	k := testKafkaConfig(t, "127.0.0.1:1")
	defer cleanupKafkaClient()
	if adm, err := adminClient(k); err != nil || adm == nil {
		t.Fatalf("first adminClient: %v, %v", adm, err)
	}
	cleanupKafkaClient()
	adm, err := adminClient(k)
	if err != nil || adm == nil {
		t.Fatalf("adminClient after cleanup returned %v, %v; want a new client", adm, err)
	}
}

// The HTTP handlers, the main loop and Close touch the shared client
// concurrently. Run with -race to check the globals are guarded.
func TestAdminClientConcurrentAccess(t *testing.T) {
	a := testKafkaConfig(t, "127.0.0.1:1")
	b := testKafkaConfig(t, "127.0.0.1:2")
	defer cleanupKafkaClient()
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				switch (i + j) % 3 {
				case 0:
					_, _ = adminClient(a)
				case 1:
					_, _ = adminClient(b)
				default:
					cleanupKafkaClient()
				}
			}
		}(i)
	}
	wg.Wait()
}
