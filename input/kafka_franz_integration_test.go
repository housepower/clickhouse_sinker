//go:build integration

package input

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/housepower/clickhouse_sinker/config"
	"github.com/housepower/clickhouse_sinker/util"
)

// An idle consumer (no new messages) must keep telling the max.poll.interval
// watchdog that it is alive; PollRecords alone blocks until records arrive.
// Run: SINKER_IT_KAFKA_BROKERS=127.0.0.1:9093 go test -tags integration ./input/ -run TestIdleConsumer -v
func TestIdleConsumerStaysActive(t *testing.T) {
	brokers := os.Getenv("SINKER_IT_KAFKA_BROKERS")
	if brokers == "" {
		t.Skip("SINKER_IT_KAFKA_BROKERS not set")
	}
	name := fmt.Sprintf("sinker_it_idle_%d", time.Now().UnixNano())
	ctx := context.Background()
	admCl, err := kgo.NewClient(kgo.SeedBrokers(brokers))
	if err != nil {
		t.Fatal(err)
	}
	defer admCl.Close()
	adm := kadm.NewClient(admCl)
	if _, err = adm.CreateTopic(ctx, 1, 1, nil, name); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, _ = adm.DeleteTopics(ctx, name)
		_, _ = adm.DeleteGroups(ctx, name)
	}()

	cfg := &config.Config{
		Clickhouse: config.ClickHouseConfig{Hosts: [][]string{{"127.0.0.1"}}, Port: 9000, DB: "default"},
		Kafka:      config.KafkaConfig{Brokers: brokers},
	}
	if err = cfg.Normallize(false, util.Credentials{}); err != nil {
		t.Fatal(err)
	}
	gCfg := &config.GroupConfig{Name: name, Topics: []string{name}, FlushInterval: 1, BufferSize: 100, MaxFetchSize: 100}
	k := NewKafkaFranz()
	if err = k.Init(cfg, gCfg, make(chan Fetches, 10), func() {}); err != nil {
		t.Fatal(err)
	}
	go k.Run()
	defer k.Stop()

	active := func() (time.Time, bool) {
		var at time.Time
		var found bool
		consumerPoller.Range(func(_, v any) bool {
			if p := v.(Poller); p.consumerName == name {
				at, found = p.active, true
				return false
			}
			return true
		})
		return at, found
	}
	deadline := time.Now().Add(60 * time.Second)
	for {
		if _, ok := active(); ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("consumer never got its partitions assigned")
		}
		time.Sleep(200 * time.Millisecond)
	}
	time.Sleep(idleWait)
	at, _ := active()
	if since := time.Since(at); since >= idleWait {
		t.Fatalf("idle consumer last reported %v ago; the watchdog would expire it", since)
	}
}

// idleWait is longer than the idle poll timeout, so an idle consumer must have
// reported in at least once during it.
const idleWait = 13 * time.Second
