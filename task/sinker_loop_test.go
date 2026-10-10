package task

import (
	"testing"

	"github.com/housepower/clickhouse_sinker/config"
	"github.com/housepower/clickhouse_sinker/util"
)

func loopCfg(maxPollMs, discoverySec, reloadSec int) *config.Config {
	cfg := &config.Config{ReloadSeriesMapInterval: reloadSec}
	cfg.Kafka.Properties.MaxPollInterval = maxPollMs
	cfg.Discovery.CheckInterval = discoverySec
	return cfg
}

// An unchanged interval must not re-arm its ticker: re-arming it on every 10s
// config poll means it never fires.
func TestLoopTickersRearmOnlyWhatChanged(t *testing.T) {
	tk := newLoopTickers(intervalsOf(loopCfg(300000, 60, 3600)))
	defer tk.stop()
	cases := []struct {
		name                    string
		cfg                     *config.Config
		poll, discovery, reload bool
	}{
		{"unchanged", loopCfg(300000, 60, 3600), false, false, false},
		{"max poll", loopCfg(600000, 60, 3600), true, false, false},
		{"same again", loopCfg(600000, 60, 3600), false, false, false},
		{"discovery", loopCfg(600000, 120, 3600), false, true, false},
		{"reload series", loopCfg(600000, 120, 1800), false, false, true},
	}
	for _, c := range cases {
		poll, discovery, reload := tk.rearm(intervalsOf(c.cfg))
		if poll != c.poll || discovery != c.discovery || reload != c.reload {
			t.Fatalf("%s: got poll=%v discovery=%v reload=%v", c.name, poll, discovery, reload)
		}
	}
}

func TestIntervalsOfNilConfigUsesLoopDefaults(t *testing.T) {
	got := intervalsOf(nil)
	want := loopIntervals{maxPollMs: config.DefaultMaxPollIntervalMs, discoverySec: config.DefaultDiscoveryIntervalSec, reloadSeriesSec: 3600}
	if got != want {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

// The /state and /live handlers read the config from HTTP goroutines while Run
// replaces it. Run with -race: GetCurrentConfig must not read s.curCfg.
func TestGetCurrentConfigIsSafeForOtherGoroutines(t *testing.T) {
	s := &Sinker{}
	if s.GetCurrentConfig() != nil {
		t.Fatal("no config is published before the first apply")
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 1000; i++ {
			cfg := loopCfg(config.DefaultMaxPollIntervalMs, 60, 3600)
			s.setCurrentConfig(cfg) // what applyConfig does on the Run goroutine
		}
	}()
	for i := 0; i < 1000; i++ {
		_ = s.GetCurrentConfig()
	}
	<-done
	if s.GetCurrentConfig() != s.curCfg {
		t.Fatal("GetCurrentConfig must return the last applied config")
	}
}

// A config change that touches only the discovery section must reach the
// current config: the discovery round reads its switch and interval there.
func TestApplyConfigTakesDiscoveryChanges(t *testing.T) {
	t.Setenv("SKIP_TOPIC_PRECHECK", "1")
	t.Setenv("SKIP_TABLE_PRECHECK", "1")
	mk := func(enabled bool, interval int) *config.Config {
		cfg := &config.Config{
			Clickhouse: config.ClickHouseConfig{Hosts: [][]string{{"127.0.0.1"}}, Port: 9000, DB: "default"},
			Kafka:      config.KafkaConfig{Brokers: "127.0.0.1:9092"},
			Discovery:  config.Discovery{Enabled: enabled, CheckInterval: interval},
			Groups:     map[string]*config.GroupConfig{},
		}
		if err := cfg.Normallize(true, util.Credentials{}); err != nil {
			t.Fatal(err)
		}
		return cfg
	}
	s := NewSinker(nil, "", &util.CmdOptions{})
	s.setCurrentConfig(mk(false, 60))
	tk := newLoopTickers(intervalsOf(s.curCfg))
	defer tk.stop()
	for round := 0; round < 3; round++ {
		if err := s.applyConfig(mk(true, 120)); err != nil {
			t.Fatal(err)
		}
		if got := s.curCfg.Discovery; !got.Enabled || got.CheckInterval != 120 {
			t.Fatalf("round %d: current discovery config %+v, want enabled every 120s", round, got)
		}
		_, discovery, _ := tk.rearm(intervalsOf(s.curCfg))
		if discovery != (round == 0) {
			t.Fatalf("round %d: discovery ticker re-armed=%v, want only on the first round", round, discovery)
		}
	}
}
