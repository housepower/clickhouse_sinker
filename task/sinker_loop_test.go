package task

import (
	"testing"
	"time"

	"github.com/housepower/clickhouse_sinker/config"
)

func loopCfg(maxPollMs, discoverySec, reloadSec int) *config.Config {
	cfg := &config.Config{ReloadSeriesMapInterval: reloadSec}
	cfg.Kafka.Properties.MaxPollInterval = maxPollMs
	cfg.Discovery.CheckInterval = discoverySec
	return cfg
}

func newTestTickers() loopTickers {
	return loopTickers{
		poll:         time.NewTicker(time.Hour),
		discovery:    time.NewTicker(time.Hour),
		reloadSeries: time.NewTicker(time.Hour),
	}
}

// An unchanged config must not reset the max.poll.interval ticker: resetting it
// on every 10s config poll means it never fires.
func TestLoopTickersUnchangedConfigResetsNothing(t *testing.T) {
	tk := newTestTickers()
	defer tk.stop()
	cfg := loopCfg(config.DefaultMaxPollIntervalMs, config.DefaultDiscoveryIntervalSec, 3600)
	poll, discovery, reload := tk.reset(intervalsOf(cfg), intervalsOf(cfg))
	if poll || discovery || reload {
		t.Fatalf("reset poll=%v discovery=%v reload=%v, want none", poll, discovery, reload)
	}
}

func TestLoopTickersResetOnlyWhatChanged(t *testing.T) {
	tk := newTestTickers()
	defer tk.stop()
	old := intervalsOf(loopCfg(300000, 60, 3600))
	cases := []struct {
		name                    string
		cfg                     *config.Config
		poll, discovery, reload bool
	}{
		{"max poll", loopCfg(600000, 60, 3600), true, false, false},
		{"discovery", loopCfg(300000, 30, 3600), false, true, false},
		{"reload series", loopCfg(300000, 60, 1800), false, false, true},
	}
	for _, c := range cases {
		poll, discovery, reload := tk.reset(old, intervalsOf(c.cfg))
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
			s.curCfg = cfg // what applyConfig does on the Run goroutine
			s.publishConfig()
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
