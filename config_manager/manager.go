package rcm

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/hjson/hjson-go/v4"

	"github.com/housepower/clickhouse_sinker/config"
)

const (
	// opTimeout bounds every call to a config center so that a stuck backend
	// cannot block the sinker main loop.
	opTimeout = 5 * time.Second
	// casMaxRetries is how many times a conflicting compare-and-swap is retried.
	casMaxRetries = 3
)

// casBackoff is the pause before the first retry; retry n waits n*casBackoff
// (200+400+600ms, well inside opTimeout). nacos serves reads from a dump that
// lags its database, so re-reading at once after a conflict is often stale.
var casBackoff = 200 * time.Millisecond

var (
	// ErrNoChange is returned by an UpdateFunc that has nothing to write.
	ErrNoChange = errors.New("config unchanged")
	// ErrPendingChanges means the config center holds unreleased edits by
	// someone else; writing now would release them too.
	ErrPendingChanges = errors.New("config center has unreleased changes")
	// ErrConflict means the config kept changing concurrently.
	ErrConflict = errors.New("config changed concurrently, retries exhausted")
)

// Source reads the raw config content. Every backend implements it.
type Source interface {
	Fetch(ctx context.Context) ([]byte, error)
	Close() error
}

// UpdateFunc receives the latest content and returns the content to write.
type UpdateFunc func(old []byte) ([]byte, error)

// Writer is optional. Update runs read -> fn -> write; backends with
// compare-and-swap make it atomic, others only make the window small.
type Writer interface {
	Update(ctx context.Context, fn UpdateFunc) error
}

// Manager is the only entry the sinker and discovery use to talk to a config center.
type Manager struct {
	name string
	src  Source
	w    Writer // nil means read-only
}

func NewManager(name string, src Source, w Writer) *Manager {
	return &Manager{name: name, src: src, w: w}
}

func (m *Manager) Name() string { return m.name }

// Writable is nil-safe so callers can hold a nil *Manager in local file mode.
func (m *Manager) Writable() bool { return m != nil && m.w != nil }

func (m *Manager) GetConfig() (*config.Config, error) {
	ctx, cancel := context.WithTimeout(context.Background(), opTimeout)
	defer cancel()
	bs, err := m.src.Fetch(ctx)
	if err != nil {
		return nil, fmt.Errorf("%s: fetch config: %w", m.name, err)
	}
	cfg, err := DecodeConfig(bs)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", m.name, err)
	}
	return cfg, nil
}

// UpdateConfig applies mutate to the latest config and writes it back.
// mutate returns false when there is nothing to change.
func (m *Manager) UpdateConfig(mutate func(*config.Config) bool) error {
	if !m.Writable() {
		return fmt.Errorf("%s: config backend is read-only", m.name)
	}
	ctx, cancel := context.WithTimeout(context.Background(), opTimeout)
	defer cancel()
	err := m.w.Update(ctx, func(old []byte) ([]byte, error) {
		cfg, err := DecodeConfig(old)
		if err != nil {
			return nil, err
		}
		if !mutate(cfg) {
			return nil, ErrNoChange
		}
		return EncodeConfig(cfg)
	})
	if errors.Is(err, ErrNoChange) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("%s: update config: %w", m.name, err)
	}
	return nil
}

// Close is nil-safe like Writable.
func (m *Manager) Close() error {
	// The lag admin client in lags.go used to be closed by NacosConfManager.Stop.
	cleanupKafkaClient()
	if m == nil {
		return nil
	}
	return m.src.Close()
}

func DecodeConfig(bs []byte) (*config.Config, error) {
	if len(bytes.TrimSpace(bs)) == 0 {
		return nil, errors.New("config is empty")
	}
	cfg := &config.Config{Groups: make(map[string]*config.GroupConfig)}
	if err := hjson.Unmarshal(bs, cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	return cfg, nil
}

// EncodeConfig renders cfg as indented JSON: valid hjson for DecodeConfig, and
// still valid content for an Apollo json namespace or any JSON reader.
func EncodeConfig(cfg *config.Config) ([]byte, error) {
	bs, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("encode config: %w", err)
	}
	return bs, nil
}

// updateWithCAS implements Writer.Update for backends with compare-and-swap.
// read returns the content and an opaque version; write returns conflict=true
// when the version no longer matches, and the whole cycle is retried.
func updateWithCAS(ctx context.Context, fn UpdateFunc,
	read func(context.Context) ([]byte, string, error),
	write func(context.Context, []byte, string) (bool, error)) error {
	for attempt := 0; attempt <= casMaxRetries; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Duration(attempt) * casBackoff):
			}
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		old, version, err := read(ctx)
		if err != nil {
			return err
		}
		neu, err := fn(old)
		if err != nil {
			return err
		}
		if bytes.Equal(neu, old) {
			return nil
		}
		conflict, err := write(ctx, neu, version)
		if err != nil {
			return err
		}
		if !conflict {
			return nil
		}
	}
	return ErrConflict
}
