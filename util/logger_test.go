package util

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestLogRotationNormalized(t *testing.T) {
	assert.Equal(t, DefaultLogRotation, LogRotation{}.normalized())
	assert.Equal(t, DefaultLogRotation, LogRotation{MaxSize: -1, MaxBackups: -1}.normalized())
	assert.Equal(t,
		LogRotation{MaxSize: 50, MaxBackups: DefaultLogRotation.MaxBackups},
		LogRotation{MaxSize: 50}.normalized())
	assert.Equal(t,
		LogRotation{MaxSize: 50, MaxBackups: 3},
		LogRotation{MaxSize: 50, MaxBackups: 3}.normalized())
}

func TestInitLoggerWithRotationReinit(t *testing.T) {
	InitLoggerWithRotation([]string{"stdout"}, LogRotation{MaxSize: 1, MaxBackups: 1})
	first := Logger

	// Same paths and same rotation: keep the existing logger.
	InitLoggerWithRotation([]string{"stdout"}, LogRotation{MaxSize: 1, MaxBackups: 1})
	assert.Same(t, first, Logger)

	// Same paths but different rotation: rebuild the logger.
	InitLoggerWithRotation([]string{"stdout"}, LogRotation{MaxSize: 2, MaxBackups: 1})
	assert.NotSame(t, first, Logger)
}

func TestSetLogRotation(t *testing.T) {
	InitLogger([]string{"stdout"})

	// No-op when the policy doesn't change.
	SetLogRotation(DefaultLogRotation)
	first := Logger
	SetLogRotation(DefaultLogRotation)
	assert.Same(t, first, Logger)

	// Rebuilds the logger when the policy changes.
	SetLogRotation(LogRotation{MaxSize: 50, MaxBackups: 3})
	assert.NotSame(t, first, Logger)

	// A rebuild must keep the current log level.
	SetLogLevel("debug")
	SetLogRotation(LogRotation{MaxSize: 60, MaxBackups: 3})
	assert.Equal(t, zap.DebugLevel, logAtomLevel.Level())
}

func TestLogRotationAppliesMaxSize(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "test.log")
	InitLoggerWithRotation([]string{p}, LogRotation{MaxSize: 1, MaxBackups: 1})

	// ~1.2MB of logs against a 1MB limit must rotate.
	msg := strings.Repeat("x", 1024)
	for i := 0; i < 1200; i++ {
		Logger.Info(msg)
	}
	require.NoError(t, Logger.Sync())

	matches, err := filepath.Glob(filepath.Join(dir, "test-*.log"))
	require.NoError(t, err)
	assert.NotEmpty(t, matches, "rotation should have produced a backup file")
}
