package task

import (
	"testing"

	"github.com/housepower/clickhouse_sinker/discovery"
)

// The config center can be unreachable at startup for longer than the
// discovery interval; the discovery tick must not dereference a nil config.
func TestRunDiscoveryBeforeFirstConfig(t *testing.T) {
	s := &Sinker{}
	s.runDiscovery(discovery.NewDiscovery(nil, nil))
}
