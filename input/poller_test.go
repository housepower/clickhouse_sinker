package input

import (
	"os"
	"slices"
	"testing"
	"time"

	"github.com/housepower/clickhouse_sinker/util"
)

func TestMain(m *testing.M) {
	util.InitLogger([]string{"stdout"})
	os.Exit(m.Run())
}

// Walk must report every expired consumer: it removes all of them from the
// poller map, so one it does not return is never restarted.
func TestWalkReturnsEveryExpiredConsumer(t *testing.T) {
	t.Cleanup(func() {
		for _, id := range []string{"id-a", "id-b", "id-c"} {
			Leave(id)
		}
	})
	NewConsumerPoller("id-a", "group-a", nil)
	NewConsumerPoller("id-b", "group-b", nil)
	NewConsumerPoller("id-c", "group-c", nil)
	for _, id := range []string{"id-a", "id-b"} {
		v, _ := consumerPoller.Load(id)
		p := v.(Poller)
		p.active = time.Now().Add(-time.Hour)
		consumerPoller.Store(id, p)
	}
	got := Walk(int(time.Minute / time.Millisecond))
	slices.Sort(got)
	if !slices.Equal(got, []string{"group-a", "group-b"}) {
		t.Fatalf("Walk = %v, want [group-a group-b]", got)
	}
	if _, ok := consumerPoller.Load("id-c"); !ok {
		t.Fatal("a live consumer must stay registered")
	}
}
