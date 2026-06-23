package task

import (
	"testing"

	"github.com/housepower/clickhouse_sinker/config"
)

func TestFilterBrokenTasks(t *testing.T) {
	s := &Sinker{}
	s.MarkTaskBroken("bad", "structural")
	newCfg := &config.Config{
		Tasks: []*config.TaskConfig{{Name: "good"}, {Name: "bad"}},
	}
	s.filterBrokenTasks(newCfg)
	for _, tc := range newCfg.Tasks {
		if tc.Name == "bad" {
			t.Fatal("broken task 'bad' should have been filtered out")
		}
	}
}
