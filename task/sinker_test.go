package task

import (
	"testing"

	"github.com/housepower/clickhouse_sinker/config"
	"github.com/housepower/clickhouse_sinker/util"
	"go.uber.org/zap"
)

func TestFilterBrokenTasks_StaysFilteredWhenUnchanged(t *testing.T) {
	util.Logger = zap.NewNop()
	s := &Sinker{}
	// Snapshot equals the newCfg entry — config unchanged, should remain quarantined.
	badCfg := &config.TaskConfig{Name: "bad", Topic: "t", TableName: "tb"}
	s.MarkTaskBroken("bad", "structural", badCfg)
	newCfg := &config.Config{Tasks: []*config.TaskConfig{
		{Name: "good"},
		{Name: "bad", Topic: "t", TableName: "tb"}, // DeepEqual to badCfg
	}}
	s.filterBrokenTasks(newCfg)
	for _, tc := range newCfg.Tasks {
		if tc.Name == "bad" {
			t.Fatal("broken task 'bad' should have been filtered out when config is unchanged")
		}
	}
	foundGood := false
	for _, tc := range newCfg.Tasks {
		if tc.Name == "good" {
			foundGood = true
		}
	}
	if !foundGood {
		t.Fatal("healthy task 'good' should survive filterBrokenTasks")
	}
}

func TestFilterBrokenTasks_RecoversWhenConfigChanged(t *testing.T) {
	util.Logger = zap.NewNop()
	s := &Sinker{}
	// Snapshot differs from newCfg entry (TableName changed) — operator fixed it, should recover.
	badCfg := &config.TaskConfig{Name: "bad", Topic: "t", TableName: "tb"}
	s.MarkTaskBroken("bad", "structural", badCfg)
	newCfg := &config.Config{Tasks: []*config.TaskConfig{
		{Name: "bad", Topic: "t", TableName: "tb_fixed"}, // changed → recover
	}}
	s.filterBrokenTasks(newCfg)
	// "bad" must still be present in newCfg.Tasks (un-quarantined).
	foundBad := false
	for _, tc := range newCfg.Tasks {
		if tc.Name == "bad" {
			foundBad = true
		}
	}
	if !foundBad {
		t.Fatal("task 'bad' should have been un-quarantined after config change")
	}
	// brokenTasks entry must have been cleared.
	if _, still := s.brokenTasks.Load("bad"); still {
		t.Fatal("brokenTasks should no longer contain 'bad' after config change")
	}
}
