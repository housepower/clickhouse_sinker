/*Copyright [2019] housepower

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package config

import (
	"os"
	"strings"
	"testing"

	"github.com/hjson/hjson-go/v4"

	"github.com/housepower/clickhouse_sinker/util"
)

// TestMain initializes util.Logger: Normallize warns through it when it meets
// deprecated config.
func TestMain(m *testing.M) {
	util.InitLogger([]string{"stdout"})
	os.Exit(m.Run())
}

func TestWriteFailureDefaults(t *testing.T) {
	cfg := &Config{
		Clickhouse: ClickHouseConfig{Hosts: [][]string{{"127.0.0.1"}}, Port: 9000, DB: "default"},
		Kafka:      KafkaConfig{Brokers: "127.0.0.1:9092"},
		Tasks:      []*TaskConfig{{Name: "t1", Topic: "tp", TableName: "tb"}},
	}
	// Normallize 触发 parseConfig 默认值填充（constructGroup=false, cred=空）
	if err := cfg.Normallize(false, util.Credentials{}); err != nil {
		t.Fatalf("Normallize failed: %v", err)
	}
	if cfg.Clickhouse.RetryTimes != 3 {
		t.Fatalf("RetryTimes default = %d, want 3", cfg.Clickhouse.RetryTimes)
	}
}

func TestLogRotationDefaults(t *testing.T) {
	newCfg := func() *Config {
		return &Config{
			Clickhouse: ClickHouseConfig{Hosts: [][]string{{"127.0.0.1"}}, Port: 9000, DB: "default"},
			Kafka:      KafkaConfig{Brokers: "127.0.0.1:9092"},
			Tasks:      []*TaskConfig{{Name: "t1", Topic: "tp", TableName: "tb"}},
		}
	}

	// Unset or non-positive values fall back to the defaults.
	cfg := newCfg()
	if err := cfg.Normallize(false, util.Credentials{}); err != nil {
		t.Fatalf("Normallize failed: %v", err)
	}
	if cfg.LogMaxSize != util.DefaultLogRotation.MaxSize || cfg.LogMaxBackups != util.DefaultLogRotation.MaxBackups {
		t.Fatalf("log rotation defaults = %dMB x %d, want %dMB x %d",
			cfg.LogMaxSize, cfg.LogMaxBackups, util.DefaultLogRotation.MaxSize, util.DefaultLogRotation.MaxBackups)
	}

	// Explicit values are kept.
	cfg = newCfg()
	cfg.LogMaxSize = 50
	cfg.LogMaxBackups = 3
	if err := cfg.Normallize(false, util.Credentials{}); err != nil {
		t.Fatalf("Normallize failed: %v", err)
	}
	if cfg.LogMaxSize != 50 || cfg.LogMaxBackups != 3 {
		t.Fatalf("explicit log rotation = %dMB x %d, want 50MB x 3", cfg.LogMaxSize, cfg.LogMaxBackups)
	}
}

func newPromTaskCfg(keyCol, valCol string, promSchema bool) *Config {
	return &Config{
		Clickhouse: ClickHouseConfig{Hosts: [][]string{{"127.0.0.1"}}, Port: 9000, DB: "default", Cluster: "abc"},
		Kafka:      KafkaConfig{Brokers: "127.0.0.1:9092"},
		Tasks: []*TaskConfig{{
			Name:             "t1",
			Topic:            "tp",
			TableName:        "tb",
			Parser:           "fastjson",
			PrometheusSchema: promSchema,
			PromLabelsArray: struct {
				KeyColumn   string
				ValueColumn string
			}{KeyColumn: keyCol, ValueColumn: valCol},
		}},
	}
}

func TestPromLabelsArrayBothSet(t *testing.T) {
	cfg := newPromTaskCfg("__labels_key__", "__labels_value__", true)
	if err := cfg.Normallize(false, util.Credentials{}); err != nil {
		t.Fatalf("Normallize failed: %v", err)
	}
	if cfg.Tasks[0].PromLabelsArray.KeyColumn != "__labels_key__" {
		t.Fatalf("KeyColumn = %q, want __labels_key__", cfg.Tasks[0].PromLabelsArray.KeyColumn)
	}
}

func TestPromLabelsArrayOnlyKeyColumnRejected(t *testing.T) {
	cfg := newPromTaskCfg("__labels_key__", "", true)
	if err := cfg.Normallize(false, util.Credentials{}); err == nil {
		t.Fatal("Normallize should reject promLabelsArray with only keyColumn set")
	}
}

func TestPromLabelsArrayOnlyValueColumnRejected(t *testing.T) {
	cfg := newPromTaskCfg("", "__labels_value__", true)
	if err := cfg.Normallize(false, util.Credentials{}); err == nil {
		t.Fatal("Normallize should reject promLabelsArray with only valueColumn set")
	}
}

func TestPromLabelsArrayClearedWithoutPrometheusSchema(t *testing.T) {
	cfg := newPromTaskCfg("__labels_key__", "__labels_value__", false)
	if err := cfg.Normallize(false, util.Credentials{}); err != nil {
		t.Fatalf("Normallize failed: %v", err)
	}
	got := cfg.Tasks[0].PromLabelsArray
	if got.KeyColumn != "" || got.ValueColumn != "" {
		t.Fatalf("PromLabelsArray = %+v, want cleared when prometheusSchema is false", got)
	}
}

// A config written while task assignment existed must still load, run every
// task on this instance, and not carry the deprecated fields when republished.
func TestDeprecatedAssignmentConfigIgnored(t *testing.T) {
	const legacy = `{
		clickhouse: {hosts: [["127.0.0.1"]], port: 9000, db: "default"}
		kafka: {
			brokers: "127.0.0.1:9092"
			assignInterval: 5
			calcLagInterval: 10
			rebalanceByLags: true
		}
		tasks: [
			{name: "t1", topic: "tp1", consumerGroup: "g1", tableName: "tb1"}
			{name: "t2", topic: "tp2", consumerGroup: "g2", tableName: "tb2"}
		]
		assignment: {
			version: 3
			updatedBy: "10.0.0.1:2112"
			map: {"10.0.0.1:2112": ["t1"], "10.0.0.2:2112": ["t2"]}
		}
	}`
	cfg := &Config{Groups: make(map[string]*GroupConfig)}
	if err := hjson.Unmarshal([]byte(legacy), cfg); err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}
	if cfg.Assignment == nil || !cfg.Kafka.RebalanceByLags {
		t.Fatal("legacy fields should still parse before Normallize")
	}
	if err := cfg.Normallize(true, util.Credentials{}); err != nil {
		t.Fatalf("Normallize failed: %v", err)
	}
	if cfg.Assignment != nil || cfg.Kafka.AssignInterval != 0 ||
		cfg.Kafka.CalcLagInterval != 0 || cfg.Kafka.RebalanceByLags {
		t.Fatalf("deprecated fields not cleared: assignment=%+v kafka=%+v", cfg.Assignment, cfg.Kafka)
	}
	for _, g := range []string{"g1", "g2"} {
		if _, ok := cfg.Groups[g]; !ok {
			t.Fatalf("group %s missing, every task must run on every instance; groups=%v", g, cfg.Groups)
		}
	}

	bs, err := hjson.Marshal(*cfg)
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}
	out := strings.ToLower(string(bs))
	for _, key := range []string{"assignment", "assigninterval", "calclaginterval", "rebalancebylags"} {
		if strings.Contains(out, key) {
			t.Fatalf("republished config still contains %q:\n%s", key, bs)
		}
	}
}

// A non-positive discovery interval would panic the main loop's ticker reset.
func TestDiscoveryCheckIntervalNonPositiveUsesDefault(t *testing.T) {
	cfg := &Config{
		Clickhouse: ClickHouseConfig{Hosts: [][]string{{"127.0.0.1"}}, Port: 9000, DB: "default"},
		Kafka:      KafkaConfig{Brokers: "127.0.0.1:9092"},
		Discovery:  Discovery{CheckInterval: -5},
	}
	if err := cfg.Normallize(false, util.Credentials{}); err != nil {
		t.Fatalf("Normallize failed: %v", err)
	}
	if cfg.Discovery.CheckInterval != DefaultDiscoveryIntervalSec {
		t.Fatalf("CheckInterval = %d, want %d", cfg.Discovery.CheckInterval, DefaultDiscoveryIntervalSec)
	}
}
