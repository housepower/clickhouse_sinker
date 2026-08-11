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
	"testing"

	"github.com/housepower/clickhouse_sinker/util"
)

func TestWriteFailureDefaults(t *testing.T) {
	cfg := &Config{
		Clickhouse: ClickHouseConfig{Hosts: [][]string{{"127.0.0.1"}}, Port: 9000, DB: "default"},
		Kafka:      KafkaConfig{Brokers: "127.0.0.1:9092"},
		Tasks:      []*TaskConfig{{Name: "t1", Topic: "tp", TableName: "tb"}},
	}
	// Normallize 触发 parseConfig 默认值填充（三参数形式：constructGroup=false, httpAddr="", cred=空）
	if err := cfg.Normallize(false, "", util.Credentials{}); err != nil {
		t.Fatalf("Normallize failed: %v", err)
	}
	if cfg.Clickhouse.RetryTimes != 3 {
		t.Fatalf("RetryTimes default = %d, want 3", cfg.Clickhouse.RetryTimes)
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
	if err := cfg.Normallize(false, "", util.Credentials{}); err != nil {
		t.Fatalf("Normallize failed: %v", err)
	}
	if cfg.Tasks[0].PromLabelsArray.KeyColumn != "__labels_key__" {
		t.Fatalf("KeyColumn = %q, want __labels_key__", cfg.Tasks[0].PromLabelsArray.KeyColumn)
	}
}

func TestPromLabelsArrayOnlyKeyColumnRejected(t *testing.T) {
	cfg := newPromTaskCfg("__labels_key__", "", true)
	if err := cfg.Normallize(false, "", util.Credentials{}); err == nil {
		t.Fatal("Normallize should reject promLabelsArray with only keyColumn set")
	}
}

func TestPromLabelsArrayOnlyValueColumnRejected(t *testing.T) {
	cfg := newPromTaskCfg("", "__labels_value__", true)
	if err := cfg.Normallize(false, "", util.Credentials{}); err == nil {
		t.Fatal("Normallize should reject promLabelsArray with only valueColumn set")
	}
}

func TestPromLabelsArrayClearedWithoutPrometheusSchema(t *testing.T) {
	cfg := newPromTaskCfg("__labels_key__", "__labels_value__", false)
	if err := cfg.Normallize(false, "", util.Credentials{}); err != nil {
		t.Fatalf("Normallize failed: %v", err)
	}
	got := cfg.Tasks[0].PromLabelsArray
	if got.KeyColumn != "" || got.ValueColumn != "" {
		t.Fatalf("PromLabelsArray = %+v, want cleared when prometheusSchema is false", got)
	}
}
