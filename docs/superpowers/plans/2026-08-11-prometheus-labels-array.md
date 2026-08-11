# prometheusSchema 双数组 label 实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 让 `prometheusSchema=true` 的任务能从一对平行的 `Array(String)` 列（`__labels_key__` / `__labels_value__`）生成 series 表的 `labels` JSON，从而把 series 表列数从「所有 label key 的并集」收敛为固定几列。

**Architecture:** 新增任务级配置 `promLabelsArray{keyColumn,valueColumn}`。`initSeriesSchema` 在 Init 期定位这两列并校验类型，把绝对下标交给 `Service`。`metric2Row` 在写新 series 时从 row 里取出这两个 `[]string`，交给 `task/labels.go` 里的纯函数配对、过滤、去重、拼接。启用后标量 String 列不再参与 `labels` 拼接。

**Tech Stack:** Go 1.x，标准库 `testing`（仓库现有测试不使用 testify，保持一致），`prometheus/client_golang`（statistics 计数器），`thanos-io/pkg/errors`（错误包装，仓库既有用法）。

## Global Constraints

- 设计文档：`docs/superpowers/specs/2026-08-11-prometheus-labels-array-design.md`，所有语义以它为准。
- `labels` JSON 的输出格式必须与现状逐字节一致：`{"k1": "v1", "k2": "v2"}`，元素间分隔符是 `, `（逗号+空格），键与值都过 `strconv.Quote`，空集合输出 `{}`。
- 本次**不**支持 `Array` 列的动态建列；数组列由使用方手工预建。不得修改 `parser/*.go` 的 `GetNewKeys`，也不得修改 `output/clickhouse.go` 的 `ChangeSchema`。
- 未配置 `promLabelsArray` 时，行为必须与改动前完全一致（回归护栏）。
- 排除规则三道，顺序不敏感：key 等于 `nameKey`、key 等于 `le`、key 命中 `promLabelsBlackList` 正则。
- 构建命令：`export PATH=/usr/local/go/bin:$PATH`，仓库根目录执行 `go build ./...` 与 `go test ./...`。
- 每个任务结束时提交，commit message 结尾附 `Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>`。

## 文件结构

| 文件 | 职责 | 动作 |
| --- | --- | --- |
| `task/labels.go` | labels JSON 的配对、过滤、去重、拼接。纯函数，无外部依赖 | 新建 |
| `task/labels_test.go` | 上述纯函数的单测 | 新建 |
| `config/config.go` | `PromLabelsArray` 字段定义与校验 | 修改 |
| `config/config_test.go` | 配置校验单测 | 修改 |
| `output/clickhouse.go` | `IdxLblKey`/`IdxLblVal` 字段、列定位与类型校验、NameKey 探测修复 | 修改 |
| `output/clickhouse_test.go` | 列定位与 NameKey 探测的单测 | 修改 |
| `task/task.go` | `Service` 新字段、`metric2Row` 接线、Array 防御 | 修改 |
| `task/task_test.go` | `metric2Row` 的端到端单测（无需 CH 连接） | 新建 |
| `statistics/statistics.go` | 数组长度不匹配计数器 | 修改 |
| `docs/configuration/config.md` | 新配置项文档 | 修改 |

---

### Task 1: labels 拼接纯函数

**Files:**
- Create: `task/labels.go`
- Test: `task/labels_test.go`

**Interfaces:**
- Consumes: 无（本任务是整个计划的基础）
- Produces:
  - `type labelPair struct { key, val string }`
  - `type labelFilter struct { nameKey string; blkList *regexp.Regexp }`
  - `func (f labelFilter) accept(key string) bool`
  - `func pairLabelArrays(keys, vals []string) (pairs []labelPair, dropped int)`
  - `func buildLabelsJSON(pairs []labelPair, f labelFilter) string`

- [ ] **Step 1: 写失败的测试**

创建 `task/labels_test.go`：

```go
package task

import (
	"regexp"
	"testing"
)

func TestBuildLabelsJSONFormat(t *testing.T) {
	pairs := []labelPair{{"host", "dev-21-47"}, {"queue", "namespace"}}
	got := buildLabelsJSON(pairs, labelFilter{})
	want := `{"host": "dev-21-47", "queue": "namespace"}`
	if got != want {
		t.Fatalf("buildLabelsJSON = %s, want %s", got, want)
	}
}

func TestBuildLabelsJSONEmpty(t *testing.T) {
	if got := buildLabelsJSON(nil, labelFilter{}); got != "{}" {
		t.Fatalf("buildLabelsJSON(nil) = %s, want {}", got)
	}
}

func TestBuildLabelsJSONQuotesSpecialChars(t *testing.T) {
	pairs := []labelPair{{`a"b`, "c\\d"}}
	got := buildLabelsJSON(pairs, labelFilter{})
	want := `{"a\"b": "c\\d"}`
	if got != want {
		t.Fatalf("buildLabelsJSON = %s, want %s", got, want)
	}
}

func TestBuildLabelsJSONDedupKeepsFirst(t *testing.T) {
	pairs := []labelPair{{"host", "first"}, {"host", "second"}}
	got := buildLabelsJSON(pairs, labelFilter{})
	want := `{"host": "first"}`
	if got != want {
		t.Fatalf("buildLabelsJSON = %s, want %s", got, want)
	}
}

func TestBuildLabelsJSONExcludesNameKeyAndLe(t *testing.T) {
	pairs := []labelPair{
		{"__name__", "some.metric"},
		{"le", "0.5"},
		{"host", "dev-21-47"},
	}
	got := buildLabelsJSON(pairs, labelFilter{nameKey: "__name__"})
	want := `{"host": "dev-21-47"}`
	if got != want {
		t.Fatalf("buildLabelsJSON = %s, want %s", got, want)
	}
}

func TestBuildLabelsJSONExcludesBlackList(t *testing.T) {
	pairs := []labelPair{{"__series_key__", "x"}, {"host", "dev-21-47"}}
	f := labelFilter{blkList: regexp.MustCompile(`^__.*__$`)}
	got := buildLabelsJSON(pairs, f)
	want := `{"host": "dev-21-47"}`
	if got != want {
		t.Fatalf("buildLabelsJSON = %s, want %s", got, want)
	}
}

func TestBuildLabelsJSONKeepsEmptyValue(t *testing.T) {
	pairs := []labelPair{{"host", ""}}
	got := buildLabelsJSON(pairs, labelFilter{})
	want := `{"host": ""}`
	if got != want {
		t.Fatalf("buildLabelsJSON = %s, want %s", got, want)
	}
}

func TestPairLabelArraysEqualLength(t *testing.T) {
	pairs, dropped := pairLabelArrays([]string{"a", "b"}, []string{"1", "2"})
	if dropped != 0 {
		t.Fatalf("dropped = %d, want 0", dropped)
	}
	if len(pairs) != 2 || pairs[0] != (labelPair{"a", "1"}) || pairs[1] != (labelPair{"b", "2"}) {
		t.Fatalf("pairs = %v, want [{a 1} {b 2}]", pairs)
	}
}

func TestPairLabelArraysMoreKeysThanValues(t *testing.T) {
	pairs, dropped := pairLabelArrays([]string{"a", "b", "c"}, []string{"1"})
	if dropped != 2 {
		t.Fatalf("dropped = %d, want 2", dropped)
	}
	if len(pairs) != 1 || pairs[0] != (labelPair{"a", "1"}) {
		t.Fatalf("pairs = %v, want [{a 1}]", pairs)
	}
}

func TestPairLabelArraysMoreValuesThanKeys(t *testing.T) {
	pairs, dropped := pairLabelArrays([]string{"a"}, []string{"1", "2", "3"})
	if dropped != 2 {
		t.Fatalf("dropped = %d, want 2", dropped)
	}
	if len(pairs) != 1 || pairs[0] != (labelPair{"a", "1"}) {
		t.Fatalf("pairs = %v, want [{a 1}]", pairs)
	}
}

func TestPairLabelArraysEmpty(t *testing.T) {
	pairs, dropped := pairLabelArrays(nil, nil)
	if len(pairs) != 0 || dropped != 0 {
		t.Fatalf("pairs = %v, dropped = %d, want empty/0", pairs, dropped)
	}
}
```

- [ ] **Step 2: 运行测试确认失败**

Run: `export PATH=/usr/local/go/bin:$PATH && go test ./task/ -run 'TestBuildLabelsJSON|TestPairLabelArrays' -v`
Expected: 编译失败，`undefined: labelPair` / `undefined: buildLabelsJSON` / `undefined: pairLabelArrays`

- [ ] **Step 3: 写最小实现**

创建 `task/labels.go`：

```go
package task

import (
	"regexp"
	"strconv"
	"strings"
)

// labelPair 是拼 labels JSON 前收集到的一个候选键值对。
type labelPair struct {
	key string
	val string
}

// labelFilter 决定一个 label key 是否应当出现在 labels JSON 中。零值不过滤任何 key。
type labelFilter struct {
	nameKey string         // metric 名所在的列，它是指标标识而非 label
	blkList *regexp.Regexp // promLabelsBlackList，可为 nil
}

func (f labelFilter) accept(key string) bool {
	if f.nameKey != "" && key == f.nameKey {
		return false
	}
	// "labels" JSON excludes "le", so that "labels" can be used as group key for histogram queries.
	if key == "le" {
		return false
	}
	return f.blkList == nil || !f.blkList.MatchString(key)
}

// pairLabelArrays 把平行的 key/value 数组按下标配对。长度不等时取较短者，
// dropped 返回被丢弃的元素个数，交由调用方观测——这是上游 ETL 的数据错误，不能静默截断。
func pairLabelArrays(keys, vals []string) (pairs []labelPair, dropped int) {
	n := len(keys)
	if len(vals) < n {
		n = len(vals)
	}
	if d := len(keys) - n; d > 0 {
		dropped += d
	}
	if d := len(vals) - n; d > 0 {
		dropped += d
	}
	pairs = make([]labelPair, 0, n)
	for i := 0; i < n; i++ {
		pairs = append(pairs, labelPair{key: keys[i], val: vals[i]})
	}
	return
}

// buildLabelsJSON 过滤、去重并拼出 series 表 labels 列的 JSON 文本。重复 key 只保留
// 第一次出现的，输出顺序即 pairs 的顺序。格式必须与历史实现逐字节一致，下游查询依赖它。
func buildLabelsJSON(pairs []labelPair, f labelFilter) string {
	var sb strings.Builder
	seen := make(map[string]struct{}, len(pairs))
	sb.WriteByte('{')
	for _, p := range pairs {
		if !f.accept(p.key) {
			continue
		}
		if _, dup := seen[p.key]; dup {
			continue
		}
		seen[p.key] = struct{}{}
		if len(seen) > 1 {
			sb.WriteString(", ")
		}
		sb.WriteString(strconv.Quote(p.key))
		sb.WriteString(": ")
		sb.WriteString(strconv.Quote(p.val))
	}
	sb.WriteByte('}')
	return sb.String()
}
```

- [ ] **Step 4: 运行测试确认通过**

Run: `export PATH=/usr/local/go/bin:$PATH && go test ./task/ -run 'TestBuildLabelsJSON|TestPairLabelArrays' -v`
Expected: 全部 PASS

- [ ] **Step 5: 提交**

```bash
git add task/labels.go task/labels_test.go
git commit -m "feat(task): add pure helpers for building series labels JSON

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

### Task 2: 配置项 promLabelsArray

**Files:**
- Modify: `config/config.go`（结构体约 226 行 `PromLabelsBlackList` 之后；校验在 `normallizeTask`，约 518-523 行）
- Test: `config/config_test.go`

**Interfaces:**
- Consumes: 无
- Produces: `config.TaskConfig.PromLabelsArray`，含 `KeyColumn string` 与 `ValueColumn string` 两个字段

- [ ] **Step 1: 写失败的测试**

在 `config/config_test.go` 末尾追加：

```go
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
```

- [ ] **Step 2: 运行测试确认失败**

Run: `export PATH=/usr/local/go/bin:$PATH && go test ./config/ -run TestPromLabelsArray -v`
Expected: 编译失败，`unknown field PromLabelsArray in struct literal`

- [ ] **Step 3: 写最小实现**

在 `config/config.go` 的 `TaskConfig` 里，紧跟 `PromLabelsBlackList string` 之后插入：

```go
	// PromLabelsArray declares a pair of Array(String) columns in the series table
	// holding parallel label keys/values: __labels_key__[i] pairs with
	// __labels_value__[i]. Once set, the "labels" JSON is built solely from this
	// pair and scalar String columns no longer contribute to it. This keeps the
	// series table from growing one column per label key. Requires PrometheusSchema be true.
	PromLabelsArray struct {
		KeyColumn   string
		ValueColumn string
	} `json:"promLabelsArray,omitempty"`
```

在 `normallizeTask` 中，把现有的

```go
	if taskCfg.PrometheusSchema {
		taskCfg.DynamicSchema.Enable = true
		taskCfg.AutoSchema = true
	} else {
		taskCfg.PromLabelsBlackList = ""
	}
```

改成：

```go
	if taskCfg.PrometheusSchema {
		taskCfg.DynamicSchema.Enable = true
		taskCfg.AutoSchema = true
		if (taskCfg.PromLabelsArray.KeyColumn == "") != (taskCfg.PromLabelsArray.ValueColumn == "") {
			err = errors.Newf("promLabelsArray requires both keyColumn and valueColumn to be set, got keyColumn=%q valueColumn=%q",
				taskCfg.PromLabelsArray.KeyColumn, taskCfg.PromLabelsArray.ValueColumn)
			return
		}
	} else {
		taskCfg.PromLabelsBlackList = ""
		taskCfg.PromLabelsArray.KeyColumn = ""
		taskCfg.PromLabelsArray.ValueColumn = ""
	}
```

- [ ] **Step 4: 运行测试确认通过**

Run: `export PATH=/usr/local/go/bin:$PATH && go test ./config/ -v`
Expected: 新增 4 个用例全部 PASS，既有用例不回归

- [ ] **Step 5: 提交**

```bash
git add config/config.go config/config_test.go
git commit -m "feat(config): add promLabelsArray task option

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

### Task 3: series 表列定位与 NameKey 探测修复

**Files:**
- Modify: `output/clickhouse.go`（结构体约 67-71 行；`initSeriesSchema` 约 482-607 行，其中 NameKey 探测在 545-552 行）
- Test: `output/clickhouse_test.go`

**Interfaces:**
- Consumes: `config.TaskConfig.PromLabelsArray`（Task 2）
- Produces:
  - `ClickHouse.IdxLblKey int` / `ClickHouse.IdxLblVal int`（导出字段，未启用时为 -1）
  - `func detectNameKey(seriesDims []*model.ColumnWithType, start int) string`
  - `func locatePromLabelsArray(dims []*model.ColumnWithType, start int, keyCol, valCol string) (idxKey, idxVal int, err error)`

背景：`c.Dims` 的布局是 `[metric 列..., __series_id__, __mgmt_id__, labels, 其余 series 列...]`，所以 series 表里非固定三列的部分从下标 `IdxSerID+3` 开始。

- [ ] **Step 1: 写失败的测试**

在 `output/clickhouse_test.go` 末尾追加：

```go
func strCol(name string, array bool) *model.ColumnWithType {
	return &model.ColumnWithType{Name: name, Type: &model.TypeInfo{Type: model.String, Array: array}}
}

func TestDetectNameKeySkipsArrayColumns(t *testing.T) {
	dims := []*model.ColumnWithType{
		{Name: "__series_id__", Type: &model.TypeInfo{Type: model.Int64}},
		{Name: "__mgmt_id__", Type: &model.TypeInfo{Type: model.Int64}},
		strCol("labels", false),
		strCol("__labels_key__", true),
		strCol("__name__", false),
	}
	if got := detectNameKey(dims, 3); got != "__name__" {
		t.Fatalf("detectNameKey = %q, want __name__", got)
	}
}

func TestDetectNameKeyDefaultsWhenNoScalarString(t *testing.T) {
	dims := []*model.ColumnWithType{
		{Name: "__series_id__", Type: &model.TypeInfo{Type: model.Int64}},
		{Name: "__mgmt_id__", Type: &model.TypeInfo{Type: model.Int64}},
		strCol("labels", false),
		strCol("__labels_key__", true),
	}
	if got := detectNameKey(dims, 3); got != "__name__" {
		t.Fatalf("detectNameKey = %q, want __name__", got)
	}
}

func TestLocatePromLabelsArrayDisabled(t *testing.T) {
	dims := []*model.ColumnWithType{strCol("labels", false)}
	k, v, err := locatePromLabelsArray(dims, 1, "", "")
	if err != nil || k != -1 || v != -1 {
		t.Fatalf("locatePromLabelsArray = %d,%d,%v; want -1,-1,nil", k, v, err)
	}
}

func TestLocatePromLabelsArrayFound(t *testing.T) {
	dims := []*model.ColumnWithType{
		strCol("labels", false),
		strCol("host", false),
		strCol("__labels_key__", true),
		strCol("__labels_value__", true),
	}
	k, v, err := locatePromLabelsArray(dims, 1, "__labels_key__", "__labels_value__")
	if err != nil {
		t.Fatalf("locatePromLabelsArray failed: %v", err)
	}
	if k != 2 || v != 3 {
		t.Fatalf("locatePromLabelsArray = %d,%d; want 2,3", k, v)
	}
}

func TestLocatePromLabelsArrayMissingColumn(t *testing.T) {
	dims := []*model.ColumnWithType{strCol("labels", false), strCol("__labels_key__", true)}
	if _, _, err := locatePromLabelsArray(dims, 1, "__labels_key__", "__labels_value__"); err == nil {
		t.Fatal("locatePromLabelsArray should fail when valueColumn is absent")
	}
}

func TestLocatePromLabelsArrayWrongType(t *testing.T) {
	dims := []*model.ColumnWithType{
		strCol("labels", false),
		strCol("__labels_key__", false), // 非 Array
		strCol("__labels_value__", true),
	}
	if _, _, err := locatePromLabelsArray(dims, 1, "__labels_key__", "__labels_value__"); err == nil {
		t.Fatal("locatePromLabelsArray should fail when keyColumn is not Array(String)")
	}
}

func TestLocatePromLabelsArrayBeforeStartIsInvisible(t *testing.T) {
	dims := []*model.ColumnWithType{
		strCol("__labels_key__", true),
		strCol("__labels_value__", true),
		strCol("labels", false),
	}
	if _, _, err := locatePromLabelsArray(dims, 2, "__labels_key__", "__labels_value__"); err == nil {
		t.Fatal("columns before start (the fixed series columns) must not be accepted")
	}
}
```

同时把 `output/clickhouse_test.go` 顶部的 import 块改成（现状只有 `context`/`testing`/`time`）：

```go
import (
	"context"
	"testing"
	"time"

	"github.com/housepower/clickhouse_sinker/model"
)
```

- [ ] **Step 2: 运行测试确认失败**

Run: `export PATH=/usr/local/go/bin:$PATH && go test ./output/ -run 'TestDetectNameKey|TestLocatePromLabelsArray' -v`
Expected: 编译失败，`undefined: detectNameKey` / `undefined: locatePromLabelsArray`

- [ ] **Step 3: 写最小实现**

在 `output/clickhouse.go` 的 `ClickHouse` 结构体中，`IdxSerID int` 之后加两个字段：

```go
	IdxSerID  int
	// IdxLblKey/IdxLblVal 是 promLabelsArray 那对 Array(String) 列在 Dims 中的绝对下标，
	// 未启用时为 -1。
	IdxLblKey int
	IdxLblVal int
```

在文件末尾追加两个纯函数：

```go
// detectNameKey 找出承载 metric 名的列：prometheus 用内建 label "__name__"，
// opentsdb 则用一个自定义的字符串列。从 start 起取第一个标量 String 列。
// Array(String) 的 Type.Type 同样是 String，必须排除，否则 __labels_key__ 这类
// 容器列会被错认成 metric 名列。
func detectNameKey(seriesDims []*model.ColumnWithType, start int) string {
	for i := start; i < len(seriesDims); i++ {
		serDim := seriesDims[i]
		if serDim.Type.Type == model.String && !serDim.Type.Array {
			return serDim.Name
		}
	}
	return "__name__"
}

// locatePromLabelsArray 在 dims[start:] 中定位 promLabelsArray 声明的那对列，返回它们的
// 绝对下标。keyCol 与 valCol 均为空表示功能未启用，返回 -1,-1,nil。
// 校验放在 Init 期而非运行期，配错立刻失败，不留到线上写数据时才炸。
func locatePromLabelsArray(dims []*model.ColumnWithType, start int, keyCol, valCol string) (idxKey, idxVal int, err error) {
	idxKey, idxVal = -1, -1
	if keyCol == "" && valCol == "" {
		return
	}
	for i := start; i < len(dims); i++ {
		switch dims[i].Name {
		case keyCol:
			idxKey = i
		case valCol:
			idxVal = i
		}
	}
	if idxKey < 0 || idxVal < 0 {
		err = errors.Newf("promLabelsArray columns %q/%q not found in the series table (they must be declared after the fixed series columns)", keyCol, valCol)
		idxKey, idxVal = -1, -1
		return
	}
	for _, i := range []int{idxKey, idxVal} {
		if t := dims[i].Type; t.Type != model.String || !t.Array {
			err = errors.Newf("promLabelsArray column %q shall be Array(String)", dims[i].Name)
			idxKey, idxVal = -1, -1
			return
		}
	}
	return
}
```

在 `initSeriesSchema` 开头，把

```go
	if !c.taskCfg.PrometheusSchema {
		c.IdxSerID = -1
		return
	}
```

改成：

```go
	c.IdxLblKey, c.IdxLblVal = -1, -1
	if !c.taskCfg.PrometheusSchema {
		c.IdxSerID = -1
		return
	}
```

把现有的 NameKey 探测块

```go
	c.NameKey = "__name__" // prometheus uses internal "__name__" label for metric name
	for i := len(expSeriesDims); i < len(seriesDims); i++ {
		serDim := seriesDims[i]
		if serDim.Type.Type == model.String {
			c.NameKey = serDim.Name // opentsdb uses "metric" tag for metric name
			break
		}
	}
	c.Dims = append(c.Dims, seriesDims[1:]...)
```

改成：

```go
	// prometheus uses internal "__name__" label for metric name;
	// opentsdb uses a custom string column instead.
	c.NameKey = detectNameKey(seriesDims, len(expSeriesDims))
	c.Dims = append(c.Dims, seriesDims[1:]...)

	if c.IdxLblKey, c.IdxLblVal, err = locatePromLabelsArray(c.Dims, c.IdxSerID+3,
		c.taskCfg.PromLabelsArray.KeyColumn, c.taskCfg.PromLabelsArray.ValueColumn); err != nil {
		err = errors.Wrapf(err, "table %s.%s", c.dbName, c.seriesTbl)
		return
	}
```

- [ ] **Step 4: 运行测试确认通过**

Run: `export PATH=/usr/local/go/bin:$PATH && go test ./output/ -run 'TestDetectNameKey|TestLocatePromLabelsArray' -v && go build ./...`
Expected: 7 个用例全部 PASS，编译通过

- [ ] **Step 5: 提交**

```bash
git add output/clickhouse.go output/clickhouse_test.go
git commit -m "feat(output): locate promLabelsArray columns and fix NameKey detection

NameKey detection took the first String column of the series table, but
Array(String) also reports Type.Type == String, so a leading __labels_key__
column would be mistaken for the metric name column.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

### Task 4: metric2Row 接线

**Files:**
- Modify: `task/task.go`（`Service` 结构体约 39-68 行；`Init` 约 133-137 行；`metric2Row` 约 268-304 行）
- Modify: `statistics/statistics.go`（变量块与 `init()`）
- Test: `task/task_test.go`（新建）

**Interfaces:**
- Consumes: `buildLabelsJSON`/`pairLabelArrays`/`labelFilter`（Task 1）、`ClickHouse.IdxLblKey`/`IdxLblVal`（Task 3）
- Produces: `Service.idxLblKey`、`Service.idxLblVal`、`Service.lblFilter`；`statistics.PromLabelsArrayMismatch`

- [ ] **Step 1: 写失败的测试**

创建 `task/task_test.go`：

```go
package task

import (
	"regexp"
	"testing"
	"time"

	"github.com/housepower/clickhouse_sinker/config"
	"github.com/housepower/clickhouse_sinker/model"
	"github.com/housepower/clickhouse_sinker/output"
	"github.com/housepower/clickhouse_sinker/parser"
	"golang.org/x/time/rate"
)

const arraySample = `{
	"__name__": "kubernetes.controller_manager.queue_work_unfinished_duration.sec",
	"__series_id": 1129336697220775,
	"__mgmt_id": 1101941880299034,
	"value": 0,
	"__labels_key__": ["host", "queue", "le", "__series_key__"],
	"__labels_value__": ["dev-21-47", "namespace", "0.5", "noise"],
	"objectType": "kubernetes"
}`

// newArrayTestService 手工装配一个 Service，绕开需要 ClickHouse 连接的 Init()。
// dims 布局与 initSeriesSchema 的产物一致：
// [value, __series_id, __mgmt_id, labels, objectType, __labels_key__, __labels_value__]
func newArrayTestService(t *testing.T, lblFilter labelFilter, withArray bool) *Service {
	t.Helper()
	taskCfg := &config.TaskConfig{Name: "test", PrometheusSchema: true}
	ck := output.NewClickHouse(&config.Config{}, taskCfg)
	ck.DimSerID = "__series_id"
	ck.DimMgmtID = "__mgmt_id"
	ck.SetSeriesQuota(&model.SeriesQuota{
		BmSeries:       make(map[int64]int64),
		NextResetQuota: time.Now().Add(time.Hour),
		Birth:          time.Now(),
	})

	str := func(name string, array bool) *model.ColumnWithType {
		return &model.ColumnWithType{Name: name, SourceName: name,
			Type: &model.TypeInfo{Type: model.String, Array: array}}
	}
	i64 := func(name string) *model.ColumnWithType {
		return &model.ColumnWithType{Name: name, SourceName: name, Type: &model.TypeInfo{Type: model.Int64}}
	}
	dims := []*model.ColumnWithType{
		{Name: "value", SourceName: "value", Type: &model.TypeInfo{Type: model.Float64}},
		i64("__series_id"),
		i64("__mgmt_id"),
		str("labels", false),
		str("objectType", false),
		str("__labels_key__", true),
		str("__labels_value__", true),
	}
	svc := &Service{
		clickhouse: ck,
		taskCfg:    taskCfg,
		dims:       dims,
		numDims:    len(dims),
		idxSerID:   1,
		nameKey:    "__name__",
		lblFilter:  lblFilter,
		idxLblKey:  -1,
		idxLblVal:  -1,
		// metric2Row 在数组长度不匹配时会用 limiter 限流打日志，手工装配时必须给上，
		// 否则那条分支一走到就 nil pointer。
		limiter: rate.NewLimiter(rate.Every(10*time.Second), 1),
	}
	if withArray {
		svc.idxLblKey = 5
		svc.idxLblVal = 6
	}
	return svc
}

func parseArraySample(t *testing.T) (model.Metric, func()) {
	t.Helper()
	pp, err := parser.NewParserPool("fastjson", nil, "", "", 1.0, "")
	if err != nil {
		t.Fatalf("NewParserPool failed: %v", err)
	}
	p, err := pp.Get()
	if err != nil {
		t.Fatalf("pp.Get failed: %v", err)
	}
	metric, err := p.Parse([]byte(arraySample))
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	return metric, func() { pp.Put(p) }
}

func TestMetric2RowBuildsLabelsFromArrays(t *testing.T) {
	svc := newArrayTestService(t, labelFilter{nameKey: "__name__"}, true)
	metric, done := parseArraySample(t)
	defer done()

	row := svc.metric2Row(metric, &model.InputMessage{})
	if row == nil {
		t.Fatal("metric2Row returned nil")
	}
	// le 被排除；objectType 是标量列，启用数组后不参与拼接
	want := `{"host": "dev-21-47", "queue": "namespace", "__series_key__": "noise"}`
	if got, _ := (*row)[3].(string); got != want {
		t.Fatalf("labels = %s, want %s", got, want)
	}
	// 数组列本身仍然原样落盘
	keys, ok := (*row)[5].([]string)
	if !ok || len(keys) != 4 || keys[0] != "host" {
		t.Fatalf("__labels_key__ column = %v (ok=%v), want the raw []string", (*row)[5], ok)
	}
}

func TestMetric2RowAppliesBlackListToArrayKeys(t *testing.T) {
	svc := newArrayTestService(t, labelFilter{
		nameKey: "__name__",
		blkList: regexp.MustCompile(`^__.*__$`),
	}, true)
	metric, done := parseArraySample(t)
	defer done()

	row := svc.metric2Row(metric, &model.InputMessage{})
	want := `{"host": "dev-21-47", "queue": "namespace"}`
	if got, _ := (*row)[3].(string); got != want {
		t.Fatalf("labels = %s, want %s", got, want)
	}
}

func TestMetric2RowWithoutArrayFallsBackToScalarColumns(t *testing.T) {
	svc := newArrayTestService(t, labelFilter{nameKey: "__name__"}, false)
	metric, done := parseArraySample(t)
	defer done()

	row := svc.metric2Row(metric, &model.InputMessage{})
	// 未启用数组时维持既有行为：只有标量 String 列进 labels，
	// __labels_key__/__labels_value__ 这类 Array 列必须被跳过而不是 panic。
	want := `{"objectType": "kubernetes"}`
	if got, _ := (*row)[3].(string); got != want {
		t.Fatalf("labels = %s, want %s", got, want)
	}
}
```

- [ ] **Step 2: 运行测试确认失败**

Run: `export PATH=/usr/local/go/bin:$PATH && go test ./task/ -run TestMetric2Row -v`
Expected: 编译失败，`unknown field lblFilter/idxLblKey/idxLblVal in struct literal of type Service`

- [ ] **Step 3: 写最小实现**

先在 `statistics/statistics.go` 的变量块里（`MsgsDropTotal` 之后）加计数器：

```go
	// PromLabelsArrayMismatch counts label array elements discarded because the
	// key and value arrays had different lengths. A mismatch is an upstream ETL
	// bug that silently loses labels, so it must stay observable.
	PromLabelsArrayMismatch = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: prefix + "prom_labels_array_mismatch_total",
			Help: "total num of label array elements dropped due to key/value length mismatch",
		},
		[]string{"task"},
	)
```

并在 `init()` 中注册：

```go
	prometheus.MustRegister(PromLabelsArrayMismatch)
```

在 `task/task.go` 的 `Service` 结构体里，把

```go
	idxSerID int
	nameKey  string
```

改成：

```go
	idxSerID int
	nameKey  string
	// idxLblKey/idxLblVal 是 promLabelsArray 那对 Array(String) 列在 dims 中的下标，
	// 未启用时为 -1。启用后 labels JSON 只由这对数组生成。
	idxLblKey int
	idxLblVal int
	lblFilter labelFilter
```

在 `Init()` 中，把

```go
	service.idxSerID = service.clickhouse.IdxSerID
	service.nameKey = service.clickhouse.NameKey
```

改成：

```go
	service.idxSerID = service.clickhouse.IdxSerID
	service.nameKey = service.clickhouse.NameKey
	service.idxLblKey = service.clickhouse.IdxLblKey
	service.idxLblVal = service.clickhouse.IdxLblVal
	service.lblFilter = labelFilter{nameKey: service.nameKey, blkList: service.lblBlkList}
```

把 `metric2Row` 中 `if newSeries { ... }` 这一整段

```go
		if newSeries {
			var labels []string
			row = append(row, mgmtID, nil) // __mgmt_id__, labels
			for i := service.idxSerID + 3; i < service.numDims; i++ {
				dim := service.dims[i]
				val := model.GetValueByType(metric, dim)
				row = append(row, val)
				if val != nil && dim.Type.Type == model.String && dim.Name != service.nameKey && dim.Name != "le" && (service.lblBlkList == nil || !service.lblBlkList.MatchString(dim.Name)) {
					// "labels" JSON excludes "le", so that "labels" can be used as group key for histogram queries.
					if !(service.taskCfg.DynamicSchema.NotNullable && val == "") {
						labelVal := val.(string)
						labels = append(labels, fmt.Sprintf(`%s: %s`, strconv.Quote(dim.Name), strconv.Quote(labelVal)))
					}
				}
			}
			row[service.idxSerID+2] = fmt.Sprintf("{%s}", strings.Join(labels, ", "))
		}
```

替换为：

```go
		if newSeries {
			var pairs []labelPair
			useArray := service.idxLblKey >= 0
			row = append(row, mgmtID, nil) // __mgmt_id__, labels
			for i := service.idxSerID + 3; i < service.numDims; i++ {
				dim := service.dims[i]
				val := model.GetValueByType(metric, dim)
				row = append(row, val)
				// 启用 promLabelsArray 后 labels 只认数组，标量列照常落各自的列但不进 JSON。
				// Array(String) 的 Type.Type 也是 String，必须显式排除，否则下面的
				// val.(string) 会对 []string panic。
				if useArray || val == nil || dim.Type.Type != model.String || dim.Type.Array {
					continue
				}
				if service.taskCfg.DynamicSchema.NotNullable && val == "" {
					continue
				}
				pairs = append(pairs, labelPair{key: dim.Name, val: val.(string)})
			}
			if useArray {
				keys, _ := row[service.idxLblKey].([]string)
				vals, _ := row[service.idxLblVal].([]string)
				var dropped int
				if pairs, dropped = pairLabelArrays(keys, vals); dropped != 0 {
					statistics.PromLabelsArrayMismatch.WithLabelValues(service.taskCfg.Name).Add(float64(dropped))
					if service.limiter.Allow() {
						util.Logger.Warn("promLabelsArray key/value length mismatch, extra elements dropped",
							zap.String("task", service.taskCfg.Name),
							zap.Int("keys", len(keys)), zap.Int("values", len(vals)))
					}
				}
			}
			row[service.idxSerID+2] = buildLabelsJSON(pairs, service.lblFilter)
		}
```

最后处理 import：`strconv` 在 `task.go` 里**只**被删掉的那行用到（原 298 行），必须从 import 块移除，否则编译报未使用。`fmt` 与 `strings` 在文件其它位置（`Init`、非 prometheus 分支的 `metric2Row`）仍有使用，保留。`statistics`、`util`、`zap` 已在 import 块中，无需新增。

- [ ] **Step 4: 运行测试确认通过**

Run: `export PATH=/usr/local/go/bin:$PATH && go build ./... && go test ./task/ -v`
Expected: 3 个 `TestMetric2Row*` 用例与 Task 1 的用例全部 PASS

- [ ] **Step 5: 补一个长度不匹配的用例并确认通过**

在 `task/task_test.go` 末尾追加：

```go
func TestMetric2RowArrayLengthMismatchTruncates(t *testing.T) {
	svc := newArrayTestService(t, labelFilter{nameKey: "__name__"}, true)
	pp, err := parser.NewParserPool("fastjson", nil, "", "", 1.0, "")
	if err != nil {
		t.Fatalf("NewParserPool failed: %v", err)
	}
	p, err := pp.Get()
	if err != nil {
		t.Fatalf("pp.Get failed: %v", err)
	}
	defer pp.Put(p)
	sample := `{"__series_id": 1, "__mgmt_id": 2, "value": 0,
		"__labels_key__": ["host", "queue", "extra"],
		"__labels_value__": ["dev-21-47"]}`
	metric, err := p.Parse([]byte(sample))
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	row := svc.metric2Row(metric, &model.InputMessage{})
	want := `{"host": "dev-21-47"}`
	if got, _ := (*row)[3].(string); got != want {
		t.Fatalf("labels = %s, want %s", got, want)
	}
}
```

Run: `export PATH=/usr/local/go/bin:$PATH && go test ./task/ -run TestMetric2Row -v`
Expected: 4 个用例全部 PASS

- [ ] **Step 6: 全量回归**

Run: `export PATH=/usr/local/go/bin:$PATH && go build ./... && go test ./... 2>&1 | tail -30`
Expected: 编译通过；无需 ClickHouse/Kafka 的包全部 PASS。需要外部服务的用例若本来就跳过或失败，与改动前保持一致即可——先 `git stash` 跑一遍基线对比，别把既有失败算到本次头上。

- [ ] **Step 7: 提交**

```bash
git add task/task.go task/task_test.go statistics/statistics.go
git commit -m "feat(task): build series labels from promLabelsArray column pair

Once promLabelsArray is configured the labels JSON is built solely from the
parallel key/value arrays, so scalar String columns no longer leak into it.
Array columns are now excluded from the scalar path as well, which also fixes
a val.(string) panic on []string when a series table happens to hold one.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

### Task 5: 文档

**Files:**
- Modify: `docs/configuration/config.md`（`prometheusSchema` 与 `promLabelsBlackList` 附近，约 170-175 行）

**Interfaces:**
- Consumes: `config.TaskConfig.PromLabelsArray`（Task 2）
- Produces: 无代码接口

- [ ] **Step 1: 在配置样例中补上新选项**

在 `docs/configuration/config.md` 里 `promLabelsBlackList` 条目之后插入：

```json
    // promLabelsArray declares a pair of Array(String) columns in the series table
    // holding parallel label keys/values: __labels_key__[i] pairs with __labels_value__[i].
    // Once set, the "labels" JSON is built solely from this pair, and scalar String
    // columns no longer contribute to it. This keeps the series table from growing one
    // column per label key when upstream label cardinality is high.
    // Both columns must be pre-created as Array(String); they are not created automatically.
    // Requires prometheusSchema be true.
    "promLabelsArray": {
        "keyColumn": "__labels_key__",
        "valueColumn": "__labels_value__"
    },
```

同时在该节补一段 DDL 说明：

````markdown
使用 `promLabelsArray` 前需要手工在 series 表建好这对列（sinker 不会自动创建）：

```sql
ALTER TABLE <db>.<metric>_series
  ADD COLUMN IF NOT EXISTS `__labels_key__`   Array(String),
  ADD COLUMN IF NOT EXISTS `__labels_value__` Array(String);
```
````

- [ ] **Step 2: 确认文档中的字段名与代码一致**

Run: `export PATH=/usr/local/go/bin:$PATH && grep -n "promLabelsArray" docs/configuration/config.md config/config.go`
Expected: 两处的 JSON 标签都是 `promLabelsArray`，子字段 `keyColumn` / `valueColumn` 与 `KeyColumn` / `ValueColumn` 对应（Go 的 JSON 解析对字段名大小写不敏感，与仓库既有风格一致）

- [ ] **Step 3: 提交**

```bash
git add docs/configuration/config.md
git commit -m "docs: document promLabelsArray

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

## 自检记录

- **spec 覆盖**：配置项→Task 2；labels 语义（只认数组、三道排除、去重、取 min、空值保留、不排序）→Task 1 + Task 4；Init 期定位与校验→Task 3；NameKey 修复与 Array 防御→Task 3 + Task 4；测试→各任务内含；DDL 与使用说明→Task 5。spec 中「不在本次范围」的三条无对应任务，符合预期。
- **占位符**：无 TBD/TODO，每个代码步骤均给出完整代码。
- **类型一致性**：`labelPair`/`labelFilter`/`pairLabelArrays`/`buildLabelsJSON`（Task 1 定义，Task 4 使用）、`IdxLblKey`/`IdxLblVal`（Task 3 定义，Task 4 使用）、`PromLabelsArray.KeyColumn`/`ValueColumn`（Task 2 定义，Task 3、Task 5 使用）、`statistics.PromLabelsArrayMismatch`（Task 4 内定义并使用）命名前后一致。
