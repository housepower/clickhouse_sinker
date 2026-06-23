# 错误分类与分级处理(错误旁路)Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 让 sinker 按错误性质分级处理写入失败 —— 瞬时错误退避重试到上限,不可重试错误按 task 可配策略(THROW 停 task / IGNORE 丢弃 / WRITE_TO_KAFKA 旁路死信)处置,任何坏数据都不再拖垮进程或阻塞管线。

**Architecture:** 新增纯函数错误分类器(`output/errclass.go`)判定可重试/不可重试;重写 `loopWrite` 为分类驱动的循环,瞬时错误退避重试到 `RetryMaxDuration`,不可重试错误经 `dispatchFailure` 按 per-task `writeFailureStrategy` 分派;原始 kafka 字节经 `model.Batch.Msgs` 从 sharder 贯通到写入层供死信重放;`THROW` 经回调通知 Sinker 隔离该 task(复用 reload tick 移除语义)。

**Tech Stack:** Go,clickhouse-go/v2(`lib/proto.Exception`),franz-go(`kgo`/`kadm` 死信 producer + 自动建 topic),prometheus client,thanos errors,zap,golang.org/x/time/rate。

## Global Constraints

- 构建/测试一律加 `-mod=mod`(仓库存在既有 vendor 不一致):`go build -mod=mod ./...`、`go test -mod=mod ./...`。
- 错误码用 `int32`(对齐 `proto.Exception.Code`)。
- `THROW` 绝不停整进程,只隔离单个 task;守住多租户隔离。
- 死信集群自身故障兜底降级为丢弃,不反压主链路。
- 默认 `writeFailureStrategy = IGNORE`;`RetryMaxDuration` 留空默认 `30m`。
- 注释/日志用中文风格与现有代码一致;指标名加 `clickhouse_sinker_` 前缀(由 statistics 包统一)。
- 提交信息结尾附:`Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>`。

---

## File Structure

- `config/config.go`(改):`ClickHouseConfig` 加 `RetryMaxDuration/RetryableErrorCodes/FatalErrorCodes`;新增 `WriteFailureConfig` 结构 + 策略常量;`TaskConfig` 加 `WriteFailure *WriteFailureConfig`;`parseConfig` 补默认值。
- `output/errclass.go`(新):错误分类纯函数 + 内置码表。
- `output/errclass_test.go`(新):分类器单测。
- `statistics/statistics.go`(改):4 个新 CounterVec + 注册 + pusher。
- `model/message.go`(改):`Batch.Msgs []*InputMessage`。
- `task/sharding.go`(改):`Sharder` 增并行消息缓冲,贯通 `Msg` 到 `Batch.Msgs`。
- `task/sharding_test.go`(新):`Msgs`/`Rows` 对齐测试。
- `output/deadletter.go`(新):`DeadLetterSink` + `dlqProducer` 接口 + franz-go 实现 + 自动建 topic + 兜底。
- `output/deadletter_test.go`(新):payload/header 构造 + 兜底降级测试(注入 fake producer)。
- `task/sinker.go`(改):`brokenTasks sync.Map` + `MarkTaskBroken` + `filterBrokenTasks`(在 `applyConfig` 调用)。
- `task/task.go`(改):`NewTaskService` 注入 `onTaskBroken` 回调到 ClickHouse。
- `output/clickhouse.go`(改):`ClickHouse` 加字段(分类码表、`retryMaxDur`、`deadLetter`、`onTaskBroken`、`broken atomic.Bool`、`limiter`);`Init` 解析配置/建死信;重写 `loopWrite`;`Send` 短路 broken;新增 `dispatchFailure`、`sleepWithCtx`。
- `output/clickhouse_test.go`(新或追加):执行器行为测试(fake write)。

---

## Task 1: 配置项

**Files:**
- Modify: `config/config.go`(`ClickHouseConfig` ~107-141、`TaskConfig` ~151、`parseConfig` ~285-347)
- Test: `config/config_test.go`(追加)

**Interfaces:**
- Produces:
  - `config.WriteFailureConfig` 结构,字段:`Strategy string`、`BootstrapServers []string`、`AutoCreateTopic bool`、`AutoCreateTopicPartitions int32`、`AutoCreateTopicReplicationFactor int16`、`TopicName string`。
  - 常量:`config.WriteFailureThrow="THROW"`、`config.WriteFailureIgnore="IGNORE"`、`config.WriteFailureWriteToKafka="WRITE_TO_KAFKA"`。
  - `ClickHouseConfig.RetryMaxDuration string`、`.RetryableErrorCodes []int`、`.FatalErrorCodes []int`。
  - `TaskConfig.WriteFailure *WriteFailureConfig`(json:`writeFailure`)。

- [ ] **Step 1: 写失败测试** —— 在 `config/config_test.go` 追加:

```go
func TestWriteFailureDefaults(t *testing.T) {
	cfg := &Config{
		Clickhouse: ClickHouseConfig{Hosts: [][]string{{"127.0.0.1"}}, Port: 9000, DB: "default"},
		Kafka:      KafkaConfig{Brokers: "127.0.0.1:9092"},
		Tasks:      []*TaskConfig{{Name: "t1", Topic: "tp", TableName: "tb"}},
	}
	cfg.Normallize() // 触发 parseConfig 默认值填充
	if cfg.Clickhouse.RetryMaxDuration != "30m" {
		t.Fatalf("RetryMaxDuration default = %q, want 30m", cfg.Clickhouse.RetryMaxDuration)
	}
}
```

> 注:确认填充默认值的函数名 —— 若不是 `Normallize`,用 `grep -n "func (cfg \*Config)" config/config.go` 找到对外的规整入口(parseConfig 可能是内部函数),测试里调用它。

- [ ] **Step 2: 运行测试确认失败**

Run: `go test -mod=mod ./config/ -run TestWriteFailureDefaults -v`
Expected: FAIL(`RetryMaxDuration` 为空)。

- [ ] **Step 3: 加结构与字段**

在 `ClickHouseConfig` 内 `RetryTimes` 附近新增:

```go
	// 瞬时错误重试总时长上限,如 "30m";留空默认 30m。与 RetryTimes 取先到者。
	RetryMaxDuration string `json:"retryMaxDuration,omitempty"`
	// 追加到内置可重试白名单的 ClickHouse 错误码。
	RetryableErrorCodes []int `json:"retryableErrorCodes,omitempty"`
	// 强制归为"不可重试"的 ClickHouse 错误码(优先级高于白名单)。
	FatalErrorCodes []int `json:"fatalErrorCodes,omitempty"`
```

在 `config.go` 合适位置(`TaskConfig` 定义前)新增:

```go
const (
	WriteFailureThrow        = "THROW"
	WriteFailureIgnore       = "IGNORE"
	WriteFailureWriteToKafka = "WRITE_TO_KAFKA"
)

// WriteFailureConfig 控制不可重试写入失败的 per-task 处置方式。
type WriteFailureConfig struct {
	// THROW(停掉该 task)| IGNORE(丢弃,默认)| WRITE_TO_KAFKA(旁路死信)
	Strategy string `json:"writeFailureStrategy"`
	// 仅 WRITE_TO_KAFKA 用:死信 kafka 集群(独立于输入端)。
	BootstrapServers                 []string `json:"bootstrapServers,omitempty"`
	AutoCreateTopic                  bool     `json:"autoCreateTopic,omitempty"`
	AutoCreateTopicPartitions        int32    `json:"autoCreateTopicPartitions,omitempty"`
	AutoCreateTopicReplicationFactor int16    `json:"autoCreateTopicReplicationFactor,omitempty"`
	TopicName                        string   `json:"topicName,omitempty"`
}
```

在 `TaskConfig` 内新增字段:

```go
	WriteFailure *WriteFailureConfig `json:"writeFailure,omitempty"`
```

在 `parseConfig`(填默认值处,`RetryTimes` 默认附近)新增:

```go
	if cfg.Clickhouse.RetryMaxDuration == "" {
		cfg.Clickhouse.RetryMaxDuration = "30m"
	}
```

- [ ] **Step 4: 运行测试确认通过**

Run: `go test -mod=mod ./config/ -run TestWriteFailureDefaults -v`
Expected: PASS。

- [ ] **Step 5: 整体构建**

Run: `go build -mod=mod ./...`
Expected: 无错误。

- [ ] **Step 6: 提交**

```bash
git add config/config.go config/config_test.go
git commit -m "feat(config): 错误旁路配置项(RetryMaxDuration/分类覆盖/WriteFailure)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 2: 错误分类器

**Files:**
- Create: `output/errclass.go`
- Test: `output/errclass_test.go`

**Interfaces:**
- Consumes:`github.com/ClickHouse/clickhouse-go/v2/lib/proto`(`*proto.Exception{Code int32}`)、`pool.ErrAllReplicasDown`。
- Produces:
  - `type ErrorClass int`;`const (ClassRetryable ErrorClass = iota; ClassFatal)`。
  - `func classifyError(err error, extraRetryable, fatalOverride map[int32]bool) (ErrorClass, string)` —— 返回 类别 + 观测 label。
  - `func chErrorCode(err error) (int32, bool)`。
  - `func buildCodeSet(codes []int) map[int32]bool` —— 供调用方把 `[]int` 配置转成集合。

- [ ] **Step 1: 写失败测试** —— `output/errclass_test.go`:

```go
package output

import (
	"context"
	"io"
	"testing"

	chproto "github.com/ClickHouse/clickhouse-go/v2/lib/proto"
	"github.com/housepower/clickhouse_sinker/pool"
	"github.com/thanos-io/thanos/pkg/errors"
)

func TestClassifyError(t *testing.T) {
	cases := []struct {
		name  string
		err   error
		class ErrorClass
		label string
	}{
		{"too_many_parts", &chproto.Exception{Code: 252, Message: "too many parts"}, ClassRetryable, "transient"},
		{"keeper", &chproto.Exception{Code: 999, Message: "zk"}, ClassRetryable, "transient"},
		{"type_mismatch", &chproto.Exception{Code: 53, Message: "type"}, ClassFatal, "data"},
		{"no_such_column", &chproto.Exception{Code: 16, Message: "col"}, ClassFatal, "structural"},
		{"unknown_code", &chproto.Exception{Code: 12345, Message: "?"}, ClassFatal, "unknown"},
		{"wrapped_str_fallback", errors.Wrapf(io.EOF, "code: 252, message: too many parts"), ClassRetryable, "transient"},
		{"all_replicas_down", errors.Wrapf(pool.ErrAllReplicasDown, "x"), ClassRetryable, "transient"},
		{"eof", io.EOF, ClassRetryable, "transient"},
		{"ctx_canceled", context.Canceled, ClassFatal, "unknown"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cls, lbl := classifyError(tc.err, nil, nil)
			if cls != tc.class || lbl != tc.label {
				t.Fatalf("got (%v,%q), want (%v,%q)", cls, lbl, tc.class, tc.label)
			}
		})
	}
}

func TestClassifyOverrides(t *testing.T) {
	// FatalErrorCodes 把 252 强制改判为不可重试
	cls, _ := classifyError(&chproto.Exception{Code: 252}, nil, map[int32]bool{252: true})
	if cls != ClassFatal {
		t.Fatalf("fatal override failed: got %v", cls)
	}
	// RetryableErrorCodes 把未知码 12345 改判为可重试
	cls, _ = classifyError(&chproto.Exception{Code: 12345}, map[int32]bool{12345: true}, nil)
	if cls != ClassRetryable {
		t.Fatalf("retryable override failed: got %v", cls)
	}
}
```

- [ ] **Step 2: 运行测试确认失败**

Run: `go test -mod=mod ./output/ -run TestClassify -v`
Expected: 编译失败(`classifyError` 未定义)。

- [ ] **Step 3: 实现 `output/errclass.go`**

```go
package output

import (
	"context"
	"errors"
	"io"
	"net"
	"regexp"
	"strconv"

	chproto "github.com/ClickHouse/clickhouse-go/v2/lib/proto"
	"github.com/housepower/clickhouse_sinker/pool"
)

type ErrorClass int

const (
	ClassRetryable ErrorClass = iota
	ClassFatal
)

// 内置可重试白名单(梳理过的常见瞬时错误码)。
var builtinRetryableCodes = map[int32]bool{
	202:  true, // TOO_MANY_SIMULTANEOUS_QUERIES
	225:  true, // NO_ZOOKEEPER
	242:  true, // TABLE_IS_READ_ONLY
	252:  true, // TOO_MANY_PARTS
	319:  true, // UNKNOWN_STATUS_OF_INSERT
	999:  true, // KEEPER_EXCEPTION
	1000: true, // POCO_EXCEPTION
}

// 仅用于观测 label —— 结构级(列/表/库)。
var structuralCodes = map[int32]bool{
	7: true, 8: true, 10: true, 15: true, 16: true, 47: true, 60: true, 81: true, 352: true,
}

// 仅用于观测 label —— 数据级(类型/解析/越界)。
var dataCodes = map[int32]bool{
	6: true, 26: true, 27: true, 41: true, 53: true, 69: true, 70: true, 72: true, 117: true, 131: true,
}

var codeRe = regexp.MustCompile(`code:\s*(\d+)`)

func buildCodeSet(codes []int) map[int32]bool {
	if len(codes) == 0 {
		return nil
	}
	m := make(map[int32]bool, len(codes))
	for _, c := range codes {
		m[int32(c)] = true
	}
	return m
}

// chErrorCode 从(可能被包装的)错误里提取 ClickHouse 错误码。
// 先尝试结构化 *proto.Exception,失败则正则匹配错误串里的 "code: NNN"。
func chErrorCode(err error) (int32, bool) {
	var ex *chproto.Exception
	if errors.As(err, &ex) {
		return ex.Code, true
	}
	if m := codeRe.FindStringSubmatch(err.Error()); m != nil {
		if n, e := strconv.Atoi(m[1]); e == nil {
			return int32(n), true
		}
	}
	return 0, false
}

func isConnLevel(err error) bool {
	if errors.Is(err, context.Canceled) {
		return false
	}
	if errors.Is(err, pool.ErrAllReplicasDown) ||
		errors.Is(err, io.EOF) ||
		errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var ne net.Error
	return errors.As(err, &ne)
}

func labelForCode(code int32, known bool) string {
	switch {
	case known && structuralCodes[code]:
		return "structural"
	case known && dataCodes[code]:
		return "data"
	default:
		return "unknown"
	}
}

// classifyError 判定错误可重试性,并返回观测 label。
// 判定顺序:fatal 覆盖 → 连接级 → 白名单/内置可重试 → 默认不可重试。
func classifyError(err error, extraRetryable, fatalOverride map[int32]bool) (ErrorClass, string) {
	code, known := chErrorCode(err)
	if known && fatalOverride[code] {
		return ClassFatal, labelForCode(code, known)
	}
	if isConnLevel(err) {
		return ClassRetryable, "transient"
	}
	if known && (builtinRetryableCodes[code] || extraRetryable[code]) {
		return ClassRetryable, "transient"
	}
	return ClassFatal, labelForCode(code, known)
}
```

- [ ] **Step 4: 运行测试确认通过**

Run: `go test -mod=mod ./output/ -run TestClassify -v`
Expected: PASS(两个测试全绿)。

- [ ] **Step 5: 提交**

```bash
git add output/errclass.go output/errclass_test.go
git commit -m "feat(output): 新增 ClickHouse 写入错误分类器

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 3: 可观测性指标

**Files:**
- Modify: `statistics/statistics.go`(变量块 ~32-136、`init` ~138-154、`reconnect` ~232-249)

**Interfaces:**
- Produces:`statistics.MsgsDroppedTotal`/`MsgsDeadLetteredTotal`(`{task,class}`)、`DeadLetterErrorsTotal`(`{task}`)、`TaskQuarantinedTotal`(`{task,reason}`)—— 均为 `*prometheus.CounterVec`。

- [ ] **Step 1: 写失败测试** —— `statistics/statistics_test.go`(新):

```go
package statistics

import "testing"

func TestErrorBypassMetricsRegistered(t *testing.T) {
	// 仅验证 metric 变量已构造且 label 维度正确,不 panic 即可。
	MsgsDroppedTotal.WithLabelValues("t", "data").Inc()
	MsgsDeadLetteredTotal.WithLabelValues("t", "transient").Inc()
	DeadLetterErrorsTotal.WithLabelValues("t").Inc()
	TaskQuarantinedTotal.WithLabelValues("t", "structural").Inc()
}
```

- [ ] **Step 2: 运行测试确认失败**

Run: `go test -mod=mod ./statistics/ -run TestErrorBypassMetricsRegistered -v`
Expected: 编译失败(变量未定义)。

- [ ] **Step 3: 新增 metric 变量**(在 `WriteSeriesSucceed` 定义之后,闭合 `)` 之前):

```go
	MsgsDroppedTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: prefix + "msgs_dropped_total",
			Help: "num of msgs dropped on non-retryable write failure (IGNORE / deadletter fallback)",
		},
		[]string{"task", "class"},
	)
	MsgsDeadLetteredTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: prefix + "msgs_dead_lettered_total",
			Help: "num of msgs bypassed to the dead-letter kafka topic",
		},
		[]string{"task", "class"},
	)
	DeadLetterErrorsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: prefix + "dead_letter_errors_total",
			Help: "num of failures when writing to the dead-letter topic",
		},
		[]string{"task"},
	)
	TaskQuarantinedTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: prefix + "task_quarantined_total",
			Help: "num of times a task was quarantined (THROW strategy)",
		},
		[]string{"task", "reason"},
	)
```

- [ ] **Step 4: 注册**(在 `init()` 末尾、`collectors.NewBuildInfoCollector()` 之前):

```go
	prometheus.MustRegister(MsgsDroppedTotal)
	prometheus.MustRegister(MsgsDeadLetteredTotal)
	prometheus.MustRegister(DeadLetterErrorsTotal)
	prometheus.MustRegister(TaskQuarantinedTotal)
```

- [ ] **Step 5: 加入 pusher**(在 `reconnect()` 的 `.Collector(WriteSeriesSucceed).` 之后):

```go
		Collector(MsgsDroppedTotal).
		Collector(MsgsDeadLetteredTotal).
		Collector(DeadLetterErrorsTotal).
		Collector(TaskQuarantinedTotal).
```

- [ ] **Step 6: 运行测试确认通过**

Run: `go test -mod=mod ./statistics/ -run TestErrorBypassMetricsRegistered -v`
Expected: PASS。

- [ ] **Step 7: 提交**

```bash
git add statistics/statistics.go statistics/statistics_test.go
git commit -m "feat(statistics): 错误旁路相关指标

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 4: 原始字节贯通(Batch.Msgs + sharder)

**Files:**
- Modify: `model/message.go`(`Batch` ~28-35)
- Modify: `task/sharding.go`(`Sharder` ~120-145、`PutElement` ~151-157、`Flush` ~159-195)
- Test: `task/sharding_test.go`(新)

**Interfaces:**
- Consumes:`model.MsgRow{Msg *InputMessage; Row *Row; Shard int}`。
- Produces:`model.Batch.Msgs []*InputMessage`(与 `*Rows` 1:1 对齐;可能为 nil 表示未贯通)。

- [ ] **Step 1: 加 `Batch.Msgs`** —— `model/message.go` 的 `Batch` 结构内 `Rows` 下方:

```go
	// Msgs 与 *Rows 1:1 对齐,携带原始 kafka 消息以支持死信重放;可能为 nil。
	Msgs []*InputMessage
```

- [ ] **Step 2: 写失败测试** —— `task/sharding_test.go`:

```go
package task

import (
	"testing"

	"github.com/housepower/clickhouse_sinker/model"
)

func TestSharderBufAlign(t *testing.T) {
	sh := &Sharder{shards: 1, msgBuf: []*model.Rows{}, msgMsgs: [][]*model.InputMessage{}}
	sh.reset(1) // 初始化 1 个 shard 的缓冲
	r1 := model.Row{1}
	r2 := model.Row{2}
	sh.putRaw(0, &r1, &model.InputMessage{Offset: 11})
	sh.putRaw(0, &r2, &model.InputMessage{Offset: 22})
	rows, msgs := sh.takeShard(0)
	if len(*rows) != 2 || len(msgs) != 2 {
		t.Fatalf("len rows=%d msgs=%d, want 2,2", len(*rows), len(msgs))
	}
	if msgs[0].Offset != 11 || msgs[1].Offset != 22 {
		t.Fatalf("msg alignment broken: %d,%d", msgs[0].Offset, msgs[1].Offset)
	}
}
```

> 说明:为可测,把缓冲读写抽成小helper `reset(shards int)`、`putRaw(shard int, row *model.Row, msg *model.InputMessage)`、`takeShard(i int) (*model.Rows, []*model.InputMessage)`。`PutElement`/`Flush` 改为调用它们。

- [ ] **Step 3: 运行测试确认失败**

Run: `go test -mod=mod ./task/ -run TestSharderBufAlign -v`
Expected: 编译失败(`msgMsgs`/helper 未定义)。

- [ ] **Step 4: 改 `Sharder`** —— 字段、构造、helper、PutElement、Flush:

```go
type Sharder struct {
	service *Service
	policy  *ShardingPolicy
	shards  int
	mux     sync.Mutex
	msgBuf  []*model.Rows
	msgMsgs [][]*model.InputMessage // 与 msgBuf 各 shard 1:1 对齐
}
```

`NewSharder` 内初始化(替换原 `msgBuf` 初始化块):

```go
	sh = &Sharder{
		service: service,
		policy:  policy,
		shards:  shards,
	}
	sh.reset(shards)
	return
```

新增 helper:

```go
// reset 重建所有 shard 的缓冲。调用者需持有 mux(或在构造期单线程)。
func (sh *Sharder) reset(shards int) {
	sh.msgBuf = make([]*model.Rows, shards)
	sh.msgMsgs = make([][]*model.InputMessage, shards)
	for i := 0; i < shards; i++ {
		rs := make(model.Rows, 0)
		sh.msgBuf[i] = &rs
		sh.msgMsgs[i] = make([]*model.InputMessage, 0)
	}
}

// putRaw 追加一行及其原始消息到指定 shard。调用者需持有 mux。
func (sh *Sharder) putRaw(shard int, row *model.Row, msg *model.InputMessage) {
	*sh.msgBuf[shard] = append(*sh.msgBuf[shard], row)
	sh.msgMsgs[shard] = append(sh.msgMsgs[shard], msg)
}

// takeShard 取出并清空指定 shard 的缓冲。调用者需持有 mux。
func (sh *Sharder) takeShard(i int) (*model.Rows, []*model.InputMessage) {
	rows, msgs := sh.msgBuf[i], sh.msgMsgs[i]
	rs := make(model.Rows, 0, len(*rows))
	sh.msgBuf[i] = &rs
	sh.msgMsgs[i] = make([]*model.InputMessage, 0, len(msgs))
	return rows, msgs
}
```

`PutElement` 改为:

```go
func (sh *Sharder) PutElement(msgRow *model.MsgRow) {
	sh.mux.Lock()
	defer sh.mux.Unlock()
	sh.putRaw(msgRow.Shard, msgRow.Row, msgRow.Msg)
	statistics.ShardMsgs.WithLabelValues(sh.service.taskCfg.Name).Inc()
}
```

`Flush` 内循环改为用 `takeShard` 并填充 `Msgs`:

```go
		for i := range sh.msgBuf {
			realSize := len(*sh.msgBuf[i])
			if realSize > 0 {
				msgCnt += realSize
				rows, msgs := sh.takeShard(i)
				batch := &model.Batch{
					Rows:     rows,
					Msgs:     msgs,
					BatchIdx: int64(i),
					GroupId:  batchId,
					RealSize: realSize,
					Wg:       wg,
				}
				batch.Wg.Add(1)
				sh.service.clickhouse.Send(batch, traceId)
			}
		}
```

- [ ] **Step 5: 运行测试确认通过**

Run: `go test -mod=mod ./task/ -run TestSharderBufAlign -v`
Expected: PASS。

- [ ] **Step 6: 整体构建**

Run: `go build -mod=mod ./...`
Expected: 无错误。

- [ ] **Step 7: 提交**

```bash
git add model/message.go task/sharding.go task/sharding_test.go
git commit -m "feat: 贯通原始 kafka 消息到 Batch.Msgs 供死信重放

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 5: 死信 sink

**Files:**
- Create: `output/deadletter.go`
- Test: `output/deadletter_test.go`

**Interfaces:**
- Consumes:`config.WriteFailureConfig`、`model.Batch`(`.Msgs`、`.RealSize`、`.GroupId`)、franz-go `kgo`/`kadm`。
- Produces:
  - `type dlqProducer interface { Produce(topic string, key, value []byte, headers map[string]string) error; Close() }`。
  - `type DeadLetterSink struct{...}`;`func NewDeadLetterSink(taskName, table string, cfg *config.WriteFailureConfig) (*DeadLetterSink, error)`。
  - `func (s *DeadLetterSink) SendBatch(b *model.Batch, label, errMsg string) error` —— 全部消息成功投递返回 nil,否则返回非 nil(调用方据此兜底)。
  - `func (s *DeadLetterSink) Close()`。

- [ ] **Step 1: 写失败测试** —— `output/deadletter_test.go`:

```go
package output

import (
	"errors"
	"testing"
	"time"

	"github.com/housepower/clickhouse_sinker/model"
)

type fakeProducer struct {
	sent     []map[string]string
	failNext bool
}

func (f *fakeProducer) Produce(topic string, key, value []byte, headers map[string]string) error {
	if f.failNext {
		return errors.New("boom")
	}
	f.sent = append(f.sent, headers)
	return nil
}
func (f *fakeProducer) Close() {}

func newTestSink(p dlqProducer) *DeadLetterSink {
	return &DeadLetterSink{taskName: "t1", table: "db.tb", topic: "dlq", prod: p}
}

func TestDeadLetterSendBatchPayload(t *testing.T) {
	fp := &fakeProducer{}
	s := newTestSink(fp)
	ts := time.Unix(1700000000, 0)
	b := &model.Batch{
		RealSize: 2,
		Msgs: []*model.InputMessage{
			{Topic: "in", Partition: 3, Offset: 100, Key: []byte("k0"), Value: []byte("v0"), Timestamp: &ts},
			{Topic: "in", Partition: 3, Offset: 101, Value: []byte("v1"), Timestamp: &ts},
		},
	}
	if err := s.SendBatch(b, "data", "code: 53"); err != nil {
		t.Fatalf("SendBatch err: %v", err)
	}
	if len(fp.sent) != 2 {
		t.Fatalf("produced %d, want 2", len(fp.sent))
	}
	h := fp.sent[0]
	if h["task"] != "t1" || h["table"] != "db.tb" || h["error_class"] != "data" ||
		h["topic"] != "in" || h["partition"] != "3" || h["offset"] != "100" {
		t.Fatalf("bad headers: %#v", h)
	}
}

func TestDeadLetterSendBatchFailurePropagates(t *testing.T) {
	s := newTestSink(&fakeProducer{failNext: true})
	b := &model.Batch{RealSize: 1, Msgs: []*model.InputMessage{{Value: []byte("v")}}}
	if err := s.SendBatch(b, "unknown", "x"); err == nil {
		t.Fatal("expected error so caller can fall back to drop")
	}
}
```

- [ ] **Step 2: 运行测试确认失败**

Run: `go test -mod=mod ./output/ -run TestDeadLetter -v`
Expected: 编译失败(类型未定义)。

- [ ] **Step 3: 实现 `output/deadletter.go`**

```go
package output

import (
	"context"
	"strconv"

	"github.com/housepower/clickhouse_sinker/config"
	"github.com/housepower/clickhouse_sinker/model"
	"github.com/housepower/clickhouse_sinker/util"
	"github.com/thanos-io/thanos/pkg/errors"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
)

type dlqProducer interface {
	Produce(topic string, key, value []byte, headers map[string]string) error
	Close()
}

// kgoProducer 是 dlqProducer 的 franz-go 实现(同步投递)。
type kgoProducer struct {
	cl *kgo.Client
}

func (p *kgoProducer) Produce(topic string, key, value []byte, headers map[string]string) error {
	hs := make([]kgo.RecordHeader, 0, len(headers))
	for k, v := range headers {
		hs = append(hs, kgo.RecordHeader{Key: k, Value: []byte(v)})
	}
	rec := &kgo.Record{Topic: topic, Key: key, Value: value, Headers: hs}
	return p.cl.ProduceSync(context.Background(), rec).FirstErr()
}

func (p *kgoProducer) Close() { p.cl.Close() }

type DeadLetterSink struct {
	taskName string
	table    string
	topic    string
	prod     dlqProducer
}

func NewDeadLetterSink(taskName, table string, cfg *config.WriteFailureConfig) (*DeadLetterSink, error) {
	if len(cfg.BootstrapServers) == 0 || cfg.TopicName == "" {
		return nil, errors.Newf("dead-letter requires bootstrapServers and topicName for task %s", taskName)
	}
	cl, err := kgo.NewClient(kgo.SeedBrokers(cfg.BootstrapServers...))
	if err != nil {
		return nil, errors.Wrapf(err, "create dead-letter producer for task %s", taskName)
	}
	if cfg.AutoCreateTopic {
		parts := cfg.AutoCreateTopicPartitions
		if parts <= 0 {
			parts = 1
		}
		rf := cfg.AutoCreateTopicReplicationFactor
		if rf <= 0 {
			rf = 1
		}
		adm := kadm.NewClient(cl)
		if _, err := adm.CreateTopic(context.Background(), parts, rf, nil, cfg.TopicName); err != nil {
			// 已存在等情形不致命,仅告警。
			util.Logger.Warn("dead-letter auto-create topic returned error (may already exist)",
				zap.String("task", taskName), zap.String("topic", cfg.TopicName), zap.Error(err))
		}
	}
	return &DeadLetterSink{taskName: taskName, table: table, topic: cfg.TopicName, prod: &kgoProducer{cl: cl}}, nil
}

// SendBatch 把整批原始消息逐条投递到死信 topic。任一条失败即返回错误,
// 由调用方兜底(降级为丢弃)。
func (s *DeadLetterSink) SendBatch(b *model.Batch, label, errMsg string) error {
	if len(b.Msgs) == 0 {
		// 没有贯通原始字节(理论上不应发生),无可旁路。
		return errors.Newf("dead-letter batch has no raw msgs (task %s)", s.taskName)
	}
	for _, m := range b.Msgs {
		if m == nil {
			continue
		}
		headers := map[string]string{
			"task":        s.taskName,
			"table":       s.table,
			"error_class": label,
			"error_msg":   errMsg,
			"topic":       m.Topic,
			"partition":   strconv.Itoa(m.Partition),
			"offset":      strconv.FormatInt(m.Offset, 10),
		}
		if err := s.prod.Produce(s.topic, m.Key, m.Value, headers); err != nil {
			return errors.Wrapf(err, "produce to dead-letter topic %s", s.topic)
		}
	}
	return nil
}

func (s *DeadLetterSink) Close() {
	if s.prod != nil {
		s.prod.Close()
	}
}
```

> 验证 `kadm.CreateTopic` 签名:`grep -rn "func (cl \*Client) CreateTopic" $(go env GOPATH)/pkg/mod/github.com/twmb/franz-go*/pkg/kadm/`。若签名不同(如返回 `CreateTopicResponses`),按实际调整;核心是"建一个 topic,已存在不报致命错"。

- [ ] **Step 4: 运行测试确认通过**

Run: `go test -mod=mod ./output/ -run TestDeadLetter -v`
Expected: PASS(两个测试)。

- [ ] **Step 5: 提交**

```bash
git add output/deadletter.go output/deadletter_test.go
git commit -m "feat(output): per-task 死信 sink(franz-go + 自动建 topic)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 6: Sinker task 隔离 + 回调接线

**Files:**
- Modify: `task/sinker.go`(`Sinker` 结构 ~53-94、`applyConfig` ~351-378)
- Modify: `task/task.go`(`NewTaskService` ~89-112)
- Modify: `output/clickhouse.go`(`ClickHouse` 结构 ~65 + 新增 setter)
- Test: `task/sinker_test.go`(新或追加)

**Interfaces:**
- Produces:
  - `func (s *Sinker) MarkTaskBroken(name, reason string)`。
  - `func (s *Sinker) filterBrokenTasks(newCfg *config.Config)`。
  - `func (c *ClickHouse) SetOnTaskBroken(fn func(reason string))`。
  - `ClickHouse.onTaskBroken func(reason string)`、`ClickHouse.broken atomic.Bool`。

- [ ] **Step 1: 写失败测试** —— `task/sinker_test.go`:

```go
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
```

> 注:确认 `config.Config` 里 task 列表字段名(`Tasks []*TaskConfig`)与 `dropTasksFromCfg` 的实现 —— `filterBrokenTasks` 应复用 `dropTasksFromCfg(newCfg, names)` 保持移除语义一致。

- [ ] **Step 2: 运行测试确认失败**

Run: `go test -mod=mod ./task/ -run TestFilterBrokenTasks -v`
Expected: 编译失败(`MarkTaskBroken`/`filterBrokenTasks` 未定义)。

- [ ] **Step 3: Sinker 加字段与方法** —— `task/sinker.go`:

`Sinker` 结构内新增:

```go
	brokenTasks sync.Map // taskName(string) -> reason(string)
```

新增方法:

```go
// MarkTaskBroken 记录某 task 因不可重试错误需被隔离。下一次 applyConfig
// 的 filterBrokenTasks 会把它从生效配置中剔除,保住兄弟 task。
func (s *Sinker) MarkTaskBroken(name, reason string) {
	if _, loaded := s.brokenTasks.LoadOrStore(name, reason); !loaded {
		util.Logger.Warn("task marked broken, will be quarantined on next reload",
			zap.String("task", name), zap.String("reason", reason))
	}
}

// filterBrokenTasks 从 newCfg 中移除所有已标记 broken 的 task。
func (s *Sinker) filterBrokenTasks(newCfg *config.Config) {
	dropped := make(map[string]bool)
	s.brokenTasks.Range(func(k, _ any) bool {
		dropped[k.(string)] = true
		return true
	})
	if len(dropped) == 0 {
		return
	}
	dropTasksFromCfg(newCfg, dropped)
}
```

在 `applyConfig` 内 `s.filterMissingTables(newCfg)` 之后加一行:

```go
	s.filterBrokenTasks(newCfg)
```

- [ ] **Step 4: ClickHouse 加回调字段与 setter** —— `output/clickhouse.go`:

`ClickHouse` 结构内新增:

```go
	onTaskBroken func(reason string)
	broken       atomic.Bool
```

(确保文件已 `import "sync/atomic"`。)新增方法:

```go
func (c *ClickHouse) SetOnTaskBroken(fn func(reason string)) {
	c.onTaskBroken = fn
}
```

- [ ] **Step 5: 接线回调** —— `task/task.go` `NewTaskService`,在 `ck := output.NewClickHouse(...)` 之后:

```go
	taskName := taskCfg.Name
	sinker := c.sinker
	ck.SetOnTaskBroken(func(reason string) {
		sinker.MarkTaskBroken(taskName, reason)
	})
```

> 注:确认 `Consumer` 有可达 Sinker 的字段(`c.sinker`,见 consumer.go 中 `c.sinker.commitsCh`)。

- [ ] **Step 6: 运行测试确认通过 + 构建**

Run: `go test -mod=mod ./task/ -run TestFilterBrokenTasks -v && go build -mod=mod ./...`
Expected: PASS 且构建无错误。

- [ ] **Step 7: 提交**

```bash
git add task/sinker.go task/task.go output/clickhouse.go task/sinker_test.go
git commit -m "feat: THROW 策略经回调隔离 task,复用 reload 移除语义

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 7: 重写 loopWrite(分级执行器 + dispatchFailure)

**Files:**
- Modify: `output/clickhouse.go`(`ClickHouse` 结构、`Init` ~122、`Send` ~139、`loopWrite` ~264-335)
- Test: `output/clickhouse_test.go`(新)

**Interfaces:**
- Consumes:`classifyError`、`buildCodeSet`、`DeadLetterSink`、`config.WriteFailure*` 常量、`ClickHouse.onTaskBroken`、`statistics.*`。
- Produces:
  - `ClickHouse` 字段:`retryMaxDur time.Duration`、`retryableCodes/fatalCodes map[int32]bool`、`deadLetter *DeadLetterSink`、`limiter *rate.Limiter`。
  - `func (c *ClickHouse) dispatchFailure(batch *model.Batch, label string, err error)`。
  - `func sleepWithCtx(ctx context.Context, d time.Duration) bool` —— 返回 false 表示被 ctx 取消。

- [ ] **Step 1: 写失败测试** —— `output/clickhouse_test.go`:

```go
package output

import (
	"context"
	"testing"
	"time"
)

func TestSleepWithCtxCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if sleepWithCtx(ctx, time.Hour) {
		t.Fatal("sleepWithCtx should return false when ctx already canceled")
	}
}

func TestSleepWithCtxElapses(t *testing.T) {
	if !sleepWithCtx(context.Background(), 10*time.Millisecond) {
		t.Fatal("sleepWithCtx should return true when timer elapses")
	}
}
```

> 完整的 `loopWrite` 行为(瞬时重试到上限、各策略分派)依赖 CK/kafka,不做纯单测;此处仅锁定最易回归的 `sleepWithCtx`(teardown 卡死修复点),`loopWrite` 主体靠构建 + 后续手测验证。`dispatchFailure` 的策略分支逻辑通过 Task 5 的死信测试 + Task 6 的隔离测试间接覆盖。

- [ ] **Step 2: 运行测试确认失败**

Run: `go test -mod=mod ./output/ -run TestSleepWithCtx -v`
Expected: 编译失败(`sleepWithCtx` 未定义)。

- [ ] **Step 3: ClickHouse 加字段 + Init 解析** —— `output/clickhouse.go`:

结构内新增:

```go
	retryMaxDur   time.Duration
	retryableCodes map[int32]bool
	fatalCodes    map[int32]bool
	deadLetter    *DeadLetterSink
	limiter       *rate.Limiter
```

(import `"golang.org/x/time/rate"`、`"time"`、`"context"`。)在 `Init()` 内(连接初始化完成后)加:

```go
	c.retryableCodes = buildCodeSet(c.cfg.Clickhouse.RetryableErrorCodes)
	c.fatalCodes = buildCodeSet(c.cfg.Clickhouse.FatalErrorCodes)
	c.limiter = rate.NewLimiter(rate.Every(10*time.Second), 1)
	if d, e := time.ParseDuration(c.cfg.Clickhouse.RetryMaxDuration); e == nil && d > 0 {
		c.retryMaxDur = d
	} else {
		c.retryMaxDur = 30 * time.Minute
	}
	if c.taskCfg.WriteFailure != nil && c.taskCfg.WriteFailure.Strategy == config.WriteFailureWriteToKafka {
		if c.deadLetter, err = NewDeadLetterSink(c.taskCfg.Name, c.dbName+"."+c.TableName, c.taskCfg.WriteFailure); err != nil {
			return err
		}
	}
```

> 注:确认 `Init` 的返回变量名为 `err`(见 `func (c *ClickHouse) Init() (err error)`),且 `c.dbName`/`c.TableName` 在 Init 该位置已就绪;若死信表名拼接时机不对,改用 `c.taskCfg.TableName`。

- [ ] **Step 4: 新增 `sleepWithCtx` 与 `dispatchFailure`**

```go
// sleepWithCtx 睡 d 或在 ctx 取消时提前返回。返回 false 表示被取消。
func sleepWithCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}

func (c *ClickHouse) writeFailureStrategy() string {
	if c.taskCfg.WriteFailure != nil && c.taskCfg.WriteFailure.Strategy != "" {
		return c.taskCfg.WriteFailure.Strategy
	}
	return config.WriteFailureIgnore
}

// dispatchFailure 按 per-task 策略处置一批最终写失败的数据。
func (c *ClickHouse) dispatchFailure(batch *model.Batch, label string, err error) {
	name := c.taskCfg.Name
	n := float64(batch.RealSize)
	switch c.writeFailureStrategy() {
	case config.WriteFailureThrow:
		c.broken.Store(true)
		if c.onTaskBroken != nil {
			c.onTaskBroken(label)
		}
		statistics.TaskQuarantinedTotal.WithLabelValues(name, label).Inc()
		statistics.MsgsDroppedTotal.WithLabelValues(name, label).Add(n)
		if c.limiter.Allow() {
			util.Logger.Warn("THROW: quarantining task on non-retryable write failure",
				zap.String("task", name), zap.String("class", label), zap.Error(err))
		}
	case config.WriteFailureWriteToKafka:
		if c.deadLetter != nil {
			if e := c.deadLetter.SendBatch(batch, label, err.Error()); e == nil {
				statistics.MsgsDeadLetteredTotal.WithLabelValues(name, label).Add(n)
				return
			} else if c.limiter.Allow() {
				util.Logger.Warn("dead-letter write failed, falling back to drop",
					zap.String("task", name), zap.Error(e))
			}
			statistics.DeadLetterErrorsTotal.WithLabelValues(name).Inc()
		}
		statistics.MsgsDroppedTotal.WithLabelValues(name, label).Add(n)
	default: // IGNORE
		statistics.MsgsDroppedTotal.WithLabelValues(name, label).Add(n)
		if c.limiter.Allow() {
			util.Logger.Warn("IGNORE: dropping batch on non-retryable write failure",
				zap.String("task", name), zap.String("class", label), zap.Error(err))
		}
	}
}
```

- [ ] **Step 5: 重写 `loopWrite`** —— 替换整个函数体(保留签名、`defer util.Rs.Dec` 与 reroute 逻辑):

```go
func (c *ClickHouse) loopWrite(batch *model.Batch, sc *pool.ShardConn, traceId string) {
	var dbVer int
	util.LogTrace(traceId, util.TraceKindWriteStart, zap.Int("realsize", batch.RealSize))
	defer func() {
		util.Rs.Dec(int64(batch.RealSize))
		util.LogTrace(traceId, util.TraceKindWriteEnd, zap.Int("success", batch.RealSize))
	}()

	canReroute := c.cfg.Clickhouse.SkipUnavailableShards &&
		c.taskCfg.ShardingKey == "" &&
		len(c.SortingKeys) == 0
	currentSc := sc
	ctx := c.cfg.Clickhouse.Ctx

	var firstFail time.Time
	backoff := 10 * time.Second
	attempts := 0
	maxAttempts := c.cfg.Clickhouse.RetryTimes // <=0 表示不按次数限制

	for {
		err := c.write(batch, currentSc, &dbVer)
		if err == nil {
			return
		}
		class, label := classifyError(err, c.retryableCodes, c.fatalCodes)

		if class == ClassFatal {
			c.dispatchFailure(batch, label, err)
			return
		}

		// ClassRetryable
		attempts++
		statistics.FlushMsgsErrorTotal.WithLabelValues(c.taskCfg.Name).Add(float64(batch.RealSize))
		util.Logger.Error("flush batch failed (retryable)",
			zap.String("task", c.taskCfg.Name), zap.String("group", batch.GroupId),
			zap.Int("try", attempts), zap.Error(err))

		// reroute:整分片不可用且可重路由时换健康分片
		if canReroute && errors.Is(err, pool.ErrAllReplicasDown) {
			currentSc.MarkUnhealthy()
			if next := pool.PickHealthyShardSkipping(batch.BatchIdx, currentSc); next != nil {
				currentSc = next
				dbVer = 0
			}
		}

		if firstFail.IsZero() {
			firstFail = time.Now()
		}
		exceeded := time.Since(firstFail) > c.retryMaxDur
		if exceeded || (maxAttempts > 0 && attempts >= maxAttempts) {
			util.Logger.Error("retryable error exceeded ceiling, dispatching as final failure",
				zap.String("task", c.taskCfg.Name), zap.String("group", batch.GroupId),
				zap.Duration("elapsed", time.Since(firstFail)), zap.Int("attempts", attempts))
			c.dispatchFailure(batch, "transient_exhausted", err)
			return
		}

		if !sleepWithCtx(ctx, backoff) {
			// ctx 取消(teardown):放弃重试,不卡死。
			return
		}
		if backoff < time.Minute {
			backoff *= 2
			if backoff > time.Minute {
				backoff = time.Minute
			}
		}
	}
}
```

> 注:确认 `time.Now()`/`time.Since` 可用(已 import `time`);确认 `pool.PickHealthyShardSkipping`、`MarkUnhealthy`、`util.TraceKind*` 名称与现有代码一致(从原 `loopWrite` 拷贝)。原函数里的 `retry.Do` 与 `retrycount` 整体删除;若 `github.com/avast/retry-go/v4` 不再被使用,移除其 import。

- [ ] **Step 6: `Send` 短路 broken task** —— `output/clickhouse.go` `Send` 开头(`sc := pool.GetShardConn(...)` 之前):

```go
	if c.broken.Load() {
		c.dispatchFailure(batch, "quarantined", errBrokenTask)
		batch.Wg.Done()
		util.Rs.Dec(int64(batch.RealSize))
		return
	}
```

在文件顶部 var 区加哨兵错误:

```go
var errBrokenTask = errors.Newf("task quarantined, short-circuiting writes")
```

> 注:`dispatchFailure` 在 broken 短路路径里对 THROW 策略会再次 `onTaskBroken`(幂等,`LoadOrStore` 已防重)。对 IGNORE/WRITE_TO_KAFKA 仍按策略处置短路批次。

- [ ] **Step 7: 运行测试 + 构建**

Run: `go test -mod=mod ./output/ -run TestSleepWithCtx -v && go build -mod=mod ./...`
Expected: PASS 且构建无错误。若报 `retry` import 未使用,删除该 import。

- [ ] **Step 8: 跑全量单测回归**

Run: `go test -mod=mod ./...`
Expected: 全绿(预存在的无关失败除外,需记录说明)。

- [ ] **Step 9: 提交**

```bash
git add output/clickhouse.go output/clickhouse_test.go
git commit -m "feat(output): 分类驱动的 loopWrite + 分级失败处置

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 8: 行级坏行精确死信(可选增强)

**Files:**
- Modify: `pool/ck_cli.go`(`write_v1_isolated` ~126、`write_v2` ~190、`Write` ~238)
- Modify: `output/clickhouse_util.go`(`writeRows` ~13)、`output/clickhouse.go`(`write` ~252 调用处)
- Test: `pool/ck_cli_test.go`(若可)

**Interfaces:**
- 现状:`Write`/`writeRows` 只回 `numBad int`(坏行数),坏行原始消息丢失。
- 目标:让坏行的 bitmap 下标可回传到 `output` 层,据 `batch.Msgs[idx]` 旁路坏行原始消息。

**说明:** 这是增强项 —— 行级坏行目前已被自动跳过(计入 `ParseMsgsErrorTotal`),不影响存活性。死信对这类坏行是"锦上添花"。鉴于 `Write` 跨 `pool`→`output` 包、签名改动面较大,**建议先交付 Task 1-7(批级死信已覆盖主诉求),Task 8 视需要再做**。

- [ ] **Step 1:** 把 `write_v1_isolated`/`write_v2` 的 `bmBad` 通过新增返回值(如 `badIdx []int`)上抛;`Write`/`writeRows` 透传。
- [ ] **Step 2:** `output/clickhouse.go` `write` 拿到 `badIdx` 后,若 `c.deadLetter != nil`,对 `batch.Msgs[idx]` 调 `c.deadLetter` 旁路(复用一个 `SendMsgs([]*model.InputMessage, label, errMsg)` 辅助方法 —— 从 `SendBatch` 抽出公共逻辑)。
- [ ] **Step 3:** 单测下标对齐 + 提交。

> 落地前重新评估:若 Task 1-7 已满足生产需求,本任务可暂缓。

---

## Self-Review

**Spec coverage（逐节核对）:**
- §3 分类模型(白名单才重试/其余 fatal/连接级)→ Task 2 ✅
- §3 writeFailureStrategy(THROW/IGNORE/WRITE_TO_KAFKA + 默认 IGNORE)→ Task 1(配置)+ Task 7(dispatchFailure)+ Task 6(THROW 隔离)✅
- §3 RetryMaxDuration 上限 + 先到者 → Task 1 + Task 7(`exceeded || maxAttempts`)✅
- §4.1 分类器纯函数 → Task 2 ✅
- §4.2 分级执行器 + sleepWithCtx(teardown 修复)+ reroute 复用 → Task 7 ✅
- §4.3 死信 sink + 自动建 topic + 兜底降级 → Task 5 + Task 7(fallback)✅
- §4.4 原始字节贯通(Batch.Msgs/sharder)→ Task 4 ✅
- §4.5 task 隔离(brokenTasks/MarkTaskBroken/filterBrokenTasks/短路)→ Task 6 + Task 7 Step 6 ✅
- §4.6 配置(ClickHouse + per-task WriteFailure 块)→ Task 1 ✅
- §4.7 四个指标 + 注册 + pusher → Task 3 ✅
- §5 测试计划 → 各 Task 内置单测;`loopWrite` 主体手测(已说明限制)✅
- §4.4 行级 bitmap 旁路 → Task 8(标记为可选增强,已说明取舍)✅

**Placeholder scan:** 无 TBD/TODO;每个 code step 均含完整代码。少数"确认签名/字段名"注记是针对仓库既有标识符的核对提示,非占位符。

**Type consistency:** `classifyError(err, extraRetryable, fatalOverride map[int32]bool)`、`buildCodeSet([]int) map[int32]bool`、`DeadLetterSink.SendBatch(*model.Batch, label, errMsg string) error`、`Batch.Msgs []*InputMessage`、`writeFailureStrategy() string` 常量 `WriteFailure{Throw,Ignore,WriteToKafka}` 跨 Task 一致。

**已知风险(执行时注意):**
1. thanos `errors.Wrapf` 的 unwrap 兼容性:`errors.As(*proto.Exception)` 若穿不透包装,正则 `code: NNN` 兜底(测试 `wrapped_str_fallback` 覆盖)。
2. `kadm.CreateTopic` 签名需按实际版本核对(Task 5 已注明)。
3. `c.cfg.Clickhouse.Ctx` 作为 `loopWrite` 的取消源 —— 确认它在 teardown 时会被 cancel;若不会,需改用 consumer 的 ctx 传入(执行时验证 `sleepWithCtx` 确实能在 stop 时返回)。
