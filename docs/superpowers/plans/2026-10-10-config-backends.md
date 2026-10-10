# 配置中心后端抽象(etcd / Apollo)实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** sinker 能从 nacos / etcd / Apollo 任一配置中心读取并热更新配置,三者都支持 discovery 写回且不冲掉他人修改。

**Architecture:** 后端只实现字节级 `Source`(读)和可选的 `Writer`(`Update(fn)` 读→改→写);
`config_manager.Manager` 统一做 hjson 编解码,sinker 与 discovery 只依赖 `*Manager`。etcd 与 nacos 共用
`updateWithCAS` 做乐观锁重试,Apollo 走 Portal Open API 且发布前检查未发布修改。后端由 `rcm.New(cmdOps)` 按参数选择。

**Tech Stack:** Go 1.25;`github.com/nacos-group/nacos-sdk-go/v2 v2.2.5`;`go.etcd.io/etcd/client/v3 v3.6.7`;
`github.com/apolloconfig/agollo/v4 v4.4.0`;hjson-go v4;标准库 `net/http` / `httptest`。

**Spec:** `docs/superpowers/specs/2026-10-10-config-backends-design.md`

## Global Constraints

- 分支:`refactor/remove-nacos-assign`(当前 HEAD `9bb71b7`)。不推送。
- 每个 shell 先执行:`export PATH=/usr/local/go/bin:$PATH GOFLAGS=-mod=mod GOPROXY=https://goproxy.cn,direct`。
  `vendor/` 被 `.gitignore` 忽略且已过期,不带 `-mod=mod` 会报 inconsistent vendoring。
- 依赖版本固定:nacos-sdk-go/v2 `v2.2.5`、etcd client/v3 `v3.6.7`、agollo/v4 `v4.4.0`。加依赖会把 grpc 升到
  `v1.71.1`、prometheus client_golang 升到 `v1.20.5`,已验证现有代码可编译、全部单测通过。
- 环境变量统一 `SINKER_` 前缀:新参数**只认**前缀名;现有参数先查 `SINKER_<NAME>` 再回退 `<NAME>`。
- 配置中心单次操作超时 `opTimeout = 5 * time.Second`;CAS 冲突重试 `casMaxRetries = 3`(即最多 4 次写尝试)。
- 所有后端错误都带后端名与 key / dataId / namespace。
- 本地文件模式(`--local-cfg-file`)行为不变。
- 测试里不得写字面量绝对路径去做删除/覆盖;集成测试只操作自己生成的唯一 key,结束时删除。
- 代码注释用英文,风格与周边一致。提交信息用英文 conventional commits,结尾加
  `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`。

## Review Focus

1. **启动时配置中心不可达超过 60 秒(nacos/etcd)** → sinker 不应 panic。现状 `Run()` 的 discovery ticker 会对
   nil 的 `s.curCfg` 解引用。Task 3 把这段抽成 `runDiscovery` 并加 `TestRunDiscoveryBeforeFirstConfig`。
2. **运行中 key / dataId 被删或被清空** → 应继续用旧配置并报错,而不是当成空配置应用。
   Task 2 的 `TestManagerGetConfigEmpty` 固定 `DecodeConfig` 对空内容报错。
3. **多个 sinker 实例同时发现同一次 shard 变化** → 只有第一个写入,其余无操作(否则 Apollo 会被连续发布多次)。
   Task 3 的 `TestApplyShardsSkipsWriteWhenLatestAlreadyMatches`。
4. **Apollo 的 env / cluster / namespace 含需转义字符** → Open API 路径必须正确转义。
   Task 6 的 `TestApolloOpenAPIEscapesPath`。
5. **新旧环境变量并存** → `SINKER_X` 优先、只设旧名 `X` 仍生效、新参数只设无前缀名时不生效。Task 1 的三条测试。

---

### Task 1: 环境变量 `SINKER_` 前缀

**Files:**
- Modify: `util/common.go:139-163`(`EnvStringVar` / `EnvIntVar` / `EnvBoolVar`)
- Test: `util/env_test.go`(新)

**Interfaces:**
- Consumes: 无
- Produces: `util.EnvPrefix = "SINKER_"`;`util.EnvStringVar(value *string, key string)`、`EnvIntVar`、`EnvBoolVar`
  (行为改为先查前缀再回退);`util.EnvStringVarPrefixed(value *string, key string)`(只查前缀)。

- [ ] **Step 1: 写失败测试** `util/env_test.go`

```go
package util

import "testing"

func TestEnvStringVarPrefersPrefixed(t *testing.T) {
	t.Setenv("NACOS_ADDR", "legacy:8848")
	t.Setenv("SINKER_NACOS_ADDR", "prefixed:8848")
	v := "default"
	EnvStringVar(&v, "nacos-addr")
	if v != "prefixed:8848" {
		t.Fatalf("got %q, want prefixed:8848", v)
	}
}

func TestEnvStringVarFallsBackToLegacy(t *testing.T) {
	t.Setenv("NACOS_ADDR", "legacy:8848")
	v := "default"
	EnvStringVar(&v, "nacos-addr")
	if v != "legacy:8848" {
		t.Fatalf("got %q, want legacy:8848", v)
	}
}

func TestEnvIntAndBoolVarPrefixed(t *testing.T) {
	t.Setenv("SINKER_HTTP_PORT", "3000")
	t.Setenv("SINKER_V", "")
	port, ver := 0, false
	EnvIntVar(&port, "http-port")
	EnvBoolVar(&ver, "v")
	if port != 3000 || !ver {
		t.Fatalf("got port=%d ver=%v, want 3000 true", port, ver)
	}
}

func TestEnvStringVarPrefixedIgnoresUnprefixed(t *testing.T) {
	t.Setenv("ETCD_KEY", "/owned/by/etcd")
	v := "default"
	EnvStringVarPrefixed(&v, "etcd-key")
	if v != "default" {
		t.Fatalf("got %q, unprefixed ETCD_KEY must be ignored", v)
	}
	t.Setenv("SINKER_ETCD_KEY", "/sinker")
	EnvStringVarPrefixed(&v, "etcd-key")
	if v != "/sinker" {
		t.Fatalf("got %q, want /sinker", v)
	}
}
```

- [ ] **Step 2: 运行确认失败**

Run: `go test ./util/ -run 'TestEnv' -count=1`
Expected: 编译失败 `undefined: EnvStringVarPrefixed`

- [ ] **Step 3: 实现** —— 用下面整段替换 `util/common.go` 中现有的 `EnvStringVar`、`EnvIntVar`、`EnvBoolVar` 三个函数

```go
// EnvPrefix namespaces sinker's environment variables so that names owned by
// other software (APOLLO_META, ETCD_*) are never picked up by accident.
const EnvPrefix = "SINKER_"

func envKey(key string) string {
	return strings.ReplaceAll(strings.ToUpper(key), "-", "_")
}

// lookupEnv prefers SINKER_<KEY> and falls back to the legacy unprefixed <KEY>.
func lookupEnv(key string) (string, bool) {
	k := envKey(key)
	if v, ok := os.LookupEnv(EnvPrefix + k); ok {
		return v, true
	}
	return os.LookupEnv(k)
}

func EnvStringVar(value *string, key string) {
	if v, ok := lookupEnv(key); ok {
		*value = v
	}
}

func EnvIntVar(value *int, key string) {
	if v, ok := lookupEnv(key); ok {
		if i, err := strconv.Atoi(v); err == nil {
			*value = i
		}
	}
}

func EnvBoolVar(value *bool, key string) {
	if _, ok := lookupEnv(key); ok {
		*value = true
	}
}

// EnvStringVarPrefixed only honors SINKER_<KEY>. Options added after the prefix
// was introduced use it, so there is no unprefixed name to collide with.
func EnvStringVarPrefixed(value *string, key string) {
	if v, ok := os.LookupEnv(EnvPrefix + envKey(key)); ok {
		*value = v
	}
}
```

- [ ] **Step 4: 运行确认通过**

Run: `go test ./util/ -count=1`
Expected: `ok`

- [ ] **Step 5: 提交**

```bash
git add util/common.go util/env_test.go
git commit -m "feat(util): support SINKER_-prefixed environment variables

Existing options read SINKER_<NAME> first and fall back to <NAME>.
EnvStringVarPrefixed reads only the prefixed name, for new options.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 2: Manager 与 Source/Writer 接口

**Files:**
- Create: `config_manager/manager.go`
- Test: `config_manager/manager_test.go`(新,含包级 `TestMain`)

**Interfaces:**
- Consumes: `config.Config`、`lags.go` 中的 `cleanupKafkaClient()`
- Produces(后续任务按此名称使用):
  - `type Source interface { Fetch(ctx context.Context) ([]byte, error); Close() error }`
  - `type UpdateFunc func(old []byte) ([]byte, error)`
  - `type Writer interface { Update(ctx context.Context, fn UpdateFunc) error }`
  - `var ErrNoChange, ErrPendingChanges, ErrConflict error`
  - `const opTimeout = 5 * time.Second`、`const casMaxRetries = 3`
  - `func NewManager(name string, src Source, w Writer) *Manager`
  - `func (m *Manager) Name() string`、`GetConfig() (*config.Config, error)`、`Writable() bool`(nil 安全)、
    `UpdateConfig(mutate func(*config.Config) bool) error`、`Close() error`
  - `func DecodeConfig(bs []byte) (*config.Config, error)`、`func EncodeConfig(cfg *config.Config) ([]byte, error)`
  - `func updateWithCAS(ctx context.Context, fn UpdateFunc, read func(context.Context) ([]byte, string, error), write func(context.Context, []byte, string) (bool, error)) error`
    (write 返回 `conflict=true` 表示版本已变)

本任务只新增文件,不删除旧的 `rcm.go`(Task 3 删除),所以全仓仍可编译。

- [ ] **Step 1: 写失败测试** `config_manager/manager_test.go`

```go
package rcm

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/housepower/clickhouse_sinker/config"
	"github.com/housepower/clickhouse_sinker/util"
)

func TestMain(m *testing.M) {
	util.InitLogger([]string{"stdout"})
	os.Exit(m.Run())
}

const twoTasks = `{
	clickhouse: {hosts: [["ch1"]], db: "default"}
	kafka: {brokers: "k:9092"}
	tasks: [{name: "t1"}, {name: "t2"}]
}`

// memBackend is an in-memory Source + Writer.
type memBackend struct {
	content  []byte
	fetchErr error
	pending  bool
	writes   int
}

func (b *memBackend) Fetch(context.Context) ([]byte, error) { return b.content, b.fetchErr }
func (b *memBackend) Close() error                          { return nil }
func (b *memBackend) Update(_ context.Context, fn UpdateFunc) error {
	if b.pending {
		return ErrPendingChanges
	}
	neu, err := fn(b.content)
	if err != nil {
		return err
	}
	b.content = neu
	b.writes++
	return nil
}

func TestManagerGetConfig(t *testing.T) {
	m := NewManager("mem", &memBackend{content: []byte(twoTasks)}, nil)
	cfg, err := m.GetConfig()
	if err != nil {
		t.Fatalf("GetConfig: %v", err)
	}
	if cfg.Kafka.Brokers != "k:9092" || len(cfg.Tasks) != 2 || cfg.Groups == nil {
		t.Fatalf("unexpected config: %+v", cfg)
	}
}

func TestManagerGetConfigEmpty(t *testing.T) {
	m := NewManager("mem", &memBackend{content: []byte(" \n ")}, nil)
	_, err := m.GetConfig()
	if err == nil || !strings.Contains(err.Error(), "mem") || !strings.Contains(err.Error(), "empty") {
		t.Fatalf("want empty-config error naming the backend, got %v", err)
	}
}

func TestManagerGetConfigInvalid(t *testing.T) {
	m := NewManager("mem", &memBackend{content: []byte("{")}, nil)
	if _, err := m.GetConfig(); err == nil || !strings.Contains(err.Error(), "mem") {
		t.Fatalf("want parse error naming the backend, got %v", err)
	}
}

func TestManagerGetConfigFetchError(t *testing.T) {
	m := NewManager("mem", &memBackend{fetchErr: errors.New("boom")}, nil)
	if _, err := m.GetConfig(); err == nil || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("want fetch error, got %v", err)
	}
}

func TestManagerUpdateConfigAppliesOnLatest(t *testing.T) {
	b := &memBackend{content: []byte(twoTasks)}
	m := NewManager("mem", b, b)
	err := m.UpdateConfig(func(c *config.Config) bool {
		c.Clickhouse.Hosts = [][]string{{"ch1"}, {"ch2"}}
		return true
	})
	if err != nil {
		t.Fatalf("UpdateConfig: %v", err)
	}
	got, err := DecodeConfig(b.content)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Clickhouse.Hosts) != 2 || len(got.Tasks) != 2 {
		t.Fatalf("hosts=%v tasks=%d, want 2 shards and both tasks kept", got.Clickhouse.Hosts, len(got.Tasks))
	}
}

func TestManagerUpdateConfigNoChange(t *testing.T) {
	b := &memBackend{content: []byte(twoTasks)}
	m := NewManager("mem", b, b)
	if err := m.UpdateConfig(func(*config.Config) bool { return false }); err != nil {
		t.Fatalf("UpdateConfig: %v", err)
	}
	if b.writes != 0 {
		t.Fatalf("writes=%d, want 0", b.writes)
	}
}

func TestManagerUpdateConfigPending(t *testing.T) {
	b := &memBackend{content: []byte(twoTasks), pending: true}
	m := NewManager("mem", b, b)
	err := m.UpdateConfig(func(*config.Config) bool { return true })
	if !errors.Is(err, ErrPendingChanges) {
		t.Fatalf("want ErrPendingChanges, got %v", err)
	}
}

func TestManagerReadOnly(t *testing.T) {
	m := NewManager("mem", &memBackend{content: []byte(twoTasks)}, nil)
	if m.Writable() {
		t.Fatal("manager without writer must not be writable")
	}
	if err := m.UpdateConfig(func(*config.Config) bool { return true }); err == nil {
		t.Fatal("UpdateConfig on read-only backend must fail")
	}
	var nilManager *Manager
	if nilManager.Writable() {
		t.Fatal("nil manager must not be writable")
	}
}

func TestUpdateWithCASGivesUpAfterRetries(t *testing.T) {
	reads, calls := 0, 0
	var seen []string
	read := func(context.Context) ([]byte, string, error) {
		reads++
		return []byte(fmt.Sprintf("c%d", reads)), fmt.Sprint(reads), nil
	}
	write := func(context.Context, []byte, string) (bool, error) { return true, nil }
	fn := func(old []byte) ([]byte, error) {
		calls++
		seen = append(seen, string(old))
		return append(old, '!'), nil
	}
	err := updateWithCAS(context.Background(), fn, read, write)
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("want ErrConflict, got %v", err)
	}
	if calls != casMaxRetries+1 {
		t.Fatalf("fn called %d times, want %d", calls, casMaxRetries+1)
	}
	if strings.Join(seen, ",") != "c1,c2,c3,c4" {
		t.Fatalf("each retry must re-read, saw %v", seen)
	}
}

func TestUpdateWithCASSucceedsAfterConflict(t *testing.T) {
	writes := 0
	read := func(context.Context) ([]byte, string, error) { return []byte("old"), "v", nil }
	write := func(context.Context, []byte, string) (bool, error) {
		writes++
		return writes == 1, nil
	}
	err := updateWithCAS(context.Background(), func([]byte) ([]byte, error) { return []byte("new"), nil }, read, write)
	if err != nil || writes != 2 {
		t.Fatalf("err=%v writes=%d, want nil and 2", err, writes)
	}
}

func TestUpdateWithCASSkipsIdenticalContent(t *testing.T) {
	read := func(context.Context) ([]byte, string, error) { return []byte("same"), "v", nil }
	write := func(context.Context, []byte, string) (bool, error) {
		t.Fatal("write must not be called when content is unchanged")
		return false, nil
	}
	if err := updateWithCAS(context.Background(), func(old []byte) ([]byte, error) { return old, nil }, read, write); err != nil {
		t.Fatal(err)
	}
}
```

- [ ] **Step 2: 运行确认失败**

Run: `go test ./config_manager/ -count=1`
Expected: 编译失败 `undefined: NewManager`

- [ ] **Step 3: 实现** `config_manager/manager.go`

```go
package rcm

import (
	"bytes"
	"context"
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

func (m *Manager) Close() error {
	// The lag admin client in lags.go used to be closed by NacosConfManager.Stop.
	cleanupKafkaClient()
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

func EncodeConfig(cfg *config.Config) ([]byte, error) {
	bs, err := hjson.Marshal(*cfg)
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
```

- [ ] **Step 4: 运行确认通过**

Run: `go test ./config_manager/ -count=1 && go build ./...`
Expected: `ok`,build 无输出

- [ ] **Step 5: 提交**

```bash
git add config_manager/manager.go config_manager/manager_test.go
git commit -m "feat(config_manager): add Manager over byte-level Source and Writer

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 3: nacos 切到 v2 SDK,全链路改用 Manager

这是唯一一个跨包的任务:删除 `RemoteConfManager` 后,main / sinker / discovery / 发布工具必须同时改,否则编不过。

**Files:**
- Modify: `go.mod`、`go.sum`(nacos v1 → v2)
- Rewrite: `config_manager/nacos.go`
- Delete: `config_manager/rcm.go`
- Create: `config_manager/select.go`、`config_manager/select_test.go`
- Modify: `cmd/clickhouse_sinker/main.go:244-275`、`task/sinker.go`、`discovery/discovery.go`、`cmd/nacos_publish_config/main.go`
- Modify: `docker-compose.yml`(nacos 1.4.1 → v2.2.3)
- Test: `discovery/discovery_test.go`(追加)、`task/sinker_discovery_test.go`(新)

**Interfaces:**
- Consumes: Task 2 的全部导出名、`updateWithCAS`、`opTimeout`
- Produces:
  - `type NacosOptions struct { Addr, Username, Password, NamespaceID, Group, DataID, CacheDir string }`
  - `func NewNacos(o NacosOptions) (*Nacos, error)`;`*Nacos` 实现 `Source` 与 `Writer`
  - `func New(cmdOps *util.CmdOptions) (*Manager, error)`(无后端时返回 `nil, nil`)
  - `func selectBackend(cmdOps *util.CmdOptions) (string, error)`,返回 `""` / `"nacos"` / `"etcd"` / `"apollo"`
  - `func NewDiscovery(cfg *config.Config, m *rcm.Manager) *Discovery`;`(d *Discovery) applyShards(shards Shards) error`
  - `func NewSinker(rcm *cm.Manager, http string, cmd *util.CmdOptions) *Sinker`;`(s *Sinker) runDiscovery(sd *discovery.Discovery)`

- [ ] **Step 1: 升级依赖**

```bash
go get github.com/nacos-group/nacos-sdk-go/v2@v2.2.5
```

- [ ] **Step 2: 写 select 的失败测试** `config_manager/select_test.go`

```go
package rcm

import (
	"strings"
	"testing"

	"github.com/housepower/clickhouse_sinker/util"
)

func TestSelectBackend(t *testing.T) {
	cases := []struct {
		name    string
		ops     util.CmdOptions
		want    string
		wantErr string
	}{
		{name: "local file", ops: util.CmdOptions{}, want: ""},
		{name: "nacos", ops: util.CmdOptions{NacosDataID: "d"}, want: "nacos"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := selectBackend(&c.ops)
			if c.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), c.wantErr) {
					t.Fatalf("want error containing %q, got %v", c.wantErr, err)
				}
				return
			}
			if err != nil || got != c.want {
				t.Fatalf("got %q, %v; want %q", got, err, c.want)
			}
		})
	}
}
```

- [ ] **Step 3: 运行确认失败**

Run: `go test ./config_manager/ -run TestSelectBackend -count=1`
Expected: 编译失败 `undefined: selectBackend`

- [ ] **Step 4: 重写** `config_manager/nacos.go`(整文件替换)

```go
package rcm

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/nacos-group/nacos-sdk-go/v2/clients"
	"github.com/nacos-group/nacos-sdk-go/v2/clients/config_client"
	"github.com/nacos-group/nacos-sdk-go/v2/common/constant"
	"github.com/nacos-group/nacos-sdk-go/v2/common/logger"
	"github.com/nacos-group/nacos-sdk-go/v2/vo"
	"go.uber.org/zap"

	"github.com/housepower/clickhouse_sinker/util"
)

// nacosCASConflictMsg is part of the error nacos 2.x returns when casMd5 no
// longer matches. Pinned by TestNacosCASConflictDetected (integration).
const nacosCASConflictMsg = "Cas publish fail"

type NacosOptions struct {
	Addr        string // comma-separated host[:port], port defaults to 8848
	Username    string
	Password    string
	NamespaceID string // Neither DEFAULT_NAMESPACE_ID("public") nor namespace name work!
	Group       string // Empty means DEFAULT_GROUP
	DataID      string
	CacheDir    string
}

// Nacos is a Source and Writer backed by one nacos dataId.
type Nacos struct {
	client config_client.IConfigClient
	group  string
	dataID string
}

var (
	_ Source = (*Nacos)(nil)
	_ Writer = (*Nacos)(nil)
)

func parseNacosServers(addr string) ([]constant.ServerConfig, error) {
	var sc []constant.ServerConfig
	for _, serverAddr := range strings.Split(addr, ",") {
		fields := strings.SplitN(strings.TrimSpace(serverAddr), ":", 2)
		var port uint64 = 8848
		if len(fields) == 2 {
			var err error
			if port, err = strconv.ParseUint(fields[1], 10, 64); err != nil {
				return nil, fmt.Errorf("invalid nacos address %q: %w", serverAddr, err)
			}
		}
		sc = append(sc, constant.ServerConfig{IpAddr: fields[0], Port: port})
	}
	return sc, nil
}

func NewNacos(o NacosOptions) (*Nacos, error) {
	if o.DataID == "" {
		return nil, errors.New("nacos: --nacos-dataid is required")
	}
	sc, err := parseNacosServers(o.Addr)
	if err != nil {
		return nil, fmt.Errorf("nacos: %w", err)
	}
	group := o.Group
	if group == "" {
		group = constant.DEFAULT_GROUP
	}
	cc := constant.ClientConfig{
		NamespaceId:         o.NamespaceID,
		TimeoutMs:           uint64(opTimeout.Milliseconds()),
		NotLoadCacheAtStart: true,
		CacheDir:            o.CacheDir,
		LogDir:              o.CacheDir,
		Username:            o.Username,
		Password:            o.Password,
	}
	client, err := clients.NewConfigClient(vo.NacosClientParam{ClientConfig: &cc, ServerConfigs: sc})
	if err != nil {
		return nil, fmt.Errorf("nacos: create client: %w", err)
	}
	// Creating the client initializes nacos' own logger, so route it into the
	// sinker log only afterwards.
	logger.SetLogger(&NacosLogger{util.Logger.Sugar()})
	return &Nacos{client: client, group: group, dataID: o.DataID}, nil
}

func (n *Nacos) Fetch(ctx context.Context) ([]byte, error) {
	content, err := n.client.GetConfig(vo.ConfigParam{DataId: n.dataID, Group: n.group})
	if err != nil {
		return nil, fmt.Errorf("dataId %s group %s: %w", n.dataID, n.group, err)
	}
	return []byte(content), nil
}

func (n *Nacos) Update(ctx context.Context, fn UpdateFunc) error {
	return updateWithCAS(ctx, fn, n.read, n.write)
}

// read returns the content with its md5 as version. A missing dataId gives an
// empty version, which publishes without CAS (first-time creation).
func (n *Nacos) read(ctx context.Context) ([]byte, string, error) {
	bs, err := n.Fetch(ctx)
	if err != nil || len(bs) == 0 {
		return bs, "", err
	}
	sum := md5.Sum(bs)
	return bs, hex.EncodeToString(sum[:]), nil
}

func (n *Nacos) write(ctx context.Context, content []byte, casMd5 string) (bool, error) {
	_, err := n.client.PublishConfig(vo.ConfigParam{
		DataId:  n.dataID,
		Group:   n.group,
		Content: string(content),
		CasMd5:  casMd5,
	})
	if err != nil {
		if strings.Contains(err.Error(), nacosCASConflictMsg) {
			return true, nil
		}
		return false, fmt.Errorf("dataId %s group %s: %w", n.dataID, n.group, err)
	}
	return false, nil
}

func (n *Nacos) Close() error {
	n.client.CloseClient()
	return nil
}

type NacosLogger struct {
	*zap.SugaredLogger
}

// override the loglevel to avoid log blow up
func (nlog *NacosLogger) Info(args ...interface{}) {
	nlog.Debug(args...)
}

// override the loglevel to avoid log blow up
func (nlog *NacosLogger) Infof(fmt string, args ...interface{}) {
	nlog.Debugf(fmt, args...)
}
```

- [ ] **Step 5: 新建** `config_manager/select.go`

```go
package rcm

import (
	"fmt"
	"path/filepath"
	"strings"

	"go.uber.org/zap"

	"github.com/housepower/clickhouse_sinker/util"
)

// selectBackend decides the config backend from the options that name a config
// location. "" means the local config file.
func selectBackend(cmdOps *util.CmdOptions) (string, error) {
	var names, flags []string
	if cmdOps.NacosDataID != "" {
		names, flags = append(names, "nacos"), append(flags, "--nacos-dataid")
	}
	if len(names) > 1 {
		return "", fmt.Errorf("only one config backend may be configured, got %s", strings.Join(flags, ", "))
	}
	if len(names) == 0 {
		return "", nil
	}
	return names[0], nil
}

// New builds the Manager for the configured backend. It returns (nil, nil) when
// no backend is configured, meaning the local config file is used.
func New(cmdOps *util.CmdOptions) (*Manager, error) {
	backend, err := selectBackend(cmdOps)
	if err != nil {
		return nil, err
	}
	switch backend {
	case "nacos":
		n, err := NewNacos(NacosOptions{
			Addr:        cmdOps.NacosAddr,
			Username:    cmdOps.NacosUsername,
			Password:    cmdOps.NacosPassword,
			NamespaceID: cmdOps.NacosNamespaceID,
			Group:       cmdOps.NacosGroup,
			DataID:      cmdOps.NacosDataID,
			CacheDir:    filepath.Join(logDir(cmdOps.LogPaths), "nacos_cache"),
		})
		if err != nil {
			return nil, err
		}
		util.Logger.Info("get config from nacos",
			zap.String("addr", cmdOps.NacosAddr), zap.String("namespaceId", cmdOps.NacosNamespaceID),
			zap.String("group", cmdOps.NacosGroup), zap.String("dataId", cmdOps.NacosDataID))
		return NewManager("nacos", n, n), nil
	}
	return nil, nil
}

// logDir is the directory of the last file in --log-paths, "." if none.
func logDir(logPaths string) string {
	dir := "."
	for _, logPath := range strings.Split(logPaths, ",") {
		if logPath != "stdout" && logPath != "stderr" {
			dir, _ = filepath.Split(logPath)
		}
	}
	if abs, err := filepath.Abs(dir); err == nil {
		return abs
	}
	return dir
}
```

- [ ] **Step 6: 删除旧接口**

```bash
git rm config_manager/rcm.go
```

- [ ] **Step 7: 改 discovery** —— 在 `discovery/discovery.go` 中:

import 增加 `"errors"`、`"sync"`、`"go.uber.org/zap"`。

`Discovery` 结构体与构造函数、`IsEnabled` 替换为:

```go
type Discovery struct {
	enabled      bool
	config       *config.Config
	conn         *pool.Conn
	m            *cm.Manager
	warnReadOnly sync.Once
}

func NewDiscovery(cfg *config.Config, m *cm.Manager) *Discovery {
	return &Discovery{config: cfg, m: m}
}

func (d *Discovery) IsEnabled() bool {
	if !d.enabled {
		return false
	}
	if !d.m.Writable() {
		// The discovery switch is hot-reloaded with the config, so this is
		// checked on every round rather than only at startup.
		d.warnReadOnly.Do(func() {
			util.Logger.Warn("discovery is enabled but the config backend is not writable, discovery is skipped")
		})
		return false
	}
	return true
}
```

`Dispatcher()` 中从 `util.Logger.Debug(fmt.Sprintf("discovery: shards: %#v", shards))`(循环之后那一行)到函数末尾,替换为:

```go
	util.Logger.Debug(fmt.Sprintf("discovery: shards: %#v", shards))
	return d.applyShards(shards)
}

// applyShards writes discovered shards back to the config center. It patches
// only clickhouse.hosts on the latest config, so edits made by others since
// d.config was loaded survive.
func (d *Discovery) applyShards(shards Shards) error {
	if !diffShards(shards, hosts2shards(d.config.Clickhouse.Hosts)) {
		util.Logger.Info("discovery: shards not changed")
		return nil
	}
	util.Logger.Info(fmt.Sprintf("discovery: shards changed, old: %v, new: %v", d.config.Clickhouse.Hosts, shards))
	err := d.m.UpdateConfig(func(c *config.Config) bool {
		if !diffShards(shards, hosts2shards(c.Clickhouse.Hosts)) {
			return false // already written, e.g. by another sinker instance
		}
		c.Clickhouse.Hosts = shards2hosts(shards)
		c.Discovery.UpdatedBy = "discovery-changed"
		c.Discovery.UpdatedAt = time.Now()
		return true
	})
	if errors.Is(err, cm.ErrPendingChanges) {
		util.Logger.Warn("discovery: config center has unreleased changes, retry next round", zap.Error(err))
		return nil
	}
	return err
}
```

删除 `Publish()` 方法。

- [ ] **Step 8: 写 discovery 测试** —— 在 `discovery/discovery_test.go` 末尾追加(import 补 `context`、`os`、
  `github.com/housepower/clickhouse_sinker/config`、`cm "github.com/housepower/clickhouse_sinker/config_manager"`、
  `github.com/housepower/clickhouse_sinker/util`):

```go
func TestMain(m *testing.M) {
	util.InitLogger([]string{"stdout"})
	os.Exit(m.Run())
}

// memBackend is an in-memory config center for discovery tests.
type memBackend struct {
	content []byte
	pending bool
	writes  int
}

func (b *memBackend) Fetch(context.Context) ([]byte, error) { return b.content, nil }
func (b *memBackend) Close() error                          { return nil }
func (b *memBackend) Update(_ context.Context, fn cm.UpdateFunc) error {
	if b.pending {
		return cm.ErrPendingChanges
	}
	neu, err := fn(b.content)
	if err != nil {
		return err
	}
	b.content = neu
	b.writes++
	return nil
}

func staleConfig() *config.Config {
	return &config.Config{
		Clickhouse: config.ClickHouseConfig{Hosts: [][]string{{"a"}}},
		Tasks:      []*config.TaskConfig{{Name: "t1"}},
	}
}

func TestApplyShardsPatchesOnlyHostsOnLatestConfig(t *testing.T) {
	// Someone added t2 after discovery loaded its (stale) copy.
	b := &memBackend{content: []byte(`{clickhouse: {hosts: [["a"]]}, kafka: {brokers: "k"}, tasks: [{name: "t1"}, {name: "t2"}]}`)}
	d := NewDiscovery(staleConfig(), cm.NewManager("mem", b, b))
	if err := d.applyShards(Shards{{"a"}, {"b"}}); err != nil {
		t.Fatal(err)
	}
	got, err := cm.DecodeConfig(b.content)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Clickhouse.Hosts) != 2 {
		t.Fatalf("hosts=%v, want 2 shards", got.Clickhouse.Hosts)
	}
	if len(got.Tasks) != 2 {
		t.Fatalf("tasks=%d, the concurrently added t2 must survive", len(got.Tasks))
	}
	if got.Discovery.UpdatedBy != "discovery-changed" {
		t.Fatalf("UpdatedBy=%q", got.Discovery.UpdatedBy)
	}
}

func TestApplyShardsSkipsWriteWhenLatestAlreadyMatches(t *testing.T) {
	// Another sinker instance already wrote the new shards.
	b := &memBackend{content: []byte(`{clickhouse: {hosts: [["a"], ["b"]]}, kafka: {brokers: "k"}}`)}
	d := NewDiscovery(staleConfig(), cm.NewManager("mem", b, b))
	if err := d.applyShards(Shards{{"a"}, {"b"}}); err != nil {
		t.Fatal(err)
	}
	if b.writes != 0 {
		t.Fatalf("writes=%d, want 0", b.writes)
	}
}

func TestApplyShardsPendingChangesIsNotAnError(t *testing.T) {
	b := &memBackend{content: []byte(`{clickhouse: {hosts: [["a"]]}, kafka: {brokers: "k"}}`), pending: true}
	d := NewDiscovery(staleConfig(), cm.NewManager("mem", b, b))
	if err := d.applyShards(Shards{{"a"}, {"b"}}); err != nil {
		t.Fatalf("pending changes must only warn, got %v", err)
	}
}

func TestIsEnabledNeedsWritableBackend(t *testing.T) {
	enabled := config.Config{Discovery: config.Discovery{Enabled: true}}
	ro := NewDiscovery(nil, cm.NewManager("ro", &memBackend{}, nil))
	ro.SetConfig(enabled)
	if ro.IsEnabled() {
		t.Fatal("read-only backend must disable discovery")
	}
	local := NewDiscovery(nil, nil)
	local.SetConfig(enabled)
	if local.IsEnabled() {
		t.Fatal("local file mode must disable discovery")
	}
	b := &memBackend{}
	rw := NewDiscovery(nil, cm.NewManager("rw", b, b))
	rw.SetConfig(enabled)
	if !rw.IsEnabled() {
		t.Fatal("writable backend with discovery enabled must be enabled")
	}
}
```

Run: `go test ./discovery/ -count=1`
Expected: `ok`。discovery 只依赖 `config_manager`,此时已可单独编译;`task`、`cmd` 还引用旧签名,
`go build ./...` 要到 Step 11 之后才能通过,Step 13 统一验证。

- [ ] **Step 9: 改 sinker** —— `task/sinker.go`:

1. 字段 `rcm      cm.RemoteConfManager` 改为 `rcm      *cm.Manager`;`NewSinker(rcm cm.RemoteConfManager, ...)` 改为 `NewSinker(rcm *cm.Manager, ...)`。
2. `sd := discovery.NewDiscovery(s.curCfg, s.rcm, s.rcm != nil)` 改为 `sd := discovery.NewDiscovery(s.curCfg, s.rcm)`。
3. `case <-sdTicker.C:` 分支整段替换为:

```go
			case <-sdTicker.C:
				s.runDiscovery(sd)
```

4. 在 `Run()` 之后新增:

```go
// runDiscovery runs one ClickHouse cluster discovery round. curCfg stays nil
// until the first config is applied, e.g. while the config center is unreachable.
func (s *Sinker) runDiscovery(sd *discovery.Discovery) {
	if s.curCfg == nil {
		return
	}
	sd.SetConfig(*s.curCfg)
	if !sd.IsEnabled() {
		return
	}
	if err := sd.GetCKConn(); err != nil {
		util.Logger.Error("sd.GetCKConn failed", zap.Error(err))
		return
	}
	if err := sd.Dispatcher(); err != nil {
		util.Logger.Error("sd.Dispatcher failed", zap.Error(err))
	}
}
```

5. `Close()` 中:

```go
	// 1. Stop rcm
	if s.rcm != nil {
		if err := s.rcm.Close(); err != nil {
			util.Logger.Error("failed to close config backend", zap.Error(err))
		}
		s.rcm = nil
	}
```

6. 新建 `task/sinker_discovery_test.go`:

```go
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
```

- [ ] **Step 10: 改 main** —— `cmd/clickhouse_sinker/main.go` 中从 `var rcm cm.RemoteConfManager` 到
  `runner = task.NewSinker(rcm, httpAddr, &cmdOps)` 之前的整段替换为:

```go
		rcm, err := cm.New(&cmdOps)
		if err != nil {
			util.Logger.Fatal("failed to set up config backend", zap.Error(err))
		}
		if rcm == nil {
			util.Logger.Info(fmt.Sprintf("get config from local file %s", cmdOps.LocalCfgFile))
		}
```

保留其后的 `runner = task.NewSinker(rcm, httpAddr, &cmdOps)`。该闭包作用域内没有外层 `err`,`:=` 不会遮蔽。
被替换的整段是 `path/filepath` 的唯一使用处,从 import 中删除它;`strings` 在 `initCmdOptions` 中仍有使用,保留。

- [ ] **Step 11: 改发布工具** —— `cmd/nacos_publish_config/main.go`:删除 `getProperties()`;把从
  `ncm := cm.NacosConfManager{}` 到 `DeepEqual` 校验结束的整段替换为:

```go
	n, err := cm.NewNacos(cm.NacosOptions{
		Addr:        *nacosAddr,
		Username:    *nacosUsername,
		Password:    *nacosPassword,
		NamespaceID: *nacosNamespaceID,
		Group:       *nacosGroup,
		DataID:      *nacosDataID,
		CacheDir:    "nacos_cache",
	})
	if err != nil {
		util.Logger.Fatal("cm.NewNacos failed", zap.Error(err))
	}
	defer n.Close()

	var bs []byte
	if bs, err = cm.EncodeConfig(cfg); err != nil {
		util.Logger.Fatal("cm.EncodeConfig failed", zap.Error(err))
	}
	// Publishing the whole file is the purpose of this tool, so old content is ignored.
	if err = n.Update(context.Background(), func([]byte) ([]byte, error) { return bs, nil }); err != nil {
		util.Logger.Fatal("publish config failed", zap.Error(err))
	}
	// Decode what was published so properties with default values are no longer nil.
	if cfg, err = cm.DecodeConfig(bs); err != nil {
		util.Logger.Fatal("cm.DecodeConfig failed", zap.Error(err))
	}
	util.Logger.Info("sleep a while")
	time.Sleep(10 * time.Second)

	var newCfg *config.Config
	if newCfg, err = cm.NewManager("nacos", n, n).GetConfig(); err != nil {
		util.Logger.Fatal("GetConfig failed", zap.Error(err))
	}
	if !reflect.DeepEqual(newCfg, cfg) {
		util.Logger.Fatal("BUG: got different config", zap.Reflect("cfg", cfg), zap.Reflect("newCfg", newCfg))
	}
```

import 增加 `"context"`。

- [ ] **Step 12: docker-compose 的 nacos 升到 2.x** —— `docker-compose.yml` 的 `nacos` 服务替换为:

```yaml
  nacos:
    image: nacos/nacos-server:v2.2.3
    container_name: nacos-standalone
    environment:
    - PREFER_HOST_MODE=hostname
    - MODE=standalone
    - NACOS_AUTH_ENABLE=false
    - NACOS_AUTH_TOKEN=U2lua2VyVGVzdE9ubHlOb3RBUmVhbFNlY3JldDAxMjM0NTY3ODk=
    - NACOS_AUTH_IDENTITY_KEY=serverIdentity
    - NACOS_AUTH_IDENTITY_VALUE=security
    ports:
    - "58848:8848"
    # v2 SDK dials gRPC at HTTP port + 1000, i.e. 58848 + 1000 on the host side.
    - "59848:9848"
```

(保留原服务中其余未列出的字段,如 `restart`、`security_opt`;只替换 image / environment / ports。)

- [ ] **Step 13: 整理依赖并全量验证**

```bash
go mod tidy
gofmt -l config_manager discovery task cmd util
go build ./... && go vet ./config_manager/... ./discovery/... ./task/... ./cmd/...
go test ./... -count=1
grep -n "nacos-sdk-go " go.mod   # 应无输出:v1 已被移除
```

Expected: gofmt 无输出;build/vet 无输出;所有包 `ok`;grep 无输出。

- [ ] **Step 14: 提交**

```bash
git add -A go.mod go.sum config_manager discovery task cmd docker-compose.yml
git commit -m "refactor!: move config access behind config_manager.Manager

Upgrade nacos-sdk-go to v2.2.5, which supports casMd5. Discovery now patches
only clickhouse.hosts on the latest config through UpdateConfig instead of
republishing a copy that may be minutes old, so concurrent edits survive.

BREAKING CHANGE: nacos server 2.x is required (the v2 SDK uses gRPC on
HTTP port + 1000).

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 4: etcd 后端

**Files:**
- Create: `config_manager/etcd.go`、`config_manager/etcd_test.go`、`config_manager/etcd_integration_test.go`
- Modify: `util/common.go`(`CmdOptions` 加字段)、`cmd/clickhouse_sinker/main.go`(参数)、`config_manager/select.go`、
  `config_manager/select_test.go`、`docker-compose.yml`(加 etcd)、`go.mod`/`go.sum`

**Interfaces:**
- Consumes: `updateWithCAS`、`opTimeout`、`NewManager`、`util.EnvStringVarPrefixed`
- Produces: `type EtcdOptions struct { Endpoints, Key, Username, Password, CAFile, CertFile, KeyFile string }`;
  `func (o EtcdOptions) validate() error`;`func NewEtcd(o EtcdOptions) (*Etcd, error)`;`*Etcd` 实现 `Source` 与 `Writer`;
  `CmdOptions` 新字段 `EtcdEndpoints, EtcdKey, EtcdUsername, EtcdPassword, EtcdCAFile, EtcdCertFile, EtcdKeyFile string`

- [ ] **Step 1: 加依赖**

```bash
go get go.etcd.io/etcd/client/v3@v3.6.7
```

- [ ] **Step 2: 写失败测试** `config_manager/etcd_test.go`

```go
package rcm

import (
	"path/filepath"
	"strings"
	"testing"
)

func TestEtcdOptionsValidate(t *testing.T) {
	cases := []struct {
		name    string
		o       EtcdOptions
		wantErr string
	}{
		{name: "ok", o: EtcdOptions{Endpoints: "127.0.0.1:2379", Key: "/k"}},
		{name: "no endpoints", o: EtcdOptions{Key: "/k"}, wantErr: "--etcd-endpoints"},
		{name: "blank endpoints", o: EtcdOptions{Endpoints: " , ", Key: "/k"}, wantErr: "--etcd-endpoints"},
		{name: "no key", o: EtcdOptions{Endpoints: "e:2379"}, wantErr: "--etcd-key"},
		{name: "cert without key", o: EtcdOptions{Endpoints: "e:2379", Key: "/k", CertFile: "c"}, wantErr: "--etcd-key-file"},
		{name: "key without cert", o: EtcdOptions{Endpoints: "e:2379", Key: "/k", KeyFile: "k"}, wantErr: "--etcd-cert-file"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := c.o.validate()
			if c.wantErr == "" {
				if err != nil {
					t.Fatalf("unexpected error %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), c.wantErr) {
				t.Fatalf("want error containing %q, got %v", c.wantErr, err)
			}
		})
	}
}

func TestEtcdOptionsEndpoints(t *testing.T) {
	got := EtcdOptions{Endpoints: " a:2379, ,b:2379 "}.endpoints()
	if strings.Join(got, "|") != "a:2379|b:2379" {
		t.Fatalf("got %v", got)
	}
}

func TestNewEtcdMissingCAFile(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "no-such-ca.pem")
	_, err := NewEtcd(EtcdOptions{Endpoints: "127.0.0.1:2379", Key: "/k", CAFile: missing})
	if err == nil || !strings.Contains(err.Error(), "TLS") {
		t.Fatalf("want TLS load error, got %v", err)
	}
}
```

并在 `select_test.go` 的 cases 中追加:

```go
		{name: "etcd", ops: util.CmdOptions{EtcdKey: "/k"}, want: "etcd"},
		{name: "nacos and etcd", ops: util.CmdOptions{NacosDataID: "d", EtcdKey: "/k"}, wantErr: "--nacos-dataid, --etcd-key"},
```

- [ ] **Step 3: 运行确认失败**

Run: `go test ./config_manager/ -run 'TestEtcd|TestSelectBackend' -count=1`
Expected: 编译失败 `undefined: EtcdOptions`

- [ ] **Step 4: 加参数字段** —— `util/common.go` 的 `CmdOptions` 在 `NacosServiceName` 之后加入:

```go
	EtcdEndpoints string
	EtcdKey       string
	EtcdUsername  string
	EtcdPassword  string
	EtcdCAFile    string
	EtcdCertFile  string
	EtcdKeyFile   string
```

`cmd/clickhouse_sinker/main.go`:在环境变量段 `util.EnvStringVar(&cmdOps.NacosServiceName, "nacos-service-name")` 之后加:

```go
	util.EnvStringVarPrefixed(&cmdOps.EtcdEndpoints, "etcd-endpoints")
	util.EnvStringVarPrefixed(&cmdOps.EtcdKey, "etcd-key")
	util.EnvStringVarPrefixed(&cmdOps.EtcdUsername, "etcd-username")
	util.EnvStringVarPrefixed(&cmdOps.EtcdPassword, "etcd-password")
	util.EnvStringVarPrefixed(&cmdOps.EtcdCAFile, "etcd-ca-file")
	util.EnvStringVarPrefixed(&cmdOps.EtcdCertFile, "etcd-cert-file")
	util.EnvStringVarPrefixed(&cmdOps.EtcdKeyFile, "etcd-key-file")
```

在 flag 段 `nacos-service-name` 那行之后加:

```go
	flag.StringVar(&cmdOps.EtcdEndpoints, "etcd-endpoints", cmdOps.EtcdEndpoints, "a list of comma-separated etcd endpoints")
	flag.StringVar(&cmdOps.EtcdKey, "etcd-key", cmdOps.EtcdKey, "etcd key holding the config")
	flag.StringVar(&cmdOps.EtcdUsername, "etcd-username", cmdOps.EtcdUsername, "etcd username")
	flag.StringVar(&cmdOps.EtcdPassword, "etcd-password", cmdOps.EtcdPassword, "etcd password")
	flag.StringVar(&cmdOps.EtcdCAFile, "etcd-ca-file", cmdOps.EtcdCAFile, "etcd CA file, enables TLS")
	flag.StringVar(&cmdOps.EtcdCertFile, "etcd-cert-file", cmdOps.EtcdCertFile, "etcd client cert file, set with --etcd-key-file for mutual TLS")
	flag.StringVar(&cmdOps.EtcdKeyFile, "etcd-key-file", cmdOps.EtcdKeyFile, "etcd client key file")
```

- [ ] **Step 5: 实现** `config_manager/etcd.go`

```go
package rcm

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"go.etcd.io/etcd/client/pkg/v3/transport"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/housepower/clickhouse_sinker/util"
)

type EtcdOptions struct {
	Endpoints string // comma-separated
	Key       string
	Username  string
	Password  string
	CAFile    string // CA only: one-way TLS
	CertFile  string // with KeyFile: mutual TLS
	KeyFile   string
}

func (o EtcdOptions) endpoints() []string {
	var eps []string
	for _, ep := range strings.Split(o.Endpoints, ",") {
		if ep = strings.TrimSpace(ep); ep != "" {
			eps = append(eps, ep)
		}
	}
	return eps
}

func (o EtcdOptions) validate() error {
	if len(o.endpoints()) == 0 {
		return errors.New("etcd: --etcd-endpoints is required")
	}
	if o.Key == "" {
		return errors.New("etcd: --etcd-key is required")
	}
	if o.CertFile != "" && o.KeyFile == "" {
		return errors.New("etcd: --etcd-cert-file requires --etcd-key-file")
	}
	if o.KeyFile != "" && o.CertFile == "" {
		return errors.New("etcd: --etcd-key-file requires --etcd-cert-file")
	}
	return nil
}

// Etcd is a Source and Writer backed by one etcd key.
type Etcd struct {
	cli *clientv3.Client
	key string
}

var (
	_ Source = (*Etcd)(nil)
	_ Writer = (*Etcd)(nil)
)

func NewEtcd(o EtcdOptions) (*Etcd, error) {
	if err := o.validate(); err != nil {
		return nil, err
	}
	cfg := clientv3.Config{
		Endpoints:   o.endpoints(),
		Username:    o.Username,
		Password:    o.Password,
		DialTimeout: opTimeout,
		Logger:      util.Logger.Named("etcd"),
	}
	if o.CAFile != "" || o.CertFile != "" {
		tlsInfo := transport.TLSInfo{CertFile: o.CertFile, KeyFile: o.KeyFile, TrustedCAFile: o.CAFile}
		tlsCfg, err := tlsInfo.ClientConfig()
		if err != nil {
			return nil, fmt.Errorf("etcd: load TLS files: %w", err)
		}
		cfg.TLS = tlsCfg
	}
	cli, err := clientv3.New(cfg)
	if err != nil {
		return nil, fmt.Errorf("etcd: create client: %w", err)
	}
	return &Etcd{cli: cli, key: o.Key}, nil
}

func (e *Etcd) Fetch(ctx context.Context) ([]byte, error) {
	bs, _, err := e.read(ctx)
	return bs, err
}

// read returns the value with its ModRevision as version. A missing key is an
// error: create it with etcdctl before starting the sinker.
func (e *Etcd) read(ctx context.Context) ([]byte, string, error) {
	resp, err := e.cli.Get(ctx, e.key)
	if err != nil {
		return nil, "", fmt.Errorf("key %s: %w", e.key, err)
	}
	if len(resp.Kvs) == 0 {
		return nil, "", fmt.Errorf("key %s does not exist", e.key)
	}
	kv := resp.Kvs[0]
	return kv.Value, strconv.FormatInt(kv.ModRevision, 10), nil
}

func (e *Etcd) write(ctx context.Context, content []byte, version string) (bool, error) {
	rev, err := strconv.ParseInt(version, 10, 64)
	if err != nil {
		return false, fmt.Errorf("key %s: bad revision %q: %w", e.key, version, err)
	}
	resp, err := e.cli.Txn(ctx).
		If(clientv3.Compare(clientv3.ModRevision(e.key), "=", rev)).
		Then(clientv3.OpPut(e.key, string(content))).
		Commit()
	if err != nil {
		return false, fmt.Errorf("key %s: %w", e.key, err)
	}
	return !resp.Succeeded, nil
}

func (e *Etcd) Update(ctx context.Context, fn UpdateFunc) error {
	return updateWithCAS(ctx, fn, e.read, e.write)
}

func (e *Etcd) Close() error {
	return e.cli.Close()
}
```

- [ ] **Step 6: 接入 select** —— `select.go` 的 `selectBackend` 在 nacos 判断之后加:

```go
	if cmdOps.EtcdKey != "" {
		names, flags = append(names, "etcd"), append(flags, "--etcd-key")
	}
```

`New` 的 switch 加:

```go
	case "etcd":
		e, err := NewEtcd(EtcdOptions{
			Endpoints: cmdOps.EtcdEndpoints,
			Key:       cmdOps.EtcdKey,
			Username:  cmdOps.EtcdUsername,
			Password:  cmdOps.EtcdPassword,
			CAFile:    cmdOps.EtcdCAFile,
			CertFile:  cmdOps.EtcdCertFile,
			KeyFile:   cmdOps.EtcdKeyFile,
		})
		if err != nil {
			return nil, err
		}
		util.Logger.Info("get config from etcd",
			zap.String("endpoints", cmdOps.EtcdEndpoints), zap.String("key", cmdOps.EtcdKey))
		return NewManager("etcd", e, e), nil
```

- [ ] **Step 7: 运行单测确认通过**

Run: `go test ./config_manager/ -count=1`
Expected: `ok`

- [ ] **Step 8: 写集成测试** `config_manager/etcd_integration_test.go`

```go
//go:build integration

package rcm

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestEtcdConcurrentUpdate: every Update that returned nil must be present in
// the final value, i.e. CAS never loses a write.
// Run: SINKER_IT_ETCD_ENDPOINTS=127.0.0.1:52379 go test -tags integration ./config_manager/ -run TestEtcd
func TestEtcdConcurrentUpdate(t *testing.T) {
	eps := os.Getenv("SINKER_IT_ETCD_ENDPOINTS")
	if eps == "" {
		t.Skip("SINKER_IT_ETCD_ENDPOINTS not set")
	}
	key := fmt.Sprintf("/clickhouse_sinker_it/%s/%d", t.Name(), time.Now().UnixNano())
	e, err := NewEtcd(EtcdOptions{Endpoints: eps, Key: key})
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	ctx := context.Background()
	if _, err = e.cli.Put(ctx, key, "start\n"); err != nil {
		t.Fatal(err)
	}
	defer e.cli.Delete(ctx, key)

	var mu sync.Mutex
	var committed []string
	var wg sync.WaitGroup
	for _, writer := range []string{"a", "b"} {
		wg.Add(1)
		go func(writer string) {
			defer wg.Done()
			for i := 0; i < 10; i++ {
				line := fmt.Sprintf("%s-%d\n", writer, i)
				err := e.Update(ctx, func(old []byte) ([]byte, error) {
					return append(bytes.Clone(old), line...), nil
				})
				if err == nil {
					mu.Lock()
					committed = append(committed, line)
					mu.Unlock()
				}
			}
		}(writer)
	}
	wg.Wait()

	final, err := e.Fetch(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(committed) == 0 {
		t.Fatal("no update succeeded")
	}
	for _, line := range committed {
		if !strings.Contains(string(final), line) {
			t.Fatalf("committed line %q lost; final:\n%s", line, final)
		}
	}
}

func TestEtcdFetchMissingKey(t *testing.T) {
	eps := os.Getenv("SINKER_IT_ETCD_ENDPOINTS")
	if eps == "" {
		t.Skip("SINKER_IT_ETCD_ENDPOINTS not set")
	}
	e, err := NewEtcd(EtcdOptions{Endpoints: eps, Key: fmt.Sprintf("/clickhouse_sinker_it/missing/%d", time.Now().UnixNano())})
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	if _, err = e.Fetch(context.Background()); err == nil || !strings.Contains(err.Error(), "does not exist") {
		t.Fatalf("want does-not-exist error, got %v", err)
	}
}
```

- [ ] **Step 9: docker-compose 加 etcd** —— `docker-compose.yml` 的 `services:` 下追加:

```yaml
  etcd:
    image: quay.io/coreos/etcd:v3.5.17
    container_name: etcd
    command:
    - etcd
    - --listen-client-urls=http://0.0.0.0:2379
    - --advertise-client-urls=http://127.0.0.1:2379
    ports:
    - "52379:2379"
```

- [ ] **Step 10: 跑集成测试**

```bash
docker compose up -d etcd
SINKER_IT_ETCD_ENDPOINTS=127.0.0.1:52379 go test -tags integration ./config_manager/ -run TestEtcd -count=1 -v
```

Expected: `TestEtcdConcurrentUpdate` 与 `TestEtcdFetchMissingKey` 均 PASS(不是 SKIP)。

- [ ] **Step 11: 全量验证并提交**

```bash
go mod tidy && go build ./... && go test ./... -count=1
git add -A go.mod go.sum config_manager util/common.go cmd/clickhouse_sinker/main.go docker-compose.yml
git commit -m "feat(config_manager): add etcd config backend

Writes use a ModRevision compare-and-swap transaction.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 5: Apollo 读(agollo)

**Files:**
- Create: `config_manager/apollo.go`、`config_manager/apollo_test.go`
- Modify: `util/common.go`、`cmd/clickhouse_sinker/main.go`、`config_manager/select.go`、`config_manager/select_test.go`、`go.mod`/`go.sum`

**Interfaces:**
- Consumes: `opTimeout`、`NewManager`
- Produces: `type ApolloOptions struct { Meta, AppID, Cluster, Namespace, Secret, Portal, Token, Env, Operator string }`
  (后四个 Task 6 使用);`func (o ApolloOptions) validateRead() error`;`const apolloContentKey = "content"`;
  `func NewApolloSource(o ApolloOptions) (*ApolloSource, error)`;`CmdOptions` 新字段
  `ApolloMeta, ApolloAppID, ApolloCluster, ApolloNamespace, ApolloSecret string`

- [ ] **Step 1: 加依赖**

```bash
go get github.com/apolloconfig/agollo/v4@v4.4.0
```

- [ ] **Step 2: 写失败测试** `config_manager/apollo_test.go`

假 Config Service 的三个接口来自原型实测:agollo 首次同步走 `/configfiles/json/{app}/{cluster}/{ns}`
(返回扁平 JSON),变更通过 `/notifications/v2` 长轮询感知后走 `/configs/{app}/{cluster}/{ns}`。

```go
package rcm

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

const testApolloNS = "clickhouse_sinker.txt"

// fakeConfigService mimics the Apollo Config Service endpoints agollo uses.
type fakeConfigService struct {
	mu      sync.Mutex
	content string
	notifID int
	srv     *httptest.Server
}

func (f *fakeConfigService) set(content string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.content = content
	f.notifID++
}

func newFakeConfigService(t *testing.T, content string) *fakeConfigService {
	f := &fakeConfigService{content: content, notifID: 1}
	mux := http.NewServeMux()
	mux.HandleFunc("/services/config", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode([]map[string]string{{
			"appName": "APOLLO-CONFIGSERVICE", "instanceId": "fake", "homepageUrl": f.srv.URL + "/",
		}})
	})
	mux.HandleFunc("/configfiles/json/", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		_ = json.NewEncoder(w).Encode(map[string]string{apolloContentKey: f.content})
	})
	mux.HandleFunc("/configs/", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		_ = json.NewEncoder(w).Encode(map[string]any{
			"appId": "sinker", "cluster": "default", "namespaceName": testApolloNS,
			"configurations": map[string]string{apolloContentKey: f.content},
			"releaseKey":     fmt.Sprint(f.notifID),
		})
	})
	// Long poll: answer as soon as the notification id moves, else 304 after 500ms.
	mux.HandleFunc("/notifications/v2", func(w http.ResponseWriter, r *http.Request) {
		var req []struct {
			NotificationID int `json:"notificationId"`
		}
		_ = json.Unmarshal([]byte(r.URL.Query().Get("notifications")), &req)
		for deadline := time.Now().Add(500 * time.Millisecond); time.Now().Before(deadline); time.Sleep(50 * time.Millisecond) {
			f.mu.Lock()
			id := f.notifID
			f.mu.Unlock()
			if len(req) > 0 && id != req[0].NotificationID {
				_ = json.NewEncoder(w).Encode([]map[string]any{{"namespaceName": testApolloNS, "notificationId": id}})
				return
			}
		}
		w.WriteHeader(http.StatusNotModified)
	})
	f.srv = httptest.NewServer(mux)
	t.Cleanup(f.srv.Close)
	return f
}

func TestApolloSourceFetchAndHotReload(t *testing.T) {
	f := newFakeConfigService(t, "{kafka: {brokers: \"k1\"}}")
	src, err := NewApolloSource(ApolloOptions{Meta: f.srv.URL, AppID: "sinker", Namespace: testApolloNS})
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()

	got, err := src.Fetch(context.Background())
	if err != nil || string(got) != "{kafka: {brokers: \"k1\"}}" {
		t.Fatalf("initial fetch got %q, %v", got, err)
	}

	f.set("{kafka: {brokers: \"k2\"}}")
	deadline := time.Now().Add(15 * time.Second)
	for {
		got, err = src.Fetch(context.Background())
		if err == nil && string(got) == "{kafka: {brokers: \"k2\"}}" {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("change not observed, last fetch %q, %v", got, err)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func TestApolloSourceUnreachableFailsFast(t *testing.T) {
	_, err := NewApolloSource(ApolloOptions{Meta: "http://127.0.0.1:1", AppID: "sinker", Namespace: testApolloNS})
	if err == nil {
		t.Fatal("start must fail when Apollo is unreachable instead of using a stale backup")
	}
	if !strings.Contains(err.Error(), testApolloNS) {
		t.Fatalf("error must name the namespace, got %v", err)
	}
}

func TestApolloOptionsValidateRead(t *testing.T) {
	ok := ApolloOptions{Meta: "http://m", AppID: "a", Namespace: "n"}
	if err := ok.validateRead(); err != nil {
		t.Fatal(err)
	}
	for _, o := range []ApolloOptions{
		{AppID: "a", Namespace: "n"},
		{Meta: "http://m", Namespace: "n"},
	} {
		if err := o.validateRead(); err == nil {
			t.Fatalf("%+v must be rejected", o)
		}
	}
}
```

`select_test.go` 的 cases 追加:

```go
		{name: "apollo", ops: util.CmdOptions{ApolloNamespace: "n"}, want: "apollo"},
		{name: "etcd and apollo", ops: util.CmdOptions{EtcdKey: "/k", ApolloNamespace: "n"}, wantErr: "--etcd-key, --apollo-namespace"},
```

- [ ] **Step 3: 运行确认失败**

Run: `go test ./config_manager/ -run 'TestApollo|TestSelectBackend' -count=1`
Expected: 编译失败 `undefined: NewApolloSource`

- [ ] **Step 4: 加参数** —— `CmdOptions` 在 etcd 字段后加:

```go
	ApolloMeta      string
	ApolloAppID     string
	ApolloCluster   string
	ApolloNamespace string
	ApolloSecret    string
```

`main.go` 默认值块(`cmdOps = util.CmdOptions{...}`)加 `ApolloCluster: "default",`;环境变量段加:

```go
	util.EnvStringVarPrefixed(&cmdOps.ApolloMeta, "apollo-meta")
	util.EnvStringVarPrefixed(&cmdOps.ApolloAppID, "apollo-app-id")
	util.EnvStringVarPrefixed(&cmdOps.ApolloCluster, "apollo-cluster")
	util.EnvStringVarPrefixed(&cmdOps.ApolloNamespace, "apollo-namespace")
	util.EnvStringVarPrefixed(&cmdOps.ApolloSecret, "apollo-secret")
```

flag 段加:

```go
	flag.StringVar(&cmdOps.ApolloMeta, "apollo-meta", cmdOps.ApolloMeta, "Apollo config service or meta server address")
	flag.StringVar(&cmdOps.ApolloAppID, "apollo-app-id", cmdOps.ApolloAppID, "Apollo appId")
	flag.StringVar(&cmdOps.ApolloCluster, "apollo-cluster", cmdOps.ApolloCluster, "Apollo cluster")
	flag.StringVar(&cmdOps.ApolloNamespace, "apollo-namespace", cmdOps.ApolloNamespace, "Apollo txt/json namespace holding the config, e.g. clickhouse_sinker.txt")
	flag.StringVar(&cmdOps.ApolloSecret, "apollo-secret", cmdOps.ApolloSecret, "Apollo access key secret")
```

- [ ] **Step 5: 实现** `config_manager/apollo.go`

```go
package rcm

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/apolloconfig/agollo/v4"
	agconfig "github.com/apolloconfig/agollo/v4/env/config"

	"github.com/housepower/clickhouse_sinker/util"
)

// apolloContentKey holds the whole text of a txt/json namespace.
const apolloContentKey = "content"

type ApolloOptions struct {
	Meta      string
	AppID     string
	Cluster   string // empty means "default"
	Namespace string
	Secret    string

	// Portal Open API, required for write-back only.
	Portal   string
	Token    string
	Env      string
	Operator string
}

func (o ApolloOptions) cluster() string {
	if o.Cluster == "" {
		return "default"
	}
	return o.Cluster
}

func (o ApolloOptions) validateRead() error {
	if o.Meta == "" || o.AppID == "" || o.Namespace == "" {
		return errors.New("apollo: --apollo-meta, --apollo-app-id and --apollo-namespace are required")
	}
	return nil
}

// ApolloSource reads one txt/json namespace through agollo's local cache,
// which agollo keeps fresh by long polling.
type ApolloSource struct {
	client    agollo.Client
	namespace string
}

var _ Source = (*ApolloSource)(nil)

// agollo's logger is process-global.
var setApolloLoggerOnce sync.Once

func NewApolloSource(o ApolloOptions) (*ApolloSource, error) {
	if err := o.validateRead(); err != nil {
		return nil, err
	}
	setApolloLoggerOnce.Do(func() {
		agollo.SetLogger(util.Logger.Named("apollo").Sugar())
	})
	client, err := agollo.StartWithConfig(func() (*agconfig.AppConfig, error) {
		return &agconfig.AppConfig{
			AppID:         o.AppID,
			Cluster:       o.cluster(),
			NamespaceName: o.Namespace,
			IP:            o.Meta,
			Secret:        o.Secret,
			// Like nacos' NotLoadCacheAtStart: never start from a stale on-disk backup.
			IsBackupConfig:    false,
			MustStart:         true,
			SyncServerTimeout: int(opTimeout / time.Second),
		}, nil
	})
	if err != nil {
		return nil, fmt.Errorf("apollo: appId %s namespace %s: %w", o.AppID, o.Namespace, err)
	}
	return &ApolloSource{client: client, namespace: o.Namespace}, nil
}

func (a *ApolloSource) Fetch(ctx context.Context) ([]byte, error) {
	cfg := a.client.GetConfig(a.namespace)
	if cfg == nil {
		return nil, fmt.Errorf("namespace %s is not loaded", a.namespace)
	}
	return []byte(cfg.GetValue(apolloContentKey)), nil
}

func (a *ApolloSource) Close() error {
	a.client.Close()
	return nil
}
```

- [ ] **Step 6: 接入 select** —— `selectBackend` 加:

```go
	if cmdOps.ApolloNamespace != "" {
		names, flags = append(names, "apollo"), append(flags, "--apollo-namespace")
	}
```

`New` 的 switch 加(Task 6 会补上写入):

```go
	case "apollo":
		o := ApolloOptions{
			Meta:      cmdOps.ApolloMeta,
			AppID:     cmdOps.ApolloAppID,
			Cluster:   cmdOps.ApolloCluster,
			Namespace: cmdOps.ApolloNamespace,
			Secret:    cmdOps.ApolloSecret,
		}
		src, err := NewApolloSource(o)
		if err != nil {
			return nil, err
		}
		util.Logger.Info("get config from apollo",
			zap.String("meta", o.Meta), zap.String("appId", o.AppID),
			zap.String("cluster", o.cluster()), zap.String("namespace", o.Namespace))
		return NewManager("apollo", src, nil), nil
```

- [ ] **Step 7: 运行确认通过**

Run: `go test ./config_manager/ -count=1 -v -run 'TestApollo|TestSelectBackend' 2>&1 | grep -E '^(=== RUN|--- |ok|FAIL)'`
Expected: 全部 PASS。`TestApolloSourceUnreachableFailsFast` 需要约 10 秒(agollo 重试),属正常。

- [ ] **Step 8: 全量验证并提交**

```bash
go mod tidy && go build ./... && go test ./... -count=1
git add -A go.mod go.sum config_manager util/common.go cmd/clickhouse_sinker/main.go
git commit -m "feat(config_manager): read config from an Apollo txt/json namespace

Startup fails when Apollo is unreachable instead of using agollo's on-disk
backup, matching nacos' NotLoadCacheAtStart.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 6: Apollo 写(Portal Open API)

**Files:**
- Create: `config_manager/apollo_openapi.go`、`config_manager/apollo_openapi_test.go`
- Modify: `util/common.go`、`cmd/clickhouse_sinker/main.go`、`config_manager/select.go`

**Interfaces:**
- Consumes: `ApolloOptions`、`apolloContentKey`、`ErrPendingChanges`、`opTimeout`、`UpdateFunc`
- Produces: `func (o ApolloOptions) writeConfigured() (bool, error)`;`func newApolloOpenAPI(o ApolloOptions) *apolloOpenAPI`
  (实现 `Writer`);`CmdOptions` 新字段 `ApolloPortal, ApolloToken, ApolloEnv, ApolloOperator string`

- [ ] **Step 1: 写失败测试** `config_manager/apollo_openapi_test.go`

```go
package rcm

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

// fakePortal mimics the Apollo Portal Open API for one namespace.
type fakePortal struct {
	mu          sync.Mutex
	edited      string // latest item value, possibly unreleased
	released    string
	failRelease bool
	puts        int
	paths       []string // escaped request paths
	lastItem    map[string]string
	srv         *httptest.Server
}

func newFakePortal(t *testing.T, edited, released string) *fakePortal {
	p := &fakePortal{edited: edited, released: released}
	p.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p.mu.Lock()
		defer p.mu.Unlock()
		p.paths = append(p.paths, r.URL.EscapedPath())
		if r.Header.Get("Authorization") != "tok" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		path := r.URL.Path
		switch {
		case r.Method == http.MethodGet && strings.HasSuffix(path, "/releases/latest"):
			_ = json.NewEncoder(w).Encode(map[string]any{"configurations": map[string]string{apolloContentKey: p.released}})
		case r.Method == http.MethodGet:
			_ = json.NewEncoder(w).Encode(map[string]any{"items": []map[string]string{{"key": apolloContentKey, "value": p.edited}}})
		case r.Method == http.MethodPut && strings.HasSuffix(path, "/items/"+apolloContentKey):
			var item map[string]string
			_ = json.NewDecoder(r.Body).Decode(&item)
			p.lastItem = item
			p.edited = item["value"]
			p.puts++
		case r.Method == http.MethodPost && strings.HasSuffix(path, "/releases"):
			if p.failRelease {
				http.Error(w, "release denied", http.StatusForbidden)
				return
			}
			p.released = p.edited
			_, _ = w.Write([]byte("{}"))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(p.srv.Close)
	return p
}

func testOpenAPIOptions(portal string) ApolloOptions {
	return ApolloOptions{AppID: "sinker", Cluster: "default", Namespace: testApolloNS,
		Portal: portal, Token: "tok", Env: "PRO", Operator: "apollo"}
}

func TestApolloOpenAPIUpdate(t *testing.T) {
	p := newFakePortal(t, "old", "old")
	w := newApolloOpenAPI(testOpenAPIOptions(p.srv.URL))
	var seen string
	err := w.Update(context.Background(), func(old []byte) ([]byte, error) {
		seen = string(old)
		return []byte("new"), nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if seen != "old" || p.edited != "new" || p.released != "new" {
		t.Fatalf("seen=%q edited=%q released=%q", seen, p.edited, p.released)
	}
	if p.lastItem["dataChangeLastModifiedBy"] != "apollo" {
		t.Fatalf("operator not sent: %v", p.lastItem)
	}
}

func TestApolloOpenAPIPendingChanges(t *testing.T) {
	p := newFakePortal(t, "half-done edit", "released")
	w := newApolloOpenAPI(testOpenAPIOptions(p.srv.URL))
	err := w.Update(context.Background(), func([]byte) ([]byte, error) { return []byte("new"), nil })
	if !errors.Is(err, ErrPendingChanges) {
		t.Fatalf("want ErrPendingChanges, got %v", err)
	}
	if p.puts != 0 || p.released != "released" {
		t.Fatalf("nothing may be written or released, puts=%d released=%q", p.puts, p.released)
	}
}

func TestApolloOpenAPINoChangeSkipsWrite(t *testing.T) {
	p := newFakePortal(t, "same", "same")
	w := newApolloOpenAPI(testOpenAPIOptions(p.srv.URL))
	if err := w.Update(context.Background(), func(old []byte) ([]byte, error) { return old, nil }); err != nil {
		t.Fatal(err)
	}
	if p.puts != 0 {
		t.Fatalf("puts=%d, want 0", p.puts)
	}
}

func TestApolloOpenAPIReleaseFailure(t *testing.T) {
	p := newFakePortal(t, "old", "old")
	p.failRelease = true
	w := newApolloOpenAPI(testOpenAPIOptions(p.srv.URL))
	err := w.Update(context.Background(), func([]byte) ([]byte, error) { return []byte("new"), nil })
	if err == nil || !strings.Contains(err.Error(), "NOT released") || !strings.Contains(err.Error(), "release denied") {
		t.Fatalf("want an error telling the edit is unreleased, got %v", err)
	}
}

func TestApolloOpenAPIEscapesPath(t *testing.T) {
	p := newFakePortal(t, "x", "x")
	o := testOpenAPIOptions(p.srv.URL)
	o.Cluster = "dc 1"
	o.Namespace = "sinker conf.txt"
	w := newApolloOpenAPI(o)
	_ = w.Update(context.Background(), func(old []byte) ([]byte, error) { return old, nil })
	if len(p.paths) == 0 || !strings.Contains(p.paths[0], "/clusters/dc%201/namespaces/sinker%20conf.txt") {
		t.Fatalf("path not escaped: %v", p.paths)
	}
}

func TestApolloWriteConfigured(t *testing.T) {
	none := ApolloOptions{}
	if ok, err := none.writeConfigured(); ok || err != nil {
		t.Fatalf("none: got %v, %v", ok, err)
	}
	all := ApolloOptions{Portal: "p", Token: "t", Env: "PRO", Operator: "o"}
	if ok, err := all.writeConfigured(); !ok || err != nil {
		t.Fatalf("all: got %v, %v", ok, err)
	}
	partial := ApolloOptions{Portal: "p", Token: "t"}
	if _, err := partial.writeConfigured(); err == nil || !strings.Contains(err.Error(), "--apollo-operator") {
		t.Fatalf("partial: want error listing the flags, got %v", err)
	}
}
```

- [ ] **Step 2: 运行确认失败**

Run: `go test ./config_manager/ -run 'TestApolloOpenAPI|TestApolloWriteConfigured' -count=1`
Expected: 编译失败 `undefined: newApolloOpenAPI`

- [ ] **Step 3: 实现** `config_manager/apollo_openapi.go`

```go
package rcm

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// writeConfigured reports whether write-back is enabled: all four Open API
// options, or none of them.
func (o ApolloOptions) writeConfigured() (bool, error) {
	set := 0
	for _, v := range []string{o.Portal, o.Token, o.Env, o.Operator} {
		if v != "" {
			set++
		}
	}
	switch set {
	case 0:
		return false, nil
	case 4:
		return true, nil
	}
	return false, errors.New("apollo: --apollo-portal, --apollo-token, --apollo-env and --apollo-operator " +
		"must be set together to enable write-back")
}

// apolloOpenAPI writes a namespace through the Apollo Portal Open API.
// Apollo has no compare-and-swap, so Update reads the latest content right
// before writing and refuses to run when someone has unreleased edits.
type apolloOpenAPI struct {
	nsURL    string // .../openapi/v1/envs/{env}/apps/{appId}/clusters/{cluster}/namespaces/{ns}
	token    string
	operator string
	ns       string
	client   *http.Client
}

var _ Writer = (*apolloOpenAPI)(nil)

func newApolloOpenAPI(o ApolloOptions) *apolloOpenAPI {
	nsURL := fmt.Sprintf("%s/openapi/v1/envs/%s/apps/%s/clusters/%s/namespaces/%s",
		strings.TrimRight(o.Portal, "/"), url.PathEscape(o.Env), url.PathEscape(o.AppID),
		url.PathEscape(o.cluster()), url.PathEscape(o.Namespace))
	return &apolloOpenAPI{
		nsURL:    nsURL,
		token:    o.Token,
		operator: o.Operator,
		ns:       o.Namespace,
		client:   &http.Client{Timeout: opTimeout},
	}
}

func (c *apolloOpenAPI) do(ctx context.Context, method, u string, body, out any) error {
	var rd io.Reader
	if body != nil {
		bs, err := json.Marshal(body)
		if err != nil {
			return err
		}
		rd = bytes.NewReader(bs)
	}
	req, err := http.NewRequestWithContext(ctx, method, u, rd)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", c.token)
	req.Header.Set("Content-Type", "application/json;charset=UTF-8")
	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode >= http.StatusMultipleChoices {
		return fmt.Errorf("%s %s: HTTP %d: %s", method, u, resp.StatusCode, bytes.TrimSpace(respBody))
	}
	if out != nil && len(bytes.TrimSpace(respBody)) > 0 {
		if err = json.Unmarshal(respBody, out); err != nil {
			return fmt.Errorf("%s %s: decode response: %w", method, u, err)
		}
	}
	return nil
}

// current returns the latest edited content and whether it differs from the
// latest release, i.e. someone has unreleased edits in this namespace.
func (c *apolloOpenAPI) current(ctx context.Context) (string, bool, error) {
	var ns struct {
		Items []struct {
			Key   string `json:"key"`
			Value string `json:"value"`
		} `json:"items"`
	}
	if err := c.do(ctx, http.MethodGet, c.nsURL, nil, &ns); err != nil {
		return "", false, err
	}
	var content string
	for _, it := range ns.Items {
		if it.Key == apolloContentKey {
			content = it.Value
		}
	}
	var rel struct {
		Configurations map[string]string `json:"configurations"`
	}
	if err := c.do(ctx, http.MethodGet, c.nsURL+"/releases/latest", nil, &rel); err != nil {
		return "", false, err
	}
	return content, content != rel.Configurations[apolloContentKey], nil
}

func (c *apolloOpenAPI) Update(ctx context.Context, fn UpdateFunc) error {
	old, pending, err := c.current(ctx)
	if err != nil {
		return fmt.Errorf("namespace %s: %w", c.ns, err)
	}
	if pending {
		// Releasing now would also release someone else's half-done edits.
		return fmt.Errorf("namespace %s: %w", c.ns, ErrPendingChanges)
	}
	neu, err := fn([]byte(old))
	if err != nil {
		return err
	}
	if string(neu) == old {
		return nil
	}
	item := map[string]string{
		"key":                      apolloContentKey,
		"value":                    string(neu),
		"dataChangeCreatedBy":      c.operator,
		"dataChangeLastModifiedBy": c.operator,
	}
	if err = c.do(ctx, http.MethodPut, c.nsURL+"/items/"+apolloContentKey+"?createIfNotExists=true", item, nil); err != nil {
		return fmt.Errorf("namespace %s: modify: %w", c.ns, err)
	}
	release := map[string]string{
		"releaseTitle":   time.Now().Format("20060102150405") + "-clickhouse_sinker",
		"releaseComment": "clickhouse_sinker discovery write-back",
		"releasedBy":     c.operator,
	}
	if err = c.do(ctx, http.MethodPost, c.nsURL+"/releases", release, nil); err != nil {
		return fmt.Errorf("namespace %s: modification committed but NOT released, release it in Apollo Portal: %w", c.ns, err)
	}
	return nil
}
```

- [ ] **Step 4: 加参数** —— `CmdOptions` 在 Apollo 读字段后加:

```go
	ApolloPortal   string
	ApolloToken    string
	ApolloEnv      string
	ApolloOperator string
```

`main.go` 环境变量段:

```go
	util.EnvStringVarPrefixed(&cmdOps.ApolloPortal, "apollo-portal")
	util.EnvStringVarPrefixed(&cmdOps.ApolloToken, "apollo-token")
	util.EnvStringVarPrefixed(&cmdOps.ApolloEnv, "apollo-env")
	util.EnvStringVarPrefixed(&cmdOps.ApolloOperator, "apollo-operator")
```

flag 段:

```go
	flag.StringVar(&cmdOps.ApolloPortal, "apollo-portal", cmdOps.ApolloPortal, "Apollo Portal address, enables write-back with the other --apollo-* Open API options")
	flag.StringVar(&cmdOps.ApolloToken, "apollo-token", cmdOps.ApolloToken, "Apollo Open API token")
	flag.StringVar(&cmdOps.ApolloEnv, "apollo-env", cmdOps.ApolloEnv, "Apollo env for the Open API, e.g. PRO")
	flag.StringVar(&cmdOps.ApolloOperator, "apollo-operator", cmdOps.ApolloOperator, "existing Apollo Portal user recorded as modifier and releaser")
```

- [ ] **Step 5: 接入 select** —— `New` 的 `case "apollo":` 整段替换为(先校验写参数,未连网前就能报错):

```go
	case "apollo":
		o := ApolloOptions{
			Meta:      cmdOps.ApolloMeta,
			AppID:     cmdOps.ApolloAppID,
			Cluster:   cmdOps.ApolloCluster,
			Namespace: cmdOps.ApolloNamespace,
			Secret:    cmdOps.ApolloSecret,
			Portal:    cmdOps.ApolloPortal,
			Token:     cmdOps.ApolloToken,
			Env:       cmdOps.ApolloEnv,
			Operator:  cmdOps.ApolloOperator,
		}
		writable, err := o.writeConfigured()
		if err != nil {
			return nil, err
		}
		src, err := NewApolloSource(o)
		if err != nil {
			return nil, err
		}
		var w Writer
		if writable {
			w = newApolloOpenAPI(o)
		}
		util.Logger.Info("get config from apollo",
			zap.String("meta", o.Meta), zap.String("appId", o.AppID),
			zap.String("cluster", o.cluster()), zap.String("namespace", o.Namespace),
			zap.Bool("writable", writable))
		return NewManager("apollo", src, w), nil
```

- [ ] **Step 6: 运行确认通过**

Run: `go test ./config_manager/ -count=1`
Expected: `ok`

- [ ] **Step 7: 全量验证并提交**

```bash
gofmt -l config_manager util cmd && go build ./... && go test ./... -count=1
git add config_manager util/common.go cmd/clickhouse_sinker/main.go
git commit -m "feat(config_manager): write back to Apollo through the Portal Open API

Write-back is enabled when --apollo-portal, --apollo-token, --apollo-env and
--apollo-operator are all set. It refuses to release while the namespace
has unreleased edits by someone else.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 7: nacos CAS 集成测试

Task 3 的 `nacosCASConflictMsg` 是按 nacos 2.x 服务端源码推断的,本任务在真实服务端上钉死它。

**Files:**
- Create: `config_manager/nacos_integration_test.go`
- Modify(仅当 Step 3 显示文案不同): `config_manager/nacos.go` 的 `nacosCASConflictMsg`

**Interfaces:**
- Consumes: `NewNacos`、`(*Nacos).write`、`(*Nacos).read`、`(*Nacos).Update`
- Produces: 无

- [ ] **Step 1: 写测试** `config_manager/nacos_integration_test.go`

```go
//go:build integration

package rcm

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

// Run: SINKER_IT_NACOS_ADDR=127.0.0.1:58848 go test -tags integration ./config_manager/ -run TestNacos -v
func newITNacos(t *testing.T) *Nacos {
	addr := os.Getenv("SINKER_IT_NACOS_ADDR")
	if addr == "" {
		t.Skip("SINKER_IT_NACOS_ADDR not set")
	}
	n, err := NewNacos(NacosOptions{
		Addr:     addr,
		Username: "nacos",
		Password: "nacos",
		DataID:   fmt.Sprintf("clickhouse_sinker_it_%d", time.Now().UnixNano()),
		CacheDir: t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = n.client.DeleteConfig(voConfigParam(n))
		_ = n.Close()
	})
	return n
}

func TestNacosCASConflictDetected(t *testing.T) {
	n := newITNacos(t)
	ctx := context.Background()
	if err := n.Update(ctx, func([]byte) ([]byte, error) { return []byte("v1"), nil }); err != nil {
		t.Fatal(err)
	}
	waitNacosContent(t, n, "v1")
	conflict, err := n.write(ctx, []byte("v2"), "00000000000000000000000000000000")
	if err != nil {
		t.Fatalf("a stale casMd5 must be reported as conflict, got error: %v", err)
	}
	if !conflict {
		t.Fatal("a stale casMd5 must be reported as conflict")
	}
}

func TestNacosConcurrentUpdate(t *testing.T) {
	n := newITNacos(t)
	ctx := context.Background()
	if err := n.Update(ctx, func([]byte) ([]byte, error) { return []byte("start\n"), nil }); err != nil {
		t.Fatal(err)
	}
	waitNacosContent(t, n, "start\n")

	var mu sync.Mutex
	var committed []string
	var wg sync.WaitGroup
	for _, writer := range []string{"a", "b"} {
		wg.Add(1)
		go func(writer string) {
			defer wg.Done()
			for i := 0; i < 5; i++ {
				line := fmt.Sprintf("%s-%d\n", writer, i)
				err := n.Update(ctx, func(old []byte) ([]byte, error) {
					return append(bytes.Clone(old), line...), nil
				})
				if err == nil {
					mu.Lock()
					committed = append(committed, line)
					mu.Unlock()
				}
			}
		}(writer)
	}
	wg.Wait()
	if len(committed) == 0 {
		t.Fatal("no update succeeded")
	}
	final, err := n.Fetch(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range committed {
		if !strings.Contains(string(final), line) {
			t.Fatalf("committed line %q lost; final:\n%s", line, final)
		}
	}
}

func waitNacosContent(t *testing.T, n *Nacos, want string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		got, err := n.Fetch(context.Background())
		if err == nil && string(got) == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("content never became %q, last %q, %v", want, got, err)
		}
		time.Sleep(200 * time.Millisecond)
	}
}
```

在同文件末尾加辅助函数(import 补 `"github.com/nacos-group/nacos-sdk-go/v2/vo"`):

```go
func voConfigParam(n *Nacos) vo.ConfigParam {
	return vo.ConfigParam{DataId: n.dataID, Group: n.group}
}
```

- [ ] **Step 2: 启动 nacos 2.x**

```bash
docker compose up -d nacos
until curl -sf 'http://127.0.0.1:58848/nacos/v1/console/health/readiness' >/dev/null; do sleep 2; done
```

- [ ] **Step 3: 运行**

Run: `SINKER_IT_NACOS_ADDR=127.0.0.1:58848 go test -tags integration ./config_manager/ -run TestNacos -count=1 -v`
Expected: 两个测试 PASS(不是 SKIP)。

若 `TestNacosCASConflictDetected` 以 "got error: ..." 失败:错误文本就是服务端真实的冲突文案。把
`nacos.go` 中 `nacosCASConflictMsg` 改为该文本中稳定的那一段(不要包含 dataId 等变量),同时改掉常量上方的注释,重跑至通过。

- [ ] **Step 4: 提交**

```bash
git add config_manager/nacos_integration_test.go config_manager/nacos.go
git commit -m "test(config_manager): pin nacos casMd5 conflict handling against nacos 2.x

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 8: 文档与系统测试

**Files:**
- Modify: `docs/guide/run.md`、`docs/configuration/flag.md`、`docs/dev/design.md`、`CHANGELOG.md`

**Interfaces:**
- Consumes: 全部前序任务
- Produces: 无

- [ ] **Step 1: `docs/guide/run.md`** —— 在 "- For Nacos:" 那一项之后追加:

```markdown
- For etcd (the key must exist, e.g. `etcdctl put /clickhouse_sinker/prod "$(cat conf.hjson)"`):

  `clickhouse_sinker --etcd-endpoints 10.0.0.1:2379,10.0.0.2:2379 --etcd-key /clickhouse_sinker/prod`

  TLS: `--etcd-ca-file` alone for one-way TLS, plus `--etcd-cert-file` and `--etcd-key-file` for mutual TLS.

- For Apollo, put the whole config into a txt or json namespace:

  `clickhouse_sinker --apollo-meta http://apollo-config:8080 --apollo-app-id sinker --apollo-namespace clickhouse_sinker.txt`

  ClickHouse cluster discovery writes `clickhouse.hosts` back to the config center. With Apollo this needs the Portal
  Open API: `--apollo-portal`, `--apollo-token`, `--apollo-env` and `--apollo-operator`, all four or none. The token's
  third-party app needs modify and release permission on the namespace, the operator must be an existing Portal user,
  and write-back cannot work when the namespace requires different users to modify and release. Write-back is skipped,
  with a warning, while the namespace has unreleased edits.

Only one of `--nacos-dataid`, `--etcd-key` and `--apollo-namespace` may be set.

Every option can also be given as an environment variable: upper case, `-` replaced by `_`, prefixed with `SINKER_`,
e.g. `SINKER_ETCD_KEY`. Options that existed before the prefix also accept the unprefixed name, e.g. `NACOS_ADDR`.

Write-back re-encodes the config, so comments and formatting in the config center are lost.
```

- [ ] **Step 2: `docs/configuration/flag.md`** —— 构建后用真实输出替换代码块中的内容:

```bash
make build && ./bin/clickhouse_sinker -h 2>&1 | sed 's#^Usage of .*#Usage of ./clickhouse_sinker:#'
```

把输出整体粘进 ```` ``` ```` 代码块,替换旧列表。

- [ ] **Step 3: `docs/dev/design.md`** —— 在 `## Task scheduling` 一节之前插入:

```markdown
## Config management

The config is one hjson document, loaded from a local file or from a config center: Nacos (`--nacos-dataid`),
etcd (`--etcd-key`) or Apollo (`--apollo-namespace`). A config center is polled every 10 seconds and changes are
applied by diff.

Backends implement a byte-level `Source` (read) and optionally a `Writer` (`config_manager/manager.go`).
`Manager` does the hjson encoding. Writes are read-modify-write on the latest content: etcd and Nacos use
compare-and-swap (ModRevision / casMd5) with retries, Apollo has no compare-and-swap and refuses to release while
the namespace has unreleased edits. ClickHouse cluster discovery uses this to patch only `clickhouse.hosts`, so
edits made by others in the meantime survive.
```

- [ ] **Step 4: `CHANGELOG.md`** —— 在 `# Changelog` 之后插入:

```markdown
#### Unreleased

Breaking Changes:

- Nacos server 2.x is required: nacos-sdk-go is upgraded to v2, which talks gRPC on HTTP port + 1000.
- Task assignment among instances (`--nacos-service-name`) is removed. Every instance runs all tasks, and Kafka
  consumer groups balance partitions among instances. `--nacos-service-name` and the config fields `assignment`,
  `kafka.assignInterval`, `kafka.calcLagInterval`, `kafka.rebalanceByLags` are ignored with a warning.

New Features:

- etcd and Apollo config backends.
- Environment variables accept the `SINKER_` prefix; new options only accept the prefixed name.

Improvements:

- ClickHouse cluster discovery patches only `clickhouse.hosts` on the latest config, so concurrent edits are no
  longer overwritten.
```

- [ ] **Step 5: 系统测试(nacos 2.x 回归)**

```bash
docker compose up -d
make systest
```

Expected: 脚本以 0 退出,三组 `check result` 计数均为 100000。该脚本只操作 docker 容器内的 ClickHouse 测试表。

- [ ] **Step 6: 最终全量验证**

```bash
go build ./... && go test ./... -count=1
SINKER_IT_ETCD_ENDPOINTS=127.0.0.1:52379 SINKER_IT_NACOS_ADDR=127.0.0.1:58848 go test -tags integration ./config_manager/ -count=1
```

Expected: 全部 `ok`,集成测试无 SKIP(加 `-v` 确认)。

- [ ] **Step 7: 提交**

```bash
git add docs CHANGELOG.md
git commit -m "docs: document etcd and Apollo config backends

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

- [ ] **Step 8: 手工验证清单**(交付到客户环境时执行,不在本仓库 CI 中)

1. 用各后端参数启动,日志出现 `get config from <backend>` 及 key / namespace。
2. 在配置中心修改一个 task 的 `flushInterval`,10 秒内日志出现 `going to apply another config`。
3. 开启 discovery,在 ClickHouse 集群增删一个 shard,配置中心里只有 `clickhouse.hosts` 与 `discovery` 段变化。
4. Apollo:在 Portal 改配置但不发布,触发 discovery,日志出现 `unreleased changes` warn,Portal 中无新发布。
