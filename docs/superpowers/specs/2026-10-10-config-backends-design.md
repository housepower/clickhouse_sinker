# 配置中心后端抽象(etcd / Apollo)设计

日期:2026-10-10
分支:`refactor/remove-nacos-assign`(基于 `a1f2675`,任务分配已移除)
状态:设计已逐节评审,待落实现计划

## 1. 背景与目标

客户环境只有 etcd 或 Apollo,没有 nacos,而 sinker 目前只能从本地文件或 nacos 读配置。
同时 aimeter 会通过 Apollo 下发 sinker 配置,所以 Apollo 不能只读:ClickHouse 节点自动发现
(`discovery`)检测到 shard 变化后,需要把新的 hosts 写回 Apollo。

目标:

- sinker 能从 nacos / etcd / Apollo 中的任意一个读取完整配置,改动在 10 秒内生效。
- 三个后端都支持写回;discovery 写回时**不会冲掉其他写入方(aimeter、人工)的修改**。
- 抽象出统一接口层,后端只关心字节读写,hjson 编解码与业务逻辑不进后端。

非目标:

- 不做多配置源合并,一次只用一个后端。
- 不改配置内容格式,仍是一整份 hjson。
- 不改本地文件模式(仍然只在启动时读一次,无热加载)。本地文件做成 Source、合并 sinker
  两条主循环是另一件事,另开任务。
- 不改成 watch/push 模型,沿用 10 秒轮询。

## 2. 现状(`a1f2675`)

- `config_manager/rcm.go`:`RemoteConfManager` 接口,4 个方法
  `Init(map[string]interface{})` / `GetConfig` / `PublishConfig` / `Stop`。唯一实现是 nacos。
- nacos SDK 是 `github.com/nacos-group/nacos-sdk-go v1.1.4`,`PublishConfig` 不支持 `casMd5`。
- `cmd/clickhouse_sinker/main.go` 用一组 `--nacos-*` 参数拼出无类型的 `properties` map 传给 `Init`。
- `task/sinker.go`:`rcm == nil` 走本地文件分支;否则每 10 秒 `GetConfig` → `Normallize`
  → `applyConfig`(整体比对后按差异应用,幂等)。
- `discovery/discovery.go`:每 `Discovery.CheckInterval`(默认 60 秒)查 `system.clusters`,
  shard 变化时改它手里的配置副本并 `PublishConfig` **整份覆盖**写回。副本最长可能是
  60 秒前的,期间别人对配置的修改会被冲掉。
- `cmd/nacos_publish_config`:把本地文件整份发布到 nacos 的工具,直接依赖 `NacosConfManager`。

## 3. 总体决策

| 决策点 | 结论 |
| --- | --- |
| 接口分层 | 后端只实现字节级的 `Source`(读,必选)和 `Writer`(写,可选);hjson 编解码收进 `Manager` |
| 写回语义 | 去掉"整份覆盖"的 `PublishConfig`,改为 `Update(fn)`:在最新内容上做"读→改→写" |
| 并发写保护 | etcd 用 `ModRevision` 做 CAS,nacos 用 `casMd5` 做 CAS,冲突重试至多 3 次;Apollo 无 CAS,尽力而为(毫秒级窗口) |
| nacos SDK | 升级到 `nacos-sdk-go/v2 v2.2.5`(与 cell、ckman 一致),以获得 `casMd5` |
| Apollo 存储格式 | 整份 hjson 放在 txt/json 类型的 namespace(如 `clickhouse_sinker.txt`),内容在固定 key `content` 下 |
| Apollo 写入 | 走 Portal Open API(修改 + 发布);配置了 Portal 参数才可写,否则只读 |
| 启动参数 | 按后端加前缀,沿用 `--nacos-*` 风格;环境变量统一加 `SINKER_` 前缀(新参数只认前缀,现有参数兼容无前缀) |
| 变更感知 | 沿用 10 秒轮询 |

## 4. 组件设计

### 4.1 接口与 Manager(`config_manager/manager.go`,新)

```go
package rcm

// Source 读配置原始内容。每个后端都必须实现。
type Source interface {
    Fetch(ctx context.Context) ([]byte, error)
    Close() error
}

// Writer 是可选能力。后端构造时根据是否配置了写入参数决定是否提供。
// fn 收到后端读到的最新内容,返回要写入的新内容;返回 ErrNoChange 表示无需写入。
// 后端负责"读→fn→写"的原子性:etcd/nacos 用 CAS,冲突时重读并重新调用 fn,至多重试 3 次;
// Apollo 只能做到读完立即写,不保证原子。
type Writer interface {
    Update(ctx context.Context, fn func(old []byte) ([]byte, error)) error
}

var (
    ErrNoChange       = errors.New("config unchanged")
    ErrPendingChanges = errors.New("config center has unreleased changes")
    ErrConflict       = errors.New("config changed concurrently, retries exhausted")
)

type Manager struct {
    name string // "nacos" / "etcd" / "apollo",用于日志和错误
    src  Source
    w    Writer // 可为 nil
}

func (m *Manager) GetConfig() (*config.Config, error)  // Fetch + hjson.Unmarshal
func (m *Manager) Writable() bool                      // w != nil
// UpdateConfig 在最新配置上应用 mutate 再写回。mutate 返回 false 时不写。
// 内部:w.Update(ctx, func(old) { 解析 old → mutate → json.MarshalIndent })。
func (m *Manager) UpdateConfig(mutate func(*config.Config) (changed bool)) error
func (m *Manager) Close() error
```

- 删除 `rcm.go` 和 `RemoteConfManager` 接口、`Init(map)`、`PublishConfig`。
- `GetConfig` 与 `UpdateConfig` 的每次后端调用都带 5 秒超时,避免配置中心卡住时拖住 sinker 主循环。
- 所有错误都包上后端名与 key/namespace,便于排查。
- 写回的内容是 `json.MarshalIndent` 的输出(JSON 同时是合法 hjson;Apollo json namespace 也能接受),
  会丢掉配置中心里手写的注释和排版,写进文档。

### 4.2 nacos 后端(`config_manager/nacos.go`,改)

- SDK 升级到 `github.com/nacos-group/nacos-sdk-go/v2 v2.2.5`。
- 构造:`NewNacos(NacosOptions) (*Nacos, error)`,字段即现有 `--nacos-*` 参数。
  沿用 `NotLoadCacheAtStart: true`(启动时不读本地缓存)。
- `Fetch`:`GetConfig`。
- `Update`:`GetConfig` 得到 old → 本地算 `md5(old)` → `fn(old)` →
  `PublishConfig{Content: new, CasMd5: md5(old)}`。服务端判定 md5 不匹配时重试。
  实现时需先在真实 nacos 2.x 上确认冲突的返回形态(`published=false` 还是特定错误码),
  只把冲突当作重试条件,网络错误等直接返回,不吞进重试。
- 写入能力始终开启。

### 4.3 etcd 后端(`config_manager/etcd.go`,新)

- 依赖 `go.etcd.io/etcd/client/v3`。
- 参数:`--etcd-endpoints`(逗号分隔)、`--etcd-key`、`--etcd-username`、`--etcd-password`,
  TLS:`--etcd-ca-file`、`--etcd-cert-file`、`--etcd-key-file`(三者都给才启用双向 TLS,只给 CA 则单向)。
- `Fetch`:`Get(key)`;key 不存在返回明确错误,不当作空配置。
- `Update`:`Get` 得到 old 与 `ModRevision` → `fn(old)` →
  `Txn(If(ModRevision(key) == rev)).Then(Put(key, new))`;`Succeeded == false` 时重试。
- 写入能力始终开启。

### 4.4 Apollo 后端(`config_manager/apollo.go` 读、`apollo_openapi.go` 写,新)

**读**(`github.com/apolloconfig/agollo/v4 v4.4.0`):

- 参数:`--apollo-meta`、`--apollo-app-id`、`--apollo-cluster`(默认 `default`)、
  `--apollo-namespace`(如 `clickhouse_sinker.txt`)、`--apollo-secret`(可选,访问密钥)。
- `IsBackupConfig: false`:与 nacos 的 `NotLoadCacheAtStart` 一致,Apollo 不可达时不拿磁盘备份启动。
- `Fetch`:从 agollo 本地缓存取 namespace 的 `content`。agollo 自己长轮询更新缓存。
- 启动时首次拉取失败 → 报错退出,不以空配置启动。

**写**(标准库 `net/http` 调 Portal Open API,不引入额外 SDK):

- 参数:`--apollo-portal`、`--apollo-token`、`--apollo-env`、`--apollo-operator`。
  **四个全配才提供 Writer;部分配置 → 启动报错;全不配 → 只读。**
- `Update` 流程:
  1. `GET .../namespaces/{ns}` 取当前 items,`GET .../namespaces/{ns}/releases/latest` 取最新发布;
     两者的 `content` 不一致即视为**有未发布的修改** → 返回 `ErrPendingChanges`,不写。
     (Apollo 的发布会把 namespace 下所有未发布修改一起发出去,这一步防止把别人的半成品带上线。)
  2. 以第 1 步读到的 `content` 为 old,调用 `fn`。
  3. `PUT .../namespaces/{ns}/items/content`(`dataChangeLastModifiedBy = operator`),
     再 `POST .../namespaces/{ns}/releases`(`releasedBy = operator`)。
  4. 修改成功、发布失败时,错误信息写明"修改已提交但未发布,需到 Portal 处理"。
- Open API 路径前缀:`/openapi/v1/envs/{env}/apps/{appId}/clusters/{cluster}`;
  鉴权头 `Authorization: {token}`。
- 部署前置要求(写进文档):第三方应用 token 需被授予该 namespace 的修改与发布权限;
  `operator` 必须是 Portal 中存在的用户;**namespace 开启"修改与发布不能为同一人"时无法写回**。

### 4.5 后端选择(`config_manager/select.go`,新)

`rcm.New(cmdOps *util.CmdOptions) (*Manager, error)`:

| 条件 | 结果 |
| --- | --- |
| `--nacos-dataid` 非空 | nacos |
| `--etcd-key` 非空 | etcd |
| `--apollo-namespace` 非空 | Apollo |
| 都为空 | 返回 `nil, nil`,走本地文件(`--local-cfg-file`),与现状一致 |
| 两个及以上非空 | 报错,列出冲突的参数 |

- 各后端的其他必填参数(etcd 的 endpoints,Apollo 的 meta、app-id)在构造时校验,缺失报错退出。
- 新参数加入 `util.CmdOptions`;`Gsypt.Unmarshal` 遍历整个结构体,`--etcd-password`、
  `--apollo-secret`、`--apollo-token` 自动支持 `ENC(...)`。

### 4.6 启动参数与环境变量

定义方式沿用 `initCmdOptions()` 的三层覆盖:结构体默认值 → 环境变量 → 命令行,
最后 `Gsypt.Unmarshal` 解密。

**环境变量统一加 `SINKER_` 前缀**(避免与 Apollo 客户端生态的 `APOLLO_*`、etcd server 的 `ETCD_*` 撞名):

- 新参数**只认**带前缀的名字(如 `SINKER_ETCD_ENDPOINTS`),不回退到无前缀名,否则撞名问题依旧。
- 现有参数**两种都认**:先查 `SINKER_<NAME>`,没有再查原来的 `<NAME>`(如 `NACOS_ADDR`、`LOG_LEVEL`),
  现有部署不用改。
- 实现:`util.EnvStringVar` / `EnvIntVar` / `EnvBoolVar` 改为先查前缀再回退;另加
  `EnvStringVarPrefixed` 只查前缀,供新参数使用。名称转换规则不变(大写,`-` → `_`)。

**etcd**(`--etcd-key` 非空即选 etcd)

| 参数 | 环境变量 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `--etcd-endpoints` | `SINKER_ETCD_ENDPOINTS` | 空 | 逗号分隔;选 etcd 时必填 |
| `--etcd-key` | `SINKER_ETCD_KEY` | 空 | 存配置的 key |
| `--etcd-username` | `SINKER_ETCD_USERNAME` | 空 | |
| `--etcd-password` | `SINKER_ETCD_PASSWORD` | 空 | 支持 `ENC(...)` |
| `--etcd-ca-file` | `SINKER_ETCD_CA_FILE` | 空 | 只给 CA 为单向 TLS |
| `--etcd-cert-file` | `SINKER_ETCD_CERT_FILE` | 空 | 与 key-file 同给为双向 TLS |
| `--etcd-key-file` | `SINKER_ETCD_KEY_FILE` | 空 | |

**Apollo 读**(`--apollo-namespace` 非空即选 Apollo)

| 参数 | 环境变量 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `--apollo-meta` | `SINKER_APOLLO_META` | 空 | Config Service / Meta Server 地址;必填 |
| `--apollo-app-id` | `SINKER_APOLLO_APP_ID` | 空 | 必填 |
| `--apollo-cluster` | `SINKER_APOLLO_CLUSTER` | `default` | |
| `--apollo-namespace` | `SINKER_APOLLO_NAMESPACE` | 空 | 如 `clickhouse_sinker.txt` |
| `--apollo-secret` | `SINKER_APOLLO_SECRET` | 空 | 访问密钥,支持 `ENC(...)` |

**Apollo 写**(四个全配才可写;部分配置启动报错;全不配为只读)

| 参数 | 环境变量 | 说明 |
| --- | --- | --- |
| `--apollo-portal` | `SINKER_APOLLO_PORTAL` | Portal 地址 |
| `--apollo-token` | `SINKER_APOLLO_TOKEN` | 第三方应用 token,支持 `ENC(...)` |
| `--apollo-env` | `SINKER_APOLLO_ENV` | 如 `PRO` |
| `--apollo-operator` | `SINKER_APOLLO_OPERATOR` | 须为 Portal 中存在的用户 |

**nacos**:`--nacos-*` 参数不变;环境变量新增 `SINKER_NACOS_*` 写法,原 `NACOS_*` 继续有效。

### 4.7 接入点

- `cmd/clickhouse_sinker/main.go`:构造 nacos `properties` 的整段替换为 `rcm.New(&cmdOps)`。
- `task/sinker.go`:`s.rcm` 类型改为 `*rcm.Manager`;主循环不变;`Close()` 中 `Stop()` → `Close()`。
- `discovery/discovery.go`:
  - `NewDiscovery(cfg, m *rcm.Manager)`,去掉 `rcm bool` 参数。启用条件改为
    `cfg.Discovery.Enabled && m != nil && m.Writable()`。
  - 开了 discovery 但后端不可写(本地文件,或未配 Portal 参数的 Apollo)→ 首次检测到时 warn 一次
    (discovery 开关随配置热更新,所以不是只在启动时检查)。
  - `Publish()` 改为 `m.UpdateConfig(func(c) { 只改 c.Clickhouse.Hosts 与 c.Discovery.UpdatedBy/UpdatedAt })`,
    hosts 无变化返回 `false`。
  - `ErrPendingChanges` 打 warn,其他错误打 error;都等下一周期重试。
- `cmd/nacos_publish_config`:改用 `NewNacos` 和 `Update`(fn 忽略 old,整份写入);
  结束时的 `GetConfig` 回读校验保留。

### 4.8 文件组织

```
config_manager/
  manager.go         Manager、Source/Writer 接口、错误定义、hjson 编解码
  select.go          New(cmdOps):后端选择与参数校验
  nacos.go           nacos 后端(v2 SDK)
  etcd.go            etcd 后端
  apollo.go          Apollo 读(agollo)
  apollo_openapi.go  Apollo 写(Open API 客户端)
  lags.go            不变
```

## 5. 测试计划

**单元测试**(`go test ./...` 默认运行,无外部依赖):

- `Manager`:fake Source/Writer 覆盖 hjson 编解码、`changed=false` 不写、`ErrNoChange` /
  `ErrPendingChanges` 透传。
- `select.go`:表驱动——各后端单独设置、全不设、两个同设、必填缺失、Apollo 写参数部分配置。
- 环境变量:现有参数 `SINKER_X` 优先于 `X`、只设 `X` 仍生效;新参数只设无前缀名(如 `ETCD_KEY`)时不生效。
  测试用 `t.Setenv`,不污染进程环境。
- discovery:fake 中的"最新配置"比 discovery 手里的副本多一个 task,写回后该 task 必须仍在。
  这条直接对应本次要解决的并发覆盖问题。
- Apollo:`httptest` 模拟 Config Service(agollo 直连)和 Portal Open API;覆盖正常修改+发布、
  有未发布修改时放弃、修改成功发布失败的报错文案。
- CAS 重试:fake 持续返回冲突,验证重试 3 次后返回 `ErrConflict`,且每次重试都用新读到的 old 重新调用 fn。

**集成测试**(build tag `integration`,需 docker):

- etcd:`quay.io/coreos/etcd` 容器;两个 goroutine 并发 `Update` 同一 key,两次修改都必须保留。
- nacos:同样的并发 `Update`,验证 v2 SDK 的 `casMd5` 在真实服务端生效。
- 不引入 etcd embed,避免把 etcd server 依赖拉进 `go.mod`。

**系统测试**(`make systest` / `go.test.sh`):

- `docker-compose.yml` 中 nacos 从 `1.4.1` 升到 2.x,端口映射增加 `59848:9848`
  (v2 SDK 按"HTTP 端口 + 1000"计算 gRPC 端口,宿主侧 58848 + 1000 = 59848)。
- 现有三组 nacos 用例保持不变,作为 SDK 升级的回归。
- etcd、Apollo 不进 systest;客户环境按下方清单手工验证。

**手工验证清单**(交付时):

1. 用各后端参数启动,日志出现所选后端名与 key/namespace。
2. 在配置中心修改一个 task 的 `flushInterval`,10 秒内日志出现 apply config。
3. 开启 discovery,在 ClickHouse 集群增删一个 shard,配置中心里只有 `clickhouse.hosts` 变化。
4. Apollo:在 Portal 里改配置但不发布,触发 discovery,日志出现 `ErrPendingChanges` warn,Portal 中无新发布。

## 6. 风险与取舍

- **nacos 1.x 服务端不兼容**:v2 SDK 走 gRPC,要求 nacos 服务端 2.x。内部环境(cell、ckman 已用 v2.2.5)
  无影响;master 上仍用 nacos 1.x 的开源用户升级后会连不上。写进 CHANGELOG 的 breaking changes。
- **Apollo 无 CAS**:Open API 无版本校验,"读最新 → 写"之间仍有毫秒级窗口;
  discovery 写回本身很少发生(只在 shard 变化时),接受。
- **nacos 内嵌存储不校验 casMd5**:单机模式内嵌 Derby 时 casMd5 被忽略(Task 7 实测:错误 md5 发布照样成功),
  只有外部 MySQL 存储时 CAS 才严格;Derby 上退化为与 Apollo 相同的尽力而为。冲突文案实测为
  "Cas publish fail,server md5 may have changed."。CAS 集成测试用 SINKER_IT_NACOS_CAS=1 开启,需 MySQL 存储的 nacos。
- **Apollo 未发布修改检测依赖内容比较**:需在客户的 Apollo 版本上确认
  `releases/latest` 接口可用、`content` 字段比较可靠;实现时先对照客户版本的 Open API 文档。
- **Apollo 发布审核**:开启"修改与发布不能为同一人"时无法写回,只能作为部署前置要求,代码保证报错清晰。
- **写回丢注释**:沿用 nacos 现状,文档注明。
