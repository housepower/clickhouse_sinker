# 错误分类与分级处理(错误旁路)设计

日期:2026-06-23
分支:`feature/error-bypass`
状态:设计已评审,待落实现计划

> 更新(2026-06-23):RetryMaxDuration 已废弃,重试上限改为仅由 RetryTimes(默认 3)兜底。

## 1. 背景与目标

clickhouse_sinker 当前对写入失败的处理过于粗暴:`loopWrite` 不区分错误类型,
所有错误一律 `retry.Do` N 次(`RetryTimes`,10s→1min backoff),N 次耗尽后
**整批丢弃**(打 Error 日志 + `FlushMsgsErrorTotal`,然后 `Wg.Done` 放行、offset 照常提交)。

由此产生的痛点:

- **不可重试错误**(列对不上、类型对不上等数据质量问题)会白白 retry N 次、阻塞
  数分钟,最后还是丢 —— 既慢又丢数据。
- **可重试错误**(CK 短暂失联、too many parts)在 N 次后被当作永久失败丢弃,
  本可等它自愈。
- 被丢弃的批次纯粹蒸发,只剩日志+指标,**无法事后排查、补数、重放**。
- 结构性错误会让一个 task 的每一批都同样失败,持续刷错误日志、空耗重试。

目标:按错误性质分级处理 —— 瞬时错误背压重试、不可重试错误按 task 可配策略处置
(中断/忽略/旁路),任何单条/单批坏数据都不再拖垮进程或阻塞整条管线。

## 2. 现状梳理(master)

失败处理目前有四个层次,从内到外:

1. **副本级 failover**(`pool/conn.go` `NextGoodReplica`):shard 内换副本,由连接
   Open 是否成功驱动;`dbVer` 机制使 `loopWrite` 每次重试都会轮转到下一个副本。
   全部副本 Open 失败才返回 `ErrAllReplicasDown`。
2. **分片级 reroute**(`output/clickhouse.go` `loopWrite`):`SkipUnavailableShards`
   且无 shardingKey/SortingKeys 时,`ErrAllReplicasDown` → 换健康分片。
3. **批级重试**(`loopWrite`):`retry.Do(Attempts(RetryTimes))`,不分类。
4. **批级放弃**:重试耗尽 → 整批丢弃(日志 + `FlushMsgsErrorTotal`)。

另有**行级隔离**(`pool/ck_cli.go` `write_v1_isolated` / `write_v2`):逐行
`Append`/`Exec`,客户端侧 marshal 失败的行进 bitmap,重发时跳过坏行,计入
`numBad → ParseMsgsErrorTotal`。**注意**:这只能捕获客户端侧暴露的坏行;只在
服务端 `Send()`/`Commit()` 才暴露的错误(列不符、类型不符、TOO_MANY_PARTS、连接断)
会让**整批** `write` 返回 err,无行级隔离。

关键数据结构:`model.Batch` 只携带已解析的 `model.Rows`(`[]interface{}` 值),
`task/sharding.go` 的 `PutElement` 只 append 了 `msgRow.Row`,**丢弃了 `msgRow.Msg`
(原始 kafka 字节 `Value`)**。要旁路原始消息,必须把 `Msg` 贯通到写入层。

## 3. 总体决策

| 决策点 | 选择 |
|---|---|
| 分类粒度 | 仅判**可重试 vs 不可重试**两类;细分(structural/data/unknown)只用于指标 label 与死信元数据,不分叉行为 |
| 可重试判定 | **白名单才重试**(可配置,默认 7 码 + 连接级条件),其余皆不可重试 |
| 可重试上限 | 退避重试到 `RetryMaxDuration`(默认 30m);超上限 → 当作最终写失败,按策略处置 |
| 不可重试处置 | per-task `writeFailureStrategy`:`THROW` / `IGNORE` / `WRITE_TO_KAFKA`,默认 `IGNORE` |
| 死信负载 | 方案 A:贯通原始 kafka 字节,死信写原始消息 + 错误元数据(可重放) |
| 死信维度 | **按 task 维度**,每 task 独立死信配置块(独立 brokers + topic + 自动建 topic) |

### 分类模型(最终)

每个写入错误经 `classifyError` 判定**可重试 / 不可重试**:

1. **命中可重试白名单**(可配置,默认 7 码)或**连接级条件** → 可重试 → 退避重试;
   超 `RetryMaxDuration` → 转入"最终写失败"按 `writeFailureStrategy` 处置。
2. **其余一切**(未知码、结构级、数据级)→ 不可重试 → 立即按 `writeFailureStrategy` 处置。

> 设计依据:不可重试错误的处置权完全交给用户按 task 配(`writeFailureStrategy`),
> sinker 不再硬编码"structural 必隔离 / data 必旁路"。想隔离 task 就配 `THROW`,
> 想保留坏数据就配 `WRITE_TO_KAFKA`,想静默跳过就配 `IGNORE`(默认)。

### 不可重试处置策略 `writeFailureStrategy`(per-task)

| 策略 | 含义 | 行为 |
|---|---|---|
| `THROW` | 中断数据写入 | **仅停掉该 task**(隔离,不影响其他 task/租户);该批连同后续批短路;等人工修复后经 reload 恢复 |
| `IGNORE` | 忽略继续任务 | 丢弃该批 + 限流 WARN 日志 + `MsgsDroppedTotal` 指标;**默认值** |
| `WRITE_TO_KAFKA` | 异常数据旁路 | 把该批原始消息 + 错误元数据写到 task 自己的死信 kafka,继续任务 |

未配置 `writeFailureStrategy` → 默认 `IGNORE`(与现有"重试耗尽则放弃批"行为最接近,升级最平滑)。

### 可重试白名单(默认值,可配置追加)

| 码 | 名称 |
|---|---|
| 202 | TOO_MANY_SIMULTANEOUS_QUERIES |
| 225 | NO_ZOOKEEPER |
| 242 | TABLE_IS_READ_ONLY |
| 252 | TOO_MANY_PARTS |
| 319 | UNKNOWN_STATUS_OF_INSERT |
| 999 | KEEPER_EXCEPTION |
| 1000 | POCO_EXCEPTION |

外加**连接级条件**(非 CH 错误码):`errors.Is(err, pool.ErrAllReplicasDown)`、
`net.Error`、`io.EOF`、`context.DeadlineExceeded`(不含 `Canceled`)→ 视为可重试。

配置覆盖:`RetryableErrorCodes []int` 追加白名单;`FatalErrorCodes []int` 强制把某些码
归为不可重试(优先级高于白名单)。

### 细分 label(仅用于观测,不分叉行为)

死信元数据与指标 label 用 `error_class` 区分,便于排查:
- `structural`:7/8/10/15/16/47/60/81/352 等(列/表/库结构问题)。
- `data`:53/6/26/27/41/72/69/70/117/131 等(类型/解析/越界)。
- `transient_exhausted`:瞬时错误重试超上限。
- `unknown`:未识别码。

## 4. 组件设计

### 4.1 错误分类器 `output/errclass.go`(新)

```go
type ErrorClass int
const (
    ClassRetryable ErrorClass = iota
    ClassFatal
)

// 返回是否可重试 + 用于观测的细分标签
func classifyError(err error, retryable, fatal map[int]bool) (ErrorClass, string /*label*/)
```

- 用 `errors.As` 解包到 `*clickhouse.Exception` 读 `.Code`(native 协议)。
- HTTP 路径若拿不到结构化 Exception,回退正则匹配错误串里的 `code: NNN`。
- 判定:`fatal` 覆盖表命中 → Fatal;`retryable` 白名单/连接级条件命中 → Retryable;
  否则 → Fatal(默认)。label 另据内置 structural/data 码表 + 来源推断。
- 纯函数,无 IO/副作用,易单测。

### 4.2 分级执行器(重写 `loopWrite`)

```
firstFail := zero
for {
    err := write(batch, currentSc, &dbVer)
    if err == nil { return }
    class, label := classify(err)
    if class == ClassRetryable {
        // 复用现有 reroute(ErrAllReplicasDown + canReroute)
        if firstFail == zero { firstFail = now }
        if now - firstFail > RetryMaxDuration {
            label = "transient_exhausted"
            dispatchFailure(batch, label, err); return   // 转最终写失败
        }
        sleepWithCtx(ctx, backoff)   // 响应 ctx.Done() → 及时返回,修复 teardown 卡死
        continue
    }
    dispatchFailure(batch, label, err); return            // 不可重试,立即处置
}
```

`dispatchFailure` 按 task `writeFailureStrategy` 分派:
- `THROW` → `markTaskBroken(taskName, label)` + 该批死信不投、按 IGNORE 计丢弃指标 + 返回。
- `IGNORE` → 限流 WARN + `MsgsDroppedTotal{task,label}` + 丢弃。
- `WRITE_TO_KAFKA` → 投递死信;死信失败则兜底降级为 IGNORE(见 4.3)。

要点:
- `RetryMaxDuration` 为主上限;`RetryTimes > 0` 时亦作 attempts 上限,**取先到者**(向后兼容)。
- backoff 沿用 capped exponential(10s 起,封顶 1min);`sleepWithCtx` 必须
  `select { case <-time.After(d): case <-ctx.Done(): return }`,解决 consumer.go
  teardown 卡死隐患。
- 副本切换无需显式处理 —— 仅可重试类才继续重试,因而仅可重试类才继续轮副本
  (结构/数据类不再空轮一遍副本)。

### 4.3 死信 sink `output/deadletter.go`(新,按 task)

每个开启 `WRITE_TO_KAFKA` 的 task 持有一个死信 producer(franz-go):
- 配置块(per-task,见 4.6):独立 `bootstrapServers`、`topicName`、自动建 topic 参数。
- payload = 原始 `Value`(+ Key);kafka header 携带元数据:`task`、`table`、
  `error_code`、`error_class`、`error_msg`、原 `topic`/`partition`/`offset`、`ts`。
- 分区策略用 franz-go 默认(`kafkaPartitionType` 已弃用,不做配置)。
- **自动建 topic**:`autoCreateTopic=true` 时,producer 初始化阶段用 kafka admin 按
  `autoCreateTopicPartitions` / `autoCreateTopicReplicationFactor` 建 `topicName`
  (已存在则跳过)。
- **兜底**:死信写入自身失败 → 限流 WARN + `DeadLetterErrorsTotal{task}` 指标,
  **不阻塞、不挂、降级为丢弃**(可加有限本地重试,如 3 次)。
- 生命周期:随 task/consumer 启停;关闭时 flush 未发完的死信。

### 4.4 原始字节贯通(方案 A)

- `model.Batch` 增加 `Msgs []*model.InputMessage`,与 `*Rows` **1:1 对齐**。
- `task/sharding.go`:`msgBuf` 由 `[]*model.Rows` 改为能同时容纳 `Msg` 的结构
  (并行数组或 `[]*model.MsgRow` 缓冲);`PutElement` 同时 append `msgRow.Msg`;
  `Flush` 生成 Batch 时填充对齐的 `Msgs`。
- 写入层据 row 下标映射回 `Msgs[i]` 取原始字节。
- 行级隔离(`ck_cli.go` bitmap)路径:`WRITE_TO_KAFKA` 时,把 bitmap 命中的坏行
  对应的 `Msgs[i]` 精确旁路(需把 `Msgs` 或回调传入 `Write`)。
- 内存影响:flush 前多留一份 `Value` 的**引用**(非拷贝),可接受。

### 4.5 task 隔离(`THROW` / 结构错误想隔离时)

- `dispatchFailure` 检出 `THROW` → 调用 sinker 暴露的 `MarkTaskBroken(taskName, reason)`。
- Sinker 维护 `brokenTasks sync.Map`;reload/precheck tick 复用 `filterMissingTables`
  的移除语义剔除 broken task,保住兄弟 task。
- **过渡期短路**:`ClickHouse.Send` / `loopWrite` 入口检查该 task 是否已 broken,
  是则直接按策略处置、不再尝试写(避免重复轮副本、刷日志)。
- 防抖:同一 task 重复检出只标记一次、告警限流。
- 恢复:人工修复表结构后,经 reload/precheck 自动恢复 —— 不引入新恢复路径。

### 4.6 配置新增

`ClickHouseConfig`(全局):
- `RetryMaxDuration string`(如 `"30m"`):瞬时重试总时长上限。留空 → 默认 `30m`。
  `RetryTimes > 0` 时同时作 attempts 上限,与时长**取先到者**(向后兼容)。
- `RetryableErrorCodes []int` / `FatalErrorCodes []int`:分类覆盖(可选)。

`TaskConfig`(per-task)新增死信配置块:
```jsonc
{
  "writeFailureStrategy": "WRITE_TO_KAFKA",   // THROW | IGNORE | WRITE_TO_KAFKA;缺省 IGNORE
  "bootstrapServers": ["192.168.31.170:9092", "192.168.31.171:9092"],
  "autoCreateTopic": true,
  "autoCreateTopicPartitions": 1,
  "autoCreateTopicReplicationFactor": 1,
  "topicName": "task_topic_errors"
}
```
仅当 `writeFailureStrategy = WRITE_TO_KAFKA` 时,`bootstrapServers`/`topicName` 必填。

### 4.7 可观测性(`statistics/statistics.go`)

新增 CounterVec(均注册进 init + pusher):
- `MsgsDroppedTotal{task, class}` —— IGNORE 丢弃(含死信兜底降级)。
- `MsgsDeadLetteredTotal{task, class}` —— 成功旁路死信。
- `DeadLetterErrorsTotal{task}` —— 死信写入自身失败。
- `TaskQuarantinedTotal{task, reason}` —— THROW 隔离 task。

配合限流 WARN(复用现有 `rate.Limiter` 模式,每 task 每 10s 一条),避免刷屏。

## 5. 测试计划

- **classifier 单测**:各 CH 错误码(白名单/structural/data/未知)、wrapped error、
  连接级错误(net/EOF/DeadlineExceeded/Canceled)、HTTP 字符串回退、配置覆盖表生效、
  label 推断正确。
- **执行器单测**:fake `write` 按类返回 → 断言 重试/到上限升级/THROW隔离/IGNORE丢弃/
  WRITE_TO_KAFKA 死信 行为;ctx 取消时及时返回(不卡死)。
- **死信 writer 单测**:mock producer 验证 payload + header 元数据;自动建 topic;
  写入失败兜底(降级丢弃、指标自增、不 panic)。
- **贯通对齐测试**:`Msgs[i]` 与 `Rows[i]` 下标对齐;行级 bitmap 旁路命中正确的原始消息。

## 6. 风险与取舍

- **默认 IGNORE 会静默丢弃不可重试坏数据**(仅日志+指标)。想保留须显式配
  `WRITE_TO_KAFKA`,想停机排查须配 `THROW`。文档需明确建议生产配置。
- **方案 A 的 plumbing 面较大**(动 `model.Batch`、`sharding.go`、写入层签名),
  需保证 `Msgs` 与 `Rows` 始终对齐,否则死信会张冠李戴 —— 单测重点覆盖。
- **`RetryMaxDuration` 默认值**:设太长则瞬时故障期 lag 累积;太短则误判永久失败。
  默认 30m,可配。
- **THROW 仅停该 task**:绝不停整进程,守住多租户隔离;恢复依赖人工修复 + reload。
- **死信独立 kafka 集群**:death-letter 与输入端解耦,需各自维护连通性;死信集群挂掉
  时兜底降级为丢弃(不反压输入),避免死信故障拖垮主链路。
