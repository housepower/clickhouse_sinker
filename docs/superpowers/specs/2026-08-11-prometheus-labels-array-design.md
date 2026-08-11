# prometheusSchema 支持双数组 label

日期：2026-08-11

## 背景与动机

`prometheusSchema=true` 时，sinker 把每条消息里的每个 label key 映射成 series 表的一个
独立列。当上游指标不规范、label key 基数很高时，单张 series 表会膨胀到上千列：

- ClickHouse 侧每列一个文件，part 合并与写入成本随列数线性增长；
- 每次发现新 key 都要 `ALTER TABLE` 并在集群内同步；
- 单条消息实际只带十几个 label，写新 series 时却要对全部列取值补 `NULL`。

上游 ETL 改为用一对平行数组承载 label：`__labels_key__[i]` 与 `__labels_value__[i]`
成对表达一个 label。这样 series 表的列数从「所有 label key 的并集」收敛为固定几列，
每条消息只携带自身真实拥有的十几个元素。

关键约束：**下游查询看到的 `labels` JSON 列内容与格式必须保持不变**，查询侧无感知。
因此数组不是绕开 `labels`，而是成为 `labels` 的唯一数据源。

## 现状：为什么现在跑不通

四处阻塞，前三处是「建不出列」，第四处是运行期 panic：

1. `parser/fastjson.go:523` — `GetNewKeys` 显式过滤 `arr==true` 与 `model.Object`，
   数组字段永远不进 `newKeys`，只会每个 key 打一条 warn。
2. 同一行 `newKeys.Store(strKey, typ)` 只存一个 int 类型码，Array 这一维度在传递中丢失。
   即便放开第 1 点，`ChangeSchema` 也只知道 `String`，无从建出 `Array(String)`。
3. `output/clickhouse.go:774-794` 的 PrometheusSchema 分支按元素类型分流，
   `Array(String)` 的 `intVal` 就是 `String`，会落到 `default` 被建成 `Nullable(String)`，类型错误。
4. `task/task.go:294` 判断「是不是标量 String 列」时只看 `dim.Type.Type == model.String`，
   没有排除 `Type.Array`。而 `WhichType("Array(String)")` 返回的正是
   `{Type: String, Array: true}`，于是 `GetValueByType` 返回的 `[]string` 通过了判断，
   下一行 `val.(string)` 直接 panic。每来一条 new series 崩一次。

同源问题还有一处尚未被触发：`output/clickhouse.go:548` 的 NameKey 探测
（「从第 4 列起找第一个 String 列」，用于识别 opentsdb 风格的 metric 名列）同样没排除
Array。只要 `__labels_key__` 在其他 String 列之前，metric 名就会被错认成 `__labels_key__`，
连带 `__name__` 反被当成普通 label 塞进 `labels` JSON。

## 设计范围

本次**不**支持 `Array` 列的动态建列。数组列由使用方在 series 表手工预建。
理由：数组列名是固定的少数几个，预建成本极低；而放开动态建列需要把 Array 标记随
`newKeys` 传到 `ChangeSchema`（`newKeys` 的 value 要从 `int` 换成结构体），
波及 fastjson/gjson/csv 三个 parser，改动面与收益不成比例。

预建列后 `knownKeys` 会在 `task/task.go:163` 从 Dims 装载，因此这些字段不会重复刷 warn，
也不会被误判为新键。

## 配置

`config.TaskConfig` 新增字段，紧邻 `PromLabelsBlackList`：

```go
// PromLabelsArray declares a pair of Array(String) columns in the series table
// holding parallel label keys/values. Once set, the "labels" JSON is built
// solely from this pair. Requires PrometheusSchema be true.
PromLabelsArray struct {
    KeyColumn   string // e.g. "__labels_key__"
    ValueColumn string // e.g. "__labels_value__"
} `json:"promLabelsArray,omitempty"`
```

校验规则（`config.go` 的 `normallizeTask`，即处理 `PromLabelsBlackList` 的同一处）：

- 两个字段都为空 = 功能关闭，走现有逻辑，既有任务零影响；
- 只填其中一个 = 配置错误，直接报错返回；
- `PrometheusSchema=false` 时清空（与 `PromLabelsBlackList` 同一处理方式）。

## labels 生成语义

启用后，`labels` JSON **只**由这对数组配对生成。series 表里的标量 String 列一律不再
参与拼接（它们照常写入各自的列，只是不进 JSON）。

这样 `__series_key__`、`__mgmt_key__`、`objectKey`、`__raw_name__` 这类非 label 的
标量字段自然进不去，且上游将来新增字段也不会污染 `labels`——无需维护黑名单。

保留的排除规则（数组里的 key 依然要过）：

- `nameKey`（`__name__`，或 opentsdb 风格探测出的 metric 名列）
- `le` —— 排除它使 `labels` 可以直接作为 histogram 查询的 group key，这是既有语义，必须保留
- `promLabelsBlackList` 正则

边界处理：

| 情况 | 处理 |
| --- | --- |
| key/value 数组长度不等 | 取 `min(len(k), len(v))`，多余部分丢弃；限流 warn + statistics 计数器。这是上游 ETL 的数据错误，必须可观测，不能静默截断 |
| 数组内重复 key | 取第一个出现的，保证 JSON 不出现重复键 |
| value 为空字符串 | 保留。标量列那条 `NotNullable && val == ""` 跳过规则不套用到数组来源——该规则存在的意义是非 Nullable 列区分不了「缺失」与「真空串」，而数组里缺失的 key 压根不会出现，空串就是真实值 |
| 输出顺序 | 按数组原始下标，不排序。`AllowWriteSeries` 按 `sid`/`mid` 去重，同一 series 只写一次，上游顺序抖动不会产生重复行，不值得付排序成本 |

键值转义沿用现有的 `strconv.Quote`。

## 数据流与结构

`initSeriesSchema` 在解析 series dims 时定位这对列，把它们在 `c.Dims` 中的绝对下标存为
`IdxLblKey` / `IdxLblVal`，`Service.Init` 取走。Init 期校验（fail-fast，不留到运行时）：

- 两列必须存在于 series 表；
- 类型必须是 `Array(String)`，即 `Type.Type == model.String && Type.Array`；
- 位置必须在 `labels` 列之后（即 `>= IdxSerID+3`）。

`metric2Row` 的 `if newSeries` 分支内，现有循环保持不变：每个 dim 取值 append 进 row，
两个数组列的 `[]string` 值照常写入 series 表落盘。循环结束后，从 row 里按下标取出这两个
`[]string`，调用纯函数拼出 `labels`，回填 `row[idxSerID+2]`。

拼接逻辑抽到新文件 `task/labels.go` 的纯函数中，`metric2Row` 只负责收集输入与回填。
这样这套规则可以脱离 ClickHouse 连接单独测试，也让 `task.go` 里那个已经偏长的
`metric2Row` 不再继续膨胀。

## 顺带修复

1. `output/clickhouse.go:548` NameKey 探测加 `&& !serDim.Type.Array`。
2. `task/task.go:294` 拼接条件加 `&& !dim.Type.Array`。在新设计下标量列本就不参与
   数组模式的拼接，但未启用数组功能、而 series 表里存在 Array 列的场景依然会走到这里，
   所以这个防御必须显式加上，不能依赖数组功能被启用。

## 测试

`task/labels_test.go`（纯函数，无外部依赖）：

- 数组正常展开成 labels JSON
- key/value 长度不等时取 min，且计数器被打点
- 数组内重复 key 只保留第一个
- `le` / `nameKey` / `promLabelsBlackList` 三道排除生效
- value 为空字符串时保留
- 未配置数组时退化成现有的标量列拼接行为（回归护栏）

`config/config_test.go`：

- 只填 KeyColumn 或只填 ValueColumn 时报错
- `prometheusSchema=false` 时 `PromLabelsArray` 被清空

## 使用方需要执行的 DDL

```sql
ALTER TABLE <db>.<metric>_series
  ADD COLUMN IF NOT EXISTS `__labels_key__`   Array(String),
  ADD COLUMN IF NOT EXISTS `__labels_value__` Array(String),
  ADD COLUMN IF NOT EXISTS `__tags_key__`     Array(String),
  ADD COLUMN IF NOT EXISTS `__tags_value__`   Array(String);
```

`__tags_*` 是 `__labels_*` 的子集（去掉 `host`、`kube_cluster_id`、`kube_cluster_name`），
仅作为普通 `Array(String)` 列存储，不参与 `labels` 生成。

## 不在本次范围

- `Array` 列的动态建列（`GetNewKeys` / `ChangeSchema` 改造）
- 消息中 `all_tags` 这类 Object 字段的直接消费。它与双数组内容等价，属于冗余，
  使用方可用 `excludeColumns` 或 `dynamicSchema.blackList` 挡掉
- `config.go:518` 中 `PrometheusSchema=true` 强制开启 `DynamicSchema` 的既有逻辑
