# local-cache-engine-go

带**过期（TTL）**、**容量上限（确定性 LRU 淘汰）**、**并发安全**、**同键回源合并**与
**后端故障降级**的本地缓存存储层。设计目标是：容量、过期、并发下的数据状态与失败结论
都能被**解释**（日志 + 计数 + 错误枚举）并**复现**（可注入的确定性时钟 + 无后台协程）。

- 包路径：`github.com/lisama569/local-cache-engine-go`（包名 `cache`）
- 泛型支持：`Cache[V]` / `Store[V]` 可存放任意类型值

## 目录与分层

| 文件 | 职责 |
| --- | --- |
| `clock.go` | 时间抽象：`SystemClock`（生产）/ `FakeClock`（测试，只进不退） |
| `entry.go` | 条目结构与过期边界判定 |
| `cache.go` | 本地缓存：TTL + 容量淘汰 + LRU + 判定日志 |
| `singleflight.go` | 同键并发调用合并 |
| `store.go` | 降级存储层：回源合并、写穿透、旧值降级、错误分类 |
| `errors.go` | 失败原因枚举 `ErrorKind` 与统一错误 `*cache.Error` |
| `stats.go` | 原子计数与一致快照 |
| `logger.go` | 日志接口（Nop / std / 测试） |

两层结构：

- `Cache[V]`：纯粹的本地键值缓存，只关心 TTL、容量、LRU，不知道后端存在。
- `Store[V]`：在 `Cache` 之上组合 `Loader`（回源后端）、`Group`（singleflight）
  与降级/写穿透规则，是业务直接使用的门面。

## 过期策略（不依赖可回拨的系统时钟）

- 缓存内部**从不直接调用 `time.Now()`**，当前时间只来自注入的 `Clock`：
  - 生产用 `SystemClock`：基于 `time.Now()`，其结果携带**单调时钟读数**，
    两个读数之间的 `Before/Sub` 比较使用单调时钟，NTP 等引起的墙上时钟回拨
    不会把新数据误判为过期。
  - 测试用 `FakeClock`：时间只在 `Advance/Set` 时变化，没有后台流逝，边界严格可复现。
- 有效期为半开区间 **[set, expireAt)**：`now < expireAt` 有效；
  **`now == expireAt` 即过期**，过期边界两侧结论固定。
- **读到过期数据绝不返回脏值**：`Cache.Get` 对过期条目返回零值与状态 `HitExpired`
  （按未命中处理）。过期条目暂时保留在容器中，仅供“后端不可用时返回上一次有效值”降级使用；
  显式删除、清扫淘汰或后端确认 `NotFound` 时会被清除。
- 没有后台清理协程：过期条目的移除发生在 ① 容量将满插入时的清扫、② 主动 `Sweep()`、
  ③ `Delete`、④ 后端 `NotFound`。因此行为不受调度时序影响。

## 容量与淘汰策略（确定、可复现）

容量为最大条目数 `Capacity`。插入**新键**且当前已满时，按以下固定顺序腾位：

1. **先清扫过期条目**：把所有已过期条目按 `(expireAt 升序, key 升序)` 排序，
   依次删除，直到条目数小于容量；计入 `EvictedExpired`，日志标记 `淘汰[expired-sweep]`。
2. **再按 LRU 淘汰**：若清扫后仍满，选 `accessed`（每次命中/写入分配的单调逻辑序号）
   最小者，即“最久未访问”；`accessed` 相同（理论上不会发生）按 `key` 升序兜底；
   计入 `EvictedLRU`，日志标记 `淘汰[lru]` 并打印 victim 与依据。

规则不依赖 Go map 的随机遍历顺序，因此相同输入序列必然得到相同淘汰结果。
另外：对已存在的键执行 `Set` 是**原地刷新**（更新值/TTL/LRU），不改变条目数，不触发淘汰。

## 并发模型

- `Cache` 内由单一 `sync.Mutex` 保护 map 与 LRU 序号；计数使用 `atomic`。
  `go test -race` 下有并发读写压力测试。
- **同热点键回源只触发一次**：`GetOrLoad` 在本地未命中时通过 `singleflight.Group`
  合并——同键并发只有一个调用真正执行 `Loader.Load`，其余阻塞并共享同一结果（含错误）。
  真正回源数计入 `Loads`，共享等待数计入 `LoadShared`；`LoadResult.Shared=true`
  表示该结果来自合并等待。不同键互不影响。
- **加载失败不写残缺值**：只有 `Load` 成功才会写缓存；失败路径完全不触碰缓存数据。

## 后端不可用时的降级规则

`StoreConfig.Fallback`：

- `FallbackStale`（默认）：
  - 本地**新鲜**：直接返回（`OutcomeFreshCache`），不接触后端，后端故障无感知；
  - 本地缺失/过期且后端失败：
    - 后端返回 `ErrBackendUnavailable`（或被 `fmt.Errorf("...%w...", cache.ErrBackendUnavailable)`
      包装）或其他加载错误，且本地保留有上一次有效值（即使已过期）
      → 返回该旧值（`OutcomeStale`），**同时**返回 `*cache.Error`，
      其 `Kind` 说明原因、`StaleServed=true` 说明已降级，`StaleFallbacks` +1；
    - 本地无任何旧值 → 返回 `KindNoFallback` 错误（不静默、不编造数据）。
  - 后端明确 `ErrNotFound`：视为“键不存在”而非故障，**不降级**，并清掉本地过期条目，
    返回 `KindNotFound`。
- `FallbackOff`：后端失败一律返回明确错误，即使有过期旧值也不返回。

### 失败原因如何被区分

所有错误都是 `*cache.Error`，用 `errors.As` 取出，按 `Kind` 分支处理：

| Kind | 含义 |
| --- | --- |
| `KindNotFound` | 本地与后端均确认无此键 |
| `KindBackendUnavailable` | 后端不可用（网络/超时/熔断） |
| `KindLoadFailed` | 后端返回的其他加载错误 |
| `KindNoFallback` | 后端失败但没有可降级旧值（或策略不允许） |
| `KindWriteFailed` | 写穿透失败（本地未被污染） |
| `KindInvalidArgument` | 配置/参数非法 |

后端也可以直接返回 `*cache.Error` 指定 `Kind`；默认分类器对 `errors.Is(err, ErrXxx)`
的包装链同样生效。

## 写穿透与计数一致性（不静默丢写）

`Store.Put` 要求后端实现 `Writable[V]`，采用**先写后端、成功后才更新本地**：

- 后端写成功：更新本地缓存，`Writes` +1；
- 后端写失败：返回 `KindWriteFailed`，**本地缓存保持原值不变**，`WriteFailures` +1。
  因此不会出现“本地新值 / 后端旧值”的计数类数据前后不一致，也不会静默丢写。

`Stats` 提供原子一致快照（命中、未命中、过期、两类淘汰、回源、合并等待、加载失败、
降级、写成功/失败），任何时刻读取都可对账。

## 快速上手

```go
store, err := cache.NewStore[string](
    cache.LoaderFunc[string](func(ctx context.Context, key string) (string, error) {
        return loadFromDB(key) // 返回 cache.ErrNotFound / cache.ErrBackendUnavailable 表达语义
    }),
    cache.StoreConfig{
        Capacity:   1024,
        DefaultTTL: time.Minute,
        Clock:      cache.SystemClock{},   // 生产
        Fallback:   cache.FallbackStale,
        Logger:     cache.NewStdLogger(),  // 可选：打印判定依据
    },
)

r, err := store.GetOrLoad(ctx, "user:1")
switch r.Outcome {
case cache.OutcomeFreshCache: // 本地新鲜命中
case cache.OutcomeBackend:    // 回源成功
case cache.OutcomeStale:      // 降级旧值；err 中的 *cache.Error 带 StaleServed=true
}
```

## 验证方法

```bash
# 全量单测（含并发、过期边界、淘汰边界、降级与写穿透）
go test -race -v ./...

# 只看竞态
go test -race ./...

# 示例（可执行文档）
go test -run Example -v ./...
```

测试设计要点（结果可复现、失败原因可区分）：

- **过期边界**：`TestTTLExpiryBoundary` 用 `FakeClock` 精确推进到 `expireAt-1ns`、
  `expireAt`、`expireAt+1h`，断言半开区间与“过期不返回脏值”。
- **淘汰边界**：`TestLRUEvictionBoundary`（访问刷新 LRU 后的 victim）、
  `TestLRUEvictOldestInsert`（无访问时淘汰最早写入）、
  `TestEvictExpiredBeforeLRU`（容量将满先清扫过期而非淘汰新鲜）、
  `TestUpdateExistingDoesNotEvict`（原地刷新不淘汰）。
- **并发**：`TestConcurrentAccessSafe`（-race 下读写删混合 + 容量不被突破）、
  `TestSingleflightCoalescesConcurrentCalls` 与 `TestGetOrLoadConcurrentSingleLoad`
  （32 并发下真正回源恰为 1，等待者 `Shared=true`）。
- **降级与一致性**：`TestStaleFallbackOnBackendUnavailable`（旧值 + 错误枚举 + 重复降级不污染）、
  `TestFallbackOffReturnsError`、`TestNotFoundIsNotFallback`、
  `TestFailureIsolatesOtherKeys`（单键失败不影响其他键）、
  `TestPutWriteThroughAndFailureConsistency`（写失败本地不变、计数对账）。
- **日志**：测试注入的 logger 会把每条 `判定[fresh|expired|miss]`、
  `淘汰[lru|expired-sweep]`、`清扫[expired]`、`singleflight[leader|shared]`、
  `回源[ok|fail]`、`降级[stale|unavailable|off]`、`写穿透[ok|fail]` 及其依据
  打印到测试日志中（`go test -v` 可见），并对关键依据做包含断言。
