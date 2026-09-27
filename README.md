# local-cache-engine-go

带过期与容量上限的并发安全本地缓存存储层。容量、过期和并发访问下的
数据状态与失败结论都可被解释和复现。

## 快速开始

```go
c := cache.New(cache.Options{
    Capacity:   1024,              // 最大条目数，<=0 不限
    TTL:        5 * time.Minute,   // 条目存活时间，<=0 永不过期
    Loader:     myLoader,          // 未命中时回源
    AllowStale: true,              // 后端不可用时返回上一次有效值
    Logger:     myLogger,          // 命中/淘汰/过期判定日志
})
val, err := c.Get(ctx, "key")
c.Set("key", val)
c.Delete("key")
```

## 过期策略

- 过期判定只使用**单调时钟**（`Clock` 接口，默认基于 `time.Since` 的
  进程单调时间），不依赖可能回拨的系统墙上时钟。
- 条目写入时记录 `expiresAt = now + TTL`；读取时 `now >= expiresAt`
  即判定过期（边界含等于，见 `TestExpiryBoundary`）。
- 过期条目**绝不作为正常读取结果返回**（不返回脏值）：
  - `AllowStale=false`：过期即删除，按未命中处理。
  - `AllowStale=true`：过期条目保留为"上一次有效值"，仅用于后端
    不可用时的降级，正常读取仍会触发回源。
- 测试可用 `ManualClock` 手动推进时间，精确复现过期边界。

## 淘汰策略

- 容量达到上限时按 **LRU（最久未使用）** 淘汰：`Get` 命中与 `Set`
  都会把条目提升为最近使用；写入后条目数超过 `Capacity` 时，从
  最久未使用的一端逐个淘汰直到不超上限。
- 每次淘汰都打印日志：`EVICT key=... reason=capacity ...`，
  并计入 `Stats().Evictions`。

## 并发与回源

- 所有读写由互斥锁保护，`-race` 下验证通过。
- 同一热点键的并发回源采用**单飞（singleflight）**：只触发一次真实
  加载，其余调用等待并共享结果（`TestSingleflightHotKey` 断言
  loader 恰好被调用 1 次）。
- 加载失败**不写入任何残缺值**，也不影响其他键
  （`TestLoadFailureNoPartialWrite`）。

## 降级规则与失败原因区分

`Get` 的错误为 `*cache.Error`，通过 `Kind` 区分失败原因：

| Kind | 含义 |
|---|---|
| `ErrKindNotFound` | 键不存在且无回源/无旧值 |
| `ErrKindBackendUnavailable` | 回源返回（或包装）`cache.ErrBackendDown`，且无旧值可降级 |
| `ErrKindLoadFailed` | 回源普通失败（非后端不可用），且无旧值可降级 |
| `ErrKindStale` | 后端不可用，返回的是上一次有效值（值随错误一起返回） |

- Loader 返回 `fmt.Errorf("...: %w", cache.ErrBackendDown)` 即被识别为
  后端不可用，触发降级；其他错误按普通加载失败处理。
- 降级期间 `Set` 仍然生效，**不静默丢写**，计数类数据前后一致
  （`TestWritesNotLostDuringDegradation`）。
- `Stats()` 返回 hits / misses / evictions / expirations / loads /
  load_fails / stale_served 计数快照，用于核对行为。

## 验证方法

```sh
go test ./... -race -v        # 全部测试，日志含命中/淘汰/过期判定依据
go test ./... -race -count=3  # 重复运行验证可复现
```

测试覆盖：

- `TestExpiryBoundary`：过期点前 1ns 命中、恰到达过期点判定过期。
- `TestEvictionBoundary`：容量 2，访问顺序决定淘汰谁，结果确定。
- `TestSingleflightHotKey`：32 并发回源同一键，loader 只调 1 次。
- `TestLoadFailureNoPartialWrite`：失败不留残缺值、不影响其他键。
- `TestConcurrentReadWrite`：并发读写不变量（只读到完整值或明确未命中）。
- `TestStaleServedWhenBackendDown` / `TestBackendDownWithoutStale` /
  `TestStaleDisabled`：三种降级路径与失败原因区分。
- `TestWritesNotLostDuringDegradation`：降级期间写入不丢、计数一致。

单测日志中每个 `HIT` / `MISS` / `EXPIRED` / `EVICT` / `LOAD` /
`STALE-SERVE` 都打印判定依据（`now`、`expiresAt`、容量等），
失败结论可由日志与 `Stats()` 计数复现。
