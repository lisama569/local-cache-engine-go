package cache

import (
	"context"
	"time"
)

// Loader 是回源后端接口。
type Loader[V any] interface {
	Load(ctx context.Context, key string) (V, error)
}

// LoaderFunc 便于用函数实现 Loader。
type LoaderFunc[V any] func(ctx context.Context, key string) (V, error)

func (f LoaderFunc[V]) Load(ctx context.Context, key string) (V, error) {
	return f(ctx, key)
}

// Writable 表示支持写穿透的后端。Store.Put 要求后端实现该接口。
type Writable[V any] interface {
	Store(ctx context.Context, key string, value V) error
}

// FallbackMode 决定后端失败时的降级策略。
type FallbackMode int

const (
	// FallbackStale：后端不可用/加载失败时，若本地保留有上一次有效值（即使已过期），
	// 则返回该旧值并同时返回带 StaleServed=true 的错误说明原因；无旧值则明确报错。
	// 注意：后端明确返回 ErrNotFound 不属于“不可用”，不会降级。
	FallbackStale FallbackMode = iota
	// FallbackOff：不降级，后端失败一律返回明确错误。
	FallbackOff
)

// StoreConfig 是降级存储层配置。
type StoreConfig struct {
	Capacity   int           // 本地缓存容量，必须 > 0
	DefaultTTL time.Duration // 默认过期时间；<= 0 表示不过期
	Clock      Clock         // 必填
	Logger     Logger        // 可选
	Fallback   FallbackMode
	LoadTTL    time.Duration // 回源结果写入缓存的 TTL；<= 0 时用 DefaultTTL
}

// Outcome 区分 GetOrLoad 的结论来源。
type Outcome int

const (
	OutcomeFreshCache Outcome = iota // 新鲜本地命中，未接触后端
	OutcomeBackend                   // 回源成功并写入缓存
	OutcomeStale                     // 后端失败，降级返回上一次有效值
)

func (o Outcome) String() string {
	switch o {
	case OutcomeFreshCache:
		return "fresh-cache"
	case OutcomeBackend:
		return "backend"
	case OutcomeStale:
		return "stale-fallback"
	default:
		return "unknown"
	}
}

// LoadResult 是 GetOrLoad 的可解释结论。
type LoadResult[V any] struct {
	Value   V
	Outcome Outcome
	Key     string
	// Shared 为 true 表示本次未真正回源，而是共享了同键并发中的另一次加载结果。
	Shared bool
}

// Store 组合本地缓存、回源合并（singleflight）与降级规则。
type Store[V any] struct {
	cache    *Cache[V]
	loader   Loader[V]
	group    *Group[V]
	stats    *Stats
	fallback FallbackMode
	loadTTL  time.Duration
	logger   Logger
}

// NewStore 构造降级存储层；loader 为 nil 或配置非法时返回错误。
func NewStore[V any](loader Loader[V], cfg StoreConfig) (*Store[V], error) {
	if loader == nil {
		return nil, &Error{Kind: KindInvalidArgument, Op: "cache.NewStore",
			Cause: sterr("loader is required")}
	}
	if cfg.Clock == nil {
		cfg.Clock = SystemClock{}
	}
	stats := &Stats{}
	c, err := NewCache[V](Config{
		Capacity:   cfg.Capacity,
		DefaultTTL: cfg.DefaultTTL,
		Clock:      cfg.Clock,
		Logger:     cfg.Logger,
	}, stats)
	if err != nil {
		return nil, err
	}
	logger := Logger(NopLogger{})
	if cfg.Logger != nil {
		logger = cfg.Logger
	}
	return &Store[V]{
		cache:    c,
		loader:   loader,
		group:    NewGroup[V](),
		stats:    stats,
		fallback: cfg.Fallback,
		loadTTL:  cfg.LoadTTL,
		logger:   logger,
	}, nil
}

// Stats 返回计数快照。
func (s *Store[V]) Stats() StatsSnapshot { return s.stats.Snapshot() }

// GetOrLoad 的判定流程（每一步都打印依据，结果可复现）：
//  1. 查本地：新鲜命中直接返回（OutcomeFreshCache），不触发回源；
//  2. 否则单飞回源：同键并发只真正加载一次，等待者共享结果；
//  3. 加载成功：写入本地后返回（OutcomeBackend）；加载失败不写任何值；
//  4. 加载失败：按 FallbackMode 决定返回上一次有效值（OutcomeStale，
//     且错误带 StaleServed 标记）或明确报错。失败原因通过 *Error.Kind 区分。
func (s *Store[V]) GetOrLoad(ctx context.Context, key string) (LoadResult[V], error) {
	if key == "" {
		return LoadResult[V]{}, &Error{Kind: KindInvalidArgument, Op: "cache.Store.GetOrLoad",
			Key: key, Cause: sterr("empty key")}
	}

	if r := s.cache.Get(key); r.State == HitFresh {
		return LoadResult[V]{Value: r.Value, Outcome: OutcomeFreshCache, Key: key}, nil
	}

	val, err, shared := s.group.Do(key, func() (V, error) {
		return s.loader.Load(ctx, key)
	})
	if shared {
		s.stats.LoadShared.Add(1)
		s.logger.Printf("singleflight[shared] key=%q 依据: 已有同键加载进行中, 合并等待并共享结果", key)
	} else {
		s.stats.Loads.Add(1)
		s.logger.Printf("singleflight[leader] key=%q 依据: 首个调用者, 真正回源一次", key)
	}

	if err == nil {
		// 仅在成功路径写缓存：失败绝不写入残缺值。
		ttl := s.loadTTL
		if ttl <= 0 {
			ttl = s.cache.ttl
		}
		s.cache.SetWithTTL(key, val, ttl)
		s.logger.Printf("回源[ok] key=%q 依据: 加载成功, 已写入缓存 ttl=%s", key, ttlStr(ttl))
		return LoadResult[V]{Value: val, Outcome: OutcomeBackend, Key: key, Shared: shared}, nil
	}

	s.stats.LoadFailures.Add(1)
	ce := classifyLoadError("cache.Store.GetOrLoad", key, err)
	s.logger.Printf("回源[fail] key=%q kind=%s 依据: 后端返回错误, 不写入缓存; err=%v", key, ce.Kind, err)

	// 后端确认“键不存在”：本地的过期旧值也应作废，且这不是需要降级的故障。
	if ce.Kind == KindNotFound {
		s.cache.Delete(key)
		return LoadResult[V]{Key: key}, ce
	}

	// 其余失败（不可用/加载错误）：按降级策略处理。
	if s.fallback == FallbackStale {
		if r := s.cache.peekExpired(key); r.State != Miss {
			s.stats.StaleFallbacks.Add(1)
			ce.StaleServed = true
			s.logger.Printf("降级[stale] key=%q kind=%s 依据: 后端失败且本地保留上次有效值(state=%s), 返回旧值并附带原因错误",
				key, ce.Kind, r.State)
			return LoadResult[V]{Value: r.Value, Outcome: OutcomeStale, Key: key}, ce
		}
		s.logger.Printf("降级[unavailable] key=%q kind=%s 依据: 后端失败且本地无任何旧值, 明确报错(不静默)",
			key, ce.Kind)
		return LoadResult[V]{Key: key}, &Error{Kind: KindNoFallback, Op: ce.Op, Key: key,
			Cause: ce}
	}

	s.logger.Printf("降级[off] key=%q kind=%s 依据: FallbackOff, 直接返回错误不使用旧值", key, ce.Kind)
	return LoadResult[V]{Key: key}, ce
}

// Put 写穿透：必须先写后端成功后才更新本地缓存。
// 后端失败时本地缓存保持不变并返回 KindWriteFailed 错误——
// 绝不静默丢写，也不会出现“本地新值/后端旧值”的计数不一致。
// 后端需要实现 Writable[V]。
func (s *Store[V]) Put(ctx context.Context, key string, value V) error {
	if key == "" {
		return &Error{Kind: KindInvalidArgument, Op: "cache.Store.Put",
			Cause: sterr("empty key")}
	}
	w, ok := s.loader.(Writable[V])
	if !ok {
		return &Error{Kind: KindInvalidArgument, Op: "cache.Store.Put", Key: key,
			Cause: sterr("backend does not implement Writable")}
	}
	if err := w.Store(ctx, key, value); err != nil {
		s.stats.WriteFailures.Add(1)
		s.logger.Printf("写穿透[fail] key=%q 依据: 后端写入失败, 本地缓存保持不变(不静默丢写); err=%v",
			key, err)
		return &Error{Kind: KindWriteFailed, Op: "cache.Store.Put", Key: key, Cause: err}
	}
	s.cache.Set(key, value)
	s.stats.Writes.Add(1)
	s.logger.Printf("写穿透[ok] key=%q 依据: 后端写入成功后才更新本地缓存", key)
	return nil
}

// Invalidate 删除本地缓存条目（不触碰后端）。
func (s *Store[V]) Invalidate(key string) bool {
	return s.cache.Delete(key)
}

func ttlStr(d time.Duration) string {
	if d <= 0 {
		return "never"
	}
	return d.String()
}
