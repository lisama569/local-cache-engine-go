package cache

import (
	"sort"
	"sync"
	"time"
)

// Config 描述缓存参数。
type Config struct {
	Capacity   int           // 最大条目数，必须 > 0
	DefaultTTL time.Duration // 默认过期时间；<= 0 表示默认不过期
	Clock      Clock         // 必填，过期判定的唯一时间来源
	Logger     Logger        // 可为 nil（使用 NopLogger）
}

// Cache 是带 TTL 与容量上限（确定性 LRU 淘汰）的并发安全本地缓存。
//
// 并发：所有读写由同一把互斥锁保护，map 与 LRU 序号在锁内一起变更。
// 过期：只通过注入的 Clock 判定；已过期条目读取时按未命中处理，绝不返回脏值，
//
//	但条目暂时保留，供后端不可用时做旧值降级（见 Store）。
//
// 淘汰：插入新键且容量不足时，先按 (expireAt, key) 顺序清扫过期条目；
//
//	仍不足时再按“最久未访问”（accessed 逻辑序号最小，平局按 key 升序）淘汰，
//	规则完全确定，结果可复现。
type Cache[V any] struct {
	mu       sync.Mutex
	items    map[string]*entry[V]
	capacity int
	ttl      time.Duration
	clock    Clock
	logger   Logger
	seq      uint64 // 受 mu 保护的访问逻辑序号，单调递增
	stats    *Stats
}

// NewCache 构造缓存；config 非法时返回 KindInvalidArgument 错误。
func NewCache[V any](cfg Config, stats *Stats) (*Cache[V], error) {
	if cfg.Capacity <= 0 {
		return nil, &Error{Kind: KindInvalidArgument, Op: "cache.NewCache",
			Cause: sterr("capacity must be > 0")}
	}
	if cfg.Clock == nil {
		return nil, &Error{Kind: KindInvalidArgument, Op: "cache.NewCache",
			Cause: sterr("clock is required")}
	}
	if stats == nil {
		stats = &Stats{}
	}
	logger := Logger(NopLogger{})
	if cfg.Logger != nil {
		logger = cfg.Logger
	}
	return &Cache[V]{
		items:    make(map[string]*entry[V]),
		capacity: cfg.Capacity,
		ttl:      cfg.DefaultTTL,
		clock:    cfg.Clock,
		logger:   logger,
		stats:    stats,
	}, nil
}

// HitState 是读取判定结论。
type HitState int

const (
	HitFresh   HitState = iota // 命中且未过期
	HitExpired                 // 命中但已过期（Value 为零值，不返回脏数据）
	Miss                       // 不存在
)

func (s HitState) String() string {
	switch s {
	case HitFresh:
		return "fresh"
	case HitExpired:
		return "expired"
	default:
		return "miss"
	}
}

// GetResult 描述一次读取的判定结论，便于上层与日志解释“为什么命中/未命中”。
type GetResult[V any] struct {
	Value    V
	State    HitState
	ExpireAt time.Time
}

// Get 读取键。
//   - 新鲜条目：刷新 LRU 序号并返回值（HitFresh）；
//   - 过期条目：不返回其值（零值 + HitExpired），条目保留供降级使用；
//   - 不存在：零值 + Miss。
func (c *Cache[V]) Get(key string) GetResult[V] {
	now := c.clock.Now()
	c.mu.Lock()
	defer c.mu.Unlock()

	e, ok := c.items[key]
	if !ok {
		c.stats.Misses.Add(1)
		c.logger.Printf("判定[miss] key=%q now=%s 原因: 本地不存在该键", key, now.Format(time.RFC3339Nano))
		return GetResult[V]{State: Miss}
	}
	if e.expiredAt(now) {
		c.stats.Expired.Add(1)
		c.stats.Misses.Add(1)
		c.logger.Printf("判定[expired] key=%q now=%s expireAt=%s 依据: now>=expireAt, 不返回旧值(按未命中处理), 旧值保留供降级",
			key, now.Format(time.RFC3339Nano), fmtExpireAt(e))
		return GetResult[V]{State: HitExpired, ExpireAt: e.expireAt}
	}
	c.seq++
	e.accessed = c.seq
	c.stats.Hits.Add(1)
	c.logger.Printf("判定[fresh] key=%q now=%s expireAt=%s accessed=#%d 依据: now<expireAt, 命中并刷新LRU",
		key, now.Format(time.RFC3339Nano), fmtExpireAt(e), e.accessed)
	return GetResult[V]{Value: e.value, State: HitFresh, ExpireAt: e.expireAt}
}

// Set 写入/刷新键，使用默认 TTL。
func (c *Cache[V]) Set(key string, value V) {
	c.SetWithTTL(key, value, c.ttl)
}

// SetWithTTL 写入/刷新键并指定本次 TTL；ttl <= 0 表示永不过期。
func (c *Cache[V]) SetWithTTL(key string, value V, ttl time.Duration) {
	now := c.clock.Now()
	expireAt := expireAtFromTTL(now, ttl)

	c.mu.Lock()
	defer c.mu.Unlock()

	c.seq++
	if e, ok := items(c, key); ok {
		// 已存在（无论是否过期）：原地刷新，不改变条目数，无需淘汰。
		e.value = value
		e.expireAt = expireAt
		e.accessed = c.seq
		c.logger.Printf("写入[update] key=%q expireAt=%s accessed=#%d 依据: 键已存在, 原地刷新不触发淘汰",
			key, fmtExpireAt(e), e.accessed)
		return
	}

	c.evictIfNeededLocked(key, now)

	e := &entry[V]{key: key, value: value, expireAt: expireAt, accessed: c.seq}
	c.items[key] = e
	c.logger.Printf("写入[insert] key=%q expireAt=%s accessed=#%d 当前条目数=%d/%d",
		key, fmtExpireAt(e), e.accessed, len(c.items), c.capacity)
}

// evictIfNeededLocked 在插入前腾出空间：先清扫过期条目，再按 LRU 淘汰。
// 调用方必须持有 c.mu。
func (c *Cache[V]) evictIfNeededLocked(incoming string, now time.Time) {
	if len(c.items) < c.capacity {
		return
	}
	// 第一步：确定性地清扫过期条目，顺序为 (expireAt 升序, key 升序)。
	expired := make([]*entry[V], 0)
	for _, e := range c.items {
		if e.expiredAt(now) {
			expired = append(expired, e)
		}
	}
	sort.Slice(expired, func(i, j int) bool {
		if !expired[i].expireAt.Equal(expired[j].expireAt) {
			return expired[i].expireAt.Before(expired[j].expireAt)
		}
		return expired[i].key < expired[j].key
	})
	for _, e := range expired {
		if len(c.items) < c.capacity {
			return
		}
		delete(c.items, e.key)
		c.stats.EvictedExpired.Add(1)
		c.logger.Printf("淘汰[expired-sweep] victim=%q expireAt=%s now=%s 依据: 容量将满(%d/%d) 且 now>=expireAt",
			e.key, fmtExpireAt(e), now.Format(time.RFC3339Nano), len(c.items)+1, c.capacity)
	}

	// 第二步：仍满，则按 LRU 淘汰：accessed 最小，平局 key 升序。
	for len(c.items) >= c.capacity {
		victim := lruVictim(c.items)
		delete(c.items, victim.key)
		c.stats.EvictedLRU.Add(1)
		c.logger.Printf("淘汰[lru] victim=%q accessed=#%d expireAt=%s 依据: accessed 为全局最小(最久未访问), 平局按key升序; 容量=%d/%d 为新键=%q腾位",
			victim.key, victim.accessed, fmtExpireAt(victim), c.capacity, c.capacity, incoming)
	}
}

// lruVictim 在当前条目中找出 LRU 牺牲者（不依赖 map 遍历顺序）。
func lruVictim[V any](m map[string]*entry[V]) *entry[V] {
	var victim *entry[V]
	for _, e := range m {
		if victim == nil || e.accessed < victim.accessed ||
			(e.accessed == victim.accessed && e.key < victim.key) {
			victim = e
		}
	}
	return victim
}

// Delete 删除键，返回删除前是否存在（含过期条目也会被清除）。
func (c *Cache[V]) Delete(key string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.items[key]
	delete(c.items, key)
	if ok {
		c.logger.Printf("删除 key=%q 依据: 显式失效", key)
	}
	return ok
}

// Len 返回当前未过期条目数量。
func (c *Cache[V]) Len() int {
	now := c.clock.Now()
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, e := range c.items {
		if !e.expiredAt(now) {
			n++
		}
	}
	return n
}

// Sweep 主动清扫所有过期条目并返回清扫条数；不依赖后台协程，行为确定可复现。
func (c *Cache[V]) Sweep() int {
	now := c.clock.Now()
	c.mu.Lock()
	defer c.mu.Unlock()

	expired := make([]*entry[V], 0)
	for _, e := range c.items {
		if e.expiredAt(now) {
			expired = append(expired, e)
		}
	}
	sort.Slice(expired, func(i, j int) bool {
		if !expired[i].expireAt.Equal(expired[j].expireAt) {
			return expired[i].expireAt.Before(expired[j].expireAt)
		}
		return expired[i].key < expired[j].key
	})
	for _, e := range expired {
		delete(c.items, e.key)
		c.stats.EvictedExpired.Add(1)
		c.logger.Printf("清扫[expired] key=%q expireAt=%s now=%s 依据: now>=expireAt",
			e.key, fmtExpireAt(e), now.Format(time.RFC3339Nano))
	}
	return len(expired)
}

// peekExpired 取出条目的当前值而不刷新 LRU、不计命中。
// 仅供 Store 在后端失败时获取“上一次有效值”降级：
// 存在返回 HitFresh/HitExpired（按当前时间），不存在返回 Miss。
func (c *Cache[V]) peekExpired(key string) GetResult[V] {
	now := c.clock.Now()
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.items[key]
	if !ok {
		return GetResult[V]{State: Miss}
	}
	state := HitFresh
	if e.expiredAt(now) {
		state = HitExpired
	}
	return GetResult[V]{Value: e.value, State: state, ExpireAt: e.expireAt}
}

func items[V any](c *Cache[V], key string) (*entry[V], bool) {
	e, ok := c.items[key]
	return e, ok
}

func fmtExpireAt[V any](e *entry[V]) string {
	if e.expireAt.IsZero() {
		return "never"
	}
	return e.expireAt.Format(time.RFC3339Nano)
}
