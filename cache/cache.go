package cache

import (
	"context"
	"errors"
	"sync"
	"time"
)

// Loader 在缓存未命中时回源加载数据。
type Loader func(ctx context.Context, key string) ([]byte, error)

// ErrBackendDown 可由 Loader 返回（或被其包装），表示后端不可用，
// 与普通加载失败区分开，用于触发降级。
var ErrBackendDown = errors.New("backend unavailable")

// Options 配置缓存。
type Options struct {
	Capacity   int           // 最大条目数，<=0 表示不限制
	TTL        time.Duration // 条目存活时间，<=0 表示永不过期
	Clock      Clock         // 单调时钟，nil 使用真实单调时钟
	Loader     Loader        // 回源加载器，nil 表示不回源
	AllowStale bool          // 后端不可用时是否返回上一次有效值
	Logger     Logger        // 决策日志，nil 不输出
}

// Cache 是带过期与容量上限的并发安全本地缓存。
//
// 过期判定只使用单调时钟（Clock），不受系统墙上时钟回拨影响。
// 过期条目在 AllowStale 时保留在缓存中作为降级用的上一次有效值，
// 正常读取绝不会返回过期数据。
type Cache struct {
	capacity   int
	ttl        time.Duration
	clock      Clock
	loader     Loader
	allowStale bool
	logger     Logger

	mu    sync.Mutex
	items *lru

	// 单飞：同一键的并发回源只触发一次真实加载。
	sfMu sync.Mutex
	sf   map[string]*call

	stats Stats
}

type call struct {
	done chan struct{}
	val  []byte
	err  error
}

// New 创建缓存。
func New(opts Options) *Cache {
	c := &Cache{
		capacity:   opts.Capacity,
		ttl:        opts.TTL,
		clock:      opts.Clock,
		loader:     opts.Loader,
		allowStale: opts.AllowStale,
		logger:     opts.Logger,
		items:      newLRU(),
		sf:         make(map[string]*call),
	}
	if c.clock == nil {
		c.clock = NewRealClock()
	}
	if c.logger == nil {
		c.logger = nopLogger{}
	}
	return c
}

// Get 读取键值。
//
// 命中且未过期：返回值，err 为 nil。
// 未命中或已过期：触发单飞回源；回源失败时若 AllowStale 且存在上一次
// 有效值，返回该旧值并附带 Kind=ErrKindStale 的错误；否则返回带
// ErrKindNotFound / ErrKindBackendUnavailable / ErrKindLoadFailed 的
// 错误，调用方可据此区分失败原因。
func (c *Cache) Get(ctx context.Context, key string) ([]byte, error) {
	now := int64(c.clock.Now())

	c.mu.Lock()
	e := c.items.get(key)
	if e != nil && !e.expired(now) {
		c.stats.Hits.Add(1)
		c.logger.Logf("HIT key=%q now=%d expiresAt=%d", key, now, e.expiresAt)
		c.mu.Unlock()
		return clone(e.value), nil
	}
	var stale *entry
	if e != nil {
		// 已过期：正常读取不得返回脏值。
		c.stats.Expirations.Add(1)
		c.logger.Logf("EXPIRED key=%q now=%d expiresAt=%d", key, now, e.expiresAt)
		if c.allowStale {
			stale = e // 保留作为降级用的上一次有效值
		} else {
			c.items.remove(key)
		}
	} else {
		c.stats.Misses.Add(1)
		c.logger.Logf("MISS key=%q", key)
	}
	c.mu.Unlock()

	if c.loader == nil {
		return c.degrade(key, stale, nil)
	}

	val, err := c.load(ctx, key)
	if err == nil {
		return val, nil
	}
	return c.degrade(key, stale, err)
}

// load 单飞回源：同一键的并发调用只触发一次真实加载。
func (c *Cache) load(ctx context.Context, key string) ([]byte, error) {
	c.sfMu.Lock()
	if cl, ok := c.sf[key]; ok {
		c.sfMu.Unlock()
		c.logger.Logf("SINGLEFLIGHT-WAIT key=%q", key)
		<-cl.done
		return cl.val, cl.err
	}
	cl := &call{done: make(chan struct{})}
	c.sf[key] = cl
	c.sfMu.Unlock()

	c.stats.Loads.Add(1)
	c.logger.Logf("LOAD key=%q", key)
	v, err := c.loader(ctx, key)
	if err != nil {
		// 加载失败不得写入残缺值。
		c.stats.LoadFails.Add(1)
		c.logger.Logf("LOAD-FAIL key=%q err=%v", key, err)
	} else {
		c.Set(key, v)
	}
	cl.val, cl.err = v, err
	close(cl.done)

	c.sfMu.Lock()
	delete(c.sf, key)
	c.sfMu.Unlock()
	return v, err
}

// degrade 按降级规则处理读取失败。
func (c *Cache) degrade(key string, stale *entry, loadErr error) ([]byte, error) {
	if c.allowStale && stale != nil {
		c.stats.StaleServed.Add(1)
		c.logger.Logf("STALE-SERVE key=%q cause=%v", key, loadErr)
		return clone(stale.value), &Error{Kind: ErrKindStale, Key: key, Err: loadErr}
	}
	if loadErr == nil {
		return nil, &Error{Kind: ErrKindNotFound, Key: key}
	}
	kind := ErrKindLoadFailed
	if errors.Is(loadErr, ErrBackendDown) {
		kind = ErrKindBackendUnavailable
	}
	return nil, &Error{Kind: kind, Key: key, Err: loadErr}
}

// Set 写入键值。容量超限时按 LRU（最久未使用）淘汰。
// 本地写入始终生效，降级期间不会静默丢写。
func (c *Cache) Set(key string, value []byte) {
	var expiresAt int64
	if c.ttl > 0 {
		expiresAt = int64(c.clock.Now()) + int64(c.ttl)
	}
	e := &entry{key: key, value: clone(value), expiresAt: expiresAt}

	c.mu.Lock()
	c.items.set(key, e)
	c.logger.Logf("SET key=%q expiresAt=%d len=%d", key, expiresAt, c.items.len())
	for c.capacity > 0 && c.items.len() > c.capacity {
		victim := c.items.removeOldest()
		c.stats.Evictions.Add(1)
		c.logger.Logf("EVICT key=%q reason=capacity(cap=%d) evictedExpiresAt=%d",
			victim.key, c.capacity, victim.expiresAt)
	}
	c.mu.Unlock()
}

// Delete 删除键。
func (c *Cache) Delete(key string) {
	c.mu.Lock()
	c.items.remove(key)
	c.mu.Unlock()
}

// Stats 返回运行计数快照。
func (c *Cache) Stats() map[string]int64 { return c.stats.Snapshot() }

func clone(b []byte) []byte {
	if b == nil {
		return nil
	}
	out := make([]byte, len(b))
	copy(out, b)
	return out
}
