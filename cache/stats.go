package cache

import "sync/atomic"

// Stats 记录缓存运行计数，用于解释命中、淘汰与过期行为。
type Stats struct {
	Hits        atomic.Int64
	Misses      atomic.Int64
	Evictions   atomic.Int64
	Expirations atomic.Int64
	Loads       atomic.Int64
	LoadFails   atomic.Int64
	StaleServed atomic.Int64
}

// Snapshot 返回当前计数的快照。
func (s *Stats) Snapshot() map[string]int64 {
	return map[string]int64{
		"hits":         s.Hits.Load(),
		"misses":       s.Misses.Load(),
		"evictions":    s.Evictions.Load(),
		"expirations":  s.Expirations.Load(),
		"loads":        s.Loads.Load(),
		"load_fails":   s.LoadFails.Load(),
		"stale_served": s.StaleServed.Load(),
	}
}
