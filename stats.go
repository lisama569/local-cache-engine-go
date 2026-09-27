package cache

import "sync/atomic"

// Stats 保存缓存与降级层的运行计数，所有字段均为原子操作，可并发读取。
// 计数类指标只在“结论确定”后递增（例如写后端成功才计 Writes，
// 失败计 WriteFailures 且本地不变），因此任何时刻快照都前后一致、可解释。
type Stats struct {
	Hits           atomic.Int64 // 新鲜本地命中
	Misses         atomic.Int64 // 本地无新鲜值（含过期被当作未命中）
	Expired        atomic.Int64 // 读到已过期条目的次数
	EvictedLRU     atomic.Int64 // 因容量按 LRU 淘汰的条数
	EvictedExpired atomic.Int64 // 插入前清扫/淘汰的过期条数
	Loads          atomic.Int64 // 实际触发后端加载的次数
	LoadShared     atomic.Int64 // 被合并到其他加载者的等待次数
	LoadFailures   atomic.Int64 // 加载失败次数（不写缓存）
	StaleFallbacks atomic.Int64 // 后端失败后用旧值降级成功次数
	Writes         atomic.Int64 // 写穿透成功次数
	WriteFailures  atomic.Int64 // 写穿透失败次数（本地未变更）
}

// StatsSnapshot 是某一时刻计数的不可变副本。
type StatsSnapshot struct {
	Hits           int64
	Misses         int64
	Expired        int64
	EvictedLRU     int64
	EvictedExpired int64
	Loads          int64
	LoadShared     int64
	LoadFailures   int64
	StaleFallbacks int64
	Writes         int64
	WriteFailures  int64
}

// Snapshot 原子读取全部计数，得到一致快照。
func (s *Stats) Snapshot() StatsSnapshot {
	return StatsSnapshot{
		Hits:           s.Hits.Load(),
		Misses:         s.Misses.Load(),
		Expired:        s.Expired.Load(),
		EvictedLRU:     s.EvictedLRU.Load(),
		EvictedExpired: s.EvictedExpired.Load(),
		Loads:          s.Loads.Load(),
		LoadShared:     s.LoadShared.Load(),
		LoadFailures:   s.LoadFailures.Load(),
		StaleFallbacks: s.StaleFallbacks.Load(),
		Writes:         s.Writes.Load(),
		WriteFailures:  s.WriteFailures.Load(),
	}
}
