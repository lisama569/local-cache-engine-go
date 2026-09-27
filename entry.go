package cache

import "time"

// entry 是缓存中的单个键值条目（包内使用）。
type entry[V any] struct {
	key      string
	value    V
	expireAt time.Time // 零值表示永不过期
	accessed uint64    // 最近一次访问/写入的逻辑序号，LRU 依据
}

// expiredAt 判断在时刻 now 该条目是否已过期。
//
// 边界规则：有效期是半开区间 [写入时刻, expireAt)。
// 即 now == expireAt 时判定为“已过期”，保证边界两侧结论固定、可复现。
// 比较走 time.Time.Before；FakeClock 与 SystemClock 的单调读数都满足单调推进。
func (e *entry[V]) expiredAt(now time.Time) bool {
	if e.expireAt.IsZero() {
		return false // 永不过期
	}
	return !now.Before(e.expireAt) // now >= expireAt
}

// ttlFor 计算写入时的绝对过期时刻：ttl <= 0 表示永不过期（零值）。
func expireAtFromTTL(now time.Time, ttl time.Duration) time.Time {
	if ttl <= 0 {
		return time.Time{}
	}
	return now.Add(ttl)
}
