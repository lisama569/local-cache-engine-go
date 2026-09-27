package cache

import (
	"sync"
	"time"
)

// Clock 返回单调递增的时间（纳秒），不依赖可能回拨的系统墙上时钟。
type Clock interface {
	Now() time.Duration
}

// realClock 基于进程启动后的单调时钟。
type realClock struct {
	start time.Time
}

// NewRealClock 返回基于单调时钟的 Clock。
func NewRealClock() Clock {
	return &realClock{start: time.Now()}
}

func (c *realClock) Now() time.Duration {
	return time.Since(c.start)
}

// ManualClock 是测试用的手动时钟，只能向前推进。
type ManualClock struct {
	mu  sync.Mutex
	now time.Duration
}

// NewManualClock 创建手动时钟。
func NewManualClock() *ManualClock {
	return &ManualClock{}
}

// Now 返回当前手动时间。
func (c *ManualClock) Now() time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// Advance 将时钟向前推进 d。
func (c *ManualClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now += d
}
