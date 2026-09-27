package cache

import "time"

// Clock 抽象“当前时间”的获取方式。
//
// 过期判定只通过该接口进行，不直接调用 time.Now，因而：
//   - 测试中可注入 FakeClock，得到完全确定、可复现的过期边界；
//   - 生产中使用 SystemClock。time.Now 返回的 time.Time 携带单调时钟
//     (monotonic clock) 读数，两个这样的值做 Before/Sub 比较时走的是
//     单调时钟，不受 NTP 等引起的墙上时钟回拨影响，不会把新值误判为过期。
type Clock interface {
	Now() time.Time
}

// SystemClock 基于 time.Now 的系统时钟（保留单调读数）。
type SystemClock struct{}

func (SystemClock) Now() time.Time { return time.Now() }

// FakeClock 是测试用的确定性手动时钟：时间只会在调用 Advance/Set 时变化，
// 不存在任何后台流逝，因此过期相关测试结果严格可复现。
type FakeClock struct {
	now time.Time
}

// NewFakeClock 从给定时刻构造假时钟。
func NewFakeClock(start time.Time) *FakeClock { return &FakeClock{now: start} }

func (c *FakeClock) Now() time.Time { return c.now }

// Advance 让假时钟向前推进 d；d 为负会被忽略（不允许回拨，模拟真实单调语义）。
func (c *FakeClock) Advance(d time.Duration) {
	if d > 0 {
		c.now = c.now.Add(d)
	}
}

// Set 直接设置当前时刻；仅用于测试搭建边界场景。
func (c *FakeClock) Set(t time.Time) { c.now = t }
