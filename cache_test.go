package cache

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// 过期边界：now < expireAt 有效；now == expireAt 即过期（半开区间），
// 且读到过期数据必须得到零值而非脏值。全部用 FakeClock，结论确定可复现。
func TestTTLExpiryBoundary(t *testing.T) {
	clk := NewFakeClock(time.Date(2026, 9, 27, 10, 0, 0, 0, time.UTC))
	c, st, log := newTestCache[string](t, 4, 10*time.Second, clk)

	c.Set("k1", "v1")

	if r := c.Get("k1"); r.State != HitFresh || r.Value != "v1" {
		t.Fatalf("未到期应新鲜命中, got=%+v", r)
	}

	clk.Advance(10*time.Second - 1) // now = expireAt - 1ns：仍有效
	if r := c.Get("k1"); r.State != HitFresh {
		t.Fatalf("边界前1ns应仍有效, got=%s", r.State)
	}

	clk.Advance(1) // now == expireAt：必须判定过期
	r := c.Get("k1")
	if r.State != HitExpired || r.Value != "" {
		t.Fatalf("now==expireAt 应判过期且返回零值(禁止脏值), got state=%s value=%q", r.State, r.Value)
	}
	if got := st.Snapshot().Expired; got != 1 {
		t.Fatalf("过期读数应为1, got=%d", got)
	}

	clk.Advance(time.Hour) // 远超过期时刻
	if r := c.Get("k1"); r.State != HitExpired || r.Value != "" {
		t.Fatalf("过期后应持续判过期且不返回脏值, got=%+v", r)
	}
	if !log.containsAll("判定[fresh]", "判定[expired]", "now>=expireAt") {
		t.Fatalf("日志缺少命中/过期判定依据:\n%s", log.String())
	}
}

// SetWithTTL 允许单键独立 TTL；ttl<=0 永不过期（FakeClock 任意推进都有效）。
func TestSetWithTTLAndNeverExpire(t *testing.T) {
	clk := NewFakeClock(time.Now())
	c, _, _ := newTestCache[string](t, 4, 0, clk)

	c.SetWithTTL("short", "s", time.Millisecond)
	c.SetWithTTL("forever", "f", 0)

	clk.Advance(time.Hour)
	if r := c.Get("short"); r.State != HitExpired {
		t.Fatalf("short 应过期, got=%s", r.State)
	}
	if r := c.Get("forever"); r.State != HitFresh || r.Value != "f" {
		t.Fatalf("ttl=0 应永不过期, got=%+v", r)
	}
}

// 淘汰边界 1：容量满且无过期条目时，按 LRU（accessed 最小）淘汰；
// 访问会刷新 LRU 顺序。
func TestLRUEvictionBoundary(t *testing.T) {
	clk := NewFakeClock(time.Now())
	c, st, log := newTestCache[string](t, 3, 0, clk)

	c.Set("a", "1")                   // accessed #1
	c.Set("b", "2")                   // #2
	c.Set("c", "3")                   // #3
	if c.Get("a").State != HitFresh { // a -> #4，最久未访问变为 b
		t.Fatal("a 应命中")
	}

	c.Set("d", "4") // 容量满：LRU 牺牲者必须是 b

	if r := c.Get("b"); r.State != Miss {
		t.Fatalf("b 应被 LRU 淘汰, got=%s", r.State)
	}
	for _, k := range []string{"a", "c", "d"} {
		if r := c.Get(k); r.State != HitFresh {
			t.Fatalf("%s 应仍在缓存中, got=%s", k, r.State)
		}
	}
	snap := st.Snapshot()
	if snap.EvictedLRU != 1 || snap.EvictedExpired != 0 {
		t.Fatalf("淘汰计数不符: %+v", snap)
	}
	if !log.containsAll("淘汰[lru]", `victim="b"`, "accessed 为全局最小") {
		t.Fatalf("日志缺少 LRU 淘汰依据:\n%s", log.String())
	}
}

// 淘汰边界 2：没有任何访问时，淘汰的是最早写入的键（平局按 key 之前的序号）。
func TestLRUEvictOldestInsert(t *testing.T) {
	clk := NewFakeClock(time.Now())
	c, _, _ := newTestCache[string](t, 2, 0, clk)
	c.Set("a", "1")
	c.Set("b", "2")
	c.Set("c", "3")
	if c.Get("a").State != Miss || c.Get("b").State != HitFresh || c.Get("c").State != HitFresh {
		t.Fatalf("应淘汰最早写入的 a")
	}
}

// 淘汰边界 3：容量将满时优先清扫过期条目，而非淘汰新鲜 LRU 条目。
func TestEvictExpiredBeforeLRU(t *testing.T) {
	clk := NewFakeClock(time.Now())
	c, st, log := newTestCache[string](t, 2, time.Hour, clk)

	c.SetWithTTL("a", "1", time.Nanosecond)
	c.Set("b", "2")               // 容量=2/2，b 新鲜
	clk.Advance(time.Millisecond) // a 已过期

	c.Set("c", "3") // 应先清扫过期的 a 为 c 腾位，b 不受影响

	if r := c.Get("a"); r.State != HitExpired && r.State != Miss {
		t.Fatalf("过期的 a 应被清扫, got=%s", r.State)
	}
	if r := c.Get("b"); r.State != HitFresh {
		t.Fatalf("新鲜的 b 不应被淘汰, got=%s", r.State)
	}
	if r := c.Get("c"); r.State != HitFresh {
		t.Fatalf("新插入的 c 应存在, got=%s", r.State)
	}
	if snap := st.Snapshot(); snap.EvictedExpired != 1 || snap.EvictedLRU != 0 {
		t.Fatalf("应只发生1次过期清扫, got=%+v", snap)
	}
	if !log.containsAll("淘汰[expired-sweep]", `victim="a"`) {
		t.Fatalf("日志缺少过期清扫淘汰依据:\n%s", log.String())
	}
}

// 容量已满但写入的是已存在键：原地刷新，不触发任何淘汰。
func TestUpdateExistingDoesNotEvict(t *testing.T) {
	clk := NewFakeClock(time.Now())
	c, st, _ := newTestCache[string](t, 1, time.Hour, clk)
	c.Set("a", "1")
	c.Set("a", "1-updated")
	if c.Len() != 1 {
		t.Fatalf("条目数应为1, got=%d", c.Len())
	}
	if r := c.Get("a"); r.Value != "1-updated" {
		t.Fatalf("应读到刷新后的值, got=%q", r.Value)
	}
	if snap := st.Snapshot(); snap.EvictedLRU != 0 {
		t.Fatalf("原地刷新不应淘汰, %+v", snap)
	}
}

// Sweep 主动清扫过期条目，顺序与计数确定。
func TestSweep(t *testing.T) {
	clk := NewFakeClock(time.Now())
	c, st, log := newTestCache[string](t, 8, 10*time.Second, clk)
	c.Set("a", "1")
	c.Set("b", "2")
	c.SetWithTTL("c", "3", 100*time.Hour)
	clk.Advance(10 * time.Second) // a,b 在边界过期；c 仍新鲜

	if n := c.Sweep(); n != 2 {
		t.Fatalf("应清扫2条, got=%d", n)
	}
	if c.Len() != 1 {
		t.Fatalf("清扫后应剩1条, got=%d", c.Len())
	}
	if snap := st.Snapshot(); snap.EvictedExpired != 2 {
		t.Fatalf("清扫计数应为2, got=%+v", snap)
	}
	if !log.containsAll("清扫[expired]") {
		t.Fatalf("日志缺少清扫依据:\n%s", log.String())
	}
}

// 并发读写压力：在 -race 下确保无数据竞争、无 panic、长度始终受容量约束。
func TestConcurrentAccessSafe(t *testing.T) {
	clk := NewFakeClock(time.Now())
	c, _, _ := newTestCache[int](t, 8, 0, clk)

	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				k := fmt.Sprintf("k%d", i%12)
				c.Set(k, g*1000+i)
				_ = c.Get(k)
				if i%50 == 0 {
					c.Delete(k)
				}
			}
		}(g)
	}
	wg.Wait()
	if c.Len() > 8 {
		t.Fatalf("容量上限被突破: %d", c.Len())
	}
}

func TestNewCacheRejectsInvalidConfig(t *testing.T) {
	if _, err := NewCache[string](Config{Capacity: 0, Clock: SystemClock{}}, nil); err == nil {
		t.Fatal("capacity=0 应报错")
	}
	if _, err := NewCache[string](Config{Capacity: 1}, nil); err == nil {
		t.Fatal("缺 Clock 应报错")
	}
}
