package cache

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// 同一热点键并发回源：真正的 fn 只执行一次，所有等待者共享同一结果。
func TestSingleflightCoalescesConcurrentCalls(t *testing.T) {
	g := NewGroup[int]()
	var calls atomic.Int32
	started := make(chan struct{})
	release := make(chan struct{})

	const n = 32
	var wg sync.WaitGroup
	vals := make([]int, n)
	errs := make([]error, n)
	shared := make([]bool, n)

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if i == 1 {
				close(started) // 尽量让 goroutine 都已就绪
			}
			vals[i], errs[i], shared[i] = g.Do("hot", func() (int, error) {
				calls.Add(1)
				<-release // 发起者阻塞，确保其余调用进入等待
				return 42, nil
			})
		}(i)
	}
	<-started
	time.Sleep(20 * time.Millisecond) // 等待其余调用注册到进行中的 call
	close(release)
	wg.Wait()

	if got := calls.Load(); got != 1 {
		t.Fatalf("真正加载次数必须为1, got=%d", got)
	}
	leaders, waiters := 0, 0
	for i := 0; i < n; i++ {
		if errs[i] != nil || vals[i] != 42 {
			t.Fatalf("调用%d结果错误: v=%d err=%v", i, vals[i], errs[i])
		}
		if shared[i] {
			waiters++
		} else {
			leaders++
		}
	}
	if leaders != 1 || waiters != n-1 {
		t.Fatalf("应有1个发起者和%d个等待者, got leaders=%d waiters=%d", n-1, leaders, waiters)
	}
}

// 失败结果同样被共享：加载失败一次，等待者拿到同一错误，不会重复回源。
func TestSingleflightSharesFailure(t *testing.T) {
	g := NewGroup[int]()
	var calls atomic.Int32
	wantErr := errors.New("boom")

	const n = 8
	var wg sync.WaitGroup
	barrier := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err, _ := g.Do("k", func() (int, error) {
				calls.Add(1)
				<-barrier
				return 0, wantErr
			})
			if !errors.Is(err, wantErr) {
				t.Errorf("应共享同一错误, got=%v", err)
			}
		}()
	}
	time.Sleep(20 * time.Millisecond)
	close(barrier)
	wg.Wait()

	if calls.Load() != 1 {
		t.Fatalf("失败加载也只应执行1次, got=%d", calls.Load())
	}
}

// 上一轮调用结束后，下一轮调用应重新执行（槽位被正确清理）。
func TestSingleflightSerialRounds(t *testing.T) {
	g := NewGroup[int]()
	var calls atomic.Int32
	fn := func() (int, error) {
		n := calls.Add(1)
		return int(n), nil
	}
	v1, _, s1 := g.Do("k", fn)
	v2, _, s2 := g.Do("k", fn)
	if v1 != 1 || v2 != 2 || s1 || s2 {
		t.Fatalf("两轮串行调用应分别执行: v1=%d v2=%d shared=%v/%v", v1, v2, s1, s2)
	}
}
