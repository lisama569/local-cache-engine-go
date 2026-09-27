package cache

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestSingleflightHotKey 同一热点键的并发回源只触发一次真实加载。
func TestSingleflightHotKey(t *testing.T) {
	var calls atomic.Int64
	release := make(chan struct{})
	loader := func(ctx context.Context, key string) ([]byte, error) {
		calls.Add(1)
		<-release // 阻塞直到所有 goroutine 都已发起请求
		return []byte("loaded"), nil
	}
	c := New(Options{Capacity: 8, Loader: loader, Logger: testLogger{t}})

	const n = 32
	var wg sync.WaitGroup
	results := make([][]byte, n)
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			v, err := c.Get(context.Background(), "hot")
			results[i], errs[i] = v, err
		}(i)
	}
	// 等所有 goroutine 进入等待后放行加载。
	time.Sleep(50 * time.Millisecond)
	close(release)
	wg.Wait()

	for i := range results {
		if errs[i] != nil || string(results[i]) != "loaded" {
			t.Fatalf("goroutine %d: got %q, %v", i, results[i], errs[i])
		}
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("loader called %d times, want exactly 1", got)
	}
	if got := c.Stats()["loads"]; got != 1 {
		t.Fatalf("loads=%d, want 1", got)
	}
	t.Logf("stats=%v", c.Stats())
}

// TestLoadFailureNoPartialWrite 加载失败不得写入残缺值，且不影响其他键。
func TestLoadFailureNoPartialWrite(t *testing.T) {
	boom := errors.New("boom")
	loader := func(ctx context.Context, key string) ([]byte, error) {
		if key == "bad" {
			return nil, boom
		}
		return []byte("ok-" + key), nil
	}
	c := New(Options{Capacity: 8, Loader: loader, Logger: testLogger{t}})

	c.Set("good", []byte("keep"))

	if _, err := c.Get(context.Background(), "bad"); errKind(err) != ErrKindLoadFailed {
		t.Fatalf("kind=%v, want ErrKindLoadFailed", errKind(err))
	}
	// 失败的键不得留下残缺值：再次读取仍应触发加载并失败。
	if _, err := c.Get(context.Background(), "bad"); errKind(err) != ErrKindLoadFailed {
		t.Fatalf("second read kind=%v, want ErrKindLoadFailed", errKind(err))
	}
	if got := c.Stats()["load_fails"]; got != 2 {
		t.Fatalf("load_fails=%d, want 2", got)
	}
	// 其他键不受影响。
	if v, err := c.Get(context.Background(), "good"); err != nil || string(v) != "keep" {
		t.Fatalf("good key affected: %q, %v", v, err)
	}
	if v, err := c.Get(context.Background(), "other"); err != nil || string(v) != "ok-other" {
		t.Fatalf("other key: %q, %v", v, err)
	}
	t.Logf("stats=%v", c.Stats())
}

// TestConcurrentReadWrite 并发读写下的安全性（配合 -race 运行）。
// 不变量：读到的值要么是某个已写入的完整值，要么是明确的未命中错误。
func TestConcurrentReadWrite(t *testing.T) {
	c := New(Options{Capacity: 16, TTL: time.Second, Logger: testLogger{t}})
	const writers = 4
	const readers = 8
	const rounds = 200

	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < rounds; i++ {
				key := fmt.Sprintf("k%d", i%16)
				c.Set(key, []byte(fmt.Sprintf("w%d-%d", w, i)))
			}
		}(w)
	}
	var badReads atomic.Int64
	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < rounds; i++ {
				key := fmt.Sprintf("k%d", i%16)
				v, err := c.Get(context.Background(), key)
				if err != nil {
					if errKind(err) != ErrKindNotFound {
						badReads.Add(1)
					}
					continue
				}
				// 值必须是完整格式 "w<d>-<d>"。
				var wn, in int
				if n, _ := fmt.Sscanf(string(v), "w%d-%d", &wn, &in); n != 2 {
					badReads.Add(1)
				}
			}
		}()
	}
	wg.Wait()
	if got := badReads.Load(); got != 0 {
		t.Fatalf("bad reads=%d, want 0", got)
	}
	t.Logf("stats=%v", c.Stats())
}
