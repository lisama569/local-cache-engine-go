package cache

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

// backend 模拟可开关的后端存储。
type backend struct {
	mu   sync.Mutex
	down bool
	data map[string]string
}

func (b *backend) setDown(down bool) { b.mu.Lock(); b.down = down; b.mu.Unlock() }

func (b *backend) load(ctx context.Context, key string) ([]byte, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.down {
		return nil, fmt.Errorf("dial tcp: %w", ErrBackendDown)
	}
	v, ok := b.data[key]
	if !ok {
		return nil, errors.New("no rows")
	}
	return []byte(v), nil
}

// TestStaleServedWhenBackendDown 后端不可用时返回上一次有效值，
// 错误 Kind 为 ErrKindStale，可与其他失败区分。
func TestStaleServedWhenBackendDown(t *testing.T) {
	clock := NewManualClock()
	be := &backend{data: map[string]string{"k": "v1"}}
	c := New(Options{
		Capacity: 8, TTL: 100 * time.Millisecond, Clock: clock,
		Loader: be.load, AllowStale: true, Logger: testLogger{t},
	})

	// 正常回源，缓存 v1。
	if v, err := c.Get(context.Background(), "k"); err != nil || string(v) != "v1" {
		t.Fatalf("initial load: %q, %v", v, err)
	}
	// 过期 + 后端宕机：应降级返回 v1，错误 Kind=ErrKindStale。
	clock.Advance(200 * time.Millisecond)
	be.setDown(true)
	v, err := c.Get(context.Background(), "k")
	if string(v) != "v1" {
		t.Fatalf("stale value=%q, want v1", v)
	}
	if errKind(err) != ErrKindStale {
		t.Fatalf("kind=%v, want ErrKindStale", errKind(err))
	}
	if !errors.Is(err, ErrBackendDown) {
		t.Fatalf("stale err should wrap ErrBackendDown: %v", err)
	}
	// 后端恢复后再次读取，拿到新值。
	be.data["k"] = "v2"
	be.setDown(false)
	if v, err := c.Get(context.Background(), "k"); err != nil || string(v) != "v2" {
		t.Fatalf("after recovery: %q, %v", v, err)
	}
	if got := c.Stats()["stale_served"]; got != 1 {
		t.Fatalf("stale_served=%d, want 1", got)
	}
	t.Logf("stats=%v", c.Stats())
}

// TestBackendDownWithoutStale 无旧值可降级时明确报 ErrKindBackendUnavailable。
func TestBackendDownWithoutStale(t *testing.T) {
	be := &backend{down: true, data: map[string]string{}}
	c := New(Options{Capacity: 8, Loader: be.load, AllowStale: true, Logger: testLogger{t}})
	v, err := c.Get(context.Background(), "missing")
	if v != nil || errKind(err) != ErrKindBackendUnavailable {
		t.Fatalf("got %q, kind=%v; want nil, ErrKindBackendUnavailable", v, errKind(err))
	}
}

// TestStaleDisabled 关闭降级时，过期+后端宕机不得返回旧值。
func TestStaleDisabled(t *testing.T) {
	clock := NewManualClock()
	be := &backend{data: map[string]string{"k": "v1"}}
	c := New(Options{
		Capacity: 8, TTL: 100 * time.Millisecond, Clock: clock,
		Loader: be.load, AllowStale: false, Logger: testLogger{t},
	})
	if _, err := c.Get(context.Background(), "k"); err != nil {
		t.Fatal(err)
	}
	clock.Advance(200 * time.Millisecond)
	be.setDown(true)
	v, err := c.Get(context.Background(), "k")
	if v != nil || errKind(err) != ErrKindBackendUnavailable {
		t.Fatalf("got %q, kind=%v; want nil, ErrKindBackendUnavailable", v, errKind(err))
	}
}

// TestWritesNotLostDuringDegradation 降级期间本地写入不丢失、计数一致。
func TestWritesNotLostDuringDegradation(t *testing.T) {
	be := &backend{down: true, data: map[string]string{}}
	c := New(Options{Capacity: 4, Loader: be.load, AllowStale: true, Logger: testLogger{t}})

	// 后端宕机期间写入计数类数据。
	c.Set("counter", []byte("41"))
	c.Set("counter", []byte("42"))

	v, err := c.Get(context.Background(), "counter")
	if err != nil || string(v) != "42" {
		t.Fatalf("write lost during degradation: %q, %v", v, err)
	}
	s := c.Stats()
	if s["hits"] != 1 || s["stale_served"] != 0 {
		t.Fatalf("inconsistent stats: %v", s)
	}
	t.Logf("stats=%v", s)
}
