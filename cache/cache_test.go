package cache

import (
	"context"
	"testing"
	"time"
)

// testLogger 把命中/淘汰/过期判定依据打印到单测日志。
type testLogger struct{ t *testing.T }

func (l testLogger) Logf(format string, args ...any) { l.t.Logf(format, args...) }

func errKind(err error) ErrKind {
	if e, ok := err.(*Error); ok {
		return e.Kind
	}
	return -1
}

func TestSetGetHit(t *testing.T) {
	c := New(Options{Capacity: 4, TTL: time.Minute, Logger: testLogger{t}})
	c.Set("a", []byte("1"))
	v, err := c.Get(context.Background(), "a")
	if err != nil || string(v) != "1" {
		t.Fatalf("got %q, %v", v, err)
	}
	if got := c.Stats()["hits"]; got != 1 {
		t.Fatalf("hits=%d, want 1", got)
	}
}

func TestExpiryBoundary(t *testing.T) {
	clock := NewManualClock()
	c := New(Options{Capacity: 8, TTL: 100 * time.Millisecond, Clock: clock, Logger: testLogger{t}})
	c.Set("k", []byte("v"))

	// 边界前 1ns：未过期，必须命中。
	clock.Advance(100*time.Millisecond - 1)
	if v, err := c.Get(context.Background(), "k"); err != nil || string(v) != "v" {
		t.Fatalf("before boundary: got %q, %v", v, err)
	}

	// 恰好到达过期点：视为过期，不得返回脏值。
	clock.Advance(1)
	v, err := c.Get(context.Background(), "k")
	if err == nil || v != nil {
		t.Fatalf("at boundary: got %q, %v; want not-found", v, err)
	}
	if errKind(err) != ErrKindNotFound {
		t.Fatalf("kind=%v, want ErrKindNotFound", errKind(err))
	}
	if got := c.Stats()["expirations"]; got != 1 {
		t.Fatalf("expirations=%d, want 1", got)
	}
	t.Logf("stats=%v", c.Stats())
}

func TestEvictionBoundary(t *testing.T) {
	c := New(Options{Capacity: 2, Logger: testLogger{t}})
	c.Set("a", []byte("1"))
	c.Set("b", []byte("2"))
	// 访问 a 使其成为最近使用，b 变为最久未使用。
	if _, err := c.Get(context.Background(), "a"); err != nil {
		t.Fatal(err)
	}
	// 写入第三个键，容量超限，应淘汰 b 而非 a。
	c.Set("c", []byte("3"))

	if _, err := c.Get(context.Background(), "b"); errKind(err) != ErrKindNotFound {
		t.Fatalf("b should be evicted, err=%v", err)
	}
	for _, k := range []string{"a", "c"} {
		if _, err := c.Get(context.Background(), k); err != nil {
			t.Fatalf("%s should survive: %v", k, err)
		}
	}
	if got := c.Stats()["evictions"]; got != 1 {
		t.Fatalf("evictions=%d, want 1", got)
	}
	t.Logf("stats=%v", c.Stats())
}

func TestDelete(t *testing.T) {
	c := New(Options{Capacity: 2, Logger: testLogger{t}})
	c.Set("x", []byte("1"))
	c.Delete("x")
	if _, err := c.Get(context.Background(), "x"); errKind(err) != ErrKindNotFound {
		t.Fatalf("after delete err=%v", err)
	}
}
