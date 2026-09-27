package cache

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

// captureLogger 同时写入测试日志（go test -v 可见）和内存切片，
// 便于断言“命中/淘汰/过期”的判定依据确实被打印。
type captureLogger struct {
	t  testing.TB
	mu sync.Mutex
	sb strings.Builder
}

func newCaptureLogger(t testing.TB) *captureLogger {
	return &captureLogger{t: t}
}

func (l *captureLogger) Printf(format string, args ...any) {
	line := fmt.Sprintf(format, args...)
	l.t.Logf("%s", line)
	l.mu.Lock()
	l.sb.WriteString(line)
	l.sb.WriteByte('\n')
	l.mu.Unlock()
}

func (l *captureLogger) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.sb.String()
}

// containsAll 判断日志是否包含全部关键字。
func (l *captureLogger) containsAll(words ...string) bool {
	s := l.String()
	for _, w := range words {
		if !strings.Contains(s, w) {
			return false
		}
	}
	return true
}

func newTestCache[V any](t testing.TB, cap int, ttl time.Duration, clk Clock) (*Cache[V], *Stats, *captureLogger) {
	t.Helper()
	log := newCaptureLogger(t)
	st := &Stats{}
	c, err := NewCache[V](Config{Capacity: cap, DefaultTTL: ttl, Clock: clk, Logger: log}, st)
	if err != nil {
		t.Fatalf("NewCache: %v", err)
	}
	return c, st, log
}
