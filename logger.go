package cache

import (
	"fmt"
	"io"
	"log"
	"os"
)

// Logger 用于输出命中、淘汰、过期判定等可解释日志，
// 测试中可注入 TestLogger，使每条判定依据都打印在单测日志里。
type Logger interface {
	Printf(format string, args ...any)
}

// NopLogger 丢弃所有日志。
type NopLogger struct{}

func (NopLogger) Printf(format string, args ...any) {}

// StdLogger 包装标准库 log.Logger，默认输出到 stderr。
type StdLogger struct {
	l *log.Logger
}

// NewStdLogger 创建输出到 stderr 的标准日志器。
func NewStdLogger() *StdLogger {
	return &StdLogger{l: log.New(os.Stderr, "cache: ", log.LstdFlags|log.Lmicroseconds)}
}

// NewLoggerTo 创建输出到指定 writer 的标准日志器。
func NewLoggerTo(w io.Writer) *StdLogger {
	return &StdLogger{l: log.New(w, "cache: ", log.LstdFlags|log.Lmicroseconds)}
}

func (l *StdLogger) Printf(format string, args ...any) {
	l.l.Output(2, fmt.Sprintf(format, args...))
}

// TestingT 是 testing.T 的最小接口（也兼容其他实现）。
type TestingT interface {
	Logf(format string, args ...any)
}

// TestLogger 把缓存内部判定依据写入测试日志，go test -v 时可见。
type TestLogger struct {
	t TestingT
}

func NewTestLogger(t TestingT) *TestLogger { return &TestLogger{t: t} }

func (l *TestLogger) Printf(format string, args ...any) {
	l.t.Logf(format, args...)
}
