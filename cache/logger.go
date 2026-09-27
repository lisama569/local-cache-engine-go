package cache

// Logger 用于在测试中打印命中、淘汰与过期判定依据。
type Logger interface {
	Logf(format string, args ...any)
}

// nopLogger 默认不输出。
type nopLogger struct{}

func (nopLogger) Logf(string, ...any) {}
