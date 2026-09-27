package cache

import "fmt"

// ErrKind 区分不同的失败原因。
type ErrKind int

const (
	// ErrKindNotFound 键不存在且无可用降级值。
	ErrKindNotFound ErrKind = iota
	// ErrKindBackendUnavailable 后端存储不可用。
	ErrKindBackendUnavailable
	// ErrKindLoadFailed 回源加载失败。
	ErrKindLoadFailed
	// ErrKindStale 返回的是上一次有效值（降级），配合值一起返回。
	ErrKindStale
)

// Error 是可区分失败原因的错误类型。
type Error struct {
	Kind ErrKind
	Key  string
	Err  error
}

func (e *Error) Error() string {
	return fmt.Sprintf("cache: kind=%d key=%q: %v", e.Kind, e.Key, e.Err)
}

func (e *Error) Unwrap() error { return e.Err }
