package cache

import (
	"errors"
	"fmt"
)

// ErrorKind 以枚举方式区分失败原因，便于调用方判断与测试断言。
type ErrorKind int

const (
	KindUnknown            ErrorKind = iota
	KindNotFound                     // 本地与后端均无此键
	KindBackendUnavailable           // 后端不可用（网络/超时/熔断等）
	KindLoadFailed                   // 后端返回了其他加载错误
	KindNoFallback                   // 后端失败且本地没有可降级的旧值，或显式关闭降级
	KindWriteFailed                  // 写后端失败（写穿透失败，本地未被污染）
	KindInvalidArgument              // 参数非法
)

func (k ErrorKind) String() string {
	switch k {
	case KindNotFound:
		return "not found"
	case KindBackendUnavailable:
		return "backend unavailable"
	case KindLoadFailed:
		return "load failed"
	case KindNoFallback:
		return "no fallback value"
	case KindWriteFailed:
		return "write failed"
	case KindInvalidArgument:
		return "invalid argument"
	default:
		return "unknown"
	}
}

// Error 是本包所有可解释失败的统一类型，可用 errors.As 取出判定 Kind。
type Error struct {
	Kind        ErrorKind
	Op          string // 发生失败的操作，如 "cache.Store.GetOrLoad"
	Key         string
	Cause       error // 底层错误
	StaleServed bool  // 本次失败是否已同时用旧值降级（Outcome==Stale）
}

func (e *Error) Error() string {
	msg := fmt.Sprintf("%s: key=%q: %s", e.Op, e.Key, e.Kind)
	if e.StaleServed {
		msg += " (stale value served)"
	}
	if e.Cause != nil {
		msg += ": " + e.Cause.Error()
	}
	return msg
}

func (e *Error) Unwrap() error { return e.Cause }

// 后端实现可用这些哨兵错误表达失败类别，默认分类器据此判定 Kind；
// 业务后端也可以直接返回 *cache.Error 指定 Kind。
var (
	// ErrBackendUnavailable 表示后端存储不可用（区别于“键不存在”）。
	ErrBackendUnavailable = sterr("backend unavailable")
	// ErrNotFound 表示后端确认该键不存在。
	ErrNotFound = sterr("key not found")
)

type sentinelError string

func (e sentinelError) Error() string { return string(e) }

func sterr(s string) error { return sentinelError(s) }

// newOpError 把后端返回的任意错误归类为本包的 *Error。
//   - 已经是 *Error（或可 errors.As 到）则保留其 Kind；
//   - ErrBackendUnavailable（含包装，errors.Is）=> KindBackendUnavailable；
//   - ErrNotFound（含包装）=> KindNotFound；
//   - 其余 => KindLoadFailed。
func classifyLoadError(op, key string, err error) *Error {
	if err == nil {
		return nil
	}
	var ce *Error
	if errors.As(err, &ce) {
		return &Error{Kind: ce.Kind, Op: op, Key: key, Cause: err}
	}
	kind := KindLoadFailed
	switch {
	case errors.Is(err, ErrBackendUnavailable):
		kind = KindBackendUnavailable
	case errors.Is(err, ErrNotFound):
		kind = KindNotFound
	}
	return &Error{Kind: kind, Op: op, Key: key, Cause: err}
}
