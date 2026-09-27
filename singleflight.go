package cache

import (
	"fmt"
	"sync"
)

// call 表示一次进行中的回源加载，等待者通过 done 通道共享同一结果。
type call[V any] struct {
	done chan struct{}
	val  V
	err  error
}

// Group 实现同一键的并发调用合并：
// 同一时刻对同一 key 的多个 Do 调用中，只有一个真正执行 fn，
// 其余阻塞等待并共享其返回值与错误（包括失败），从而保证热点键只回源一次。
type Group[V any] struct {
	mu    sync.Mutex
	calls map[string]*call[V]
}

// NewGroup 创建一个合并组。
func NewGroup[V any]() *Group[V] {
	return &Group[V]{calls: make(map[string]*call[V])}
}

// Do 执行或加入对 key 的合并调用。
// 返回值 shared 为 true 表示本次调用复用了别人的结果（未真正执行 fn）。
// fn 的 panic 不会被吞掉：会被转换为错误传播给所有等待者，并清理调用槽。
func (g *Group[V]) Do(key string, fn func() (V, error)) (v V, err error, shared bool) {
	g.mu.Lock()
	if c, ok := g.calls[key]; ok {
		g.mu.Unlock()
		<-c.done
		return c.val, c.err, true
	}
	c := &call[V]{done: make(chan struct{})}
	g.calls[key] = c
	g.mu.Unlock()

	g.doCall(c, key, fn)
	return c.val, c.err, false
}

func (g *Group[V]) doCall(c *call[V], key string, fn func() (V, error)) {
	defer func() {
		g.mu.Lock()
		delete(g.calls, key)
		g.mu.Unlock()
		close(c.done)
	}()
	defer func() {
		if r := recover(); r != nil {
			c.val, c.err = zero[V](), fmt.Errorf("loader panic: %v", r)
		}
	}()
	c.val, c.err = fn()
}

func zero[V any]() V { var z V; return z }
