package cache

import "container/list"

// entry 是缓存条目。
type entry struct {
	key       string
	value     []byte
	expiresAt int64 // 单调时钟纳秒，0 表示永不过期
}

// expired 报告在 now（单调时钟）下是否已过期。
func (e *entry) expired(now int64) bool {
	return e.expiresAt != 0 && now >= e.expiresAt
}

// lru 是基于访问顺序的淘汰结构（非并发安全，由 Cache 加锁保护）。
type lru struct {
	ll   *list.List
	keys map[string]*list.Element
}

func newLRU() *lru {
	return &lru{ll: list.New(), keys: make(map[string]*list.Element)}
}

// get 查找并提升为最近使用；未找到返回 nil。
func (l *lru) get(key string) *entry {
	el, ok := l.keys[key]
	if !ok {
		return nil
	}
	l.ll.MoveToFront(el)
	return el.Value.(*entry)
}

// peek 查找但不改变顺序。
func (l *lru) peek(key string) *entry {
	if el, ok := l.keys[key]; ok {
		return el.Value.(*entry)
	}
	return nil
}

// set 写入或更新，并置于最新位置。
func (l *lru) set(key string, e *entry) {
	if el, ok := l.keys[key]; ok {
		el.Value = e
		l.ll.MoveToFront(el)
		return
	}
	l.keys[key] = l.ll.PushFront(e)
}

// removeOldest 移除最久未使用的条目，返回被移除的条目；空时返回 nil。
func (l *lru) removeOldest() *entry {
	el := l.ll.Back()
	if el == nil {
		return nil
	}
	l.ll.Remove(el)
	e := el.Value.(*entry)
	delete(l.keys, e.key)
	return e
}

// remove 删除指定键。
func (l *lru) remove(key string) {
	if el, ok := l.keys[key]; ok {
		l.ll.Remove(el)
		delete(l.keys, key)
	}
}

func (l *lru) len() int { return l.ll.Len() }
