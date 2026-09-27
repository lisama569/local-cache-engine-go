package cache_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	cache "github.com/lisama569/local-cache-engine-go"
)

// remoteDB 模拟一个可能不可用的后端。
type remoteDB struct {
	mu      sync.Mutex
	rows    map[string]string
	down    bool
	loadCnt int
}

func (db *remoteDB) Load(ctx context.Context, key string) (string, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.loadCnt++
	if db.down {
		return "", cache.ErrBackendUnavailable
	}
	v, ok := db.rows[key]
	if !ok {
		return "", cache.ErrNotFound
	}
	return v, nil
}

func (db *remoteDB) Store(ctx context.Context, key, value string) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.down {
		return cache.ErrBackendUnavailable
	}
	db.rows[key] = value
	return nil
}

// ExampleStore_getOrLoad 展示基本的“读取-回源-命中”路径。
func ExampleStore_getOrLoad() {
	db := &remoteDB{rows: map[string]string{"user:1": "alice"}}
	store, _ := cache.NewStore[string](
		cache.LoaderFunc[string](db.Load),
		cache.StoreConfig{
			Capacity:   16,
			DefaultTTL: time.Minute,
			Clock:      cache.SystemClock{},
			Fallback:   cache.FallbackStale,
		},
	)
	ctx := context.Background()

	r1, _ := store.GetOrLoad(ctx, "user:1")
	fmt.Println("first:", r1.Outcome, r1.Value)

	r2, _ := store.GetOrLoad(ctx, "user:1")
	fmt.Println("second:", r2.Outcome, r2.Value)
	fmt.Println("backend loads:", db.loadCnt)
	// Output:
	// first: backend alice
	// second: fresh-cache alice
	// backend loads: 1
}

// ExampleStore_staleFallback 展示后端不可用时返回上一次有效值，
// 并通过 *cache.Error 区分失败原因与“已降级”。
func ExampleStore_staleFallback() {
	db := &remoteDB{rows: map[string]string{"cfg": "v1"}}
	clk := cache.NewFakeClock(time.Date(2026, 9, 27, 0, 0, 0, 0, time.UTC))
	store, _ := cache.NewStore[string](
		cache.LoaderFunc[string](db.Load),
		cache.StoreConfig{
			Capacity: 4, DefaultTTL: 10 * time.Second,
			Clock: clk, Fallback: cache.FallbackStale,
		},
	)
	ctx := context.Background()

	store.GetOrLoad(ctx, "cfg") // 先建立上一次有效值
	clk.Advance(10 * time.Second)
	db.down = true // 本地到期，且后端此刻不可用

	r, err := store.GetOrLoad(ctx, "cfg")
	var ce *cache.Error
	if errors.As(err, &ce) {
		fmt.Println("kind:", ce.Kind, "staleServed:", ce.StaleServed)
	}
	fmt.Println("outcome:", r.Outcome, "value:", r.Value)
	// Output:
	// kind: backend unavailable staleServed: true
	// outcome: stale-fallback value: v1
}

// ExampleStore_writeThroughFailure 展示写穿透失败时本地不被污染。
func ExampleStore_writeThroughFailure() {
	db := &remoteDB{rows: map[string]string{}}
	store, _ := cache.NewStore[string](
		&wrappingLoader{db: db},
		cache.StoreConfig{Capacity: 4, Clock: cache.SystemClock{}},
	)
	ctx := context.Background()

	fmt.Println("put1:", store.Put(ctx, "k", "a"))
	db.down = true
	err := store.Put(ctx, "k", "b")
	var ce *cache.Error
	if errors.As(err, &ce) {
		fmt.Println("put2:", ce.Kind)
	}
	db.down = false
	r, _ := store.GetOrLoad(ctx, "k")
	fmt.Println("local value after failed write:", r.Value)
	// Output:
	// put1: <nil>
	// put2: write failed
	// local value after failed write: a
}

// wrappingLoader 同时实现 Loader 与 Writable。
type wrappingLoader struct{ db *remoteDB }

func (w *wrappingLoader) Load(ctx context.Context, key string) (string, error) {
	return w.db.Load(ctx, key)
}
func (w *wrappingLoader) Store(ctx context.Context, key, value string) error {
	return w.db.Store(ctx, key, value)
}
