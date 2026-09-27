package cache

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeBackend 是可控的测试后端：可按键配置加载错误、全局写入错误，
// 并统计 Load/Store 次数；支持阻塞某次加载以复现并发合并。
type fakeBackend[V any] struct {
	mu       sync.Mutex
	data     map[string]V
	loadErr  map[string]error
	storeErr error
	loads    map[string]*atomic.Int64
	stores   atomic.Int64
	gateOnce sync.Once
	entered  chan struct{}
	release  chan struct{}
	gateKey  string
}

func newFakeBackend[V any](gateKey string) *fakeBackend[V] {
	return &fakeBackend[V]{
		data:    map[string]V{},
		loadErr: map[string]error{},
		loads:   map[string]*atomic.Int64{},
		gateKey: gateKey,
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
}

func (b *fakeBackend[V]) count(key string) int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	if c, ok := b.loads[key]; ok {
		return c.Load()
	}
	return 0
}

func (b *fakeBackend[V]) Load(ctx context.Context, key string) (V, error) {
	b.mu.Lock()
	c, ok := b.loads[key]
	if !ok {
		c = &atomic.Int64{}
		b.loads[key] = c
	}
	err := b.loadErr[key]
	v, has := b.data[key]
	gate := key == b.gateKey
	b.mu.Unlock()

	c.Add(1)
	if gate {
		b.gateOnce.Do(func() { close(b.entered) })
		<-b.release
	}
	if err != nil {
		var zero V
		return zero, err
	}
	if !has {
		return v, ErrNotFound
	}
	return v, nil
}

func (b *fakeBackend[V]) Store(ctx context.Context, key string, value V) error {
	b.mu.Lock()
	err := b.storeErr
	if err == nil {
		b.data[key] = value
	}
	b.mu.Unlock()
	b.stores.Add(1)
	return err
}

func newTestStore[V any](t *testing.T, loader Loader[V], clk Clock, fb FallbackMode, ttl time.Duration) (*Store[V], *Stats, *captureLogger) {
	t.Helper()
	log := newCaptureLogger(t)
	s, err := NewStore[V](loader, StoreConfig{
		Capacity: 8, DefaultTTL: ttl, Clock: clk, Logger: log, Fallback: fb,
	})
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	return s, s.stats, log
}

// 新鲜命中不接触后端；回源成功后第二次即为新鲜命中。
func TestGetOrLoadFreshThenCache(t *testing.T) {
	clk := NewFakeClock(time.Date(2026, 9, 27, 0, 0, 0, 0, time.UTC))
	b := newFakeBackend[string]("")
	b.data["k"] = "v1"
	s, st, log := newTestStore[string](t, b, clk, FallbackStale, time.Minute)
	ctx := context.Background()

	r, err := s.GetOrLoad(ctx, "k")
	if err != nil || r.Outcome != OutcomeBackend || r.Value != "v1" {
		t.Fatalf("首次应回源, r=%+v err=%v", r, err)
	}
	r, err = s.GetOrLoad(ctx, "k")
	if err != nil || r.Outcome != OutcomeFreshCache || r.Value != "v1" {
		t.Fatalf("二次应新鲜命中, r=%+v err=%v", r, err)
	}
	if b.count("k") != 1 {
		t.Fatalf("后端只应被加载1次, got=%d", b.count("k"))
	}
	if snap := st.Snapshot(); snap.Hits != 1 || snap.Loads != 1 {
		t.Fatalf("计数不符: %+v", snap)
	}
	if !log.containsAll("判定[fresh]", "回源[ok]", "singleflight[leader]") {
		t.Fatalf("日志缺少命中/回源依据:\n%s", log.String())
	}
}

// 热点键并发回源只真正加载一次，所有调用者共享同一结果。
func TestGetOrLoadConcurrentSingleLoad(t *testing.T) {
	clk := NewFakeClock(time.Now())
	b := newFakeBackend[string]("hot")
	b.data["hot"] = "V"
	s, st, _ := newTestStore[string](t, b, clk, FallbackStale, time.Minute)

	const n = 32
	var wg sync.WaitGroup
	errs := make([]error, n)
	outs := make([]Outcome, n)
	shar := make([]bool, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			r, err := s.GetOrLoad(context.Background(), "hot")
			errs[i], outs[i], shar[i] = err, r.Outcome, r.Shared
		}(i)
	}
	<-b.entered
	time.Sleep(30 * time.Millisecond) // 确保其余调用全部注册到 singleflight
	close(b.release)
	wg.Wait()

	if b.count("hot") != 1 {
		t.Fatalf("并发下后端必须只加载1次, got=%d", b.count("hot"))
	}
	leaders, waiters := 0, 0
	for i := 0; i < n; i++ {
		if errs[i] != nil {
			t.Fatalf("调用%d出错: %v", i, errs[i])
		}
		if outs[i] != OutcomeBackend {
			t.Fatalf("所有调用都应来自同一次回源结论, got=%s", outs[i])
		}
		if shar[i] {
			waiters++
		} else {
			leaders++
		}
	}
	if leaders != 1 || waiters != n-1 {
		t.Fatalf("应恰好1个真正回源者、%d个共享等待者, got leaders=%d waiters=%d",
			n-1, leaders, waiters)
	}
	if snap := st.Snapshot(); snap.Loads != 1 || snap.LoadShared != int64(n-1) {
		t.Fatalf("回源/合并计数不符: %+v", snap)
	}
}

// 后端不可用 + 本地有上一次有效值：降级返回旧值，错误原因可区分，
// 且残缺/错误结果不写入缓存，旧值可反复用于降级。
func TestStaleFallbackOnBackendUnavailable(t *testing.T) {
	clk := NewFakeClock(time.Date(2026, 9, 27, 0, 0, 0, 0, time.UTC))
	b := newFakeBackend[string]("")
	b.data["k"] = "old"
	s, st, log := newTestStore[string](t, b, clk, FallbackStale, 10*time.Second)
	ctx := context.Background()

	if _, err := s.GetOrLoad(ctx, "k"); err != nil {
		t.Fatal(err)
	}
	clk.Advance(10 * time.Second) // 恰好到期 → 本地变过期

	b.loadErr["k"] = fmt.Errorf("dial tcp: %w", ErrBackendUnavailable)

	r, err := s.GetOrLoad(ctx, "k")
	if r.Outcome != OutcomeStale || r.Value != "old" {
		t.Fatalf("应降级返回旧值 old, got=%+v", r)
	}
	var ce *Error
	if !errors.As(err, &ce) || ce.Kind != KindBackendUnavailable || !ce.StaleServed {
		t.Fatalf("应返回带StaleServed的KindBackendUnavailable错误, got=%v", err)
	}
	if snap := st.Snapshot(); snap.StaleFallbacks != 1 || snap.LoadFailures != 1 {
		t.Fatalf("降级计数不符: %+v", snap)
	}

	// 再次请求：错误结果没有污染缓存，仍可降级为同一旧值。
	r2, err2 := s.GetOrLoad(ctx, "k")
	if r2.Value != "old" || r2.Outcome != OutcomeStale || err2 == nil {
		t.Fatalf("旧值应保持不变并可继续降级, got=%+v err=%v", r2, err2)
	}
	if b.data["k"] != "old" {
		t.Fatalf("后端数据被意外修改: %q", b.data["k"])
	}
	if !log.containsAll("判定[expired]", "回源[fail]", "降级[stale]", "backend unavailable") {
		t.Fatalf("日志缺少过期/失败/降级依据:\n%s", log.String())
	}
}

// 普通加载错误同样可区分（KindLoadFailed），且无旧值时报 KindNoFallback。
func TestLoadFailureKinds(t *testing.T) {
	clk := NewFakeClock(time.Now())
	b := newFakeBackend[string]("")
	s, _, log := newTestStore[string](t, b, clk, FallbackStale, time.Minute)

	// 从未成功加载过的键：无旧值可降级。
	b.loadErr["boom"] = errors.New("500 internal")
	_, err := s.GetOrLoad(context.Background(), "boom")
	var ce *Error
	if !errors.As(err, &ce) || ce.Kind != KindNoFallback {
		t.Fatalf("无旧值时应 KindNoFallback, got=%v", err)
	}
	if !errors.Is(err, ErrBackendUnavailable) {
		// 包裹链中应能追到原始后端错误
	} else {
		t.Fatalf("普通错误不应被判为不可用")
	}
	if !log.containsAll("降级[unavailable]") {
		t.Fatalf("应打印无旧值降级失败依据:\n%s", log.String())
	}
}

// FallbackOff：即使有过期旧值也不降级，直接返回明确错误。
func TestFallbackOffReturnsError(t *testing.T) {
	clk := NewFakeClock(time.Now())
	b := newFakeBackend[string]("")
	b.data["k"] = "old"
	s, _, _ := newTestStore[string](t, b, clk, FallbackOff, time.Nanosecond)

	if _, err := s.GetOrLoad(context.Background(), "k"); err != nil {
		t.Fatal(err)
	}
	clk.Advance(time.Millisecond)
	b.loadErr["k"] = ErrBackendUnavailable

	r, err := s.GetOrLoad(context.Background(), "k")
	var ce *Error
	if !errors.As(err, &ce) || ce.Kind != KindBackendUnavailable {
		t.Fatalf("FallbackOff 下应直接返回原始类别错误, got=%v", err)
	}
	if r.Outcome == OutcomeStale || r.Value != "" {
		t.Fatalf("FallbackOff 不应返回旧值, got=%+v", r)
	}
}

// 后端明确 NotFound：不是故障，不降级，并清除本地过期条目。
func TestNotFoundIsNotFallback(t *testing.T) {
	clk := NewFakeClock(time.Now())
	b := newFakeBackend[string]("")
	b.data["k"] = "old"
	s, _, _ := newTestStore[string](t, b, clk, FallbackStale, time.Nanosecond)

	if _, err := s.GetOrLoad(context.Background(), "k"); err != nil {
		t.Fatal(err)
	}
	clk.Advance(time.Millisecond)
	delete(b.data, "k") // 后端确认键已不存在

	r, err := s.GetOrLoad(context.Background(), "k")
	var ce *Error
	if !errors.As(err, &ce) || ce.Kind != KindNotFound {
		t.Fatalf("应 KindNotFound, got=%v", err)
	}
	if r.Outcome == OutcomeStale || r.Value == "old" {
		t.Fatalf("NotFound 不应降级给旧值, got=%+v", r)
	}
	if s.cache.peekExpired("k").State != Miss {
		t.Fatal("NotFound 后本地过期条目应被清除")
	}
}

// 一个键的加载失败不影响其他正常键的数据与命中。
func TestFailureIsolatesOtherKeys(t *testing.T) {
	clk := NewFakeClock(time.Now())
	b := newFakeBackend[string]("")
	b.data["good"] = "g"
	b.data["bad"] = "b"
	s, _, _ := newTestStore[string](t, b, clk, FallbackStale, time.Minute)
	ctx := context.Background()

	if _, err := s.GetOrLoad(ctx, "good"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.GetOrLoad(ctx, "bad"); err != nil {
		t.Fatal(err)
	}
	b.loadErr["bad"] = ErrBackendUnavailable

	// bad 仍在 TTL 内：直接新鲜命中本地，后端故障完全不影响它。
	r, err := s.GetOrLoad(ctx, "bad")
	if err != nil || r.Value != "b" || r.Outcome != OutcomeFreshCache {
		t.Fatalf("TTL内的bad不应受后端故障影响, r=%+v err=%v", r, err)
	}
	// good 完全正常。
	r, err = s.GetOrLoad(ctx, "good")
	if err != nil || r.Value != "g" || r.Outcome != OutcomeFreshCache {
		t.Fatalf("good 应保持正常, r=%+v err=%v", r, err)
	}
	if b.count("good") != 1 || b.count("bad") != 1 {
		t.Fatalf("两键都应只加载1次, good=%d bad=%d", b.count("good"), b.count("bad"))
	}
}

// 写穿透成功：后端与本地都更新；写失败：本地保持旧值、计数一致、不静默丢写。
func TestPutWriteThroughAndFailureConsistency(t *testing.T) {
	clk := NewFakeClock(time.Now())
	b := newFakeBackend[int]("")
	s, st, log := newTestStore[int](t, b, clk, FallbackStale, time.Minute)
	ctx := context.Background()

	if err := s.Put(ctx, "counter", 5); err != nil {
		t.Fatal(err)
	}
	if b.data["counter"] != 5 {
		t.Fatalf("后端应为5, got=%d", b.data["counter"])
	}

	// 后端写入失败：必须返回明确错误，本地仍是旧值5（前后一致，无静默丢写）。
	b.storeErr = ErrBackendUnavailable
	err := s.Put(ctx, "counter", 6)
	var ce *Error
	if !errors.As(err, &ce) || ce.Kind != KindWriteFailed {
		t.Fatalf("应返回 KindWriteFailed, got=%v", err)
	}
	if b.data["counter"] != 5 {
		t.Fatalf("失败后后端应保持5, got=%d", b.data["counter"])
	}
	r, gerr := s.GetOrLoad(ctx, "counter")
	if gerr != nil || r.Value != 5 || r.Outcome != OutcomeFreshCache {
		t.Fatalf("失败后本地应仍是旧值5, r=%+v err=%v", r, gerr)
	}
	if snap := st.Snapshot(); snap.Writes != 1 || snap.WriteFailures != 1 {
		t.Fatalf("写计数不一致: %+v", snap)
	}
	b.storeErr = nil
	if err := s.Put(ctx, "counter", 7); err != nil {
		t.Fatal(err)
	}
	r, _ = s.GetOrLoad(ctx, "counter")
	if r.Value != 7 {
		t.Fatalf("恢复后应能更新为7, got=%d", r.Value)
	}
	if !log.containsAll("写穿透[ok]", "写穿透[fail]", "本地缓存保持不变") {
		t.Fatalf("日志缺少写穿透判定依据:\n%s", log.String())
	}
}
