package collapser

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

// The three paths a request can take are benchmarked separately, because they
// cost wildly different amounts and a benchmark that mixes them reports the
// cheapest one. In particular: a "high contention" benchmark whose backend is
// slower than the cache TTL spends almost every iteration in the cache, not in
// the deduplication path.

// BenchmarkCollapser_CacheHitPath measures a request served entirely from the
// result cache: one RLock, one map lookup, one time comparison.
func BenchmarkCollapser_CacheHitPath(b *testing.B) {
	c := NewCollapser(Config{
		ResultCacheDuration: 1 * time.Hour, // never expires during the run
		BackendTimeout:      10 * time.Second,
		CleanupInterval:     1 * time.Second,
	})
	_ = c.Start()
	defer func() { _ = c.Stop() }()

	fn := func(ctx context.Context) ([]byte, error) { return []byte("data"), nil }

	if _, err := c.Execute(context.Background(), "cached-key", fn); err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _ = c.Execute(context.Background(), "cached-key", fn)
		}
	})
}

// BenchmarkCollapser_CollapsePath measures the deduplication path itself. The
// cache is disabled, so every iteration either leads a backend call or joins one
// already in flight — the cache never absorbs the load.
//
// ns/op here is dominated by the simulated backend latency that followers wait
// on, which is the point: the number that matters is allocs/op, which shows a
// follower joining an inflight call allocates nothing.
func BenchmarkCollapser_CollapsePath(b *testing.B) {
	c := NewCollapser(Config{
		ResultCacheDuration: 0, // no caching: force every request through dedup
		BackendTimeout:      10 * time.Second,
		CleanupInterval:     1 * time.Second,
	})
	_ = c.Start()
	defer func() { _ = c.Stop() }()

	var backendCalls int64
	fn := func(ctx context.Context) ([]byte, error) {
		atomic.AddInt64(&backendCalls, 1)
		time.Sleep(2 * time.Millisecond) // simulate backend latency
		return []byte("data"), nil
	}

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _ = c.Execute(context.Background(), "hot-key", fn)
		}
	})
	b.StopTimer()

	calls := atomic.LoadInt64(&backendCalls)
	if calls > 0 {
		b.ReportMetric(float64(b.N)/float64(calls), "collapsed/backend-call")
	}
}

// BenchmarkCollapser_LeaderPath measures the worst case for the collapser: every
// request has a distinct key, so nothing is ever deduplicated or cached and each
// caller pays the full leader cost. This is the overhead a workload with no
// repeated keys would see.
func BenchmarkCollapser_LeaderPath(b *testing.B) {
	c := NewCollapser(Config{
		ResultCacheDuration: 100 * time.Millisecond,
		BackendTimeout:      10 * time.Second,
		CleanupInterval:     1 * time.Second,
	})
	_ = c.Start()
	defer func() { _ = c.Stop() }()

	fn := func(ctx context.Context) ([]byte, error) { return []byte("data"), nil }

	var i int64
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			key := fmt.Sprintf("key-%d", atomic.AddInt64(&i, 1))
			_, _ = c.Execute(context.Background(), key, fn)
		}
	})
}

// BenchmarkBackend_Direct is the baseline: calling fn with no collapser at all.
func BenchmarkBackend_Direct(b *testing.B) {
	fn := func(ctx context.Context) ([]byte, error) { return []byte("data"), nil }

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _ = fn(context.Background())
		}
	})
}
