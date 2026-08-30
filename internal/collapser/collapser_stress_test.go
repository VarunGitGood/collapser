package collapser

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestStress_HighConcurrency(t *testing.T) {
	c := NewCollapser(Config{
		ResultCacheDuration: 100 * time.Millisecond,
		BackendTimeout:      10 * time.Second,
		CleanupInterval:     1 * time.Second,
	})
	_ = c.Start()
	defer func() { _ = c.Stop() }()

	var backendCalls int64
	var errCount int64

	fn := func(ctx context.Context) ([]byte, error) {
		atomic.AddInt64(&backendCalls, 1)
		time.Sleep(10 * time.Millisecond)
		return []byte("ok"), nil
	}

	const N = 10000
	var wg sync.WaitGroup
	wg.Add(N)

	start := time.Now()
	for i := 0; i < N; i++ {
		go func() {
			defer wg.Done()
			_, err := c.Execute(context.Background(), "key", fn)
			if err != nil {
				atomic.AddInt64(&errCount, 1)
			}
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)

	t.Logf("High Concurrency Test:")
	t.Logf("  Requests: %d", N)
	t.Logf("  Backend calls: %d", backendCalls)
	t.Logf("  Errors: %d", errCount)
	t.Logf("  Collapse ratio: %.1f:1", float64(N)/float64(backendCalls))
	t.Logf("  Duration: %v", elapsed)
	t.Logf("  Throughput: %.0f req/s", float64(N)/elapsed.Seconds())

	if errCount > 0 {
		t.Errorf("Expected 0 errors, got %d", errCount)
	}
	if backendCalls > 10 {
		t.Errorf("Too many backend calls: %d (expected <10 for 10k requests)", backendCalls)
	}
}

func TestStress_SustainedLoad(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping sustained load test in short mode")
	}

	c := NewCollapser(Config{
		ResultCacheDuration: 100 * time.Millisecond,
		BackendTimeout:      10 * time.Second,
		CleanupInterval:     1 * time.Second,
	})
	_ = c.Start()
	defer func() { _ = c.Stop() }()

	var totalRequests int64
	var backendCalls int64
	var errCount int64

	fn := func(ctx context.Context) ([]byte, error) {
		atomic.AddInt64(&backendCalls, 1)
		time.Sleep(50 * time.Millisecond)
		return []byte("ok"), nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// 100 workers making requests
	var wg sync.WaitGroup
	for worker := 0; worker < 100; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				default:
					atomic.AddInt64(&totalRequests, 1)
					_, err := c.Execute(ctx, "sustained-key", fn)
					if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
						atomic.AddInt64(&errCount, 1)
					}
					time.Sleep(10 * time.Millisecond)
				}
			}
		}()
	}
	wg.Wait()

	t.Logf("Sustained Load Test (60s):")
	t.Logf("  Total requests: %d", totalRequests)
	t.Logf("  Backend calls: %d", backendCalls)
	t.Logf("  Errors: %d", errCount)
	t.Logf("  Collapse ratio: %.1f:1", float64(totalRequests)/float64(backendCalls))
	t.Logf("  RPS: %.0f", float64(totalRequests)/60.0)

	if errCount > totalRequests/100 {
		t.Errorf("Too many errors: %d (>1%%)", errCount)
	}
}

func TestStress_MemoryLeak(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping memory leak test in short mode")
	}

	runtime.GC()
	var m1 runtime.MemStats
	runtime.ReadMemStats(&m1)

	c := NewCollapser(Config{
		ResultCacheDuration: 100 * time.Millisecond,
		BackendTimeout:      10 * time.Second,
		CleanupInterval:     1 * time.Second,
	})
	_ = c.Start()
	defer func() { _ = c.Stop() }()

	fn := func(ctx context.Context) ([]byte, error) {
		time.Sleep(10 * time.Millisecond)
		return []byte("ok"), nil
	}

	for batch := 0; batch < 100; batch++ {
		var wg sync.WaitGroup
		wg.Add(1000)
		for i := 0; i < 1000; i++ {
			go func() {
				defer wg.Done()
				_, _ = c.Execute(context.Background(), "key", fn)
			}()
		}
		wg.Wait()

		if batch%10 == 0 {
			runtime.GC()
		}
	}

	runtime.GC()
	time.Sleep(200 * time.Millisecond)
	runtime.GC()

	var m2 runtime.MemStats
	runtime.ReadMemStats(&m2)

	allocDiff := int64(m2.Alloc) - int64(m1.Alloc)
	if allocDiff < 0 {
		allocDiff = 0
	}

	t.Logf("Memory Leak Test:")
	t.Logf("  Before: %d MB", m1.Alloc/1024/1024)
	t.Logf("  After:  %d MB", m2.Alloc/1024/1024)
	t.Logf("  Diff:   %d MB", allocDiff/1024/1024)

	if allocDiff > 50*1024*1024 {
		t.Errorf("Possible memory leak: %d MB growth", allocDiff/1024/1024)
	}
}

func TestStress_GoroutineLeak(t *testing.T) {
	runtime.GC()
	before := runtime.NumGoroutine()

	c := NewCollapser(Config{
		ResultCacheDuration: 100 * time.Millisecond,
		BackendTimeout:      10 * time.Second,
		CleanupInterval:     1 * time.Second,
	})
	_ = c.Start()

	fn := func(ctx context.Context) ([]byte, error) {
		return []byte("ok"), nil
	}

	var wg sync.WaitGroup
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = c.Execute(context.Background(), "key", fn)
		}()
	}
	wg.Wait()

	_ = c.Stop()
	time.Sleep(200 * time.Millisecond)
	runtime.GC()

	after := runtime.NumGoroutine()
	diff := after - before

	t.Logf("Goroutine Leak Test:")
	t.Logf("  Before: %d", before)
	t.Logf("  After:  %d", after)
	t.Logf("  Diff:   %d", diff)

	if diff > 5 {
		t.Errorf("Possible goroutine leak: %d goroutines remain", diff)
	}
}
