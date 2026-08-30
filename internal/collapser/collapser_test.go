package collapser

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestCollapser_BasicCollapse(t *testing.T) {
	c := NewCollapser(Config{
		ResultCacheDuration: 100 * time.Millisecond,
		BackendTimeout:      10 * time.Second,
		CleanupInterval:     1 * time.Second,
	})
	_ = c.Start()
	defer func() { _ = c.Stop() }()

	var backendCalls int64
	fn := func(ctx context.Context) ([]byte, error) {
		atomic.AddInt64(&backendCalls, 1)
		time.Sleep(50 * time.Millisecond)
		return []byte("result"), nil
	}

	// Send 100 concurrent requests
	var wg sync.WaitGroup
	wg.Add(100)

	for i := 0; i < 100; i++ {
		go func() {
			defer wg.Done()
			data, err := c.Execute(context.Background(), "key1", fn)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if string(data) != "result" {
				t.Errorf("expected 'result', got %s", data)
			}
		}()
	}

	wg.Wait()

	if backendCalls != 1 {
		t.Errorf("expected 1 backend call, got %d", backendCalls)
	}
}

func TestCollapser_CacheHit(t *testing.T) {
	c := NewCollapser(Config{
		ResultCacheDuration: 200 * time.Millisecond,
		BackendTimeout:      5 * time.Second,
		CleanupInterval:     1 * time.Second,
	})
	_ = c.Start()
	defer func() { _ = c.Stop() }()

	var backendCalls int64
	fn := func(ctx context.Context) ([]byte, error) {
		atomic.AddInt64(&backendCalls, 1)
		return []byte("cached"), nil
	}

	// First call
	_, _ = c.Execute(context.Background(), "key1", fn)

	// Wait for execution to complete
	time.Sleep(10 * time.Millisecond)

	// Second call (should hit cache)
	data, err := c.Execute(context.Background(), "key1", fn)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(data) != "cached" {
		t.Errorf("expected 'cached', got %s", data)
	}

	if backendCalls != 1 {
		t.Errorf("expected 1 backend call (cache hit), got %d", backendCalls)
	}
}

func TestCollapser_CacheExpiry(t *testing.T) {
	c := NewCollapser(Config{
		ResultCacheDuration: 50 * time.Millisecond,
		BackendTimeout:      5 * time.Second,
		CleanupInterval:     10 * time.Millisecond,
	})
	_ = c.Start()
	defer func() { _ = c.Stop() }()

	var backendCalls int64
	fn := func(ctx context.Context) ([]byte, error) {
		atomic.AddInt64(&backendCalls, 1)
		return []byte("result"), nil
	}

	// First call
	_, _ = c.Execute(context.Background(), "key1", fn)

	// Wait for cache to expire
	time.Sleep(100 * time.Millisecond)

	// Second call (cache expired, should execute again)
	_, _ = c.Execute(context.Background(), "key1", fn)

	if backendCalls != 2 {
		t.Errorf("expected 2 backend calls (cache expired), got %d", backendCalls)
	}
}

func TestCollapser_ClientCancellation(t *testing.T) {
	c := NewCollapser(Config{
		ResultCacheDuration: 100 * time.Millisecond,
		BackendTimeout:      10 * time.Second,
		CleanupInterval:     1 * time.Second,
	})
	_ = c.Start()
	defer func() { _ = c.Stop() }()

	var backendCalls int64
	var backendCompleted int64

	fn := func(ctx context.Context) ([]byte, error) {
		atomic.AddInt64(&backendCalls, 1)
		time.Sleep(100 * time.Millisecond)
		atomic.AddInt64(&backendCompleted, 1)
		return []byte("result"), nil
	}

	// Client 1: cancels immediately
	ctx1, cancel1 := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, err := c.Execute(ctx1, "key1", fn)
		if !errors.Is(err, context.Canceled) {
			t.Errorf("expected context.Canceled, got %v", err)
		}
	}()

	// Cancel immediately
	cancel1()

	// Client 2: waits for result
	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(10 * time.Millisecond) // Start after client1
		data, err := c.Execute(context.Background(), "key1", fn)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if string(data) != "result" {
			t.Errorf("expected 'result', got %s", data)
		}
	}()

	wg.Wait()

	// Backend should complete despite client1 cancellation
	if backendCompleted != 1 {
		t.Errorf("backend should complete, got %d completions", backendCompleted)
	}
}

func TestCollapser_ErrorPropagation(t *testing.T) {
	c := NewCollapser(Config{
		ResultCacheDuration: 100 * time.Millisecond,
		BackendTimeout:      10 * time.Second,
		CleanupInterval:     1 * time.Second,
	})
	_ = c.Start()
	defer func() { _ = c.Stop() }()

	expectedErr := errors.New("backend error")
	fn := func(ctx context.Context) ([]byte, error) {
		return nil, expectedErr
	}

	// Multiple requests should all get the same error
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := c.Execute(context.Background(), "key1", fn)
			if !errors.Is(err, expectedErr) {
				t.Errorf("expected backend error, got %v", err)
			}
		}()
	}

	wg.Wait()
}

func TestCollapser_MultipleKeys(t *testing.T) {
	c := NewCollapser(Config{
		ResultCacheDuration: 100 * time.Millisecond,
		BackendTimeout:      10 * time.Second,
		CleanupInterval:     1 * time.Second,
	})
	_ = c.Start()
	defer func() { _ = c.Stop() }()

	var backendCalls int64
	fn := func(ctx context.Context) ([]byte, error) {
		atomic.AddInt64(&backendCalls, 1)
		time.Sleep(50 * time.Millisecond)
		return []byte("result"), nil
	}

	var wg sync.WaitGroup

	// 50 requests for key1
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = c.Execute(context.Background(), "key1", fn)
		}()
	}

	// 50 requests for key2
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = c.Execute(context.Background(), "key2", fn)
		}()
	}

	wg.Wait()

	// Should have 2 backend calls (one per key)
	if backendCalls != 2 {
		t.Errorf("expected 2 backend calls, got %d", backendCalls)
	}
}

// A panicking backend must not strand followers: they should all receive an
// error rather than block until their context expires.
func TestCollapser_PanicRecovery(t *testing.T) {
	c := NewCollapser(Config{
		ResultCacheDuration: 100 * time.Millisecond,
		BackendTimeout:      5 * time.Second,
		CleanupInterval:     time.Second,
	})
	_ = c.Start()
	defer func() { _ = c.Stop() }()

	release := make(chan struct{})
	fn := func(ctx context.Context) ([]byte, error) {
		<-release
		panic("backend exploded")
	}

	const followers = 50
	errs := make(chan error, followers)
	for i := 0; i < followers; i++ {
		go func() {
			_, err := c.Execute(context.Background(), "panic-key", fn)
			errs <- err
		}()
	}

	time.Sleep(50 * time.Millisecond) // let followers pile onto the leader
	close(release)

	for i := 0; i < followers; i++ {
		select {
		case err := <-errs:
			if err == nil {
				t.Fatal("expected an error after the backend panicked, got nil")
			}
			if status.Code(err) != codes.Internal {
				t.Errorf("panic surfaced as %v, want codes.Internal", status.Code(err))
			}
		case <-time.After(5 * time.Second):
			t.Fatal("a caller was stranded after the backend panicked")
		}
	}

	// The panic must not have left the key registered as in flight.
	c.mu.RLock()
	stuck := len(c.inflight)
	c.mu.RUnlock()
	if stuck != 0 {
		t.Errorf("inflight map still holds %d entries after a panic", stuck)
	}
}

// Errors must not be cached by default, or one transient failure is replayed to
// every caller for the whole TTL.
func TestCollapser_ErrorsNotCachedByDefault(t *testing.T) {
	c := NewCollapser(Config{
		ResultCacheDuration: time.Hour,
		BackendTimeout:      5 * time.Second,
		CleanupInterval:     time.Second,
	})
	_ = c.Start()
	defer func() { _ = c.Stop() }()

	var calls int32
	fn := func(ctx context.Context) ([]byte, error) {
		if atomic.AddInt32(&calls, 1) == 1 {
			return nil, errors.New("transient")
		}
		return []byte("recovered"), nil
	}

	if _, err := c.Execute(context.Background(), "k", fn); err == nil {
		t.Fatal("expected the first call to fail")
	}
	got, err := c.Execute(context.Background(), "k", fn)
	if err != nil {
		t.Fatalf("second call should have retried the backend, got %v", err)
	}
	if string(got) != "recovered" {
		t.Errorf("got %q, want %q", got, "recovered")
	}
}

func TestCollapser_CacheErrorsWhenEnabled(t *testing.T) {
	c := NewCollapser(Config{
		ResultCacheDuration: time.Hour,
		BackendTimeout:      5 * time.Second,
		CleanupInterval:     time.Second,
		CacheErrors:         true,
	})
	_ = c.Start()
	defer func() { _ = c.Stop() }()

	var calls int32
	fn := func(ctx context.Context) ([]byte, error) {
		atomic.AddInt32(&calls, 1)
		return nil, errors.New("down")
	}

	for i := 0; i < 3; i++ {
		if _, err := c.Execute(context.Background(), "k", fn); err == nil {
			t.Fatal("expected an error")
		}
	}
	if n := atomic.LoadInt32(&calls); n != 1 {
		t.Errorf("backend called %d times, want 1 (the error should be cached)", n)
	}
}

func TestCollapser_MaxCacheEntries(t *testing.T) {
	const limit = 8
	c := NewCollapser(Config{
		ResultCacheDuration: time.Hour,
		BackendTimeout:      5 * time.Second,
		CleanupInterval:     time.Hour, // never evict during the test
		MaxCacheEntries:     limit,
	})
	_ = c.Start()
	defer func() { _ = c.Stop() }()

	fn := func(ctx context.Context) ([]byte, error) { return []byte("v"), nil }
	for i := 0; i < limit*4; i++ {
		if _, err := c.Execute(context.Background(), fmt.Sprintf("k-%d", i), fn); err != nil {
			t.Fatal(err)
		}
	}

	c.mu.RLock()
	size := len(c.cache)
	c.mu.RUnlock()
	if size > limit {
		t.Errorf("cache grew to %d entries, want at most %d", size, limit)
	}
}

// Stop is reachable from a signal handler and from defer in tests, so calling it
// twice must not panic on a closed channel.
func TestCollapser_StopIsIdempotent(t *testing.T) {
	c := NewCollapser(Config{
		ResultCacheDuration: time.Millisecond,
		BackendTimeout:      time.Second,
		CleanupInterval:     time.Millisecond,
	})
	_ = c.Start()

	if err := c.Stop(); err != nil {
		t.Fatalf("first Stop: %v", err)
	}
	if err := c.Stop(); err != nil {
		t.Fatalf("second Stop: %v", err)
	}

	if _, err := c.Execute(context.Background(), "k", func(context.Context) ([]byte, error) {
		t.Error("backend must not be called after Stop")
		return nil, nil
	}); !errors.Is(err, ErrClosed) {
		t.Errorf("Execute after Stop returned %v, want ErrClosed", err)
	}
}
