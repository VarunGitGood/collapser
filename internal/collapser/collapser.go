package collapser

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/VarunGitGood/collapser-grpc/internal/logger"
	"github.com/VarunGitGood/collapser-grpc/internal/monitoring"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ErrClosed is returned by Execute after Stop has been called.
var ErrClosed = errors.New("collapser: closed")

type Config struct {
	ResultCacheDuration time.Duration
	BackendTimeout      time.Duration
	CleanupInterval     time.Duration
	// CacheErrors controls whether backend errors are stored in the result cache.
	// Defaults to false — a single transient error should not block all requests
	// for the full ResultCacheDuration.
	CacheErrors bool
	// MaxCacheEntries caps the result cache so a key-diverse workload cannot grow
	// it without bound between cleanup ticks. Zero means unlimited.
	MaxCacheEntries int
}

type Collapser struct {
	mu     sync.RWMutex
	config Config

	inflight map[string]*inflightCall
	cache    map[string]*cachedResult

	closed   bool
	stopCh   chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup
}

// inflightCall is the shared state between the leader executing the backend
// call and every follower waiting on it.
//
// data and err are written by the leader before it closes done, and read by
// followers only after receiving from done. The close/receive pair establishes
// the happens-before edge, so no additional lock is needed on the result and a
// follower joining an inflight call allocates nothing.
type inflightCall struct {
	done chan struct{}
	data []byte
	err  error
}

type cachedResult struct {
	data      []byte
	err       error
	expiresAt time.Time
}

func NewCollapser(cfg Config) *Collapser {
	return &Collapser{
		config:   cfg,
		inflight: make(map[string]*inflightCall),
		cache:    make(map[string]*cachedResult),
		stopCh:   make(chan struct{}),
	}
}

func (c *Collapser) Start() error {
	c.wg.Add(1)
	go c.cleanupLoop()
	return nil
}

// Stop shuts down the cleanup loop and rejects new work. It is safe to call
// more than once.
//
// Stop deliberately does not cancel calls already in flight: the leader owns
// data and err until it closes done, so tearing that state down from another
// goroutine would race. Every leader is bounded by BackendTimeout, and the gRPC
// server's GracefulStop drains in-flight requests before Stop is reached.
func (c *Collapser) Stop() error {
	c.mu.Lock()
	c.closed = true
	c.mu.Unlock()

	c.stopOnce.Do(func() { close(c.stopCh) })
	c.wg.Wait()
	return nil
}

// Execute deduplicates concurrent calls for the same key. The first caller
// becomes the leader and runs fn; every caller arriving while that call is in
// flight becomes a follower and receives the leader's result.
func (c *Collapser) Execute(ctx context.Context, key string, fn func(context.Context) ([]byte, error)) ([]byte, error) {
	monitoring.RequestsTotal.Inc()

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	// 1. Result cache.
	c.mu.RLock()
	if cached, exists := c.cache[key]; exists && time.Now().Before(cached.expiresAt) {
		c.mu.RUnlock()
		monitoring.CacheHitsTotal.Inc()
		return cached.data, cached.err
	}
	c.mu.RUnlock()

	// 2. Join an inflight call, or become the leader.
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, ErrClosed
	}
	if call, exists := c.inflight[key]; exists {
		c.mu.Unlock()
		monitoring.CollapsedRequestsTotal.Inc()

		// Zero allocations on this path: no per-waiter channel, no bookkeeping.
		select {
		case <-call.done:
			return call.data, call.err
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	call := &inflightCall{done: make(chan struct{})}
	c.inflight[key] = call
	monitoring.InflightRequests.Inc()
	monitoring.BackendCallsTotal.Inc()
	c.mu.Unlock()

	// 3. Run the backend call on a context detached from the caller, so one
	// client cancelling does not abort the shared work for every follower.
	backendCtx, cancel := context.WithTimeout(context.Background(), c.config.BackendTimeout)
	defer cancel()

	start := time.Now()
	data, err := c.callWithRecovery(backendCtx, fn)
	monitoring.BackendLatency.Observe(time.Since(start).Seconds())

	// 4. Publish the result to every follower. The close must happen before the
	// key is removed from the inflight map, so a follower that grabbed the
	// pointer is never left waiting on a call nobody will finish.
	call.data = data
	call.err = err
	close(call.done)

	// 5. Retire the inflight entry and cache the result.
	c.mu.Lock()
	delete(c.inflight, key)
	monitoring.InflightRequests.Dec()
	c.cacheResultLocked(key, data, err)
	c.mu.Unlock()

	return data, err
}

// cacheResultLocked stores a result under key. Callers must hold c.mu.
func (c *Collapser) cacheResultLocked(key string, data []byte, err error) {
	// Errors are cached only when explicitly opted in: a transient backend
	// failure should not be replayed to every caller for the full TTL.
	if err != nil && !c.config.CacheErrors {
		return
	}
	if c.config.ResultCacheDuration <= 0 {
		return
	}
	_, replacing := c.cache[key]
	if !replacing && c.config.MaxCacheEntries > 0 && len(c.cache) >= c.config.MaxCacheEntries {
		return
	}
	c.cache[key] = &cachedResult{
		data:      data,
		err:       err,
		expiresAt: time.Now().Add(c.config.ResultCacheDuration),
	}
	// Only count a genuinely new entry; replacing an expired-but-uncollected
	// entry would otherwise drift the gauge upward forever.
	if !replacing {
		monitoring.CachedResults.Inc()
	}
}

// callWithRecovery runs fn and converts any panic into an error, so the leader
// always reaches the close(call.done) below and followers are never stranded.
func (c *Collapser) callWithRecovery(ctx context.Context, fn func(context.Context) ([]byte, error)) (data []byte, err error) {
	defer func() {
		if r := recover(); r != nil {
			logger.Error("panic in backend call", zap.Any("panic", r))
			data = nil
			err = status.Errorf(codes.Internal, "collapser: panic in backend call: %v", r)
		}
	}()
	return fn(ctx)
}

func (c *Collapser) cleanupLoop() {
	defer c.wg.Done()
	ticker := time.NewTicker(c.config.CleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.cleanup()
		case <-c.stopCh:
			return
		}
	}
}

func (c *Collapser) cleanup() {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := time.Now()
	for key, cached := range c.cache {
		if now.After(cached.expiresAt) {
			delete(c.cache, key)
			monitoring.CachedResults.Dec()
		}
	}
}
