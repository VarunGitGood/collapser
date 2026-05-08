package collapser

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/VarunGitGood/collapser-grpc/internal/logger"
	"github.com/VarunGitGood/collapser-grpc/internal/monitoring"
	"go.uber.org/zap"
)

type State int32

const (
	StateExecuting State = 0
	StateDone      State = 1
)

type Config struct {
	ResultCacheDuration time.Duration
	BackendTimeout      time.Duration
	CleanupInterval     time.Duration
	// CacheErrors controls whether backend errors are stored in the result cache.
	// Defaults to false — a single transient error should not block all requests
	// for the full ResultCacheDuration.
	CacheErrors bool
}

type Collapser struct {
	mu     sync.RWMutex
	config Config

	inflight map[string]*inflightCall
	cache    map[string]*cachedResult

	stopCh chan struct{}
	wg     sync.WaitGroup
}

type inflightCall struct {
	state   atomic.Int32
	waiters []chan result
	res     *result
	mu      sync.Mutex
}

type cachedResult struct {
	data      []byte
	err       error
	expiresAt time.Time
}

type result struct {
	data []byte
	err  error
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

func (c *Collapser) Stop() error {
	close(c.stopCh)
	c.wg.Wait()

	c.mu.Lock()
	defer c.mu.Unlock()

	for key, call := range c.inflight {
		call.mu.Lock()
		waiters := call.waiters
		call.waiters = nil
		call.mu.Unlock()
		c.notifyWaiters(call, result{err: fmt.Errorf("shutting down")}, waiters...)
		delete(c.inflight, key)
	}

	return nil
}

func (c *Collapser) Execute(ctx context.Context, key string, fn func(context.Context) ([]byte, error)) ([]byte, error) {
	monitoring.RequestsTotal.Inc()

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	// 1. Check result cache
	c.mu.RLock()
	if cached, exists := c.cache[key]; exists {
		if time.Now().Before(cached.expiresAt) {
			c.mu.RUnlock()
			monitoring.CacheHitsTotal.Inc()
			return cached.data, cached.err
		}
	}
	c.mu.RUnlock()

	// 2. Check inflight
	c.mu.Lock()
	if call, exists := c.inflight[key]; exists {
		monitoring.CollapsedRequestsTotal.Inc()
		waiterCh := make(chan result, 1)

		call.mu.Lock()
		// Double check if it just finished
		if State(call.state.Load()) == StateDone {
			res := *call.res
			call.mu.Unlock()
			c.mu.Unlock()
			return res.data, res.err
		}
		call.waiters = append(call.waiters, waiterCh)
		call.mu.Unlock()
		c.mu.Unlock()

		select {
		case res := <-waiterCh:
			return res.data, res.err
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	// 3. Become leader
	call := &inflightCall{
		waiters: make([]chan result, 0),
	}
	call.state.Store(int32(StateExecuting))
	c.inflight[key] = call
	monitoring.InflightRequests.Inc()
	monitoring.BackendCallsTotal.Inc()
	c.mu.Unlock()

	// Detached context for backend
	backendCtx, cancel := context.WithTimeout(context.Background(), c.config.BackendTimeout)
	defer cancel()

	start := time.Now()
	data, err := c.callWithRecovery(fn, backendCtx)
	monitoring.BackendLatency.Observe(time.Since(start).Seconds())

	res := result{data: data, err: err}

	// 4. Update inflight state and notify
	call.mu.Lock()
	call.res = &res
	call.state.Store(int32(StateDone))
	waiters := call.waiters
	call.waiters = nil
	call.mu.Unlock()

	c.notifyWaiters(call, res, waiters...)

	// 5. Cache result and move from inflight to cache
	c.mu.Lock()
	delete(c.inflight, key)
	monitoring.InflightRequests.Dec()
	// Only cache errors when explicitly opted in — a transient backend failure
	// should not block all callers for the full ResultCacheDuration.
	if err == nil || c.config.CacheErrors {
		c.cache[key] = &cachedResult{
			data:      data,
			err:       err,
			expiresAt: time.Now().Add(c.config.ResultCacheDuration),
		}
		monitoring.CachedResults.Inc()
	}
	c.mu.Unlock()

	return data, err
}

// callWithRecovery runs fn and converts any panic into an error so followers
// are always notified and never left hanging.
func (c *Collapser) callWithRecovery(fn func(context.Context) ([]byte, error), ctx context.Context) (data []byte, err error) {
	defer func() {
		if r := recover(); r != nil {
			logger.Error("panic in backend call", zap.Any("panic", r))
			err = fmt.Errorf("internal panic: %v", r)
		}
	}()
	return fn(ctx)
}

func (c *Collapser) notifyWaiters(call *inflightCall, res result, waiters ...chan result) {
	for _, ch := range waiters {
		func(waiterCh chan result) {
			defer func() {
				if r := recover(); r != nil {
					logger.Error("panic notifying waiter", zap.Any("panic", r))
				}
			}()
			select {
			case waiterCh <- res:
			default:
			}
			close(waiterCh)
		}(ch)
	}
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
