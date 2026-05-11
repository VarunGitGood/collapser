# Production-Grade gRPC Request Collapser Proxy

A high-performance gRPC sidecar proxy that prevents thundering-herd effects by collapsing identical in-flight requests.

## Architecture

The proxy sits between clients and a backend gRPC service. It generates a hash based on the gRPC method and request payload.

- **Request arrived**: Generate key `SHA256(method + payload)`.
- **Check Cache**: If the result is in the 100ms TTL cache, return immediately.
- **Check Inflight**: If the same request is already executing, wait for the result (become follower).
- **Execute**: If not inflight, become leader and call the backend using a detached context.
- **Broadcast**: Once the leader finishes, all followers receive the same result.

The proxy is schema-agnostic — it uses a passthrough codec that forwards raw gRPC frames unchanged, so it works with any protobuf service without needing generated stubs. A single persistent `*grpc.ClientConn` is shared across all calls to the backend, making full use of HTTP/2 multiplexing.

## Features

- **Envoy-Style Request Collapsing**: True request deduplication without window-based batching.
- **Schema-Agnostic Passthrough**: A custom passthrough codec replaces the default proto codec so raw bytes are forwarded unchanged — no service descriptor required.
- **Persistent Backend Connection**: A single `*grpc.ClientConn` is dialed at startup and reused for every leader call, eliminating per-request TCP + TLS handshakes.
- **Detached Backend Context**: Client cancellations do not stop the backend execution for others.
- **Result Caching**: Configurable TTL (default 100ms) for bursts. Errors are not cached by default — a transient backend failure does not block all callers for the full TTL.
- **Streaming RPC Support**: Client-streaming and bidi-streaming RPCs are detected and forwarded directly to the backend without collapsing (streaming cannot be meaningfully deduplicated). Unary and server-streaming RPCs go through the collapser.
- **Panic Recovery**: Panics in the backend call path are caught, converted to errors, and broadcast to all waiting followers — no goroutine leaks.
- **Graceful Shutdown**: On SIGTERM/SIGINT the proxy drains all in-flight gRPC calls (`GracefulStop`) before exiting, compatible with Kubernetes rolling deploys.
- **Structured Logging**: JSON logs via `uber-go/zap`.
- **Prometheus Metrics**: Detailed metrics for collapse ratio, latency, and cache performance.

## Why not `singleflight`?

`golang.org/x/sync/singleflight` deduplicates concurrent calls for the same key but has no TTL cache (burst protection), no per-call metrics, and cancels the shared call if the originating context is cancelled. This proxy adds a configurable result cache, a detached backend context, and Prometheus instrumentation on top of the same deduplication semantics.

## Quick Start

### 1. Build
```bash
make deps
make build
```

### 2. Run with Example Backend
```bash
# Terminal 1 — test backend on :50051
go run cmd/backend/main.go

# Terminal 2 — proxy on :50052
BACKEND_ADDRESS=localhost:50051 go run cmd/proxy/main.go

# Terminal 3 — sends 100 concurrent requests
go run cmd/client/main.go
```

## Configuration

All configuration is via environment variables.

| Variable | Description | Default |
|----------|-------------|---------|
| `GRPC_PORT` | Proxy listening port | `50052` |
| `METRICS_PORT` | Prometheus & health check port | `2112` |
| `BACKEND_ADDRESS` | Backend gRPC address (`host:port`) | **Required** |
| `BACKEND_TIMEOUT` | Per-call timeout for backend calls | `10s` |
| `BACKEND_USE_TLS` | Enable TLS for backend connection | `false` |
| `COLLAPSER_CACHE_DURATION` | Result cache TTL | `100ms` |
| `COLLAPSER_CLEANUP_INTERVAL` | How often expired cache entries are evicted | `1s` |
| `COLLAPSER_CACHE_ERRORS` | Cache backend errors for the TTL duration | `false` |
| `LOG_LEVEL` | `debug`, `info`, `warn`, `error` | `info` |
| `LOG_FORMAT` | `json` for structured, `console` for human-readable | `json` |

> **Note on `COLLAPSER_CACHE_ERRORS`**: The default is `false`. With caching enabled a single transient error (`Unavailable`, `DeadlineExceeded`) would be served to every caller for the full `COLLAPSER_CACHE_DURATION`. Only enable this if you explicitly want negative caching.

## Monitoring

| Endpoint | Purpose |
|----------|---------|
| `http://localhost:2112/metrics` | Prometheus metrics |
| `http://localhost:2112/health` | Liveness check (always 200) |

Key metrics:

| Metric | Description |
|--------|-------------|
| `collapser_requests_total` | Total requests received |
| `collapser_collapsed_requests_total` | Requests that joined an inflight call |
| `collapser_backend_calls_total` | Actual backend calls made |
| `collapser_cache_hits_total` | Requests served from the result cache |
| `collapser_inflight_requests` | Current number of inflight leader calls |
| `collapser_backend_latency_seconds` | Backend call latency histogram (0.1ms–13s buckets) |

Collapse ratio = `collapsed_requests_total / backend_calls_total`.

## Benchmarking

```bash
make bench
```

### Benchmark Results
Obtained on an 11th Gen Intel(R) Core(TM) i7:

| Scenario | Performance | Memory | Allocations |
|----------|-------------|--------|-------------|
| **High Contention** | ~82 ns/op | 0 B/op | 0 allocs/op |
| **No Contention** | ~2700 ns/op | 522 B/op | 10 allocs/op |
| **Cache Hits** | ~71 ns/op | 0 B/op | 0 allocs/op |

*High Contention simulates 10k+ concurrent requests for the same key, demonstrating the near-zero overhead of the deduplication engine.*

## Known Limitations

- **Server-streaming RPCs**: Without a proto service descriptor the proxy cannot distinguish a server-streaming RPC from a unary one on the incoming connection (both have the client sending a single message then half-closing). Server-streaming RPCs are currently forwarded as unary — only the first response frame is returned. Client-streaming and bidi-streaming are detected and forwarded correctly.
- **TLS termination**: `BACKEND_USE_TLS=true` enables TLS on the backend connection using the system trust store. Custom CA bundles and mTLS are not yet supported.
