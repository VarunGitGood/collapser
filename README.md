# Collapser — a gRPC request-collapsing sidecar

A Go sidecar that sits between clients and a backend gRPC service and collapses
identical in-flight requests into a single backend call.

When N callers ask the same question at the same time, the backend should answer
it once. Envoy — which the sidecar runs alongside — load-balances, retries and
reports on those N requests, but it has no notion that they are the same request.
This fills that gap.

**Measured on a local Kubernetes cluster with Istio: 2,000 concurrent identical
requests reached the backend as 1 call.** Verified three ways — the backend's own
counter, Envoy's `istio_requests_total`, and the proxy's metrics.

---

## How it works

```
                    ┌──────────────────────────────────────────┐
   client ────┐     │  collapser sidecar                       │
   client ────┼────▶│                                          │
   client ────┤     │   key = SHA256(method ‖ payload ‖ hdrs)   │
   client ────┘     │                                          │
        (N)         │   ① cached result, still fresh?  ──▶ return
                    │   ② same key already in flight?  ──▶ wait on it
                    │   ③ otherwise: become leader     ──▶ call backend ──┐
                    │                                          │          │
                    │   leader publishes once, every           │          ▼
                    │   waiter wakes on the same result ◀──────┼──── backend
                    └──────────────────────────────────────────┘        (1)
```

The first caller for a key becomes the **leader** and calls the backend. Everyone
arriving while that call is in flight becomes a **follower** and blocks on a
single channel the leader closes when it has an answer. No polling, no per-waiter
bookkeeping, and no allocation on the follower path.

The proxy is **schema-agnostic**: a passthrough codec forwards raw gRPC frames
unchanged, so it works with any protobuf service without generated stubs. One
persistent `*grpc.ClientConn` is shared across all backend calls, so HTTP/2
multiplexing is used properly and there are no per-request handshakes.

### Why not `singleflight`?

`golang.org/x/sync/singleflight` deduplicates concurrent calls for a key, and
that part is the same idea. What it does not do, and what a sidecar needs:

| | `singleflight` | this |
|---|---|---|
| Result TTL cache (burst protection past the in-flight window) | ✗ | ✓ |
| Backend call survives the originating caller's cancellation | ✗ (cancels shared work) | ✓ (detached context) |
| Per-call Prometheus metrics | ✗ | ✓ |
| Collapse key aware of identity headers | ✗ | ✓ |

---

## Measured results

Full raw output, with hardware and toolchain, is in
[`deploy/evidence/results.txt`](deploy/evidence/results.txt). Reproduce it with
`make check`, `make bench`, and `make cluster && make deploy && make demo`.

### On the cluster (kind + Istio 1.28.1, sidecar-injected, STRICT mTLS)

Backend calls are counted **at the backend**, then cross-checked against Envoy's
`istio_requests_total{reporter="destination"}` — never taken from the proxy's own
accounting.

| Concurrent identical requests | Direct to backend | Through collapser | Backend load removed |
|---:|---:|---:|---:|
| 100 | 100 calls | **1 call** | 99.0% |
| 500 | 500 calls | **1 call** | 99.8% |
| 1,000 | 1,000 calls | **1 call** | 99.9% |
| 2,000 | — | **1 call** | 108 ms wall, 50 ms backend |

### In-process stress (race detector enabled)

| Test | Result |
|---|---|
| 10,000 concurrent, one key | 2 backend calls — **5,000:1** |
| 60 s sustained, 100 workers | 391,400 requests → 392 calls — **998:1**, 6,523 RPS, 0 errors |
| Goroutine leak | 0 |
| Heap growth over 100k requests | 0 MB |

### Benchmarks

11th Gen Intel Core i7-1165G7, 8 threads, Go 1.25.5.

Each path is benchmarked separately, because they cost very different amounts and
a benchmark that mixes them reports whichever is cheapest.

| Path | Time | Memory | Allocations |
|---|---:|---:|---:|
| **Cache hit** — result still fresh | 45.7 ns/op | 0 B | **0** |
| **Collapse** — joining an in-flight call | dominated by backend wait | 54 B | **0** |
| **Leader** — every key distinct, nothing to collapse | 1,594 ns/op | 552 B | 10 |
| Baseline — no collapser at all | 0.33 ns/op | 0 B | 0 |

> The collapse-path row is the one that matters, and the number to read is
> `allocs/op`, not `ns/op`: a follower's wall time is just however long the
> backend takes. `0 allocs/op` is the claim — joining an in-flight call allocates
> nothing, because followers share one channel rather than each registering their
> own.

Test coverage: `internal/collapser` 97.5%, `internal/config` 95.8%.

---

## Quick start

### Locally

```bash
make build

# terminal 1 — demo backend on :50051 (50 ms simulated work)
go run ./cmd/backend

# terminal 2 — proxy on :50052
BACKEND_ADDRESS=localhost:50051 go run ./cmd/proxy

# terminal 3 — 100 concurrent identical requests
CONCURRENCY=100 go run ./cmd/client

curl -s localhost:8080/calls    # backend calls that actually happened
curl -s localhost:2112/metrics | grep collapser_
```

### On a local Kubernetes cluster with Istio

`kind` runs a full cluster inside Docker — nothing cloud, nothing paid.
Needs `docker`, `kind`, `kubectl`, and `istioctl` on `PATH`.

```bash
make cluster    # kind cluster + Istio (demo profile) + sidecar injection
make deploy     # build images, side-load into kind, apply k8s + Istio manifests
make demo       # drive load, report the collapse ratio measured at the backend
make cluster-down
```

---

## Configuration

All configuration is via environment variables.

| Variable | Description | Default |
|---|---|---|
| `GRPC_PORT` | Proxy listening port | `50052` |
| `METRICS_PORT` | Prometheus, health and readiness port | `2112` |
| `BACKEND_ADDRESS` | Backend gRPC address (`host:port`) | **required** |
| `BACKEND_TIMEOUT` | Per-call timeout for backend calls | `10s` |
| `BACKEND_USE_TLS` | TLS for the backend connection | `false` |
| `COLLAPSER_CACHE_DURATION` | Result cache TTL; `0` disables caching | `100ms` |
| `COLLAPSER_CLEANUP_INTERVAL` | How often expired entries are evicted | `1s` |
| `COLLAPSER_CACHE_ERRORS` | Cache backend errors for the TTL | `false` |
| `COLLAPSER_MAX_CACHE_ENTRIES` | Cache entry cap; `0` is unlimited | `10000` |
| `COLLAPSER_KEY_HEADERS` | Headers folded into the collapse key | *(none)* |
| `LOG_LEVEL` | `debug`, `info`, `warn`, `error` | `info` |
| `LOG_FORMAT` | `json` or `console` | `json` |

### `COLLAPSER_KEY_HEADERS` — read this before deploying

Collapsing is only safe between requests whose responses are interchangeable.
By default the key is the method plus the payload, so **two callers sending an
identical payload share one response even if they authenticated as different
users**. If the backend varies its response by an identity header, that is a
cross-tenant data leak.

Name those headers and they become part of the key, so such requests get separate
backend calls:

```bash
COLLAPSER_KEY_HEADERS=authorization,x-tenant-id
```

Requests are served with the *leader's* metadata — the follower's headers reach
the backend only when they are in the key.

### `COLLAPSER_CACHE_ERRORS`

Off by default. With it on, one transient `Unavailable` is replayed to every
caller for the full TTL. Turn it on only if you specifically want negative
caching.

---

## Observability

| Endpoint | Purpose |
|---|---|
| `:2112/metrics` | Prometheus metrics |
| `:2112/health` | Liveness — never depends on the backend, so a backend outage cannot trigger restarts that remove the collapsing protecting it |
| `:2112/ready` | Readiness — reports the backend channel state, so a rolling deploy does not route into a proxy that cannot reach anything |

| Metric | Description |
|---|---|
| `collapser_requests_total` | Requests received |
| `collapser_collapsed_requests_total` | Requests that joined an in-flight call |
| `collapser_backend_calls_total` | Backend calls actually made |
| `collapser_cache_hits_total` | Requests served from the result cache |
| `collapser_cached_results` | Entries currently cached |
| `collapser_inflight_requests` | Leader calls currently running |
| `collapser_backend_latency_seconds` | Backend latency histogram (0.1 ms–13 s) |

Backend load removed = `1 - backend_calls_total / requests_total`.

---

## Design notes

**Detached backend context.** The leader runs the backend call on a context
derived from `context.Background()`, not from its own caller. One client
disconnecting must not cancel work that other followers are waiting on. The
trade-off is that the leader keeps going until `BACKEND_TIMEOUT` even if every
caller has left.

**Errors are not cached by default.** A backend failure should not be replayed to
every caller for the whole TTL window.

**Panics become `codes.Internal`.** A panicking backend call is recovered and
turned into an error, so the leader still reaches the publish step and no
follower is ever stranded waiting on a call nobody will finish.

**Graceful shutdown.** On SIGTERM/SIGINT the gRPC server drains in-flight calls
(`GracefulStop`) before the collapser stops, which is what a Kubernetes rolling
deploy needs. `Stop` deliberately does not tear down calls already running: the
leader owns that result until it publishes, so cancelling it from another
goroutine would race. Every leader is bounded by `BACKEND_TIMEOUT` anyway.

**Cache cap.** A key-diverse workload would otherwise grow the map without bound
between cleanup ticks, so entries are capped and the cap is configurable.

## Known limitations

- **Server-streaming RPCs are treated as unary.** Without a proto service
  descriptor the proxy cannot tell a server-streaming RPC from a unary one on the
  incoming connection — both look like one client message followed by a
  half-close — so only the first response frame is returned. Client-streaming and
  bidi-streaming *are* detected and forwarded correctly, and bypass collapsing
  entirely (streams cannot be meaningfully deduplicated).
- **TLS.** `BACKEND_USE_TLS=true` uses the system trust store. Custom CA bundles
  and mTLS to the backend are not implemented — inside a mesh, Istio's sidecars
  handle that instead.
- **Single-process cache.** Each replica collapses independently, so N replicas
  can produce up to N backend calls for the same key. Sharing state across
  replicas would trade the latency this exists to save.
- **The passthrough codec registers globally** under the name `proto`, replacing
  the default codec for the whole process. That is intentional and is what makes
  the proxy schema-agnostic, but it means `internal/proxy` must not be imported
  into a process that needs normal protobuf marshalling.

## Layout

```
cmd/proxy      the sidecar
cmd/backend    demo backend; reports its own call count for measurement
cmd/client     concurrent load generator (CONCURRENCY, PROXY_ADDRESS)
internal/collapser   deduplication engine, TTL cache, metrics
internal/proxy       gRPC handler, passthrough codec, key derivation
deploy/k8s           Deployment + Service for both workloads
deploy/istio         VirtualService, DestinationRule, PeerAuthentication
deploy/scripts       cluster-up.sh, demo.sh
deploy/evidence      raw measurement output
```
