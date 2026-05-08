# Collapser-gRPC: Senior Engineering Review + Sidecar Transition Plan

## Context

`collapser-grpc` is a learning project that implements an Envoy-style request-collapsing gRPC proxy. Goals of this document:

1. Honest senior-engineer review (Go + Kubernetes lens)
2. Bucketed list of bugs/improvements at HIGH / MED / LOW severity
3. Plan to transition this from a standalone binary into an Istio-style injectable Kubernetes sidecar

The collapsing engine itself (`internal/collapser/collapser.go`) is well-designed — single-leader semantics, detached backend context, tiny TTL cache, strong unit/stress/bench coverage. The weakness is the **proxy layer around it**: it has correctness gaps that may make the end-to-end claim ("any gRPC method, any payload") not actually work, plus operational gaps that block sidecar deployment.

Note: this document is based on **static analysis only** — no tests/benchmarks were executed during the review. Several items below are flagged "needs runtime verification" — running `make test` and the `cmd/backend` + `cmd/proxy` + `cmd/client` triple is the first verification step.

---

## Part 1 — Code Review Findings

### HIGH severity (correctness, not just polish)

**H1. The transparent-proxy codec is incomplete — likely broken end-to-end** — `internal/proxy/handler.go:64-70`

`RawMessage` implements `Reset()` / `String()` / `ProtoMessage()` but has no `Marshal` / `Unmarshal`. The default gRPC `proto` codec calls `proto.Marshal(v.(proto.Message))`, which on a non-`protoreflect`/non-legacy-`Marshaler` type produces empty bytes or panics depending on grpc-go version. The standard fix used by `mwitkow/grpc-proxy` and Envoy-go bindings is to **register a custom passthrough codec** via `encoding.RegisterCodecV2` (or legacy `encoding.RegisterCodec`) that returns the raw `[]byte` unchanged, plus declare it on the server (`grpc.ForceServerCodecV2(...)`) and the outbound call (`grpc.CallContentSubtype(...)`).

Verification: run `cmd/backend` + `cmd/proxy` + `cmd/client`; confirm response is literally `"Hello World"` (not empty). If the test passes, examine *why* — it likely works only because both client and server speak the same proto and the codec round-trips through generated stubs by accident.

**H2. `Forward()` opens a brand-new gRPC connection on every leader call** — `internal/proxy/forward.go:11-24`

Each leader pays a full TCP + HTTP/2 handshake. The whole point of HTTP/2 is multiplexing — a single connection should carry thousands of concurrent calls. The fix is to dial the backend **once** at proxy startup, store the `*grpc.ClientConn` in the `Handler`, and reuse it for every `Invoke`. This single change will likely 5-10× the proxy's no-cache throughput. README claims "high-performance" while the hot path does the opposite.

**H3. Graceful shutdown does nothing** — `cmd/proxy/main.go:82-84` + `internal/proxy/handler.go:28-31`

`grpc.NewServer` is created inside `Handle.Serve` and never returned. On SIGTERM, `main` logs "Shutting down gracefully..." and exits. The gRPC server is killed mid-stream; in-flight RPCs are dropped; `c.Stop()` runs via `defer` but the listener is already gone. Fix: create the `*grpc.Server` in `main`, call `srv.GracefulStop()` on signal, then `c.Stop()`. This matters double for k8s — without it, rolling deploys drop traffic during pod termination.

**H4. `Collapser.Stop()` never notifies waiters (variadic-arg bug)** — `internal/collapser/collapser.go:79-82`

```go
for key, call := range c.inflight {
    c.notifyWaiters(call, result{err: fmt.Errorf("shutting down")})
    delete(c.inflight, key)
}
```

`notifyWaiters` signature is `(call, res, waiters ...chan result)`. Called with zero waiters → loop body is empty → **every blocked follower hangs on its channel until its own ctx deadline expires.** Fix: pass `call.waiters...`. This bug is masked in tests because they use generous contexts and the test goroutines exit anyway, but in production this is a leaked goroutine on every shutdown with active traffic.

**H5. Errors are cached for the full TTL** — `internal/collapser/collapser.go:165-168`

A single transient backend failure (`Unavailable`, `DeadlineExceeded`, network blip) gets cached for 100 ms and **every** request to the same key during that window receives the same error. This converts 1 backend hiccup into a 100 ms total outage for that key. Fix: either don't cache errors at all, cache only "deterministic" gRPC codes (`NotFound`, `InvalidArgument`, `PermissionDenied`), or shorten the negative-cache TTL aggressively (10 ms). Make it configurable.

**H6. No streaming RPC support** — `internal/proxy/handler.go:33-57`

`Handle` does exactly one `RecvMsg` and one `SendMsg`. Server-streaming, client-streaming, and bidi RPCs will silently misbehave (only the first message round-trips). Either: (a) detect streaming via `grpc.MethodFromServerStream` + service descriptor and bypass the collapser (forward stream-to-stream), or (b) document loudly that this proxy is **unary-only**. Streaming methods cannot meaningfully be collapsed anyway, so option (a) is correct.

**H7. No panic recovery in the request path**

If `Forward` or any backend interaction panics, the goroutine dies, the inflight entry never moves to `StateDone`, and every follower waits forever. Add a `defer recover()` that converts panics into `result{err: ...}` and notifies waiters. The existing recover in `notifyWaiters` only protects channel sends.

---

### MEDIUM severity

**M1. Result cache is unbounded** — `internal/collapser/collapser.go:31`

A high-cardinality workload (many unique keys per second, e.g. user-scoped requests) grows the cache linearly until cleanup runs. With the 1 s cleanup interval and 100 ms TTL, peak size is roughly `qps × 1.1 s`. Set a max size + LRU eviction (`hashicorp/golang-lru/v2`) or shard the map and run cleanup per-shard.

**M2. Cleanup tick takes the same write lock as `Execute`** — `internal/collapser/collapser.go:208-218`

A large cache + a 1 s tick = a stop-the-world pause on the request path. Sharding the cache (16 stripes, lock per stripe) makes both reads and cleanup cheap. Worth doing once cache growth is bounded.

**M3. `BACKEND_USE_TLS` config is wired in `config.Config` but never read** — `internal/config/config.go:19` + `internal/proxy/forward.go:12`

`Forward` hardcodes `insecure.NewCredentials()`. Plumbing exists, just not connected. Add the conditional, plus optional CA bundle / SNI for production.

**M4. `grpc.DialContext` is deprecated** — `internal/proxy/forward.go:11` + `cmd/client/main.go:15`

Move to `grpc.NewClient`. Affects only forward calls (since H2 reduces this to one dial at startup).

**M5. No interceptors registered**

Production sidecars need: panic-recovery interceptor (covers H7), per-request structured-log interceptor, optional OpenTelemetry tracing (so collapsed-vs-leader is visible in traces), and a request-id propagator. Add unary + stream interceptor chains in `main.go`.

**M6. The unbounded-cache memory-leak test doesn't actually exercise unbounded growth** — `internal/collapser/collapser_stress_test.go:124-182`

Uses one key (`"key"`), so cache size never exceeds 1. Add a sibling test that uses unique keys per request and asserts cache size stays bounded.

**M7. `Validate()` is too permissive** — `internal/config/config.go:43-57`

Doesn't check `BackendAddress` looks like `host:port`. Doesn't check `ResultCacheDuration ≥ 0`. Doesn't check `CleanupInterval > 0` (a zero ticker panics).

**M8. Generated proto is committed but `.gitignore` excludes it** — `.gitignore:36-37`

`*.pb.go` is in `.gitignore` yet `proto/hello/hello.pb.go` is tracked. Pick one approach. The pragmatic path is: keep generated code committed and remove the `.gitignore` rule, so contributors don't need `protoc` locally.

**M9. `cmd/client/main.go` uses `grpc.Dial` and has no Dial timeout.**

**M10. `CONTRIBUTING.md` describes a directory layout that doesn't exist** (`pkg/`, `api/`).

---

### LOW severity (polish)

- **L1.** Typo in metrics help: `"Backend backend call duration"` — `internal/monitoring/metrics.go:41`.
- **L2.** Logger is a package-level singleton with implicit init — fine, but resets on `Init()` failure leave the default config; consider returning `*Logger`.
- **L3.** No build info embedded — add `-ldflags "-X main.version=..."` and a `/version` endpoint.
- **L4.** `LogFormat` only checks `"json"` vs anything-else; document that `"console"` is the human-readable value.
- **L5.** `make` lacks a `coverage` target to render `coverage.out` as HTML.
- **L6.** Default `gocyclo` threshold of 15 will start firing once `Execute` grows; consider extracting the leader/follower branches.
- **L7.** `BackendLatency` histogram uses `prometheus.DefBuckets` (0.005 .. 10 s). For a request-collapser, p99 should be bucketed sub-millisecond — use exponential buckets `0.0001 .. 1` so dashboards aren't useless.
- **L8.** No `singleflight` comparison in README — readers will ask "why not `golang.org/x/sync/singleflight`?". Answer: TTL cache + detached ctx + per-call metrics. Worth saying.
- **L9.** No version info, no `go.mod`-pinned tool versions, no `tools.go`.
- **L10.** Stress test bound `backendCalls > 10` for 10k requests is reasonable but brittle if the test's collapse window shrinks.

---

## Part 2 — Recommended Path to Production Viability

Order the HIGH items first. Until H1 is verified, every other claim is suspect.

| # | Item | Why first |
|---|------|-----------|
| 1 | Verify H1 by running `make test`, `cmd/backend` + `cmd/proxy` + `cmd/client` end-to-end | Establishes whether transparent proxying works at all |
| 2 | Fix H1 (passthrough codec) if broken | Unblocks "works for any service" claim |
| 3 | Fix H2 (persistent backend conn) | Biggest perf win, simplest patch |
| 4 | Fix H3 (graceful shutdown) | Required for k8s rolling deploys |
| 5 | Fix H4 (waiter notify on Stop) | Real goroutine leak |
| 6 | Add H7 (panic recovery) + H6 (streaming bypass or refusal) | Crash-safety |
| 7 | Fix H5 (error caching policy) + M1 (bounded cache) + M5 (interceptors) | Production-readiness |
| 8 | Then proceed to sidecar packaging (Part 3) | Don't sidecar a broken proxy |

---

## Part 3 — Kubernetes Sidecar Transition Plan

### 3.1 Phasing

Three phases, increasing in operational sophistication. Each phase is independently shippable.

**Phase A — Containerize + Manual Sidecar**
Goal: a user can add `collapser` to their Deployment by hand and have it work.

**Phase B — Helm Chart + Operator-Less Auto-Injection (annotation-based)**
Goal: a user adds an annotation to a Deployment and the collapser is injected at apply-time via Helm post-render or Kustomize.

### 3.2 Phase A — Containerize + Manual Sidecar

**Deliverables:**

- `Dockerfile` (multi-stage, distroless final stage):
  - Builder: `golang:1.25-alpine`, build statically with `CGO_ENABLED=0`
  - Runtime: `gcr.io/distroless/static-debian12:nonroot`
  - Single binary `/collapser`, exposes `50052/tcp` and `2112/tcp`
- `Makefile` targets: `docker-build`, `docker-push`, `docker-run`
- `deploy/manifests/sidecar-example.yaml` — sample Deployment with `collapser` as second container, demonstrating:
  - App configured via env to point at `localhost:50052`
  - Collapser configured via env: `BACKEND_ADDRESS=real-backend.svc:50051`
  - Native k8s sidecar (init container with `restartPolicy: Always`, requires k8s 1.29+) so it starts before app and survives app restarts
  - `terminationGracePeriodSeconds: 60` + `preStop: sleep 10` on collapser to drain
  - Resource requests/limits (start: `cpu: 50m / 200m`, `memory: 64Mi / 256Mi`)
  - Liveness on `/health`; readiness should also verify backend reachability — extend `/ready` endpoint to do a TCP dial of `BACKEND_ADDRESS`
- Scrape annotations: `prometheus.io/scrape: "true"`, `prometheus.io/port: "2112"`, `prometheus.io/path: "/metrics"`
- Document the "app must dial localhost:50052" constraint in README — this is the trade-off vs iptables redirection (Phase D, optional)

**File targets:**
- `Dockerfile` (new, repo root)
- `.dockerignore` (new)
- `deploy/manifests/sidecar-example.yaml` (new)
- `internal/proxy/handler.go` — extend `/health` into `/health` (liveness, always 200) and `/ready` (does the backend dial)
- `internal/config/config.go` — add `READINESS_BACKEND_CHECK` knob

### 3.3 Phase B — Helm Chart

**Deliverables:**

- `deploy/helm/collapser/` chart with:
  - `values.yaml`: image, target backend, cache duration, log level, resource limits, prometheus annotations toggle
  - Templates for `Deployment` (standalone proxy mode) and a separate `_sidecar.tpl` partial that consumers `include` in their own Deployment templates
  - `NOTES.txt` explaining manual sidecar wiring
- `make helm-lint`, `make helm-package` Makefile targets
- Optional: a `chart-releaser` GitHub Action publishing to `gh-pages`

The chart is *not* an injector yet — it produces YAML the user pastes into their own Deployment. This is a deliberate intermediate step before Phase C.

## Part 4 — Critical Files to Modify

For Part 1 (proxy hardening):

- `internal/proxy/handler.go` — passthrough codec wiring (H1), persistent server reference (H3), streaming detection (H6), panic-recovery interceptor (H7), readiness endpoint (Phase A)
- `internal/proxy/forward.go` — single persistent client conn (H2), TLS support (M3), `grpc.NewClient` (M4)
- `internal/collapser/collapser.go` — fix variadic notify (H4, line 80), error-cache policy (H5), bounded cache (M1), sharded cleanup (M2)
- `cmd/proxy/main.go` — wire graceful shutdown (H3), build interceptor chain (M5), construct persistent backend conn (H2)
- `internal/config/config.go` — `BACKEND_USE_TLS` plumbing (M3), tighter validation (M7), error-cache config (H5)
- `internal/monitoring/metrics.go` — fix help-string typo (L1), tune histogram buckets (L7)

For Part 3 (sidecar):

- `Dockerfile`, `.dockerignore` (new)
- `deploy/manifests/sidecar-example.yaml` (new — Phase A)
- `deploy/helm/collapser/` (new — Phase B)
- `cmd/injector/main.go`, `internal/injector/*.go` (new — Phase C1)
- `deploy/helm/collapser-injector/` (new — Phase C3)
- `Makefile` — `docker-*`, `helm-*`, `kind-up`, `e2e` targets

---

## Part 5 — Verification

Each phase has a clear gate.

**Part 1 (Proxy fixes):**
- `make test` (unit) — all green; in particular re-run after H4 fix and confirm `Stop()` notifies waiters
- `make race` — clean
- `make stress` — collapse ratio > 100:1 still holds after error-cache + bounded-cache changes
- `make bench` — confirm H2 (persistent conn) drops `BenchmarkBackend_Direct`-equivalent path latency by 5-10×
- e2e: `cmd/backend` + `cmd/proxy` + `cmd/client` produces 100 successful "Hello World" responses; metrics endpoint shows `collapser_collapsed_requests_total` ≫ `collapser_backend_calls_total`
- Negative test: kill the backend mid-flight, confirm cached errors don't pin the proxy in a failed state

**Phase A (Containerize):**
- `docker run` the image locally with `BACKEND_ADDRESS` pointing at host backend, run client, verify
- Apply `sidecar-example.yaml` to a `kind` cluster, exec into the app container, hit `localhost:50052`, verify response

**Phase B (Helm):**
- `helm lint` clean
- `helm install` in `kind` produces a working Deployment
- Annotations actually map to env vars

---

## Part 6 — Open Decisions

These shape scope and should be answered before execution:

4. **Streaming RPCs** — Bypass them through the proxy un-collapsed (H6 option a), or refuse them with a clear error (option b)? Recommendation: bypass — most real workloads have a mix.
5. **Error caching default** — Leave default as "cache all errors for 100ms" for back-compat, or change default to "don't cache errors"? Recommendation: change default; the current behavior is a foot-gun.
