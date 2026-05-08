package monitoring

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	RequestsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "collapser_requests_total",
		Help: "Total number of requests received",
	})

	CollapsedRequestsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "collapser_collapsed_requests_total",
		Help: "Total number of requests that joined inflight",
	})

	BackendCallsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "collapser_backend_calls_total",
		Help: "Total backend calls made",
	})

	CacheHitsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "collapser_cache_hits_total",
		Help: "Total cache hits",
	})

	InflightRequests = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "collapser_inflight_requests",
		Help: "Current number of inflight requests",
	})

	CachedResults = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "collapser_cached_results",
		Help: "Current number of cached results",
	})

	BackendLatency = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "collapser_backend_latency_seconds",
		Help:    "Backend call duration in seconds",
		Buckets: prometheus.ExponentialBuckets(0.0001, 2, 18), // 0.1ms–13s
	})
)
