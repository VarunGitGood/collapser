package main

import (
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/VarunGitGood/collapser-grpc/internal/collapser"
	"github.com/VarunGitGood/collapser-grpc/internal/config"
	"github.com/VarunGitGood/collapser-grpc/internal/logger"
	"github.com/VarunGitGood/collapser-grpc/internal/proxy"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	if err := logger.Init(cfg.LogLevel, cfg.LogFormat); err != nil {
		log.Fatalf("failed to initialize logger: %v", err)
	}
	defer logger.Sync()

	logger.Info("Starting Collapser Proxy",
		zap.Int("grpc_port", cfg.GRPCPort),
		zap.Int("metrics_port", cfg.MetricsPort),
		zap.String("backend_address", cfg.BackendAddress))

	var transportCreds credentials.TransportCredentials
	if cfg.BackendUseTLS {
		transportCreds = credentials.NewTLS(nil)
	} else {
		transportCreds = insecure.NewCredentials()
	}
	conn, err := grpc.NewClient(cfg.BackendAddress, grpc.WithTransportCredentials(transportCreds))
	if err != nil {
		logger.Fatal("failed to create backend connection", zap.Error(err))
	}
	defer func() { _ = conn.Close() }()

	collapserCfg := collapser.Config{
		ResultCacheDuration: cfg.ResultCacheDuration,
		BackendTimeout:      cfg.BackendTimeout,
		CleanupInterval:     cfg.CleanupInterval,
		CacheErrors:         cfg.CacheErrors,
		MaxCacheEntries:     cfg.MaxCacheEntries,
	}
	c := collapser.NewCollapser(collapserCfg)
	if err := c.Start(); err != nil {
		logger.Fatal("failed to start collapser", zap.Error(err))
	}

	proxyHandler := proxy.NewHandler(c, conn, cfg.KeyHeaders)

	// Metrics + health server.
	go func() {
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.Handler())
		// Liveness: the process is up. Restarting on backend trouble would only
		// make an outage worse, so this never depends on the backend.
		mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		})
		// Readiness: only take traffic once the backend channel is usable, so a
		// rolling deploy does not route into a proxy that cannot reach anything.
		mux.HandleFunc("/ready", func(w http.ResponseWriter, r *http.Request) {
			switch conn.GetState() {
			case connectivity.Ready, connectivity.Idle:
				w.WriteHeader(http.StatusOK)
			default:
				http.Error(w, "backend not ready", http.StatusServiceUnavailable)
			}
		})
		logger.Info("Metrics server starting", zap.Int("port", cfg.MetricsPort))
		if err := http.ListenAndServe(":"+strconv.Itoa(cfg.MetricsPort), mux); err != nil {
			logger.Error("metrics server failed", zap.Error(err))
		}
	}()

	lis, err := net.Listen("tcp", ":"+strconv.Itoa(cfg.GRPCPort))
	if err != nil {
		logger.Fatal("failed to listen", zap.Error(err))
	}

	// Server is owned by main so GracefulStop can be called on signal (H3).
	srv := proxyHandler.NewServer()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		if err := srv.Serve(lis); err != nil {
			logger.Error("proxy server failed", zap.Error(err))
		}
	}()

	<-sigCh
	logger.Info("Shutting down gracefully...")
	srv.GracefulStop() // drain in-flight gRPC calls before closing
	_ = c.Stop()       // stop the cache cleanup loop
}
