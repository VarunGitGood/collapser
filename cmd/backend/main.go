// Command backend is the demo origin service the collapser sits in front of.
//
// It deliberately reports its own request count over HTTP: the collapse ratio is
// only convincing if the number of calls that actually reached the backend is
// measured at the backend, not inferred from the proxy's own accounting.
package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/VarunGitGood/collapser-grpc/proto/hello"
	"google.golang.org/grpc"
)

type server struct {
	hello.UnimplementedHelloServiceServer
	latency time.Duration
	calls   atomic.Int64
}

func (s *server) SayHello(ctx context.Context, in *hello.HelloRequest) (*hello.HelloResponse, error) {
	n := s.calls.Add(1)
	log.Printf("backend call #%d for %q", n, in.GetName())
	time.Sleep(s.latency) // stand in for real backend work
	return &hello.HelloResponse{Message: "Hello " + in.GetName()}, nil
}

func envDuration(key string, fallback time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
		log.Printf("invalid %s, using %s", key, fallback)
	}
	return fallback
}

func envInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
		log.Printf("invalid %s, using %d", key, fallback)
	}
	return fallback
}

func main() {
	grpcPort := envInt("BACKEND_PORT", 50051)
	statsPort := envInt("BACKEND_STATS_PORT", 8080)
	srv := &server{latency: envDuration("BACKEND_LATENCY", 50*time.Millisecond)}

	// Call counter and reset, so a demo run can measure a clean window.
	go func() {
		mux := http.NewServeMux()
		mux.HandleFunc("/calls", func(w http.ResponseWriter, r *http.Request) {
			_, _ = fmt.Fprintf(w, "%d\n", srv.calls.Load())
		})
		mux.HandleFunc("/reset", func(w http.ResponseWriter, r *http.Request) {
			srv.calls.Store(0)
			w.WriteHeader(http.StatusNoContent)
		})
		mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		})
		addr := ":" + strconv.Itoa(statsPort)
		log.Printf("backend stats listening on %s", addr)
		if err := http.ListenAndServe(addr, mux); err != nil {
			log.Printf("stats server failed: %v", err)
		}
	}()

	lis, err := net.Listen("tcp", ":"+strconv.Itoa(grpcPort))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	hello.RegisterHelloServiceServer(s, srv)
	log.Printf("backend gRPC listening on :%d (latency %s)", grpcPort, srv.latency)
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
