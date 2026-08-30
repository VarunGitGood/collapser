package main

import (
	"context"
	"log"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/VarunGitGood/collapser-grpc/proto/hello"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	target := "localhost:50052"
	if v := os.Getenv("PROXY_ADDRESS"); v != "" {
		target = v
	}
	numRequests := 100
	if v := os.Getenv("CONCURRENCY"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			numRequests = n
		}
	}
	// distinctKeys spreads the burst over N different payloads, so a run can
	// exercise several collapse groups at once instead of one giant group.
	distinctKeys := 1
	if v := os.Getenv("DISTINCT_KEYS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			distinctKeys = n
		}
	}

	conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer func() { _ = conn.Close() }()
	c := hello.NewHelloServiceClient(conn)

	// LOOP keeps firing bursts forever, for driving a cluster long enough to
	// produce useful telemetry. Unset, the client sends one burst and exits.
	loop := os.Getenv("LOOP") == "true"
	pause := envDuration("BURST_INTERVAL", 250*time.Millisecond)

	for {
		runBurst(c, numRequests, distinctKeys)
		if !loop {
			return
		}
		time.Sleep(pause)
	}
}

func envDuration(key string, fallback time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return fallback
}

func runBurst(c hello.HelloServiceClient, numRequests, distinctKeys int) {
	var wg sync.WaitGroup
	wg.Add(numRequests)

	start := time.Now()
	for i := 0; i < numRequests; i++ {
		go func(id int) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			name := "World"
			if distinctKeys > 1 {
				name = "World-" + strconv.Itoa(id%distinctKeys)
			}
			r, err := c.SayHello(ctx, &hello.HelloRequest{Name: name})
			if err != nil {
				log.Printf("could not greet: %v", err)
				return
			}
			if r.GetMessage() != "Hello "+name {
				log.Printf("unexpected response: %s", r.GetMessage())
			}
		}(i)
	}

	wg.Wait()
	log.Printf("Completed %d requests in %v", numRequests, time.Since(start))
}
