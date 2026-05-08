package main

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/VarunGitGood/collapser-grpc/proto/hello"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	conn, err := grpc.NewClient("localhost:50052", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := hello.NewHelloServiceClient(conn)

	var wg sync.WaitGroup
	numRequests := 100
	wg.Add(numRequests)

	start := time.Now()
	for i := 0; i < numRequests; i++ {
		go func(id int) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			r, err := c.SayHello(ctx, &hello.HelloRequest{Name: "World"})
			if err != nil {
				log.Printf("could not greet: %v", err)
				return
			}
			if r.GetMessage() != "Hello World" {
				log.Printf("unexpected response: %s", r.GetMessage())
			}
		}(i)
	}

	wg.Wait()
	log.Printf("Completed %d requests in %v", numRequests, time.Since(start))
}
