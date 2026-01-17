package main

import (
	"context"
	"log"
	"net/http"
	"os"

	"github.com/ZestOfLife/scalable-live-summarizer-and-transcriber-app/internal/transport/http/query_server"
	"github.com/redis/go-redis/v9"
)

func main() {
	ctx := context.Background()

	c := redis.NewClient(&redis.Options{
		Addr:     os.Getenv("REDIS_SERVER_ADDR") + ":" + os.Getenv("REDIS_SERVER_PORT"),
		Password: "",
		DB:       0,
	})

	// Ping to check if the connection is alive
	if err := c.Ping(ctx).Err(); err != nil {
		panic(err)
	}

	mux := http.NewServeMux()
	query_server.RegisterQueryRoute(mux, c)

	// Serve
	if err := http.ListenAndServe(":"+os.Getenv("QUERY_PORT"), mux); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
