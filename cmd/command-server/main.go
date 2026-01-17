package main

import (
	"log"
	"net"
	"os"

	command_server "github.com/ZestOfLife/scalable-live-summarizer-and-transcriber-app/internal/transport/grpc/command-server"
	pb "github.com/ZestOfLife/scalable-live-summarizer-and-transcriber-app/pkg/gen/proto/v1"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"google.golang.org/grpc"
)

func main() {
	config := &kafka.ConfigMap{
		"bootstrap.servers": os.Getenv("KAFKA_BOOTSTRAP_SERVERS"),
		"client.id":         "command-server",
		"acks":              "all",
	}

	p, err := kafka.NewProducer(config)
	if err != nil {
		log.Fatalf("Failed to create producer: %s", err)
	}

	listener, err := net.Listen("tcp", ":"+os.Getenv("COMMAND_SERVER_PORT"))
	if err != nil {
		log.Fatalf("Failed to listen on port %v: %v", os.Getenv("COMMAND_SERVER_PORT"), err)
	}

	s := grpc.NewServer()
	pb.RegisterInferenceServiceServer(s, &command_server.Server{Producer: p})

	log.Printf("Server listening at %v", listener.Addr())

	// Serve
	if err := s.Serve(listener); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
