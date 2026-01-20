package main

import (
	"log"
	"net"
	"os"

	"github.com/ZestOfLife/scalable-live-summarizer-and-transcriber-app/internal/transport/grpc/controller"
	pb "github.com/ZestOfLife/scalable-live-summarizer-and-transcriber-app/pkg/gen/proto/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	conn, err := grpc.NewClient(os.Getenv("COMMAND_SERVER_ADDR")+":"+os.Getenv("COMMAND_SERVER_PORT"), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Did not connect: %v", err)
	}
	defer conn.Close()

	listener, err := net.Listen("tcp", ":"+os.Getenv("CONTROLLER_PORT"))
	if err != nil {
		log.Fatalf("Failed to listen on port %v: %v", os.Getenv("CONTROLLER_PORT"), err)
	}

	c := pb.NewInferenceServiceClient(conn)
	s := grpc.NewServer()

	pb.RegisterInferenceServiceServer(s, &controller.Server{Client: c})

	log.Printf("Server listening at %v", listener.Addr())

	// Serve
	if err := s.Serve(listener); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
