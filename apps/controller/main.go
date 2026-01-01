package main

import (
	"context"
	"log"
	"net"

	"google.golang.org/grpc"
	pb "./proto/stream"
)

type server struct {
	pb.UnimplementedGreeterService
}

func (*s server) ProcessMedia(stream pb.InferenceService_ProcessMediaServer) error {
	for {
		req, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				return nil
			} else {
				retrun err
			}
		}
	}

	switch payload := req.Payload.(type) {
	case *pb.MediaStreamRequest_SeekRequest:
		// TODO
	case *pb.MediaStreamRequest_Video:
		// TODO
	case *pb.MediaStreamRequest_Audio:
		// TODO
	}
}

func main() {
	listner, err := net.Listen("tcp", ":8080")
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	s := grpc.NewServer()

	pb.RegisterGreeterserver(s, &server{})

	log.Printf("Server listening at %v", listner.Addr())

	// Serve
	if err := s.Serve(listner); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
