package main

import (
	"context"
	"log"
	"net"
	"os"

	"google.golang.org/grpc"
	pb "./proto/stream"
)

type Server struct {
	pb.UnimplementedGreeterService
	Writer *kafka.Writer
}

func (*s Server) ProcessMedia(stream pb.InferenceService_ProcessMediaServer) error {
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
		fallthrough
	case *pb.MediaStreamRequest_Audio:
		go SendToQueue(s.Writer, CompressData(payload.data), payload.id, paylod.timestamp_start, payload.timestamp_end)
	}
}

func main() {
	// Kafka config
	brokers := strings.Split(os.Getenv("KAFKA_BROKERS"), ",")
	topic := os.Getenv("KAFKA_TOPIC")

	writer := &kafka.Writer{
		Addr:     kafka.TCP(brokers...),
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	}
	defer w.Close()


	listner, err := net.Listen("tcp", ":8080")
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	s := grpc.NewServer()

	pb.RegisterYourServiceServer(s, &Server{Writer: writer})

	log.Printf("Server listening at %v", listner.Addr())

	// Serve
	if err := s.Serve(listner); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}package main

import (
	"context"
	"log"
	"net"
	"os"

	"google.golang.org/grpc"
	pb "./proto/stream"
)

type Server struct {
	pb.UnimplementedGreeterService
	Writer *kafka.Writer
}

func (*s Server) ProcessMedia(stream pb.InferenceService_ProcessMediaServer) error {
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
	// Kafka config
	brokers := strings.Split(os.Getenv("KAFKA_BROKERS"), ",")
	topic := os.Getenv("KAFKA_TOPIC")

	writer := &kafka.Writer{
		Addr:     kafka.TCP(brokers...),
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	}
	defer w.Close()


	listner, err := net.Listen("tcp", ":8080")
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	s := grpc.NewServer()

	pb.RegisterYourServiceServer(s, &Server{Writer: writer})

	log.Printf("Server listening at %v", listner.Addr())

	// Serve
	if err := s.Serve(listner); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
