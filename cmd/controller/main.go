package main

import (
	"context"
	"log"
	"net"
	"os"

	"google.golang.org/grpc"
	pb "../../api/stream"
)

type Server struct {
	pb.UnimplementedGreeterService
	Client ChatServiceClient
}

func (*s Server) ProcessMedia(stream pb.InferenceService_ProcessMediaServer) (*pb.Success, error) {
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
	case *pb.MediaStreamRequest_Video:
		fallthrough
	case *pb.MediaStreamRequest_Audio:
		payload.Data = CompressData(payload.Data)
		fallthrough
	case *pb.MediaStreamRequest_SeekRequest:
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		res, err := s.Client.SendMedia(ctx, payload)
		if err != nil {
			ret := &pb.Success {
				Success: false,
			}
			return ret, err
		}

		if !res.success {
			ret := &pb.Success {
				Success: false,
			}
			return ret, nil
		}
	}
	ret := &pb.Success {
		Success: true,
	}
	return ret, nil
}

func main() {
	conn, err := grpc.Dial(os.Getenv("COMMAND_SERVER_ADDR") + ":" + os.Getenv("COMMAND_SERVER_PORT"), grpc.WithTransportCredentials(insecure.NewCredentials())) 
	if err != nil {
		log.Fatalf("Did not connect: %v", err)
	}
	defer conn.Close()

	c := pb.NewChatServiceClient(conn)
	s := grpc.NewServer()

	pb.RegisterYourServiceServer(s, &Server{Client: c})

	log.Printf("Server listening at %v", listner.Addr())

	// Serve
	if err := s.Serve(listner); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
