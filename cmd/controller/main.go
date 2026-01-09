package main

import (
	"context"
	"io"
	"log"
	"net"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	pb "github.com/ZestOfLife/scalable-live-summarizer-and-transcriber-app/api"
)

type Server struct {
	pb.UnimplementedInferenceServiceServer
	Client pb.InferenceServiceClient
}

func (s *Server) ProcessMedia(stream pb.InferenceService_ProcessMediaServer) error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	clientStream, err := s.Client.ProcessMedia(ctx)
    	if err != nil {
        	return err
    	}

	for {
		req, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				res, err := clientStream.CloseAndRecv()
            			if err != nil {
                			return err
            			}
				return stream.SendAndClose(res)
			} else {
				return err
			}
		}
	

		switch payload := req.Payload.(type) {
		case *pb.MediaStreamRequest_Video:
			payload.Video.Data = CompressData(payload.Video.Data)
		case *pb.MediaStreamRequest_Audio:
			payload.Audio.Data = CompressData(payload.Audio.Data)
		case *pb.MediaStreamRequest_Seek:
			// Do nothing cause this will be handled by the model server
		}	

		errClient := clientStream.Send(req)
		if errClient != nil {
			return errClient
		}
	}
}

func main() {
	conn, err := grpc.Dial(os.Getenv("COMMAND_SERVER_ADDR") + ":" + os.Getenv("COMMAND_SERVER_PORT"), grpc.WithTransportCredentials(insecure.NewCredentials())) 
	if err != nil {
		log.Fatalf("Did not connect: %v", err)
	}
	defer conn.Close()

	listener, err := net.Listen("tcp", ":" + os.Getenv("CONTROLLER_PORT")) 
    	if err != nil {
        	log.Fatalf("Failed to listen on port %v: %v", os.Getenv("CONTROLLER_PORT"), err)
    	}

	c := pb.NewInferenceServiceClient(conn)
	s := grpc.NewServer()

	pb.RegisterInferenceServiceServer(s, &Server{Client: c})


	log.Printf("Server listening at %v", listener.Addr())

	// Serve
	if err := s.Serve(listener); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
