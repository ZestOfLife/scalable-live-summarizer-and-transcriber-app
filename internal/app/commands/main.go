package main

import (
	"context"
	"io"
	"log"
	"net"
	"os"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	pb "github.com/ZestOfLife/scalable-live-summarizer-and-transcriber-app/api"
)

type Server struct {
	pb.UnimplementedInferenceServiceServer
	w *kafka.Writer
}

func SendToQueue(w *Kafka.Writer, req_type string, data any) error {
	topic := os.Getenv("KAFKA_TOPIC_"+strings.ToUpper(req_type))
	return writer.Produce(&kafka.Message{
		TopicPartition: kaffka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny}
		Value:          data,
		Headers: []kafka.Header{
			{Key: "content-type", Value: []byte("application/protobuf")},
			{Key: "target-service", Value: []byte("model-"+req_type)},
		},
	}, nil)
}

func (s *Server) ProcessMedia(stream pb.InferenceService_ProcessMediaServer) error {
	for {
		req, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				res := &pb.Success{Success: true}
				return stream.SendAndClose(res)
			} else {
				return err
			}
		}

		switch payload := req.Payload.(type) {
		case *pb.MediaStreamRequest_Video:
			err := SendToQueue(s.w, "video", payload.Video)
			if err != nil {
				reutrn err
			}
		case *pb.MediaStreamRequest_Audio:
			err := SendToQueue(s.w, "audio", payload.Audio)
			if err != nil {
				return err
			}
		case *pb.MediaStreamRequest_Seek:
			err1 := SendToQueue(s.w, "video", payload.Seek)
			err2 := SendToQueue(s.w, "audio", payload.Seek)
			if err1 != nil {
				return err1
			} else if err2 != nil {
				return err2
			}
		}
	}

}

func main() {
	brokers := strings.Split(os.Getenv("KAFKA_BROKERS"), ",")

	w := &kafka.Writer {
        	Addr:     kafka.TCP(brokers...),
        	Balancer: &kafka.LeastBytes{},
	}
    	defer w.Close()
	
	listener, err := net.Listen("tcp", ":" + os.Getenv("CONTROLLER_PORT")) 
    	if err != nil {
        	log.Fatalf("Failed to listen on port %v: %v", os.Getenv("CONTROLLER_PORT"), err)
    	}

	c := pb.NewInferenceServiceClient(conn)
	s := grpc.NewServer()

	pb.RegisterInferenceServiceServer(s, &Server{Client: w})


	log.Printf("Server listening at %v", listener.Addr())

	// Serve
	if err := s.Serve(listener); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
