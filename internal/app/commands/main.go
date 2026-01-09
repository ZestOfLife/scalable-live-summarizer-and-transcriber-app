package main

import (
	"io"
	"log"
	"net"
	"os"
	"strings"

	"google.golang.org/grpc"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"google.golang.org/protobuf/proto"
	pb "github.com/ZestOfLife/scalable-live-summarizer-and-transcriber-app/api"
)

type KafkaProducerInterface interface {
	Produce(msg *kafka.Message, deliveryChan chan kafka.Event) error
	Close()
}

type Server struct {
	pb.UnimplementedInferenceServiceServer
	Producer KafkaProducerInterface
}

func SendToQueue(p KafkaProducerInterface, req_type string, data []byte) error {
	topic := os.Getenv("KAFKA_TOPIC_"+strings.ToUpper(req_type))
	return p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
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
			data, err1 := proto.Marshal(payload.Video)
			if err1 != nil {
				return err1
			}
			err2 := SendToQueue(s.Producer, "video", data)
			if err2 != nil {
				return err2
			}
		case *pb.MediaStreamRequest_Audio:
			data, err1 := proto.Marshal(payload.Audio)
			if err1 != nil {
				return err1
			}
			err2 := SendToQueue(s.Producer, "audio", data)
			if err2 != nil {
				return err2
			}
		case *pb.MediaStreamRequest_Seek:
			data, err1 := proto.Marshal(payload.Seek)
			if err1 != nil {
				return err1
			}
			err2 := SendToQueue(s.Producer, "video", data)
			err3 := SendToQueue(s.Producer, "audio", data)
			if err2 != nil {
				return err1
			} else if err3 != nil {
				return err2
			}
		}
	}

}

func main() {
	config := &kafka.ConfigMap{
		"bootstrap.servers": os.Getenv("KAFKA_BROKERS"),
		"client.id":         "command-server",
		"acks":              "all",
	}

	p, err := kafka.NewProducer(config)
	if err != nil {
		log.Fatalf("Failed to create producer: %s", err)
	}	
	
	listener, err := net.Listen("tcp", ":" + os.Getenv("CONTROLLER_PORT")) 
    	if err != nil {
        	log.Fatalf("Failed to listen on port %v: %v", os.Getenv("CONTROLLER_PORT"), err)
    	}

	s := grpc.NewServer()
	pb.RegisterInferenceServiceServer(s, &Server{Producer: p})


	log.Printf("Server listening at %v", listener.Addr())

	// Serve
	if err := s.Serve(listener); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
