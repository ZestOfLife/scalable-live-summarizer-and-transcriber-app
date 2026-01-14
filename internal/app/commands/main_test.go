package main

import (
	"context"
	"crypto/rand"
	"io"
	"testing"
	"time"

	pb "github.com/ZestOfLife/scalable-live-summarizer-and-transcriber-app/pkg/gen/proto/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/grpc/test/bufconn"
	"github.com/google/uuid"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// Setup
const BUFSIZE = 64 * 1024
var listener *bufconn.Listener

//Mock Stream
type MockInferenceStream struct {
    pb.InferenceService_ProcessMediaClient
    mock.Mock
}

func (m *MockInferenceStream) Send(req *pb.MediaStreamRequest) error {
    args := m.Called(req)
    return args.Error(0)
}

func (m *MockInferenceStream) CloseAndRecv() (*pb.Success, error) {
    args := m.Called()
    return args.Get(0).(*pb.Success), args.Error(1)
}

// Mock Server
type MockProcessMediaServer struct {
	pb.InferenceService_ProcessMediaServer
	// Req channel
	Requests chan *pb.MediaStreamRequest
	// Response
	Response *pb.Success
}

func (m *MockProcessMediaServer) Recv() (*pb.MediaStreamRequest, error) {
	req, ok := <-m.Requests
	if !ok {
		return nil, io.EOF
	}
	return req, nil
}

func (m *MockProcessMediaServer) SendAndClose(res *pb.Success) error {
	m.Response = res
	return nil
}

func (m *MockProcessMediaServer) Context() context.Context {
	return context.Background()
}

// Mock Kafka

type MockKafkaProducer struct {
    mock.Mock
}

func (m *MockKafkaProducer) Produce(msg *kafka.Message, deliveryChan chan kafka.Event) error {
    args := m.Called(msg, deliveryChan)
    return args.Error(0)
}

func (m *MockKafkaProducer) Close() {
	// Nothing to implement
}

// Data generation
func generateRandomBytes(size int) ([]byte, error) {
	blk := make([]byte, size)
	_, err := rand.Read(blk)
	if err != nil {
		return nil, err
	}
	return blk, nil
}

func generateUUID() (string, int64) {
	return uuid.NewString(), time.Now().UnixMilli()
}


// Tests
func TestCreateProcessMedia(t *testing.T) {
	t.Setenv("KAFKA_TOPIC_VIDEO", "video-command")
	t.Setenv("KAFKA_TOPIC_AUDIO", "audio-command")
	t.Setenv("KAFKA_TOPIC_SEEK", "seek-command")
	t.Setenv("KAFKA_BROKERS", "localhost:9092")

	k := new(MockKafkaProducer)

	stream := new(MockInferenceStream)

	s := &Server{Producer: k}

	mockStream := &MockProcessMediaServer {
		Requests: make(chan *pb.MediaStreamRequest, 3),
	}

	id, ts := generateUUID()
	videoData, _ := generateRandomBytes(BUFSIZE)
	audioData, _ := generateRandomBytes(BUFSIZE)

	k.On("Produce", mock.MatchedBy(func(m *kafka.Message) bool {
        	return (*m.TopicPartition.Topic == "video-command" || *m.TopicPartition.Topic == "audio-command" || *m.TopicPartition.Topic == "seek-command") && len(m.Value) > 0
    	}), mock.Anything).Return(nil)

	chunks := []*pb.MediaStreamRequest {
		{
			Payload: &pb.MediaStreamRequest_Video {
				Video: &pb.VideoChunk {
					Id: id,
					Data: videoData,
					TimestampStart: ts,
					TimestampEnd: ts+300,
				},
			}, 
		},
		{
			Payload: &pb.MediaStreamRequest_Audio {
				Audio: &pb.AudioChunk {
					Id: id,
					Data: audioData,
					TimestampStart: ts,
					TimestampEnd: ts+300,
				},
			},
		},
		{
			Payload: &pb.MediaStreamRequest_Seek {
				Seek: &pb.SeekRequest {
					Id: id,
					TimestampSeek: ts+333,
				},
			},
		},
	}

	for _, c := range chunks {
		mockStream.Requests <- c
	}
	close(mockStream.Requests)

	err := s.ProcessMedia(mockStream)
	assert.NoError(t, err)
	stream.AssertExpectations(t)	
}
