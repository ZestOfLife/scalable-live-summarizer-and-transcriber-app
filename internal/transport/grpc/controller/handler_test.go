package controller

import (
	"context"
	"crypto/rand"
	"io"
	"testing"
	"time"

	pb "github.com/ZestOfLife/scalable-live-summarizer-and-transcriber-app/pkg/gen/proto/v1"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
)

// Setup
const BUFSIZE = 64 * 1024

var listener *bufconn.Listener

// Mock Client
type MockInferenceClient struct {
	mock.Mock
	pb.InferenceServiceClient
}

func (m *MockInferenceClient) ProcessMedia(ctx context.Context, opts ...grpc.CallOption) (pb.InferenceService_ProcessMediaClient, error) {
	args := m.Called(ctx)
	return args.Get(0).(pb.InferenceService_ProcessMediaClient), args.Error(1)
}

// Mock Stream
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
	c := new(MockInferenceClient)

	stream := new(MockInferenceStream)

	c.On("ProcessMedia", mock.Anything, mock.Anything).Return(stream, nil)
	stream.On("Send", mock.Anything).Return(nil)
	stream.On("CloseAndRecv").Return(&pb.Success{Success: true}, nil)

	s := &Server{Client: c}

	mockStream := &MockProcessMediaServer{
		Requests: make(chan *pb.MediaStreamRequest, 3),
	}

	id, ts := generateUUID()
	videoData, _ := generateRandomBytes(BUFSIZE)
	audioData, _ := generateRandomBytes(BUFSIZE)

	chunks := []*pb.MediaStreamRequest{
		{
			Payload: &pb.MediaStreamRequest_Video{
				Video: &pb.VideoChunk{
					Id:             id,
					Data:           videoData,
					TimestampStart: ts,
					TimestampEnd:   ts + 300,
				},
			},
		},
		{
			Payload: &pb.MediaStreamRequest_Audio{
				Audio: &pb.AudioChunk{
					Id:             id,
					Data:           audioData,
					TimestampStart: ts,
					TimestampEnd:   ts + 300,
				},
			},
		},
		{
			Payload: &pb.MediaStreamRequest_Seek{
				Seek: &pb.SeekRequest{
					Id:            id,
					TimestampSeek: ts + 333,
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
	c.AssertExpectations(t)
	stream.AssertExpectations(t)
}
