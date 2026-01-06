import (
	"context"
	"crypto/rand"
	"fmt"
	"net"
	"testing"

	pb "../../api/stream"
	"github.com/stretchr/testify/mock"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

// Setup
const BUFSIZE = 64 * 1024

func setupServer() *grpc.Server {
	listner := bufcon.Listen(BUFSIZE)
	s := grpc.NewServer()

	return s
}

func setupClient(t *testing.T) (*pb.MockChatServiceClient, func()) {
    ctrl := gomock.NewController(t)
    mock := pb.NewMockChatServiceClient(ctrl)
    
    return mock, func() { ctrl.Finish() }
}

func bufDialer(context.Context, string) (net.Conn, error) {
	return lis.Dial()
}

func generateRandomBytes(size int) ([]byte, error) {
	blk := make([]byte, size)
	_, err := rand.Read(blk)
	if err != nil {
		return nil, err
	}
	return blk, nil
}

func generateUUID() (string, time.Time) {
	return uuid.NewString(), time.Now()
}

// Tests
func TestCreateProcessMedia(t *testing.T) {
	s := setupServer()
	c, cleanup := setupClient(t)
	defer cleanup()

	pb.RegisterYourServiceServer(s, &Server{Client: c})

	uuid, ts := generateUUID()
	videoData, _ := generateRandomBytes(BUFSIZE)
	audioData, _ := generateRandomBytes(BUFSIZE)

	videoChunkRequest := &pb.VideoChunk {
		Id: uuid,
		Data: videoData,
		timestamp_start: ts,
		timestamp_end: ts+300,
	}
	audioChunkRequest := &pb.AudioChunk {
		Id: uuid,
		Data: audioData,
		TimestampStart: ts,
		TimestampEnd: ts+300,
	}
	seekRequest := &pb.SeekRequest {
		Id: uuid,
		TimestampSeek: ts+333,
	}

	res1, err1 := s.ProcessMedia(context.Background(), videoChunkRequest)
	res2, err2 := s.ProcessMedia(context.Background(), audioChunkRequest)
	res3, err3 := s.ProcessMedia(context.Background(), seekRequest)

	if err1 != nil || err2 != nill || err3 != nil {
		t.Fatalf("Unexpected error returned: %v\n%v\n%v\n", err1, err2, err3)
	}
	if !res1.Success || !res2.Success || !res3.Success {
		t.Errorf("Something went wrong... Video Chunk returned %v, Audio Chunk returned %v, and Seek Request returned %v", res1.Success, res2.Success, res3.Success)
	}
}
