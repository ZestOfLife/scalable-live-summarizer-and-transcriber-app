package command_server

import (
	"io"

	"github.com/ZestOfLife/scalable-live-summarizer-and-transcriber-app/internal/app/commands"
	pb "github.com/ZestOfLife/scalable-live-summarizer-and-transcriber-app/pkg/gen/proto/v1"
	"google.golang.org/protobuf/proto"
)

type Server struct {
	pb.UnimplementedInferenceServiceServer
	Producer commands.KafkaProducerInterface
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
			err2 := commands.SendToQueue(s.Producer, "video", payload.Video.Id, data)
			if err2 != nil {
				return err2
			}
		case *pb.MediaStreamRequest_Audio:
			data, err1 := proto.Marshal(payload.Audio)
			if err1 != nil {
				return err1
			}
			err2 := commands.SendToQueue(s.Producer, "audio", payload.Audio.Id, data)
			if err2 != nil {
				return err2
			}
		case *pb.MediaStreamRequest_Seek:
			data, err1 := proto.Marshal(payload.Seek)
			if err1 != nil {
				return err1
			}
			err2 := commands.SendToQueue(s.Producer, "seek", payload.Seek.Id, data)
			if err2 != nil {
				return err2
			}
		}
	}

}
