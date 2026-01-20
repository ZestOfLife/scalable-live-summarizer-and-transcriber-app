package controller

import (
	"context"
	"io"
	"time"

	"github.com/ZestOfLife/scalable-live-summarizer-and-transcriber-app/internal/compressutil"
	pb "github.com/ZestOfLife/scalable-live-summarizer-and-transcriber-app/pkg/gen/proto/v1"
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
			payload.Video.Data = compressutil.CompressData(payload.Video.Data)
		case *pb.MediaStreamRequest_Audio:
			payload.Audio.Data = compressutil.CompressData(payload.Audio.Data)
		case *pb.MediaStreamRequest_Seek:
			// Do nothing cause this will be handled by the model server
		}

		errClient := clientStream.Send(req)
		if errClient != nil {
			return errClient
		}
	}
}
