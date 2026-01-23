package orchestrator

import (
	pb "github.com/ZestOfLife/scalable-live-summarizer-and-transcriber-app/pkg/gen/proto/v1"
	"github.com/gorilla/websocket"
	"google.golang.org/protobuf/proto"
)

func sendToTriton(data []byte)

func (w *Worker) ProcessVideo(data []byte) error {
	videoChunk := pb.VideoChunk{}
	err := proto.Unmarshal(data, &videoChunk)
	if err != nil {
		return err
	}
	return nil
}

func (w *Worker) ProcessAudio(data []byte) error {
	audioChunk := pb.AudioChunk{}
	err := proto.Unmarshal(data, &audioChunk)
	if err != nil {
		return err
	}
	err2 := w.WhisperLiveClient.WriteMessage(websocket.BinaryMessage, audioChunk.Data)
	return err2
}

func (w *Worker) ProcessSeek(data []byte) error {
	seekReq := pb.SeekRequest{}
	err := proto.Unmarshal(data, &seekReq)
	if err != nil {
		return err
	}
	return nil
}
