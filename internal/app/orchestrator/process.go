package orchestrator

import (
	"container/list"
	"log"

	pb "github.com/ZestOfLife/scalable-live-summarizer-and-transcriber-app/pkg/gen/proto/v1"
	"github.com/gorilla/websocket"
	"google.golang.org/protobuf/proto"
)

func (w *Worker) ProcessVideo(data []byte) error {
	videoChunk := pb.VideoChunk{}
	err := proto.Unmarshal(data, &videoChunk)
	if err != nil {
		return err
	}
	w.videoMu.RLock()
	s, exists := w.VideoSessions[videoChunk.Id]
	w.videoMu.RUnlock()

	if !exists {
		timestamps := list.New()
		s = w.setupNewStream(videoChunk.Id, videoChunk.TimestampStart, timestamps)
	}

	select {
	case s.In <- data:
	default:
		log.Printf("User %s buffer full, dropping data", videoChunk.Id)
	}

	return nil
}

func (w *Worker) ProcessAudio(data []byte) error {
	audioChunk := pb.AudioChunk{}
	err := proto.Unmarshal(data, &audioChunk)
	if err != nil {
		return err
	}
	ws, err2 := w.getSession(audioChunk.Id, audioChunk.TimestampStart, audioChunk.TimestampEnd)
	if err2 != nil {
		return err2
	}
	ws.WriteMessage(websocket.BinaryMessage, audioChunk.Data)
	return nil
}

func (w *Worker) ProcessSeek(data []byte) error {
	seekReq := pb.SeekRequest{}
	err := proto.Unmarshal(data, &seekReq)
	if err != nil {
		return err
	}
	return nil
}
