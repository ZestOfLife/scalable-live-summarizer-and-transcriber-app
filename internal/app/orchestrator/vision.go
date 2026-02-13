package orchestrator

import (
	"container/list"
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"os/exec"

	pb "github.com/ZestOfLife/scalable-live-summarizer-and-transcriber-app/pkg/gen/proto/v1"
	triton "github.com/ZestOfLife/scalable-live-summarizer-and-transcriber-app/pkg/gen/triton_proto/v1"
	"github.com/corona10/goimagehash"
	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/proto"
)

const SIMILAR_THRESHOLD = 10
const BUF_SIZE = 1024 * 1024 // 1MB buffer

type UserStream struct {
	In             chan []byte
	Ctx            context.Context
	Cancel         context.CancelFunc
	lastHash       *goimagehash.ImageHash
	id             string
	timestampStart int64
	timestamps     *list.List
}

func (w *Worker) setupNewStream(id string, timestamp_start int64, timestamps *list.List) *UserStream {
	ctx, cancel := context.WithCancel(context.Background())
	s := &UserStream{
		In:             make(chan []byte, 100),
		Ctx:            ctx,
		Cancel:         cancel,
		id:             id,
		timestampStart: timestamp_start,
		timestamps:     timestamps,
	}

	go func() {
		cmd := exec.Command("ffmpeg", "-i", "pipe:0", "-f", "image2pipe", "-vcodec", "mjpeg", "pipe:1")
		stdin, _ := cmd.StdinPipe()
		stdout, _ := cmd.StdoutPipe()
		cmd.Start()

		// Feed stdin from the channel
		go func() {
			for data := range s.In {
				videoChunk := pb.VideoChunk{}
				err := proto.Unmarshal(data, &videoChunk)
				if err != nil {
					fmt.Errorf("Error in unmarshal: %v", err)
					return
				}
				s.timestamps.PushBack(videoChunk.TimestampStart)
				s.timestamps.PushBack(videoChunk.TimestampEnd)
				stdin.Write(data)
			}
		}()

		// Read stdout for data
		for {
			frame := make([]byte, BUF_SIZE)
			n, err := stdout.Read(frame)
			if err != nil {
				fmt.Errorf("Error buffering: %v", err)
				return
			}
			new_timestamp_start := s.timestamps.Front().Value.(int64)
			s.timestamps.Remove(s.timestamps.Front())
			new_timestamp_end := s.timestamps.Front().Value.(int64)
			s.timestamps.Remove(s.timestamps.Front())
			if n > 0 {
				if s.checkDifference(frame[:n], SIMILAR_THRESHOLD) {
					go w.processVisionModel(s.id, s.timestampStart, new_timestamp_end, frame[:n])
				}
				s.timestampStart = new_timestamp_start
			}
		}
	}()

	return s
}

func (w *Worker) processVisionModel(id string, timestamp_start int64, timestamp_end int64, imageBytes []byte) {
	// Prompt
	fullPrompt := "<|image|><|begin_of_text|>" + os.Getenv("TRITON_VISION_SYSPROMPT")
	textData := make([]byte, 4+len(fullPrompt))
	binary.LittleEndian.PutUint32(textData[0:4], uint32(len(fullPrompt)))
	copy(textData[4:], []byte(fullPrompt))

	// Img
	imageData := make([]byte, 4+len(imageBytes))
	binary.LittleEndian.PutUint32(imageData[0:4], uint32(len(imageBytes)))
	copy(imageData[4:], imageBytes)

	// Tensors
	request := &triton.ModelInferRequest{
		ModelName: os.Getenv("TRITON_VISION_NAME"),
		Inputs: []*triton.ModelInferRequest_InferInputTensor{
			{
				Name:     "text_input",
				Datatype: "BYTES",
				Shape:    []int64{1},
			},
			{
				Name:     "pixel_values",
				Datatype: "BYTES",
				Shape:    []int64{1},
			},
		},
		RawInputContents: [][]byte{textData, imageData},
	}

	// Inference
	response, err := w.TritonClient.ModelInfer(context.Background(), request)
	if err != nil {
		println("Error infering: %v", err)
	}
	text, err2 := parseTritonResponse(response)
	if err2 != nil {
		println("Error parsing response: %v", err)
	}
	key := fmt.Sprintf("id:%s:type:visual", id)
	member := fmt.Sprintf("[start=%d end=%d] %s", timestamp_start, timestamp_end, text)
	w.RedisClient.ZAdd(context.Background(), key, redis.Z{
		Score:  float64(timestamp_end),
		Member: member,
	})
}
