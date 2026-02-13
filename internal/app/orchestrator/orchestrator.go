package orchestrator

import (
	"log"
	"os"
	"sync"

	"github.com/IBM/sarama"
	triton "github.com/ZestOfLife/scalable-live-summarizer-and-transcriber-app/pkg/gen/triton_proto/v1"
	"github.com/redis/go-redis/v9"
)

var (
	VIDEO_TOPIC = os.Getenv("KAFKA_TOPIC_VIDEO")
	AUDIO_TOPIC = os.Getenv("KAFKA_TOPIC_AUDIO")
	SEEK_TOPIC  = os.Getenv("KAFKA_TOPIC_SEEK")
)

type Worker struct {
	Ready         chan bool
	RedisClient   *redis.Client
	TritonClient  triton.GRPCInferenceServiceClient
	AudioSessions map[string]*UserSession
	VideoSessions map[string]*UserStream
	audioMu       sync.RWMutex
	videoMu       sync.RWMutex
}

func (w *Worker) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

func (w *Worker) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (w *Worker) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		currentTopic := message.Topic

		switch currentTopic {
		case VIDEO_TOPIC:
			w.ProcessVideo(message.Value)
		case AUDIO_TOPIC:
			w.ProcessAudio(message.Value)
		case SEEK_TOPIC:
			w.ProcessSeek(message.Value)
		default:
			log.Printf("Unknown topic: %s", currentTopic)
		}

		session.MarkMessage(message, "")
	}
	return nil
}
