package orchestrator

import (
	"log"
	"os"

	"github.com/IBM/sarama"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
)

var (
	VIDEO_TOPIC = os.Getenv("KAFKA_TOPIC_VIDEO")
	AUDIO_TOPIC = os.Getenv("KAFKA_TOPIC_AUDIO")
	SEEK_TOPIC  = os.Getenv("KAFKA_TOPIC_SEEK")
)

type Worker struct {
	Ready        chan bool
	RedisClient  *redis.Client
	TritonClient *grpc.ClientConn
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
			ProcessVideo(message.Value)
		case AUDIO_TOPIC:
			ProcessAudio(message.Value)
		case SEEK_TOPIC:
			ProcessSeek(message.Value)
		default:
			log.Printf("Unknown topic: %s", currentTopic)
		}

		session.MarkMessage(message, "")
	}
	return nil
}
