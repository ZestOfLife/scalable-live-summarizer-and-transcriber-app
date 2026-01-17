package commands

import (
	"os"
	"strings"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type KafkaProducerInterface interface {
	Produce(msg *kafka.Message, deliveryChan chan kafka.Event) error
	Close()
}

func SendToQueue(p KafkaProducerInterface, req_type string, id string, data []byte) error {
	topic := os.Getenv("KAFKA_TOPIC_" + strings.ToUpper(req_type))
	return p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          data,
		Headers: []kafka.Header{
			{Key: "content-type", Value: []byte("application/protobuf")},
			{Key: "target-service", Value: []byte("model-" + req_type)},
		},
	}, nil)
}
