package main;

import (
	"logs"
	"os"

	"githut.com/klauspost/compress/zstd"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

var encoder, _ = zstd.NewWriter(nil)

func CompressData(bytes[] data) bytes[] {
	compressed := encoder.EncodeAll(data, make([]byte, 0, len(data)))
	return compressed
}


func SendToQueue(Writer writer, bytes[] data, string id, int64 timestamp_start, int64 timestamp_end) error {
	topic := os.Getenv("KAFKA_TOPIC")
	err := writer.Produce(&kafka.Message{
		TopicPartition: kaffka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny}
		Value:          data,
		Headers: []kafka.Header{
			{Key: "id":, Value: []byte(id)},
			{Key: "timestamp_start", Value: []byte(timestamp_start)},
			{Key: "timestamp_end", Value: []byte(timestamp_end)},
		},
	}, nil)
	return err
}
