package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/IBM/sarama"
	"github.com/ZestOfLife/scalable-live-summarizer-and-transcriber-app/internal/app/orchestrator"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	VIDEO_TOPIC = os.Getenv("KAFKA_TOPIC_VIDEO")
	AUDIO_TOPIC = os.Getenv("KAFKA_TOPIC_AUDIO")
	SEEK_TOPIC  = os.Getenv("KAFKA_TOPIC_SEEK")
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	redisClient := redis.NewClient(&redis.Options{
		Addr:     os.Getenv("REDIS_CACHE_SERVER_ADDR") + ":" + os.Getenv("REDIS_CACHE_SERVER_PORT"),
		Password: "",
		DB:       0,
	})

	// Ping to check if the connection is alive
	if err := redisClient.Ping(ctx).Err(); err != nil {
		panic(err)
	}

	tritonClient, err := grpc.NewClient(os.Getenv("TRITON_ADDR_IP")+":"+os.Getenv("TRITON_ADDR_PORT"), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect to Triton: %v", err)
	}

	config := sarama.NewConfig()
	config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategyRoundRobin()}
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	brokers := []string{os.Getenv("KAFKA_BOOTSTRAP_SERVERS")}
	groupID := "video-processing-group"
	topics := []string{VIDEO_TOPIC, AUDIO_TOPIC, SEEK_TOPIC}

	client, err := sarama.NewConsumerGroup(brokers, groupID, config)
	if err != nil {
		log.Panicf("Error creating consumer group client: %v", err)
	}

	worker := &orchestrator.Worker{RedisClient: redisClient, TritonClient: tritonClient}

	go func() {
		for {
			if err := client.Consume(ctx, topics, worker); err != nil {
				log.Printf("Error from consumer: %v", err)
			}
			if ctx.Err() != nil {
				return
			}
		}
	}()

	log.Println("Consumer started...")

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
	<-sigterm

	log.Println("Shutting down...")
	cancel()
	if err = client.Close(); err != nil {
		log.Panicf("Error closing client: %v", err)
	}
}
