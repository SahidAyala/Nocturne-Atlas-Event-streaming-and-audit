package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/SahidAyala/Nocturne-Atlas-Event-streaming-and-audit/internal/application/consume"
	"github.com/SahidAyala/Nocturne-Atlas-Event-streaming-and-audit/internal/config"
	"github.com/SahidAyala/Nocturne-Atlas-Event-streaming-and-audit/internal/infrastructure/elasticsearch"
	"github.com/SahidAyala/Nocturne-Atlas-Event-streaming-and-audit/internal/infrastructure/kafka"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(log)

	cfg := config.Load()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Ensure all required Kafka topics exist before the consumer starts reading.
	// This covers the case where auto.create.topics.enable is disabled in the broker.
	topicManager := kafka.NewTopicManager(cfg.Kafka, log)
	if err := topicManager.EnsureTopics(ctx, []kafka.TopicSpec{
		{
			Name:        cfg.Kafka.Topic,
			Partitions:  cfg.Kafka.TopicPartitions,
			Replication: cfg.Kafka.TopicReplication,
		},
		{
			Name:        cfg.Kafka.DLQTopicName(),
			Partitions:  1, // DLQ is low-volume; single partition is fine
			Replication: cfg.Kafka.TopicReplication,
		},
	}); err != nil {
		log.Error("failed to ensure kafka topics", "error", err)
		os.Exit(1)
	}

	indexer, err := elasticsearch.NewIndexer(cfg.Elasticsearch)
	if err != nil {
		log.Error("failed to init elasticsearch indexer", "error", err)
		os.Exit(1)
	}
	defer indexer.Close() //nolint:errcheck

	consumer := kafka.NewConsumer(cfg.Kafka, log)
	defer consumer.Close() //nolint:errcheck

	dlq := kafka.NewDLQProducer(cfg.Kafka)
	defer dlq.Close() //nolint:errcheck

	svc := consume.NewService(consumer, indexer, dlq, log)

	log.Info("consumer-service started",
		"topic", cfg.Kafka.Topic,
		"dlq_topic", cfg.Kafka.DLQTopicName(),
		"group", cfg.Kafka.GroupID,
		"brokers", cfg.Kafka.Brokers,
	)

	if err := svc.Run(ctx); err != nil {
		log.Error("consumer-service error", "error", err)
		os.Exit(1)
	}

	log.Info("consumer-service stopped")
}
