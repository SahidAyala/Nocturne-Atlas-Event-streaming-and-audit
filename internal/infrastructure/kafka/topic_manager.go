package kafka

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/SahidAyala/Nocturne-Atlas-Event-streaming-and-audit/internal/config"
)

// TopicSpec describes a Kafka topic to be provisioned.
type TopicSpec struct {
	Name        string
	Partitions  int
	Replication int
}

// TopicManager provisions Kafka topics at service startup.
//
// Why this exists instead of relying on auto.create.topics.enable:
//   - Auto-create is typically disabled in production brokers.
//   - Auto-created topics use broker defaults (often 1 partition, 1 replica)
//     which are wrong for production workloads.
//   - Explicit provisioning gives control over partition count, replication
//     factor, and retention — without needing manual ops steps.
//
// EnsureTopics is idempotent: already-existing topics are left unchanged.
type TopicManager struct {
	brokers []string
	log     *slog.Logger
}

func NewTopicManager(cfg config.KafkaConfig, log *slog.Logger) *TopicManager {
	return &TopicManager{brokers: cfg.Brokers, log: log}
}

// EnsureTopics creates any topics that do not already exist.
// It connects to the controller broker (required for topic creation) and
// retries up to 5 times with linear backoff to tolerate a broker that is
// still finishing startup when this is called.
func (m *TopicManager) EnsureTopics(ctx context.Context, specs []TopicSpec) error {
	const maxAttempts = 5

	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if lastErr = m.tryEnsureTopics(ctx, specs); lastErr == nil {
			return nil
		}
		wait := time.Duration(attempt) * 2 * time.Second
		m.log.Warn("kafka not ready, retrying topic provisioning",
			"attempt", attempt,
			"max", maxAttempts,
			"wait", wait.String(),
			"error", lastErr,
		)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(wait):
		}
	}
	return fmt.Errorf("ensure topics after %d attempts: %w", maxAttempts, lastErr)
}

func (m *TopicManager) tryEnsureTopics(ctx context.Context, specs []TopicSpec) error {
	// Connect to any broker to discover the cluster.
	conn, err := kafkago.DialContext(ctx, "tcp", m.brokers[0])
	if err != nil {
		return fmt.Errorf("dial kafka: %w", err)
	}
	defer conn.Close()

	// Read all existing topic partitions to know what already exists.
	partitions, err := conn.ReadPartitions()
	if err != nil {
		return fmt.Errorf("read partitions: %w", err)
	}
	existing := make(map[string]bool, len(partitions))
	for _, p := range partitions {
		existing[p.Topic] = true
	}

	// Determine which topics need to be created.
	var toCreate []kafkago.TopicConfig
	for _, s := range specs {
		if existing[s.Name] {
			m.log.Info("kafka topic exists", "topic", s.Name)
			continue
		}
		toCreate = append(toCreate, kafkago.TopicConfig{
			Topic:             s.Name,
			NumPartitions:     s.Partitions,
			ReplicationFactor: s.Replication,
		})
	}

	if len(toCreate) == 0 {
		return nil
	}

	// Topic creation must be sent to the controller broker specifically.
	controller, err := conn.Controller()
	if err != nil {
		return fmt.Errorf("get controller: %w", err)
	}

	controllerConn, err := kafkago.DialContext(ctx, "tcp",
		net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)),
	)
	if err != nil {
		return fmt.Errorf("dial controller %s:%d: %w", controller.Host, controller.Port, err)
	}
	defer controllerConn.Close()

	if err := controllerConn.CreateTopics(toCreate...); err != nil {
		return fmt.Errorf("create topics: %w", err)
	}

	for _, t := range toCreate {
		m.log.Info("kafka topic created",
			"topic", t.Topic,
			"partitions", t.NumPartitions,
			"replication", t.ReplicationFactor,
		)
	}

	return nil
}
