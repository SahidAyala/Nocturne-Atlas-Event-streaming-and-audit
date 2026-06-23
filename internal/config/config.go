package config

import (
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	HTTPAddr      string
	GRPCAddr      string
	Postgres      PostgresConfig
	Kafka         KafkaConfig
	Elasticsearch ElasticsearchConfig
	Auth          AuthConfig
}

// AuthConfig controls how the ingest-api authenticates incoming requests.
// Mode "simple" uses a static API key (X-API-Key header) — suitable for local/dev.
// Mode "jwt" validates HMAC-signed Bearer tokens — suitable for multi-tenant production.
type AuthConfig struct {
	Mode      string // "simple" | "jwt"
	APIKey    string
	JWTSecret string
	// AdminKey protects the tenant provisioning endpoint (POST /tenants).
	// Keep this secret and separate from regular API keys.
	AdminKey string
}

type PostgresConfig struct {
	DSN string

	// Pool size limits. Defaults are chosen to be safe for a single-process
	// deployment; set POSTGRES_POOL_MAX lower when running multiple instances
	// on the same database to avoid exhausting max_connections.
	// Rule: (PoolMax × instance_count) must be < 80% of postgres max_connections.
	PoolMax            int           // POSTGRES_POOL_MAX, default 20
	PoolMin            int           // POSTGRES_POOL_MIN, default 2
	PoolMaxConnIdleTime time.Duration // POSTGRES_POOL_IDLE_SECS, default 5m
}

// KafkaConfig holds all Kafka-related settings.
//
// Topic strategy:
//   - Default is a single topic "events.v1". The .v1 suffix is the escape hatch
//     for future breaking schema changes — increment to v2 without renaming.
//   - Set KAFKA_TOPIC_ROUTES to enable domain-based routing via stream_id prefix.
//     Format: "order:events.order.v1,user:events.user.v1"
//   - DLQ topic defaults to "<topic>.dlq" (e.g. "events.v1.dlq") if not set explicitly.
//
// Provisioning:
//   - KAFKA_TOPIC_PARTITIONS controls partition count for new topics (default: 6).
//     Higher partition counts enable horizontal consumer scaling; this cannot be
//     decreased after creation without recreating the topic.
//   - KAFKA_TOPIC_REPLICATION controls the replication factor (default: 1 for dev,
//     set to 3 in production for fault tolerance).
type KafkaConfig struct {
	Brokers  []string
	Topic    string
	DLQTopic string // empty = auto-derived as Topic + ".dlq"
	GroupID  string

	// TopicRoutes enables domain-based routing via stream_id prefix.
	// Empty map = single-topic mode (default).
	// Populated from KAFKA_TOPIC_ROUTES env var.
	TopicRoutes map[string]string

	// Provisioning config — used by TopicManager at startup.
	TopicPartitions  int
	TopicReplication int
}

// DLQTopicName returns the resolved DLQ topic name.
// If KAFKA_DLQ_TOPIC is set explicitly it is returned as-is.
// Otherwise the DLQ is derived as "<main-topic>.dlq".
func (c KafkaConfig) DLQTopicName() string {
	if c.DLQTopic != "" {
		return c.DLQTopic
	}
	return c.Topic + ".dlq"
}

type ElasticsearchConfig struct {
	Addresses []string
	Index     string
	Username  string
	Password  string
}

func Load() *Config {
	kafkaTopic := getEnv("KAFKA_TOPIC", "events.v1")

	return &Config{
		HTTPAddr: getEnv("HTTP_ADDR", ":8080"),
		GRPCAddr: getEnv("GRPC_ADDR", ":50051"),
		Postgres: PostgresConfig{
			DSN:                 getEnv("POSTGRES_DSN", "postgres://events:events@localhost:5433/events?sslmode=disable"),
			PoolMax:             getEnvInt("POSTGRES_POOL_MAX", 20),
			PoolMin:             getEnvInt("POSTGRES_POOL_MIN", 2),
			PoolMaxConnIdleTime: getEnvDuration("POSTGRES_POOL_IDLE_SECS", 300) * time.Second,
		},
		Kafka: KafkaConfig{
			Brokers:  strings.Split(getEnv("KAFKA_BROKERS", "localhost:9094"), ","),
			Topic:    kafkaTopic,
			DLQTopic: getEnv("KAFKA_DLQ_TOPIC", ""), // empty = auto-derive from Topic
			GroupID:  getEnv("KAFKA_GROUP_ID", "consumer-service"),

			TopicRoutes: parseTopicRoutes(getEnv("KAFKA_TOPIC_ROUTES", "")),

			TopicPartitions:  getEnvInt("KAFKA_TOPIC_PARTITIONS", 6),
			TopicReplication: getEnvInt("KAFKA_TOPIC_REPLICATION", 1),
		},
		Elasticsearch: ElasticsearchConfig{
			Addresses: strings.Split(getEnv("ELASTICSEARCH_ADDRS", "http://localhost:9200"), ","),
			Index:     getEnv("ELASTICSEARCH_INDEX", "events"),
			Username:  getEnv("ELASTICSEARCH_USERNAME", ""),
			Password:  getEnv("ELASTICSEARCH_PASSWORD", ""),
		},
		Auth: AuthConfig{
			Mode:      getEnv("AUTH_MODE", "simple"),
			APIKey:    getEnv("AUTH_API_KEY", "dev-api-key"),
			JWTSecret: getEnv("AUTH_JWT_SECRET", ""),
			AdminKey:  getEnv("ADMIN_KEY", ""),
		},
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getEnvInt(key string, fallback int) int {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return fallback
	}
	return n
}

func getEnvDuration(key string, fallbackSeconds int) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return time.Duration(fallbackSeconds)
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return time.Duration(fallbackSeconds)
	}
	return time.Duration(n)
}

// parseTopicRoutes parses KAFKA_TOPIC_ROUTES into a map.
// Format: "order:events.order.v1,user:events.user.v1"
// Returns an empty map if the input is blank (single-topic mode).
func parseTopicRoutes(raw string) map[string]string {
	routes := make(map[string]string)
	if raw == "" {
		return routes
	}
	for _, pair := range strings.Split(raw, ",") {
		// Use SplitN(2) so topic names containing ":" are handled correctly.
		parts := strings.SplitN(strings.TrimSpace(pair), ":", 2)
		if len(parts) == 2 && parts[0] != "" && parts[1] != "" {
			routes[parts[0]] = parts[1]
		}
	}
	return routes
}
