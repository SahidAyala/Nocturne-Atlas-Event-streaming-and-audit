package kafka

import (
	"strings"

	"github.com/SahidAyala/Nocturne-Atlas-Event-streaming-and-audit/internal/config"
	"github.com/SahidAyala/Nocturne-Atlas-Event-streaming-and-audit/internal/domain/event"
)

// TopicResolver decides which Kafka topic an event should be published to.
//
// Having this as an interface is the key architectural decision: the Producer
// never references topic names directly. Swapping routing strategies is a
// one-line change in main — no Producer or Consumer code changes needed.
//
// Evolution path:
//
//	v1  SingleTopicResolver("events.v1")       — everything to one topic
//	v2  PrefixTopicResolver(routes, fallback)  — domain-based routing via stream_id prefix
//	v3  config-file driven, schema-aware, etc. — whatever the future needs
type TopicResolver interface {
	// Resolve returns the topic name for the given event.
	Resolve(e *event.Event) string

	// Topics returns every topic this resolver may route to.
	// Used by TopicManager to ensure all topics exist before the first message.
	Topics() []string
}

// NewTopicResolver constructs the right resolver from config.
//
//   - No KAFKA_TOPIC_ROUTES set → SingleTopicResolver (default, zero-config)
//   - KAFKA_TOPIC_ROUTES set   → PrefixTopicResolver (domain routing)
func NewTopicResolver(cfg config.KafkaConfig) TopicResolver {
	if len(cfg.TopicRoutes) == 0 {
		return newSingleTopicResolver(cfg.Topic)
	}
	return newPrefixTopicResolver(cfg.TopicRoutes, cfg.Topic)
}

// =============================================================================
// SingleTopicResolver — all events to one topic (v1 default)
// =============================================================================

type singleTopicResolver struct {
	topic string
}

func newSingleTopicResolver(topic string) *singleTopicResolver {
	return &singleTopicResolver{topic: topic}
}

func (r *singleTopicResolver) Resolve(_ *event.Event) string { return r.topic }
func (r *singleTopicResolver) Topics() []string              { return []string{r.topic} }

// =============================================================================
// PrefixTopicResolver — routes by stream_id prefix (v2 domain routing)
//
// StreamID convention: "{domain}:{id}" — e.g. "order:1", "user:42".
// The prefix before the first ":" is matched against the routes map.
//
// Example config (KAFKA_TOPIC_ROUTES):
//   order:events.order.v1,user:events.user.v1
//
// "order:1"     → events.order.v1
// "user:42"     → events.user.v1
// "payment:99"  → fallback (events.v1)  ← unknown prefixes always have a home
// =============================================================================

type prefixTopicResolver struct {
	routes   map[string]string // domain prefix → topic name
	fallback string            // topic for unmatched prefixes (main topic)
}

func newPrefixTopicResolver(routes map[string]string, fallback string) *prefixTopicResolver {
	return &prefixTopicResolver{routes: routes, fallback: fallback}
}

func (r *prefixTopicResolver) Resolve(e *event.Event) string {
	if i := strings.Index(e.StreamID, ":"); i > 0 {
		if topic, ok := r.routes[e.StreamID[:i]]; ok {
			return topic
		}
	}
	return r.fallback
}

func (r *prefixTopicResolver) Topics() []string {
	seen := map[string]bool{r.fallback: true}
	topics := []string{r.fallback}
	for _, t := range r.routes {
		if !seen[t] {
			seen[t] = true
			topics = append(topics, t)
		}
	}
	return topics
}
