package elasticsearch

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"

	"github.com/SheykoWk/event-streaming-and-audit/internal/config"
	"github.com/SheykoWk/event-streaming-and-audit/internal/domain/event"
)

// mapping defines the Elasticsearch index schema for events.
// keyword fields enable exact-match filtering and aggregations;
// occurred_at is a date so range queries and sorting work correctly.
// tenant_id is keyword for exact-match tenant isolation queries.
const mapping = `{
  "mappings": {
    "properties": {
      "id":             { "type": "keyword" },
      "tenant_id":      { "type": "keyword" },
      "stream_id":      { "type": "keyword" },
      "type":           { "type": "keyword" },
      "source":         { "type": "keyword" },
      "version":        { "type": "long"    },
      "occurred_at":    { "type": "date"    },
      "correlation_id": { "type": "keyword" },
      "payload":        { "type": "object", "dynamic": true },
      "metadata":       { "type": "object", "dynamic": true }
    }
  }
}`

// Indexer writes events into an Elasticsearch index.
// Index calls are idempotent: the event UUID is used as the document ID,
// so reprocessing a message produces an overwrite, not a duplicate.
type Indexer struct {
	client *elasticsearch.Client
	index  string
}

func NewIndexer(cfg config.ElasticsearchConfig) (*Indexer, error) {
	client, err := elasticsearch.NewClient(elasticsearch.Config{
		Addresses: cfg.Addresses,
	})
	if err != nil {
		return nil, fmt.Errorf("create elasticsearch client: %w", err)
	}

	idx := &Indexer{client: client, index: cfg.Index}

	// Retry with linear backoff — Elasticsearch may still be initialising even
	// after its healthcheck passes. 10 attempts × up to 10s each = ~90s max.
	const maxAttempts = 10
	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		lastErr = idx.ensureIndex(ctx)
		cancel()
		if lastErr == nil {
			return idx, nil
		}
		wait := time.Duration(attempt) * 2 * time.Second
		slog.Info("elasticsearch not ready, retrying",
			"attempt", attempt,
			"max", maxAttempts,
			"wait", wait.String(),
			"error", lastErr,
		)
		time.Sleep(wait)
	}
	return nil, fmt.Errorf("ensure index after %d attempts: %w", maxAttempts, lastErr)
}

// ensureIndex creates the index with the canonical mapping if it does not exist.
func (i *Indexer) ensureIndex(ctx context.Context) error {
	res, err := i.client.Indices.Exists(
		[]string{i.index},
		i.client.Indices.Exists.WithContext(ctx),
	)
	if err != nil {
		return fmt.Errorf("check index: %w", err)
	}
	defer res.Body.Close()

	if res.StatusCode == 200 {
		return nil // already exists
	}

	createRes, err := i.client.Indices.Create(
		i.index,
		i.client.Indices.Create.WithBody(bytes.NewReader([]byte(mapping))),
		i.client.Indices.Create.WithContext(ctx),
	)
	if err != nil {
		return fmt.Errorf("create index request: %w", err)
	}
	defer createRes.Body.Close()

	if createRes.IsError() {
		b, _ := io.ReadAll(createRes.Body)
		return fmt.Errorf("create index [%s]: %s", createRes.Status(), b)
	}
	return nil
}

// Index upserts the event as a document. Uses op_type=index (create-or-overwrite)
// so at-least-once delivery from Kafka does not produce duplicate documents.
func (i *Indexer) Index(ctx context.Context, e *event.Event) error {
	body, err := json.Marshal(e)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}

	req := esapi.IndexRequest{
		Index:      i.index,
		DocumentID: e.ID.String(),
		Body:       bytes.NewReader(body),
		OpType:     "index",
		Refresh:    "false",
	}

	res, err := req.Do(ctx, i.client)
	if err != nil {
		return fmt.Errorf("elasticsearch request: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		b, _ := io.ReadAll(res.Body)
		return fmt.Errorf("elasticsearch [%s]: %s", res.Status(), b)
	}
	return nil
}

// Search queries events from Elasticsearch.
//
// Filtering:
//   - q.StreamID non-empty → filter by stream (used by QueryByStream)
//   - q.StreamID empty     → no stream filter, return events across all streams (used by ListAll)
//   - q.TenantID non-empty → always applied when present
//
// Sorting:
//   - default              → version ASC, id ASC (deterministic within a single stream)
//   - SortByOccurredAtDesc → occurred_at DESC, id DESC (chronological feed, for cross-stream queries)
func (i *Indexer) Search(ctx context.Context, q event.SearchQuery) ([]*event.Event, int64, error) {
	// Build filters — only add a clause when the value is non-empty.
	var filters []map[string]any
	if q.StreamID != "" {
		filters = append(filters, map[string]any{
			"term": map[string]any{"stream_id": q.StreamID},
		})
	}
	if q.TenantID != "" {
		filters = append(filters, map[string]any{
			"term": map[string]any{"tenant_id": q.TenantID},
		})
	}

	// Build query — match_all when there are no filters, bool/filter otherwise.
	var esQuery map[string]any
	if len(filters) == 0 {
		esQuery = map[string]any{"match_all": map[string]any{}}
	} else {
		esQuery = map[string]any{
			"bool": map[string]any{"filter": filters},
		}
	}

	// Build sort — within a stream, version order is canonical; cross-stream,
	// occurred_at gives a meaningful chronological feed.
	var sort []map[string]any
	if q.SortByOccurredAtDesc {
		sort = []map[string]any{
			{"occurred_at": map[string]string{"order": "desc"}},
			{"id": map[string]string{"order": "desc"}},
		}
	} else {
		sort = []map[string]any{
			{"version": map[string]string{"order": "asc"}},
			{"id": map[string]string{"order": "asc"}},
		}
	}

	body := map[string]any{
		"query":            esQuery,
		"sort":             sort,
		"from":             q.Offset,
		"size":             q.Limit,
		"track_total_hits": true,
	}

	data, err := json.Marshal(body)
	if err != nil {
		return nil, 0, fmt.Errorf("marshal search query: %w", err)
	}

	res, err := i.client.Search(
		i.client.Search.WithContext(ctx),
		i.client.Search.WithIndex(i.index),
		i.client.Search.WithBody(bytes.NewReader(data)),
	)
	if err != nil {
		return nil, 0, fmt.Errorf("elasticsearch search request: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		b, _ := io.ReadAll(res.Body)
		return nil, 0, fmt.Errorf("elasticsearch search [%s]: %s", res.Status(), b)
	}

	var result struct {
		Hits struct {
			Total struct {
				Value int64 `json:"value"`
			} `json:"total"`
			Hits []struct {
				Source json.RawMessage `json:"_source"`
			} `json:"hits"`
		} `json:"hits"`
	}

	if err := json.NewDecoder(res.Body).Decode(&result); err != nil {
		return nil, 0, fmt.Errorf("decode search response: %w", err)
	}

	events := make([]*event.Event, 0, len(result.Hits.Hits))
	for _, h := range result.Hits.Hits {
		var e event.Event
		if err := json.Unmarshal(h.Source, &e); err != nil {
			return nil, 0, fmt.Errorf("unmarshal event source: %w", err)
		}
		events = append(events, &e)
	}

	return events, result.Hits.Total.Value, nil
}

// Close is a no-op; elasticsearch.Client uses http.DefaultTransport which
// manages its own connection pool.
func (i *Indexer) Close() error { return nil }
