package trace

import "context"

type contextKey struct{}

// WithCorrelationID returns a child context carrying the given correlation ID.
func WithCorrelationID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, contextKey{}, id)
}

// FromContext returns the correlation ID stored by WithCorrelationID,
// or an empty string if none was set.
func FromContext(ctx context.Context) string {
	id, _ := ctx.Value(contextKey{}).(string)
	return id
}

// Coalesce returns the correlation ID from ctx when non-empty, otherwise
// returns fallback. Use this in service logs that have access to both a
// context (potentially carrying a request-scoped ID) and a domain value.
func Coalesce(ctx context.Context, fallback string) string {
	if id := FromContext(ctx); id != "" {
		return id
	}
	return fallback
}
