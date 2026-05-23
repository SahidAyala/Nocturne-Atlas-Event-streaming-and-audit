// Package trace provides lightweight W3C TraceContext propagation without
// requiring the full OpenTelemetry SDK. The structures and wire format are
// intentionally compatible with OpenTelemetry so that adopting the OTel SDK
// later is additive — no field renames or data migrations needed (ADR-014).
//
// Wire format (W3C traceparent):
//
//	00-{traceId32hex}-{parentSpanId16hex}-{flags2hex}
//	  version   : 00  (always)
//	  traceId   : 128-bit trace identifier, 32 lowercase hex chars
//	  parentSpanId : 64-bit span ID of the sending service, 16 lowercase hex chars
//	  flags     : 01 (sampled) | 00 (not sampled)
//
// Example:
//
//	traceparent: 00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01
package trace

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strings"
)

// TraceContext carries the W3C TraceContext fields extracted from a traceparent header.
type TraceContext struct {
	// TraceID is the 128-bit trace identifier (32 lowercase hex chars).
	// This is the same value stored on the Event domain entity.
	TraceID string
	// ParentSpanID is the 64-bit span ID of the sending service (16 lowercase hex chars).
	// This is the span that produced the message/request we are processing.
	ParentSpanID string
	// Sampled indicates whether the trace is sampled.
	Sampled bool
}

// Valid returns true when the TraceContext contains a well-formed traceId.
func (tc TraceContext) Valid() bool {
	return len(tc.TraceID) == 32
}

// Traceparent formats the context as a W3C traceparent header value.
// spanID is the current service's span (the new child span being created).
func (tc TraceContext) Traceparent(spanID string) string {
	flags := "00"
	if tc.Sampled {
		flags = "01"
	}
	return fmt.Sprintf("00-%s-%s-%s", tc.TraceID, spanID, flags)
}

// ParseTraceparent parses a W3C traceparent header value.
// Returns a zero TraceContext and false if the value is malformed.
func ParseTraceparent(tp string) (TraceContext, bool) {
	parts := strings.Split(tp, "-")
	if len(parts) != 4 {
		return TraceContext{}, false
	}
	if parts[0] != "00" { // only version 00 is defined
		return TraceContext{}, false
	}
	traceID := parts[1]
	parentSpanID := parts[2]
	flagHex := parts[3]

	if len(traceID) != 32 || !isHex(traceID) {
		return TraceContext{}, false
	}
	if len(parentSpanID) != 16 || !isHex(parentSpanID) {
		return TraceContext{}, false
	}
	if len(flagHex) != 2 {
		return TraceContext{}, false
	}

	sampled := flagHex == "01"
	return TraceContext{TraceID: traceID, ParentSpanID: parentSpanID, Sampled: sampled}, true
}

// NewTraceID generates a cryptographically random 128-bit trace ID (32 hex chars).
func NewTraceID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic("trace: crypto/rand unavailable: " + err.Error())
	}
	return hex.EncodeToString(b[:])
}

// NewSpanID generates a cryptographically random 64-bit span ID (16 hex chars).
func NewSpanID() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic("trace: crypto/rand unavailable: " + err.Error())
	}
	return hex.EncodeToString(b[:])
}

func isHex(s string) bool {
	_, err := hex.DecodeString(s)
	return err == nil
}

// --- Context storage ---

type contextKey struct{}
type traceContextKey struct{}

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
// returns fallback. Use in service logs that have both a context and a domain value.
func Coalesce(ctx context.Context, fallback string) string {
	if id := FromContext(ctx); id != "" {
		return id
	}
	return fallback
}

// WithTraceContext returns a child context carrying the W3C TraceContext.
func WithTraceContext(ctx context.Context, tc TraceContext) context.Context {
	return context.WithValue(ctx, traceContextKey{}, tc)
}

// TraceContextFromContext returns the TraceContext stored by WithTraceContext.
// Returns a zero TraceContext if none was set.
func TraceContextFromContext(ctx context.Context) TraceContext {
	tc, _ := ctx.Value(traceContextKey{}).(TraceContext)
	return tc
}

// TraceIDFromContext returns the TraceID from the context, or empty string.
func TraceIDFromContext(ctx context.Context) string {
	return TraceContextFromContext(ctx).TraceID
}
