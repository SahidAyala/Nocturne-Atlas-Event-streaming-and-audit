package middleware

import (
	"net/http"

	"github.com/SheykoWk/event-streaming-and-audit/internal/pkg/trace"
)

// TraceContext is chi middleware that extracts the W3C traceparent header,
// stores the trace context in the request context, and generates a new span ID
// for the current service. The resolved traceId is echoed back to the caller
// in the response header (ADR-014).
//
// Priority order:
//  1. traceparent header (W3C standard — carries both traceId and parentSpanId)
//  2. X-Trace-ID header (legacy single-field propagation)
//  3. Fresh traceId generated for this request (no upstream trace)
//
// The middleware never fails a request due to a malformed traceparent; it falls
// back gracefully so that tracing is always best-effort.
func TraceContext(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var tc trace.TraceContext

		if tp := r.Header.Get("traceparent"); tp != "" {
			if parsed, ok := trace.ParseTraceparent(tp); ok {
				tc = parsed
			}
		}

		// Fallback: X-Trace-ID carries just the traceId without span context.
		if !tc.Valid() {
			if xTrace := r.Header.Get("X-Trace-ID"); xTrace != "" && len(xTrace) == 32 {
				tc = trace.TraceContext{TraceID: xTrace, Sampled: true}
			}
		}

		// Last resort: generate a new trace for this request.
		if !tc.Valid() {
			tc = trace.TraceContext{TraceID: trace.NewTraceID(), Sampled: true}
		}

		// Store in context so downstream handlers can read it.
		ctx := trace.WithTraceContext(r.Context(), tc)

		// Echo the trace ID to callers so they can correlate their own logs.
		spanID := trace.NewSpanID()
		w.Header().Set("traceparent", tc.Traceparent(spanID))
		w.Header().Set("X-Trace-ID", tc.TraceID)

		next.ServeHTTP(w, r.WithContext(ctx))
	})
}
