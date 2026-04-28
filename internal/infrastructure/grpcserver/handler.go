package grpcserver

import (
	"context"
	"log/slog"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	eventsv1 "github.com/SheykoWk/event-streaming-and-audit/gen/proto"
	"github.com/SheykoWk/event-streaming-and-audit/internal/application/replay"
	"github.com/SheykoWk/event-streaming-and-audit/internal/domain/event"
	"github.com/SheykoWk/event-streaming-and-audit/internal/pkg/trace"
)

// Handler implements the EventServiceServer gRPC interface.
// Only Replay is implemented here; Ingest is exposed via the HTTP API.
type Handler struct {
	eventsv1.UnimplementedEventServiceServer
	replaySvc *replay.Service
	log       *slog.Logger
}

func NewHandler(replaySvc *replay.Service, log *slog.Logger) *Handler {
	return &Handler{replaySvc: replaySvc, log: log}
}

// Replay reads events from the PostgreSQL event store (source of truth)
// and returns them ordered by version ASC.
// Returns codes.InvalidArgument for bad input.
// Returns codes.DataLoss if a version gap is detected (data integrity violation).
func (h *Handler) Replay(ctx context.Context, req *eventsv1.ReplayRequest) (*eventsv1.EventStream, error) {
	if req.StreamId == "" {
		return nil, status.Error(codes.InvalidArgument, "stream_id is required")
	}

	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if vals := md.Get("correlation-id"); len(vals) > 0 && vals[0] != "" {
			ctx = trace.WithCorrelationID(ctx, vals[0])
		}
	}

	events, err := h.replaySvc.Replay(ctx, replay.Command{
		StreamID:    req.StreamId,
		FromVersion: req.FromVersion,
	})
	if err != nil {
		h.log.Error("replay request failed",
			"correlation_id", trace.FromContext(ctx),
			"stream_id", req.StreamId,
			"from_version", req.FromVersion,
			"error", err,
		)
		return nil, status.Errorf(codes.DataLoss, "replay failed: %v", err)
	}

	protoEvents := make([]*eventsv1.Event, 0, len(events))
	for _, e := range events {
		protoEvents = append(protoEvents, domainToProto(e))
	}

	return &eventsv1.EventStream{
		StreamId: req.StreamId,
		Events:   protoEvents,
	}, nil
}

// domainToProto converts a domain Event to its protobuf representation.
// Payload is transmitted as raw JSON bytes (proto field type: bytes) — no
// intermediate deserialization, no float64 coercion, no precision loss.
func domainToProto(e *event.Event) *eventsv1.Event {
	return &eventsv1.Event{
		Id:            e.ID.String(),
		StreamId:      e.StreamID,
		Type:          e.Type,
		Source:        e.Source,
		Version:       e.Version,
		OccurredAt:    timestamppb.New(e.OccurredAt),
		Payload:       []byte(e.Payload), // json.RawMessage → []byte: no conversion, immutable bytes
		Metadata:      e.Metadata,
		CorrelationId: e.CorrelationID,
	}
}
