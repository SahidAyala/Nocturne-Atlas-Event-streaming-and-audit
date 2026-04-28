package main

import (
	"context"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"

	"google.golang.org/grpc"

	eventsv1 "github.com/SheykoWk/event-streaming-and-audit/gen/proto"
	"github.com/SheykoWk/event-streaming-and-audit/internal/application/replay"
	"github.com/SheykoWk/event-streaming-and-audit/internal/config"
	"github.com/SheykoWk/event-streaming-and-audit/internal/infrastructure/grpcserver"
	"github.com/SheykoWk/event-streaming-and-audit/internal/infrastructure/postgres"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(log)

	cfg := config.Load()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Replay reads only from PostgreSQL — no Kafka or Elasticsearch dependency.
	store, err := postgres.NewEventStore(ctx, cfg.Postgres)
	if err != nil {
		log.Error("failed to init event store", "error", err)
		os.Exit(1)
	}
	defer store.Close()

	replaySvc := replay.NewService(store, log)
	handler := grpcserver.NewHandler(replaySvc, log)

	srv := grpc.NewServer()
	eventsv1.RegisterEventServiceServer(srv, handler)

	lis, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		log.Error("failed to bind grpc listener", "addr", cfg.GRPCAddr, "error", err)
		os.Exit(1)
	}

	go func() {
		log.Info("replay-service started", "addr", cfg.GRPCAddr)
		if err := srv.Serve(lis); err != nil {
			log.Error("grpc server error", "error", err)
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	log.Info("shutdown signal received")
	srv.GracefulStop()
	log.Info("replay-service stopped")
}
