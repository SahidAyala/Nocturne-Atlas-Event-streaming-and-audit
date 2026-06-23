// Package main is the entrypoint for the database migration tool.
//
// Usage:
//
//	go run ./cmd/migrate          # apply all pending migrations
//	POSTGRES_DSN=<dsn> go run ./cmd/migrate
//
// Migrations are versioned SQL files embedded from db/migrations/*.sql.
// Files are executed in lexicographic order (001_, 002_, …).
// Applied migrations are tracked in the schema_migrations table so each
// file is executed exactly once, even across restarts.
package main

import (
	"context"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	migrations "github.com/SahidAyala/Nocturne-Atlas-Event-streaming-and-audit/db/migrations"
	"github.com/SahidAyala/Nocturne-Atlas-Event-streaming-and-audit/internal/config"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(log)

	cfg := config.Load()

	ctx := context.Background()

	pool, err := pgxpool.New(ctx, cfg.Postgres.DSN)
	if err != nil {
		log.Error("connect postgres", "error", err)
		os.Exit(1)
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		log.Error("ping postgres", "error", err)
		os.Exit(1)
	}

	if err := run(ctx, pool, log); err != nil {
		log.Error("migrate failed", "error", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, pool *pgxpool.Pool, log *slog.Logger) error {
	// Ensure the tracking table exists (idempotent).
	if _, err := pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			filename   TEXT        PRIMARY KEY,
			applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)`); err != nil {
		return fmt.Errorf("create schema_migrations: %w", err)
	}

	// Load already-applied migrations.
	rows, err := pool.Query(ctx, `SELECT filename FROM schema_migrations ORDER BY filename`)
	if err != nil {
		return fmt.Errorf("query applied migrations: %w", err)
	}
	applied := make(map[string]bool)
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return fmt.Errorf("scan migration row: %w", err)
		}
		applied[name] = true
	}
	rows.Close()

	// Collect migration files from the embedded FS.
	entries, err := fs.ReadDir(migrations.FS, ".")
	if err != nil {
		return fmt.Errorf("read migrations FS: %w", err)
	}

	var files []string
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".sql") {
			files = append(files, e.Name())
		}
	}
	sort.Strings(files)

	// Apply any pending migrations.
	pending := 0
	for _, name := range files {
		if applied[name] {
			log.Info("skip", "migration", name, "status", "already applied")
			continue
		}

		sql, err := fs.ReadFile(migrations.FS, name)
		if err != nil {
			return fmt.Errorf("read %s: %w", name, err)
		}

		if err := applyOne(ctx, pool, name, string(sql)); err != nil {
			return fmt.Errorf("apply %s: %w", name, err)
		}

		log.Info("applied", "migration", name)
		pending++
	}

	if pending == 0 {
		log.Info("schema is up to date", "total", len(files))
	} else {
		log.Info("migrations complete", "applied", pending, "total", len(files))
	}
	return nil
}

// applyOne executes a single migration inside a transaction and records it
// in schema_migrations. The transaction is rolled back automatically if
// either the SQL or the tracking insert fails.
func applyOne(ctx context.Context, pool *pgxpool.Pool, name, sql string) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	if _, err := tx.Exec(ctx, sql); err != nil {
		return fmt.Errorf("exec sql: %w", err)
	}

	if _, err := tx.Exec(ctx,
		`INSERT INTO schema_migrations (filename) VALUES ($1)`, name,
	); err != nil {
		return fmt.Errorf("record migration: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit: %w", err)
	}

	// Consume the pgx warning that Rollback returns after a successful Commit.
	_ = pgx.ErrTxClosed
	return nil
}
