package sentinel_test

import (
	"context"
	"log/slog"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/qadam-uz/sentinel"
)

// Sentinel embedded in an application: no service to deploy, no port.
func ExampleNew() {
	ctx := context.Background()
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	rep, err := sentinel.New(ctx, sentinel.Config{
		Service: "my-app",
		Env:     "prod",
		DSN:     os.Getenv("POSTGRES_DSN"),
		Alert:   sentinel.Telegram(os.Getenv("ALERTS_BOT_TOKEN"), -1001234567890),
		Logger:  logger,
	})
	if err != nil {
		logger.Error("sentinel", "err", err)
		return
	}
	// Late in shutdown, so errors raised by the shutdown itself still alert.
	defer rep.Close(ctx)

	if err := doWork(); err != nil {
		rep.Report(sentinel.ErrorInfo{
			Operation: "charge order", // the alert cooldown groups by this
			Message:   err.Error(),
			Details:   map[string]string{"order_id": "42"},
		})
	}
}

// Every logger.Error call alerts, with no reporting code at the call sites.
func ExampleNewSlogHandler() {
	ctx := context.Background()

	// The base logger. Sentinel logs its OWN failures here; wrapping it below
	// would make a failing alert path alert about itself.
	base := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	rep, err := sentinel.New(ctx, sentinel.Config{
		Service: "my-app",
		Env:     "prod",
		DSN:     os.Getenv("POSTGRES_DSN"),
		Alert:   sentinel.Telegram(os.Getenv("ALERTS_BOT_TOKEN"), -1001234567890),
		Logger:  base,
	})
	if err != nil {
		base.Error("sentinel", "err", err)
		return
	}
	defer rep.Close(ctx)

	logger := slog.New(sentinel.NewSlogHandler(base.Handler(), rep, nil))
	slog.SetDefault(logger)

	// Message = operation, attributes = alert details.
	logger.Error("charge order", "err", "card declined", "order_id", 42)
}

// Sentinel can share the application's pool instead of opening its own; it
// writes only to Config.Schema and does not close a borrowed pool.
func ExampleConfig_pool() {
	ctx := context.Background()

	pool, err := pgxpool.New(ctx, os.Getenv("POSTGRES_DSN"))
	if err != nil {
		return
	}
	defer pool.Close()

	rep, err := sentinel.New(ctx, sentinel.Config{
		Service: "my-app",
		Env:     "prod",
		Pool:    pool,
		Schema:  "sentinel", // kept apart from the application's tables
		Alert:   sentinel.NoAlerts(),
	})
	if err != nil {
		return
	}
	defer rep.Close(ctx)
}

func doWork() error { return nil }
