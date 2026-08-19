package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/qadam-uz/sentinel/app"
	"github.com/qadam-uz/sentinel/config"
)

func main() {
	ctx := context.Background()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	cfg, err := config.LoadConfig()
	if err != nil {
		logger.ErrorContext(ctx, "Failed to load config", slog.Any("error", err))
		os.Exit(1)
	}

	srv, err := app.NewServer(logger, cfg)
	if err != nil {
		logger.ErrorContext(ctx, "Failed to create server", slog.Any("error", err))
		os.Exit(1)
	}

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGTERM, syscall.SIGINT)

	if err := run(srv, quit); err != nil {
		logger.ErrorContext(ctx, "Server error", slog.Any("error", err))
		os.Exit(1)
	}
}

// server is the part of app.Server that run drives.
type server interface {
	Start() error
	Stop()
}

// run serves until a signal arrives and then shuts down — on the CALLER's
// goroutine, so main cannot return while the shutdown is still working.
//
// Shutting down from the signal handler's own goroutine instead looks
// equivalent and is not: GracefulStop releases Serve as soon as the listener
// closes, Start returns, main returns behind it, and the process exits while
// Stop is still flushing. Everything queued at shutdown is lost that way —
// including the alert for whatever killed the service, which is the one that
// mattered.
func run(srv server, quit <-chan os.Signal) error {
	// Buffered: after a signal nobody reads this, and the goroutine must not
	// be left blocked on the send.
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Start() }()

	select {
	case err := <-serveErr:
		// Failed before any signal arrived — the listener, most likely.
		return err

	case <-quit:
		srv.Stop()
		return nil
	}
}
