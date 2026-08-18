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

	// Graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGTERM, syscall.SIGINT)

	go func() {
		<-quit
		srv.Stop()
	}()

	if err := srv.Start(); err != nil {
		logger.ErrorContext(ctx, "Server error", slog.Any("error", err))
		os.Exit(1)
	}
}
