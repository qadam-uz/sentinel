// Package app is sentinel's standalone gRPC service: one deployment that many
// applications report to over the network.
//
// It is a thin shell over the embedded reporter in the root package — both
// share the same storage, cooldown and alerting — so an application can start
// with sentinel embedded (no service to deploy) and move to a central service
// later without changing what it reports. See the root package docs for the
// embedded mode.
package app

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"net"
	"runtime"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/recovery"
	"github.com/qadam-uz/sentinel"
	"github.com/qadam-uz/sentinel/config"
	"github.com/qadam-uz/sentinel/pb"
	"github.com/qadam-uz/sentinel/server"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
)

const (
	// startupTimeout bounds the service's first contact with Postgres —
	// connect, plus creating or upgrading the schema. Unbounded, a database
	// that accepts the connection but never answers would leave the service
	// hanging in a state no orchestrator can tell from a slow start.
	startupTimeout = 30 * time.Second

	// stopTimeout bounds the shutdown of the reporter behind the gRPC service.
	stopTimeout = 10 * time.Second
)

// Server is the standalone sentinel gRPC service.
//
// To run sentinel inside your own application instead, use sentinel.New from
// the root package — it needs no listener and no port.
type Server struct {
	logger *slog.Logger
	cfg    config.Config

	reporter *sentinel.Reporter
	grpc     *grpc.Server
}

// NewServer creates a new Server instance. It connects to the database,
// initializes the notifier, and sets up the gRPC server.
func NewServer(
	logger *slog.Logger,
	cfg config.Config,
) (*Server, error) {
	ctx, cancel := context.WithTimeout(context.Background(), startupTimeout)
	defer cancel()

	alerting, err := alertingFrom(cfg)
	if err != nil {
		return nil, err
	}

	reporter, err := sentinel.New(ctx, sentinel.Config{
		// Service is not set: each report carries the name of the
		// application that sent it.
		Env:             cfg.Environment,
		DSN:             cfg.DSN(),
		Schema:          cfg.PostgresSchema,
		Alert:           alerting,
		Logger:          logger,
		CooldownMinutes: cfg.AlertCooldownMinutes,
		RetentionDays:   cfg.AlertRetentionDays,
		// A service serves many clients at once, unlike an embedded reporter
		// with its single delivery goroutine; match pgxpool's own default.
		MaxConns: int32(max(4, runtime.NumCPU())), //nolint:gosec // NumCPU is small
		// Store what clients send; the notifiers cap what a chat message can
		// carry on their own.
		MaxDetailLen: -1,
	})
	if err != nil {
		return nil, err
	}

	sentinelServer := server.NewSentinelServer(logger, reporter)

	grpcPanicRecoveryHandler := func(p any) (err error) {
		buf := new(bytes.Buffer)
		stack := make([]byte, 2048)
		stackSize := runtime.Stack(stack, true)
		buf.Write(stack[:stackSize])

		err = status.Errorf(codes.Internal, "%s", p)

		logger.ErrorContext(ctx, "Panic recovered", slog.Any("error", err), slog.Any("panic", p), slog.String("stack", buf.String()))
		return err
	}

	grpcServer := grpc.NewServer(
		grpc.ChainUnaryInterceptor(
			recovery.UnaryServerInterceptor(
				recovery.WithRecoveryHandler(grpcPanicRecoveryHandler))))

	pb.RegisterSentinelServiceServer(grpcServer, sentinelServer)
	reflection.Register(grpcServer)

	return &Server{
		logger:   logger,
		cfg:      cfg,
		reporter: reporter,
		grpc:     grpcServer,
	}, nil
}

// Start begins serving gRPC requests. It blocks until the server is stopped.
func (s *Server) Start() error {
	listener, err := net.Listen("tcp", fmt.Sprintf("%s:%s", s.cfg.GrpcHost, s.cfg.GrpcPort))
	if err != nil {
		return fmt.Errorf("sentinel: listen: %w", err)
	}

	s.logger.Info("Server started", slog.String("address", listener.Addr().String()))

	if err := s.grpc.Serve(listener); err != nil {
		return fmt.Errorf("sentinel: serve: %w", err)
	}

	return nil
}

// Stop gracefully stops the server and closes the database connection.
func (s *Server) Stop() {
	s.grpc.GracefulStop()

	ctx, cancel := context.WithTimeout(context.Background(), stopTimeout)
	defer cancel()
	if err := s.reporter.Close(ctx); err != nil {
		s.logger.Error("Shutdown incomplete", slog.Any("error", err))
	}

	s.logger.Info("Server stopped")
}

// alertingFrom maps the environment-driven config onto the alert destination.
// config.LoadConfig already rejects an unknown provider; this guards the
// callers that build a config.Config by hand.
func alertingFrom(cfg config.Config) (sentinel.Alerting, error) {
	if cfg.AlertDisable {
		return sentinel.NoAlerts(), nil
	}

	switch cfg.AlertProvider {
	case config.AlertProviderTelegram:
		return sentinel.Telegram(cfg.TelegramBotToken, cfg.TelegramsChatIDs...), nil

	case config.AlertProviderDiscord:
		return sentinel.Discord(cfg.DiscordBotToken, cfg.DiscordChannelIDs...), nil

	default:
		return sentinel.Alerting{}, fmt.Errorf("sentinel: invalid alert provider: %q", cfg.AlertProvider)
	}
}
