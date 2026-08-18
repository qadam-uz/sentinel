package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/qadam-uz/sentinel"
	"github.com/qadam-uz/sentinel/pb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

// ErrorSender stores one error report and triggers its alert.
// *sentinel.Reporter implements it, so the gRPC service and an application
// that embeds sentinel run the exact same code below the wire.
type ErrorSender interface {
	SendError(ctx context.Context, e sentinel.ErrorInfo) error
}

func NewSentinelServer(
	log *slog.Logger,
	sender ErrorSender,
) pb.SentinelServiceServer {
	return &server{
		log:    log,
		sender: sender,
	}
}

type server struct {
	log    *slog.Logger
	sender ErrorSender
	pb.UnimplementedSentinelServiceServer
}

func (s *server) SendError(ctx context.Context, in *pb.ErrorInfo) (*emptypb.Empty, error) {
	err := s.sender.SendError(ctx, sentinel.ErrorInfo{
		Operation: in.GetOperation(),
		Message:   in.GetMessage(),
		Code:      in.GetCode(),
		Service:   in.GetService(),
		Details:   in.GetDetails(),
	})
	if err != nil {
		s.log.ErrorContext(ctx, fmt.Sprintf("server.SendError: %v", err))

		switch {
		// The client's mistake, and no amount of retrying will fix it.
		case errors.Is(err, sentinel.ErrNoService), errors.Is(err, sentinel.ErrNoOperation):
			return nil, status.Error(codes.InvalidArgument, err.Error())

		// Shutting down. Genuinely transient, and the one case where a client
		// retrying against another replica is the right thing to do.
		case errors.Is(err, sentinel.ErrClosed):
			return nil, status.Error(codes.Unavailable, "sentinel: shutting down")

		// Everything else is a storage failure. The message is deliberately
		// blank: a pgx connection error spells out the host, user and
		// database, and it would go to whoever can reach this port. It is in
		// the log above instead.
		//
		// Not Unavailable: that is the code standard retry middleware acts
		// on, and a database outage would have every client re-sending error
		// reports into the database that is already down.
		default:
			return nil, status.Error(codes.Internal, "sentinel: storage unavailable")
		}
	}

	return &emptypb.Empty{}, nil
}
