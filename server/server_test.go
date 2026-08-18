package server

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"strings"
	"testing"

	"github.com/qadam-uz/sentinel"
	"github.com/qadam-uz/sentinel/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

type fakeSender struct {
	err  error
	got  sentinel.ErrorInfo
	seen bool
}

func (f *fakeSender) SendError(_ context.Context, e sentinel.ErrorInfo) error {
	f.got, f.seen = e, true
	return f.err
}

// dial stands the service up over an in-memory connection, so the codes under
// test are the ones a real client would see rather than the ones the handler
// returned.
func dial(t *testing.T, sender ErrorSender) pb.SentinelServiceClient {
	t.Helper()

	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	pb.RegisterSentinelServiceServer(srv, NewSentinelServer(
		slog.New(slog.NewTextHandler(io.Discard, nil)), sender))

	go func() { _ = srv.Serve(lis) }()

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() {
		conn.Close()
		srv.Stop()
		lis.Close()
	})

	return pb.NewSentinelServiceClient(conn)
}

func TestSendErrorPassesTheReportThrough(t *testing.T) {
	sender := &fakeSender{}
	client := dial(t, sender)

	_, err := client.SendError(context.Background(), &pb.ErrorInfo{
		Code:      "PAYMENT",
		Message:   "card declined",
		Service:   "checkout",
		Operation: "charge order",
		Details:   map[string]string{"order_id": "42"},
	})
	if err != nil {
		t.Fatalf("SendError: %v", err)
	}

	if !sender.seen {
		t.Fatal("the report never reached the reporter")
	}
	if sender.got.Service != "checkout" || sender.got.Operation != "charge order" {
		t.Errorf("got %+v, want the wire values", sender.got)
	}
	if sender.got.Details["order_id"] != "42" {
		t.Errorf("details = %v, want the wire details", sender.got.Details)
	}
}

func TestSendErrorStatusCodes(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want codes.Code
	}{
		{"missing service", sentinel.ErrNoService, codes.InvalidArgument},
		{"missing operation", sentinel.ErrNoOperation, codes.InvalidArgument},
		{"shutting down", sentinel.ErrClosed, codes.Unavailable},
		{"storage down", errors.New("failed to connect"), codes.Internal},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := dial(t, &fakeSender{err: tt.err})

			_, err := client.SendError(context.Background(), &pb.ErrorInfo{
				Service: "svc", Operation: "op",
			})
			if got := status.Code(err); got != tt.want {
				t.Errorf("code = %v, want %v (err: %v)", got, tt.want, err)
			}
		})
	}
}

// A pgx failure spells out the host, user and database it could not reach.
// That belongs in the service's log, not in a status message handed to
// whoever can open a connection to the port.
func TestStorageFailuresDoNotLeakConnectionDetails(t *testing.T) {
	client := dial(t, &fakeSender{
		err: errors.New(`failed to connect to \'host=db.internal user=postgres database=sentinel\'`),
	})

	_, err := client.SendError(context.Background(), &pb.ErrorInfo{Service: "svc", Operation: "op"})

	msg := status.Convert(err).Message()
	for _, secret := range []string{"db.internal", "user=", "database="} {
		if strings.Contains(msg, secret) {
			t.Errorf("status message leaked %q: %q", secret, msg)
		}
	}
}
