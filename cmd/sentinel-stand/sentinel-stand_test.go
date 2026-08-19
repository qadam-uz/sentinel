package main

import (
	"errors"
	"os"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// fakeServer behaves like app.Server during a shutdown: Stop releases Serve
// first — that is what GracefulStop does — and only then does the slow part,
// flushing queued alerts.
type fakeServer struct {
	serving   chan struct{}
	stopWork  time.Duration
	startErr  error
	stopEnded atomic.Bool
}

func (f *fakeServer) Start() error {
	if f.startErr != nil {
		return f.startErr
	}
	<-f.serving
	return nil
}

func (f *fakeServer) Stop() {
	close(f.serving)
	time.Sleep(f.stopWork)
	f.stopEnded.Store(true)
}

func TestRunWaitsForTheShutdownToFinish(t *testing.T) {
	srv := &fakeServer{serving: make(chan struct{}), stopWork: 200 * time.Millisecond}
	quit := make(chan os.Signal, 1)

	done := make(chan error, 1)
	go func() { done <- run(srv, quit) }()

	quit <- syscall.SIGTERM

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("run: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("run never returned")
	}

	// The whole point. Returning here lets main return and the process exit
	// while the reporter is still flushing, which drops the alerts a shutdown
	// produces — the fatal error that caused it, above all.
	if !srv.stopEnded.Load() {
		t.Fatal("run returned while Stop was still flushing")
	}
}

func TestRunReturnsAStartupFailure(t *testing.T) {
	boom := errors.New("listen tcp :5001: address already in use")
	srv := &fakeServer{serving: make(chan struct{}), startErr: boom}

	err := run(srv, make(chan os.Signal, 1))

	if !errors.Is(err, boom) {
		t.Fatalf("run = %v, want it to report %v", err, boom)
	}
	if srv.stopEnded.Load() {
		t.Error("Stop ran for a server that never started")
	}
}
