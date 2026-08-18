package sentinel

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/qadam-uz/sentinel/entity"
	"github.com/qadam-uz/sentinel/repository/store"
	"github.com/qadam-uz/sentinel/usecase"
)

// fakeUC stands in for the usecase so the reporter can be tested without a
// database. gate, when non-nil, holds every delivery until the test releases
// it — that is how the queue is filled deterministically.
type fakeUC struct {
	mu   sync.Mutex
	got  []entity.ErrorInfo
	err  error
	gate chan struct{}
}

func (f *fakeUC) SendError(_ context.Context, e entity.ErrorInfo) error {
	if f.gate != nil {
		<-f.gate
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.got = append(f.got, e)
	return f.err
}

func (f *fakeUC) delivered() []entity.ErrorInfo {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]entity.ErrorInfo(nil), f.got...)
}

var _ usecase.UseCase = (*fakeUC)(nil)

func testReporter(uc usecase.UseCase, tune func(*Config)) *Reporter {
	cfg := Config{
		Service:    "test-svc",
		Logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
		CloseGrace: -1, // no grace: the fake usecase has no async notify
	}
	if tune != nil {
		tune(&cfg)
	}
	cfg.applyDefaults()
	return newReporter(uc, cfg)
}

func closeReporter(t *testing.T, r *Reporter) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := r.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestReportDeliversAndClosesFlushes(t *testing.T) {
	uc := &fakeUC{}
	r := testReporter(uc, nil)

	for _, op := range []string{"a", "b", "c"} {
		r.Report(ErrorInfo{Operation: op, Message: "boom"})
	}
	closeReporter(t, r)

	if got := len(uc.delivered()); got != 3 {
		t.Fatalf("delivered %d reports, want 3", got)
	}
	if s := r.Stats(); s.Sent != 3 || s.Dropped != 0 || s.Failed != 0 {
		t.Fatalf("stats = %+v, want {Sent:3}", s)
	}
}

func TestReportFillsDefaults(t *testing.T) {
	uc := &fakeUC{}
	r := testReporter(uc, nil)

	r.Report(ErrorInfo{Operation: "charge order"})                    // no message
	r.Report(ErrorInfo{Operation: "sync", Service: "other-svc"})      // service override
	r.Report(ErrorInfo{Operation: "x", Details: map[string]string{}}) // empty details
	closeReporter(t, r)

	got := uc.delivered()
	if len(got) != 3 {
		t.Fatalf("delivered %d reports, want 3", len(got))
	}
	if got[0].Message != "charge order" {
		t.Errorf("Message = %q, want it to fall back to the operation", got[0].Message)
	}
	if got[0].Service != "test-svc" {
		t.Errorf("Service = %q, want the config default", got[0].Service)
	}
	if got[1].Service != "other-svc" {
		t.Errorf("Service = %q, want the per-report override", got[1].Service)
	}
	for i, e := range got {
		if e.ID == "" {
			t.Errorf("report %d has no ID", i)
		}
		if e.CreatedAt.IsZero() {
			t.Errorf("report %d has no CreatedAt", i)
		}
		// The details column is JSONB and NOT NULL-adjacent: a nil map would
		// store a JSON null that every reader has to special-case.
		if e.Details == nil {
			t.Errorf("report %d has nil Details", i)
		}
	}
}

func TestReportRejectsIncompleteReports(t *testing.T) {
	uc := &fakeUC{}
	r := testReporter(uc, func(c *Config) { c.Service = "" })

	r.Report(ErrorInfo{Message: "no operation, no service"})
	r.Report(ErrorInfo{Service: "svc"}) // still no operation
	closeReporter(t, r)

	if got := uc.delivered(); len(got) != 0 {
		t.Fatalf("delivered %d reports, want none", len(got))
	}
	if s := r.Stats(); s.Dropped != 2 {
		t.Fatalf("stats = %+v, want Dropped:2", s)
	}
}

func TestSendErrorValidates(t *testing.T) {
	uc := &fakeUC{}
	r := testReporter(uc, func(c *Config) { c.Service = "" })
	defer closeReporter(t, r)

	if err := r.SendError(context.Background(), ErrorInfo{Operation: "x"}); !errors.Is(err, ErrNoService) {
		t.Errorf("err = %v, want ErrNoService", err)
	}
	if err := r.SendError(context.Background(), ErrorInfo{Service: "s"}); !errors.Is(err, ErrNoOperation) {
		t.Errorf("err = %v, want ErrNoOperation", err)
	}
}

func TestSendErrorIsSynchronous(t *testing.T) {
	uc := &fakeUC{}
	r := testReporter(uc, nil)
	defer closeReporter(t, r)

	if err := r.SendError(context.Background(), ErrorInfo{Operation: "op"}); err != nil {
		t.Fatalf("SendError: %v", err)
	}
	// No waiting, no channel: the report is already through when it returns.
	if got := uc.delivered(); len(got) != 1 {
		t.Fatalf("delivered %d reports, want 1", len(got))
	}
	if s := r.Stats(); s.Sent != 1 {
		t.Fatalf("stats = %+v, want Sent:1", s)
	}
}

func TestSendErrorCountsFailures(t *testing.T) {
	sentinelErr := errors.New("storage down")
	uc := &fakeUC{err: sentinelErr}
	r := testReporter(uc, nil)
	defer closeReporter(t, r)

	err := r.SendError(context.Background(), ErrorInfo{Operation: "op"})
	if !errors.Is(err, sentinelErr) {
		t.Fatalf("err = %v, want it to wrap %v", err, sentinelErr)
	}
	if s := r.Stats(); s.Failed != 1 || s.Sent != 0 {
		t.Fatalf("stats = %+v, want Failed:1", s)
	}
}

func TestReportCountsDeliveryFailures(t *testing.T) {
	uc := &fakeUC{err: errors.New("storage down")}
	r := testReporter(uc, nil)

	r.Report(ErrorInfo{Operation: "op"})
	closeReporter(t, r)

	if s := r.Stats(); s.Failed != 1 || s.Sent != 0 {
		t.Fatalf("stats = %+v, want Failed:1", s)
	}
}

func TestReportDropsWhenQueueIsFull(t *testing.T) {
	uc := &fakeUC{gate: make(chan struct{})}
	r := testReporter(uc, func(c *Config) { c.QueueSize = 1 })

	// The worker blocks on the first delivery, so at most two reports can be
	// accepted: one in flight and one buffered. The rest must be dropped
	// rather than block the caller.
	const n = 10
	done := make(chan struct{})
	go func() {
		defer close(done)
		for range n {
			r.Report(ErrorInfo{Operation: "storm"})
		}
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Report blocked when the queue was full")
	}

	if d := r.Stats().Dropped; d < n-2 {
		t.Fatalf("dropped %d of %d, want at least %d", d, n, n-2)
	}

	close(uc.gate)
	closeReporter(t, r)
}

func TestReportAfterCloseDrops(t *testing.T) {
	uc := &fakeUC{}
	r := testReporter(uc, nil)
	closeReporter(t, r)

	// A leftover goroutine logging an error after shutdown must be counted,
	// not send on a closed channel.
	r.Report(ErrorInfo{Operation: "straggler"})

	if got := uc.delivered(); len(got) != 0 {
		t.Fatalf("delivered %d reports after Close, want none", len(got))
	}
	if s := r.Stats(); s.Dropped != 1 {
		t.Fatalf("stats = %+v, want Dropped:1", s)
	}
	if err := r.SendError(context.Background(), ErrorInfo{Operation: "straggler"}); !errors.Is(err, ErrClosed) {
		t.Errorf("SendError after Close = %v, want ErrClosed", err)
	}
}

func TestCloseIsIdempotent(t *testing.T) {
	r := testReporter(&fakeUC{}, nil)
	closeReporter(t, r)
	closeReporter(t, r)
}

func TestCloseSkipsTheGraceWhenNothingWasSent(t *testing.T) {
	// The grace exists for the alert of the LAST stored report. An
	// application that reported nothing — or reported hours ago — must not
	// pay it at shutdown.
	r := testReporter(&fakeUC{}, func(c *Config) { c.CloseGrace = time.Minute })

	start := time.Now()
	closeReporter(t, r)
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("Close took %v with nothing to flush, want no grace at all", elapsed)
	}
}

func TestCloseWaitsForTheLastAlert(t *testing.T) {
	const grace = 300 * time.Millisecond
	r := testReporter(&fakeUC{}, func(c *Config) { c.CloseGrace = grace })

	if err := r.SendError(context.Background(), ErrorInfo{Operation: "op"}); err != nil {
		t.Fatalf("SendError: %v", err)
	}

	// Sentinel posts the alert from its own goroutine after the report is
	// stored; Close owes it the rest of the window.
	start := time.Now()
	closeReporter(t, r)
	if elapsed := time.Since(start); elapsed < grace/2 {
		t.Fatalf("Close took %v right after a report, want about %v", elapsed, grace)
	}
}

func TestCloseReportsUnflushedQueue(t *testing.T) {
	uc := &fakeUC{gate: make(chan struct{})}
	r := testReporter(uc, nil)
	r.Report(ErrorInfo{Operation: "stuck"})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := r.Close(ctx); err == nil {
		t.Fatal("Close returned nil, want an error when the flush is cut short")
	}

	close(uc.gate) // let the worker finish so the goroutine does not leak
}

func TestTruncate(t *testing.T) {
	tests := []struct {
		name  string
		in    string
		limit int
		want  string
	}{
		{"under the limit", "short", 10, "short"},
		{"exactly at the limit", "abcde", 5, "abcde"},
		{"ascii cut", "abcdefgh", 4, "abcd…"},
		{"no limit", strings.Repeat("x", 100), -1, strings.Repeat("x", 100)},
		// "aыы" is 1+2+2 bytes. A limit of 4 falls inside the second rune, so
		// the cut moves back to 3 rather than emitting half of it.
		{"multibyte on the boundary", "aыы", 3, "aы…"},
		{"multibyte would be split", "aыы", 4, "aы…"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := truncate(tt.in, tt.limit); got != tt.want {
				t.Errorf("truncate(%q, %d) = %q, want %q", tt.in, tt.limit, got, tt.want)
			}
		})
	}
}

func TestReportTruncatesDetails(t *testing.T) {
	uc := &fakeUC{}
	r := testReporter(uc, func(c *Config) { c.MaxDetailLen = 8 })

	r.Report(ErrorInfo{
		Operation: "op",
		Message:   strings.Repeat("m", 50),
		Details:   map[string]string{"body": strings.Repeat("b", 50)},
	})
	closeReporter(t, r)

	got := uc.delivered()
	if len(got) != 1 {
		t.Fatalf("delivered %d reports, want 1", len(got))
	}
	if want := strings.Repeat("m", 8) + "…"; got[0].Message != want {
		t.Errorf("Message = %q, want %q", got[0].Message, want)
	}
	if want := strings.Repeat("b", 8) + "…"; got[0].Details["body"] != want {
		t.Errorf("detail = %q, want %q", got[0].Details["body"], want)
	}
	// The operation is the cooldown's grouping key and is never truncated.
	if got[0].Operation != "op" {
		t.Errorf("Operation = %q, want %q", got[0].Operation, "op")
	}
}

func TestReportCopiesDetails(t *testing.T) {
	uc := &fakeUC{}
	r := testReporter(uc, nil)

	details := map[string]string{"k": "v"}
	r.Report(ErrorInfo{Operation: "op", Details: details})
	details["k"] = "mutated after the call"
	closeReporter(t, r)

	got := uc.delivered()
	if len(got) != 1 {
		t.Fatalf("delivered %d reports, want 1", len(got))
	}
	if got[0].Details["k"] != "v" {
		t.Errorf("detail = %q, want the value at report time", got[0].Details["k"])
	}
}

// fakeStore serves the retention sweep, which is the only part of the
// Reporter that talks to the store directly.
type fakeStore struct {
	store.Store // the sweep calls one method; the rest must never be reached

	mu      sync.Mutex
	batches []int
	deleted []int64
	err     error
}

func (f *fakeStore) DeleteOlderThan(_ context.Context, _, _, batch int) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.batches = append(f.batches, batch)
	if f.err != nil {
		return 0, f.err
	}
	if len(f.deleted) == 0 {
		return 0, nil
	}
	n := f.deleted[0]
	f.deleted = f.deleted[1:]
	return n, nil
}

func (f *fakeStore) rounds() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.batches)
}

func TestSweepStopsOnAShortBatch(t *testing.T) {
	r := testReporter(&fakeUC{}, nil)
	defer closeReporter(t, r)

	// A full batch means there is probably more; a short one means the
	// window is clean, or another replica holds the lock and returned zero.
	st := &fakeStore{deleted: []int64{janitorBatch, janitorBatch, 17}}
	r.sweep(st, 30, 5)

	if got := st.rounds(); got != 3 {
		t.Fatalf("swept %d batches, want 3", got)
	}
}

func TestSweepGivesUpOnError(t *testing.T) {
	r := testReporter(&fakeUC{}, nil)
	defer closeReporter(t, r)

	st := &fakeStore{err: errors.New("statement timeout")}
	r.sweep(st, 30, 5)

	if got := st.rounds(); got != 1 {
		t.Fatalf("swept %d batches after an error, want 1", got)
	}
}

func TestSweepIsBounded(t *testing.T) {
	r := testReporter(&fakeUC{}, nil)
	defer closeReporter(t, r)

	// Never let one run hold a connection — possibly one of only two — until
	// the whole backlog is gone.
	full := make([]int64, janitorMaxBatches*2)
	for i := range full {
		full[i] = janitorBatch
	}
	st := &fakeStore{deleted: full}
	r.sweep(st, 30, 5)

	if got := st.rounds(); got != janitorMaxBatches {
		t.Fatalf("swept %d batches, want the cap of %d", got, janitorMaxBatches)
	}
}

// A nil *Reporter is the documented way to switch alerting off, so call sites
// do not have to branch. Every method has to survive it.
func TestNilReporterIsANoOp(t *testing.T) {
	var r *Reporter

	r.Report(ErrorInfo{Operation: "op"})
	if err := r.SendError(context.Background(), ErrorInfo{Operation: "op"}); err != nil {
		t.Errorf("SendError on a nil reporter = %v, want nil", err)
	}
	if got := r.Stats(); got != (Stats{}) {
		t.Errorf("Stats on a nil reporter = %+v, want the zero value", got)
	}
	if err := r.Close(context.Background()); err != nil {
		t.Errorf("Close on a nil reporter = %v, want nil", err)
	}
}

func TestNewRequiresExactlyOneDatabase(t *testing.T) {
	ctx := context.Background()

	if _, err := New(ctx, Config{Service: "s"}); err == nil {
		t.Error("New with neither Pool nor DSN returned no error")
	}
	if _, err := New(ctx, Config{Service: "s", DSN: "host=localhost pool_max_conns=notanumber"}); err == nil {
		t.Error("New with an unparseable DSN returned no error")
	}
}

func TestApplyDefaults(t *testing.T) {
	var cfg Config
	cfg.applyDefaults()

	if cfg.Schema != DefaultSchema {
		t.Errorf("Schema = %q, want %q", cfg.Schema, DefaultSchema)
	}
	if cfg.CooldownMinutes != DefaultCooldownMinutes {
		t.Errorf("CooldownMinutes = %d, want %d", cfg.CooldownMinutes, DefaultCooldownMinutes)
	}
	if cfg.QueueSize != DefaultQueueSize {
		t.Errorf("QueueSize = %d, want %d", cfg.QueueSize, DefaultQueueSize)
	}
	if cfg.MaxDetailLen != DefaultMaxDetailLen {
		t.Errorf("MaxDetailLen = %d, want %d", cfg.MaxDetailLen, DefaultMaxDetailLen)
	}
	if cfg.RetentionDays != DefaultRetentionDays {
		t.Errorf("RetentionDays = %d, want %d", cfg.RetentionDays, DefaultRetentionDays)
	}
	if cfg.Alert.build == nil {
		t.Error("Alert has no builder; the zero value should mean NoAlerts")
	}
	if cfg.Logger == nil {
		t.Error("Logger is nil")
	}

	// A negative cap means "store it all" and must survive defaulting.
	uncapped := Config{MaxDetailLen: -1, CloseGrace: -1, RetentionDays: -1}
	uncapped.applyDefaults()
	if uncapped.MaxDetailLen != -1 {
		t.Errorf("MaxDetailLen = %d, want -1 to be preserved", uncapped.MaxDetailLen)
	}
	if uncapped.RetentionDays != -1 {
		t.Errorf("RetentionDays = %d, want -1 (keep everything) to be preserved", uncapped.RetentionDays)
	}
	if uncapped.CloseGrace != 0 {
		t.Errorf("CloseGrace = %v, want a negative grace to become none", uncapped.CloseGrace)
	}
}

func TestAlertingBuilders(t *testing.T) {
	tests := []struct {
		name    string
		a       Alerting
		wantErr bool
	}{
		{"no alerts", NoAlerts(), false},
		{"telegram without a token", Telegram("", 1), true},
		{"telegram without chats", Telegram("token"), true},
		{"discord without a token", Discord("", "1"), true},
		{"discord without channels", Discord("token"), true},
		{"custom notifier is nil", CustomNotifier(nil), true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n, err := tt.a.build("test")
			if (err != nil) != tt.wantErr {
				t.Fatalf("build error = %v, wantErr %v", err, tt.wantErr)
			}
			if err == nil && n == nil {
				t.Error("build returned no notifier and no error")
			}
		})
	}
}
