// Package sentinel runs error collection and alerting INSIDE your application
// process — no separate service to deploy, no port to open, no gRPC hop.
// Errors are stored in Postgres (in sentinel's own schema, which it creates
// itself), de-duplicated per service+operation with a cooldown, and posted to
// Telegram or Discord.
//
//	rep, err := sentinel.New(ctx, sentinel.Config{
//		Service: "my-app",
//		Env:     "prod",
//		DSN:     dsn,
//		Alert:   sentinel.Telegram(botToken, chatID),
//		Logger:  logger,
//	})
//	if err != nil {
//		return err
//	}
//	defer rep.Close(ctx)
//
//	rep.Report(sentinel.ErrorInfo{Operation: "charge order", Message: err.Error()})
//
// The standalone gRPC service is still available in package app; both are
// built on the same core, so an application can start embedded and move to a
// central service later without changing what it reports.
//
// Design rules, in order:
//
//  1. The host application must never slow down or break because of alerting.
//     Report is fire-and-forget through a bounded queue serviced by one
//     goroutine; when the queue is full or delivery fails, reports are counted
//     (see Stats) and dropped, never awaited. Use SendError when you do want
//     to wait for the outcome.
//
//  2. No recursion. Sentinel logs its own troubles to Config.Logger, which
//     MUST be the base logger — never one wrapped with NewSlogHandler — and
//     never above Warn level, so a failing alert path cannot generate alerts
//     about itself.
package sentinel

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/qadam-uz/sentinel/entity"
	"github.com/qadam-uz/sentinel/repository/store"
	"github.com/qadam-uz/sentinel/usecase"
)

// Defaults applied to the zero value of the matching Config field.
const (
	DefaultSchema          = "sentinel"
	DefaultCooldownMinutes = 5
	DefaultQueueSize       = 256
	DefaultSendTimeout     = 5 * time.Second
	DefaultCloseGrace      = 2 * time.Second
	DefaultMaxDetailLen    = 500
	DefaultMaxConns        = 2
	DefaultRetentionDays   = 30
)

// Retention sweep shape. One connection is held per batch, and this may be
// one of only two (DefaultMaxConns), so a run is bounded rather than deleting
// until the table is clean: what it does not finish this hour it finishes the
// next.
const (
	janitorInterval     = time.Hour
	janitorBatch        = 10_000
	janitorMaxBatches   = 20
	janitorBatchTimeout = 30 * time.Second
)

var (
	// ErrClosed is returned by SendError after Close.
	ErrClosed = errors.New("sentinel: reporter is closed")
	// ErrNoOperation is returned when a report carries no Operation. The
	// operation is the key the alert cooldown groups by, so it is required.
	ErrNoOperation = errors.New("sentinel: ErrorInfo.Operation is required")
	// ErrNoService is returned when neither Config.Service nor
	// ErrorInfo.Service names the reporting service.
	ErrNoService = errors.New("sentinel: no service name: set Config.Service or ErrorInfo.Service")
)

// Config configures an embedded sentinel. Every field except the database
// (exactly one of Pool or DSN) has a usable default.
type Config struct {
	// Service names the reporting application in stored rows and alerts. It
	// is the default for reports that do not set ErrorInfo.Service; hosts
	// that receive the name per report — such as the gRPC service in package
	// app — may leave it empty.
	Service string

	// Env is shown in alerts to tell prod apart from staging.
	Env string

	// Pool is an existing Postgres pool to borrow. Sentinel writes only to
	// its own Schema and does NOT close a borrowed pool. Mutually exclusive
	// with DSN.
	Pool *pgxpool.Pool

	// DSN makes sentinel open — and, on Close, close — a small pool of its
	// own (see MaxConns), so alerting never competes for the application's
	// connections. Mutually exclusive with Pool.
	DSN string

	// Schema holds sentinel's tables, kept apart from the application's.
	// Created on New if missing. Default DefaultSchema.
	Schema string

	// Alert selects alert delivery: Telegram, Discord, CustomNotifier or
	// NoAlerts. The zero value stores errors without alerting.
	Alert Alerting

	// Logger receives sentinel's OWN failures, at Warn level. It MUST NOT be
	// a logger wrapped with NewSlogHandler. Default slog.Default().
	Logger *slog.Logger

	// CooldownMinutes is the window in which one service+operation alerts at
	// most once. Errors outside the window are still stored.
	// Default DefaultCooldownMinutes.
	CooldownMinutes int

	// QueueSize bounds reports waiting for delivery; beyond it Report drops.
	// Default DefaultQueueSize.
	QueueSize int

	// SendTimeout bounds storing one report. The alert that follows is posted
	// from sentinel's own goroutine, under its own timeout.
	// Default DefaultSendTimeout.
	SendTimeout time.Duration

	// CloseGrace is how long Close waits for the last alert, which sentinel
	// posts asynchronously after the error is stored.
	// Default DefaultCloseGrace.
	CloseGrace time.Duration

	// RetentionDays is how long stored errors are kept. Sentinel deletes
	// older rows hourly, in batches, from one replica at a time. Nothing ever
	// reads a row older than the cooldown window — the alert decision and the
	// frequency count both look at minutes — so this only reclaims space, and
	// sentinel writes into a database it does not own. Negative keeps
	// everything, and leaves the sweep switched off entirely.
	// Default DefaultRetentionDays.
	RetentionDays int

	// MaxDetailLen caps each detail value and the message, in bytes, on a
	// UTF-8 boundary — alerts are chat messages, not dumps. Negative means no
	// cap. Default DefaultMaxDetailLen.
	MaxDetailLen int

	// MaxConns sizes the pool opened for DSN, and takes precedence over a
	// pool_max_conns in the DSN itself. Zero leaves the DSN's own setting
	// alone, or applies DefaultMaxConns when it says nothing. Ignored when
	// Pool is set.
	MaxConns int32
}

// ErrorInfo is one error report. Sentinel fills in the ID and timestamp.
type ErrorInfo struct {
	// Operation is what the application was doing ("charge order",
	// "sync catalog"). Together with the service it is the alert cooldown's
	// grouping key, so it should be a stable identifier, not a formatted
	// string with ids in it. Required.
	Operation string

	// Message is the human-readable failure, usually err.Error().
	// Defaults to Operation when empty.
	Message string

	// Code is an optional application error code, shown in the alert.
	Code string

	// Service overrides Config.Service for this report.
	Service string

	// Details are extra key/value context shown in the alert. Values are
	// capped at Config.MaxDetailLen and escaped by the notifier.
	Details map[string]string
}

// Stats counts what happened to reports handed to this Reporter. Together the
// three account for every report once delivery has caught up — a report still
// queued is in none of them yet, and one that races Close may be discarded
// uncounted, because Report deliberately takes no lock.
type Stats struct {
	// Sent was stored by sentinel. The alert itself is posted asynchronously
	// afterwards and may still be suppressed by the cooldown.
	Sent int64
	// Dropped never reached sentinel: the queue was full, the reporter was
	// closed, or the report was missing an operation or service.
	Dropped int64
	// Failed reached sentinel and errored there (storage unreachable, and so
	// on).
	Failed int64
}

// Reporter is an embedded sentinel. Create it with New, hand it errors with
// Report or SendError, and flush it with Close during shutdown. All methods
// are safe for concurrent use.
//
// A nil *Reporter is valid and every method on it does nothing, so
// alerting can be switched off by leaving the variable nil rather than by
// branching at each call site.
type Reporter struct {
	uc      usecase.UseCase
	service string

	maxDetailLen int
	sendTimeout  time.Duration
	closeGrace   time.Duration

	pool *pgxpool.Pool // non-nil only when sentinel opened it and must close it

	jobs   chan entity.ErrorInfo
	quit   chan struct{} // Close signals the loop; jobs is NEVER closed, so a
	done   chan struct{} // stray Report after Close drops rather than panics
	closed atomic.Bool

	janitorDone chan struct{} // nil when retention is off

	sent     atomic.Int64
	dropped  atomic.Int64
	failed   atomic.Int64
	lastSent atomic.Int64 // UnixNano of the last stored report; 0 = none

	failOnce   sync.Once // first delivery failure is warned about, then counted
	rejectOnce sync.Once // first malformed report likewise

	log *slog.Logger // base logger, Warn level only (see package docs)
}

// New boots an embedded sentinel: it connects to Postgres (or borrows
// Config.Pool), creates its schema and tables if they are missing, builds the
// notifier, and starts the delivery goroutine.
//
// ctx bounds the setup, which — because the pool connects lazily — is
// sentinel's entire first contact with the database. Give it a deadline: a
// slow cold start is the difference between a failed boot and an application
// that runs for days with alerting quietly off.
func New(ctx context.Context, cfg Config) (*Reporter, error) {
	cfg.applyDefaults()

	if (cfg.Pool == nil) == (cfg.DSN == "") {
		return nil, errors.New("sentinel: set exactly one of Config.Pool or Config.DSN")
	}

	pool, owned := cfg.Pool, false
	if pool == nil {
		pgCfg, err := pgxpool.ParseConfig(cfg.DSN)
		if err != nil {
			return nil, fmt.Errorf("sentinel: parse dsn: %w", err)
		}
		// ParseConfig fills MaxConns with pgx's own default when the DSN is
		// silent, so the DSN has to be inspected to tell "unset" from "set to
		// pgx's default" — otherwise a deliberate pool_max_conns would be
		// silently overwritten here.
		switch {
		case cfg.MaxConns > 0:
			pgCfg.MaxConns = cfg.MaxConns
		case !strings.Contains(cfg.DSN, "pool_max_conns"):
			pgCfg.MaxConns = DefaultMaxConns
		}
		pool, err = pgxpool.NewWithConfig(ctx, pgCfg)
		if err != nil {
			return nil, fmt.Errorf("sentinel: connect: %w", err)
		}
		owned = true
	}
	closeOwned := func() {
		if owned {
			pool.Close()
		}
	}

	st, err := store.NewPgStore(ctx, pool, cfg.Schema)
	if err != nil {
		closeOwned()
		return nil, fmt.Errorf("sentinel: init store: %w", err)
	}

	ntf, err := cfg.Alert.build(cfg.Env)
	if err != nil {
		closeOwned()
		return nil, fmt.Errorf("sentinel: notifier: %w", err)
	}

	r := newReporter(usecase.New(cfg.Logger, st, ntf, cfg.CooldownMinutes), cfg)
	if owned {
		r.pool = pool
	}
	if cfg.RetentionDays > 0 {
		r.janitorDone = make(chan struct{})
		go r.janitor(st, cfg.RetentionDays, cfg.CooldownMinutes)
	}
	return r, nil
}

// newReporter is the database-free core, also used by tests with a fake
// usecase. cfg must already have had applyDefaults called on it.
func newReporter(uc usecase.UseCase, cfg Config) *Reporter {
	r := &Reporter{
		uc:           uc,
		service:      cfg.Service,
		maxDetailLen: cfg.MaxDetailLen,
		sendTimeout:  cfg.SendTimeout,
		closeGrace:   cfg.CloseGrace,
		jobs:         make(chan entity.ErrorInfo, cfg.QueueSize),
		quit:         make(chan struct{}),
		done:         make(chan struct{}),
		log:          cfg.Logger,
	}
	go r.loop()
	return r
}

func (cfg *Config) applyDefaults() {
	if cfg.Schema == "" {
		cfg.Schema = DefaultSchema
	}
	if cfg.Alert.build == nil {
		cfg.Alert = NoAlerts()
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.CooldownMinutes <= 0 {
		cfg.CooldownMinutes = DefaultCooldownMinutes
	}
	if cfg.QueueSize <= 0 {
		cfg.QueueSize = DefaultQueueSize
	}
	if cfg.SendTimeout <= 0 {
		cfg.SendTimeout = DefaultSendTimeout
	}
	if cfg.CloseGrace < 0 {
		cfg.CloseGrace = 0
	} else if cfg.CloseGrace == 0 {
		cfg.CloseGrace = DefaultCloseGrace
	}
	if cfg.MaxDetailLen == 0 {
		cfg.MaxDetailLen = DefaultMaxDetailLen
	}
	if cfg.RetentionDays == 0 {
		cfg.RetentionDays = DefaultRetentionDays
	}
	// MaxConns is deliberately not defaulted here: New has to tell an unset
	// field from an explicit one to leave the DSN's pool_max_conns alone.
}

// Report hands one error to sentinel and returns immediately. It never
// blocks, never panics and never returns an error: when the queue is full —
// an error storm — or the reporter is closed, the report is counted in
// Stats.Dropped and forgotten. That is deliberate: the first reports of a
// storm already rang the bell, and the cooldown would have suppressed the
// rest anyway.
func (r *Reporter) Report(e ErrorInfo) {
	if r == nil {
		return // alerting is switched off
	}
	rec, err := r.record(e)
	if err != nil {
		r.dropped.Add(1)
		r.rejectOnce.Do(func() {
			r.log.Warn("sentinel: report rejected, alerts are being dropped", "err", err)
		})
		return
	}
	if r.closed.Load() {
		r.dropped.Add(1)
		return
	}
	select {
	case r.jobs <- rec:
	default:
		r.dropped.Add(1)
	}
}

// SendError stores the error on the calling goroutine and returns the storage
// error, so the caller learns whether the report was kept. Use it when
// somebody is waiting for that answer — a background job, a gRPC handler
// replying to a client — and Report everywhere on a request path.
//
// The alert itself is still posted asynchronously afterwards, subject to the
// cooldown, so a nil error means stored, not yet delivered.
func (r *Reporter) SendError(ctx context.Context, e ErrorInfo) error {
	if r == nil {
		return nil // alerting is switched off
	}
	rec, err := r.record(e)
	if err != nil {
		r.dropped.Add(1)
		return err
	}
	if r.closed.Load() {
		r.dropped.Add(1)
		return ErrClosed
	}
	if err := r.uc.SendError(ctx, rec); err != nil {
		r.failed.Add(1)
		return fmt.Errorf("sentinel: send error: %w", err)
	}
	r.markSent()
	return nil
}

// Stats reports what happened to everything handed to this Reporter so far.
// Dropped and Failed both zero means nothing was lost.
func (r *Reporter) Stats() Stats {
	if r == nil {
		return Stats{}
	}
	return Stats{
		Sent:    r.sent.Load(),
		Dropped: r.dropped.Load(),
		Failed:  r.failed.Load(),
	}
}

// Close stops intake, flushes queued reports (bounded by ctx), gives
// sentinel's asynchronous alert a short grace to finish (Config.CloseGrace),
// and closes the pool if sentinel opened it. Call it late in shutdown so
// errors raised by the shutdown itself still alert. A second Close is a
// no-op, and a Report from a leftover goroutine afterwards is dropped —
// counted wherever the drain still catches it — never a panic.
//
// It returns a non-nil error only when ctx ended before the queue was
// flushed; the reporter is shut down either way.
func (r *Reporter) Close(ctx context.Context) error {
	if r == nil {
		return nil
	}
	if r.closed.Swap(true) {
		return nil
	}
	close(r.quit)

	var err error
	select {
	case <-r.done:
		r.drainStragglers()
		r.waitForLastAlert(ctx)
		// Reports that arrived during the grace window, from a goroutine that
		// read closed as false just before the swap.
		r.drainStragglers()
	case <-ctx.Done():
		err = fmt.Errorf("sentinel: close: queued reports not flushed: %w", ctx.Err())
	}

	// The sweep holds a pool connection while it deletes; let it notice quit
	// and finish before the pool is taken away underneath it.
	if r.janitorDone != nil {
		select {
		case <-r.janitorDone:
		case <-ctx.Done():
		}
	}

	if s := r.Stats(); s.Dropped > 0 || s.Failed > 0 {
		r.log.Warn("sentinel: alerts lost", "dropped", s.Dropped, "failed", s.Failed)
	}
	if r.pool != nil {
		r.pool.Close()
	}
	return err
}

// drainStragglers counts reports that reached the queue after the delivery
// goroutine stopped. It cannot be airtight — Report takes no lock, so one can
// always arrive a moment later — and such a report is discarded either way;
// the drain only keeps the loss visible in Stats.
func (r *Reporter) drainStragglers() {
	for {
		select {
		case <-r.jobs:
			r.dropped.Add(1)
		default:
			return
		}
	}
}

// waitForLastAlert gives the alert of the most recently stored report time to
// be posted: sentinel stores the error and then notifies from its own
// goroutine, so without this a fatal error at exit would be stored and never
// posted. The wait counts from that report, not from now, so an application
// whose last error was hours ago shuts down immediately.
func (r *Reporter) waitForLastAlert(ctx context.Context) {
	last := r.lastSent.Load()
	if last == 0 || r.closeGrace <= 0 {
		return
	}
	wait := r.closeGrace - time.Since(time.Unix(0, last))
	if wait <= 0 {
		return
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-ctx.Done():
	}
}

// janitor deletes rows past the retention window, hourly. It runs on the
// schedule rather than at startup so that booting a fleet does not turn into
// a fleet of deletes; the first sweep is an hour in.
func (r *Reporter) janitor(st store.Store, retentionDays, cooldownMinutes int) {
	defer close(r.janitorDone)

	ticker := time.NewTicker(janitorInterval)
	defer ticker.Stop()

	for {
		select {
		case <-r.quit:
			return
		case <-ticker.C:
			r.sweep(st, retentionDays, cooldownMinutes)
		}
	}
}

func (r *Reporter) sweep(st store.Store, retentionDays, cooldownMinutes int) {
	for range janitorMaxBatches {
		ctx, cancel := context.WithTimeout(context.Background(), janitorBatchTimeout)
		deleted, err := st.DeleteOlderThan(ctx, retentionDays, cooldownMinutes, janitorBatch)
		cancel()

		if err != nil {
			// Warn, like every other trouble of sentinel's own: a database
			// that cannot be swept must not become an alert about itself.
			r.log.Warn("sentinel: retention sweep failed", "err", err)
			return
		}
		// A short batch means the window is clean — or another replica holds
		// the lock and returned zero immediately.
		if deleted < janitorBatch {
			return
		}

		select {
		case <-r.quit:
			return
		default:
		}
	}
}

func (r *Reporter) loop() {
	defer close(r.done)
	for {
		select {
		case e := <-r.jobs:
			r.send(e)
		case <-r.quit:
			// drain what is already queued, then stop
			for {
				select {
				case e := <-r.jobs:
					r.send(e)
				default:
					return
				}
			}
		}
	}
}

func (r *Reporter) send(e entity.ErrorInfo) {
	ctx, cancel := context.WithTimeout(context.Background(), r.sendTimeout)
	err := r.uc.SendError(ctx, e)
	cancel()
	if err != nil {
		r.failed.Add(1)
		// Warn, not Error: the alerting path must never alert about itself.
		r.failOnce.Do(func() {
			r.log.Warn("sentinel: delivery failing, alerts are being dropped", "err", err)
		})
		return
	}
	r.markSent()
}

// markSent records a stored report, and when it happened so that Close knows
// how much of the alert grace is still owed.
func (r *Reporter) markSent() {
	r.sent.Add(1)
	r.lastSent.Store(time.Now().UnixNano())
}

// record validates one report and turns it into the stored entity.
func (r *Reporter) record(e ErrorInfo) (entity.ErrorInfo, error) {
	service := e.Service
	if service == "" {
		service = r.service
	}
	if service == "" {
		return entity.ErrorInfo{}, ErrNoService
	}
	if e.Operation == "" {
		return entity.ErrorInfo{}, ErrNoOperation
	}

	msg := e.Message
	if msg == "" {
		msg = e.Operation
	}

	// Never nil: the details column is JSONB and an absent map would store a
	// JSON null that readers have to special-case.
	details := make(map[string]string, len(e.Details))
	for k, v := range e.Details {
		details[k] = truncate(v, r.maxDetailLen)
	}

	return entity.ErrorInfo{
		ID:        uuid.New().String(),
		Code:      e.Code,
		Message:   truncate(msg, r.maxDetailLen),
		Details:   details,
		Service:   service,
		Operation: e.Operation,
		CreatedAt: time.Now(),
		Alerted:   false,
	}, nil
}

// truncate caps s at limit bytes without cutting a UTF-8 rune in half — the
// values are often non-ASCII, and a split rune renders as garbage in a chat
// message. A negative limit means no cap.
func truncate(s string, limit int) string {
	if limit < 0 || len(s) <= limit {
		return s
	}
	cut := limit
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut] + "…"
}
