// The cooldown transaction, its advisory lock and the idempotent DDL are the
// parts of sentinel that cannot be reasoned about without a real Postgres:
// they are made of NOW(), row locks and catalog races. These tests need one.
//
//	docker compose -f docker-compose.test.yml up -d
//	SENTINEL_TEST_DSN='postgres://postgres:postgres@localhost:5433/sentinel_test?sslmode=disable' \\
//	  go test ./repository/store/
//
// Without SENTINEL_TEST_DSN they skip, so `go test ./...` stays runnable
// anywhere.
package store

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/qadam-uz/sentinel/entity"
)

func testPool(t *testing.T) *pgxpool.Pool {
	t.Helper()

	dsn := os.Getenv("SENTINEL_TEST_DSN")
	if dsn == "" {
		t.Skip("SENTINEL_TEST_DSN is not set; skipping the Postgres tests")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Fatalf("ping: %v", err)
	}
	t.Cleanup(pool.Close)

	return pool
}

// newTestStore gives each test its own schema, so they cannot see each
// other's rows. Advisory locks are per DATABASE rather than per schema, so
// tests still have to keep their service names distinct.
func newTestStore(t *testing.T) (*pgStore, string) {
	t.Helper()

	pool := testPool(t)
	schema := "sentinel_test_" + uuid.New().String()[:8]

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	st, err := NewPgStore(ctx, pool, schema)
	if err != nil {
		t.Fatalf("NewPgStore: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_, _ = pool.Exec(ctx, fmt.Sprintf(`DROP SCHEMA IF EXISTS %q CASCADE;`, schema))
	})

	return st, schema
}

func errorAt(service, operation string, createdAt time.Time) entity.ErrorInfo {
	return entity.ErrorInfo{
		ID:        uuid.New().String(),
		Code:      "CODE",
		Message:   "boom",
		Details:   map[string]string{},
		Service:   service,
		Operation: operation,
		CreatedAt: createdAt,
	}
}

func mustAdd(t *testing.T, st *pgStore, e entity.ErrorInfo) entity.ErrorInfo {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := st.Add(ctx, e); err != nil {
		t.Fatalf("Add: %v", err)
	}
	return e
}

func TestInitDBIsIdempotentAndConcurrentSafe(t *testing.T) {
	pool := testPool(t)
	schema := "sentinel_test_" + uuid.New().String()[:8]
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_, _ = pool.Exec(ctx, fmt.Sprintf(`DROP SCHEMA IF EXISTS %q CASCADE;`, schema))
	})

	// Several replicas booting against a fresh database at the same moment.
	// CREATE ... IF NOT EXISTS is idempotent but not atomic, so without the
	// DDL advisory lock one of these fails on a catalog duplicate key.
	const replicas = 8
	errs := make(chan error, replicas)
	var wg sync.WaitGroup
	for range replicas {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			_, err := NewPgStore(ctx, pool, schema)
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)

	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent NewPgStore: %v", err)
		}
	}
}

func TestCooldownClaimIsGrantedOnceThenSuppressed(t *testing.T) {
	st, _ := newTestStore(t)
	ctx := context.Background()

	first := mustAdd(t, st, errorAt("svc-claim", "op", time.Now()))
	if err := st.CheckAndMarkAlerted(ctx, first, 5); err != nil {
		t.Fatalf("first claim: %v", err)
	}

	second := mustAdd(t, st, errorAt("svc-claim", "op", time.Now()))
	err := st.CheckAndMarkAlerted(ctx, second, 5)
	if !errors.Is(err, ErrAlertCooldown) {
		t.Fatalf("second claim = %v, want ErrAlertCooldown", err)
	}

	// A different operation is a different window.
	other := mustAdd(t, st, errorAt("svc-claim", "other op", time.Now()))
	if err := st.CheckAndMarkAlerted(ctx, other, 5); err != nil {
		t.Fatalf("claim for a different operation: %v", err)
	}
}

// The window has to be measured from when the alert was SENT. Measuring from
// created_at meant that alerting on an error which had waited ten minutes in
// a queue immediately reopened the window for the next one.
func TestCooldownIsMeasuredFromTheAlertNotTheError(t *testing.T) {
	st, _ := newTestStore(t)
	ctx := context.Background()

	old := mustAdd(t, st, errorAt("svc-anchor", "op", time.Now().Add(-30*time.Minute)))
	if err := st.CheckAndMarkAlerted(ctx, old, 5); err != nil {
		t.Fatalf("claim on an old error: %v", err)
	}

	// The alert went out just now, so the next report must be suppressed even
	// though the error it alerted on is half an hour old.
	fresh := mustAdd(t, st, errorAt("svc-anchor", "op", time.Now()))
	err := st.CheckAndMarkAlerted(ctx, fresh, 5)
	if !errors.Is(err, ErrAlertCooldown) {
		t.Fatalf("claim = %v, want ErrAlertCooldown", err)
	}
}

func TestCooldownExpires(t *testing.T) {
	st, _ := newTestStore(t)
	ctx := context.Background()

	first := mustAdd(t, st, errorAt("svc-expire", "op", time.Now()))
	if err := st.CheckAndMarkAlerted(ctx, first, 5); err != nil {
		t.Fatalf("first claim: %v", err)
	}

	// A zero-minute window is already over.
	second := mustAdd(t, st, errorAt("svc-expire", "op", time.Now()))
	if err := st.CheckAndMarkAlerted(ctx, second, 0); err != nil {
		t.Fatalf("claim after the window = %v, want it granted", err)
	}
}

// The lock is what stops two replicas both deciding to alert for the same
// service+operation in the same instant.
func TestConcurrentClaimsGrantExactlyOne(t *testing.T) {
	st, _ := newTestStore(t)

	const racers = 10
	reports := make([]entity.ErrorInfo, racers)
	for i := range reports {
		reports[i] = mustAdd(t, st, errorAt("svc-race", "op", time.Now()))
	}

	results := make(chan error, racers)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := range racers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			results <- st.CheckAndMarkAlerted(ctx, reports[i], 5)
		}()
	}
	close(start)
	wg.Wait()
	close(results)

	granted, suppressed := 0, 0
	for err := range results {
		switch {
		case err == nil:
			granted++
		case errors.Is(err, ErrAlertCooldown):
			suppressed++
		default:
			t.Fatalf("unexpected claim error: %v", err)
		}
	}
	if granted != 1 || suppressed != racers-1 {
		t.Fatalf("granted %d and suppressed %d of %d, want exactly one alert",
			granted, suppressed, racers)
	}
}

// Delivery failed, so the window is cut short rather than cleared: the next
// error waits a little instead of either waiting the full cooldown or asking
// the failing chat API again at once.
func TestBackdateAlertShortensTheWindow(t *testing.T) {
	st, _ := newTestStore(t)
	ctx := context.Background()

	first := mustAdd(t, st, errorAt("svc-backdate", "op", time.Now()))
	if err := st.CheckAndMarkAlerted(ctx, first, 5); err != nil {
		t.Fatalf("claim: %v", err)
	}

	// Still inside the shortened window.
	if err := st.BackdateAlert(ctx, first.ID, 4); err != nil {
		t.Fatalf("BackdateAlert: %v", err)
	}
	second := mustAdd(t, st, errorAt("svc-backdate", "op", time.Now()))
	if err := st.CheckAndMarkAlerted(ctx, second, 5); !errors.Is(err, ErrAlertCooldown) {
		t.Fatalf("claim one minute into a five minute window = %v, want ErrAlertCooldown", err)
	}

	// Past it.
	if err := st.BackdateAlert(ctx, first.ID, 2); err != nil {
		t.Fatalf("BackdateAlert: %v", err)
	}
	third := mustAdd(t, st, errorAt("svc-backdate", "op", time.Now()))
	if err := st.CheckAndMarkAlerted(ctx, third, 5); err != nil {
		t.Fatalf("claim after the shortened window = %v, want it granted", err)
	}
}

func TestBackdateAlertReportsAMissingRow(t *testing.T) {
	st, _ := newTestStore(t)

	err := st.BackdateAlert(context.Background(), uuid.New().String(), 4)
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("BackdateAlert on a missing row = %v, want ErrNotFound", err)
	}
}

// Rows written before alerted_at existed keep NULL. There is no backfill —
// rewriting a multi-million-row table under an ACCESS EXCLUSIVE lock at every
// boot is worse than the problem — so the cooldown has to read them through
// created_at instead.
func TestLegacyRowsWithoutAlertedAtStillSuppress(t *testing.T) {
	st, schema := newTestStore(t)
	ctx := context.Background()

	legacy := mustAdd(t, st, errorAt("svc-legacy", "op", time.Now()))
	_, err := st.pool.Exec(ctx, fmt.Sprintf(
		`UPDATE %q.errors SET alerted = true, alerted_at = NULL WHERE id = $1;`, schema), legacy.ID)
	if err != nil {
		t.Fatalf("age the row: %v", err)
	}

	next := mustAdd(t, st, errorAt("svc-legacy", "op", time.Now()))
	if err := st.CheckAndMarkAlerted(ctx, next, 5); !errors.Is(err, ErrAlertCooldown) {
		t.Fatalf("claim = %v, want a pre-upgrade row to keep suppressing", err)
	}

	// And an old one no longer does.
	_, err = st.pool.Exec(ctx, fmt.Sprintf(
		`UPDATE %q.errors SET created_at = NOW() - INTERVAL '1 hour' WHERE id = $1;`, schema), legacy.ID)
	if err != nil {
		t.Fatalf("age the row: %v", err)
	}
	if err := st.CheckAndMarkAlerted(ctx, next, 5); err != nil {
		t.Fatalf("claim = %v, want it granted once the legacy row is stale", err)
	}
}

func TestClaimReportsAMissingRow(t *testing.T) {
	st, _ := newTestStore(t)

	// Retention deleted the row between storing it and alerting on it.
	ghost := errorAt("svc-ghost", "op", time.Now())
	err := st.CheckAndMarkAlerted(context.Background(), ghost, 5)
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("claim on a missing row = %v, want ErrNotFound", err)
	}
}

func TestGetErrorFrequencyCountsTheWindowOnly(t *testing.T) {
	st, _ := newTestStore(t)
	ctx := context.Background()

	for range 3 {
		mustAdd(t, st, errorAt("svc-freq", "op", time.Now()))
	}
	mustAdd(t, st, errorAt("svc-freq", "op", time.Now().Add(-time.Hour)))
	mustAdd(t, st, errorAt("svc-freq", "other op", time.Now()))
	mustAdd(t, st, errorAt("other svc", "op", time.Now()))

	got, err := st.GetErrorFrequency(ctx, "svc-freq", "op", 5)
	if err != nil {
		t.Fatalf("GetErrorFrequency: %v", err)
	}
	if got != 3 {
		t.Errorf("frequency = %d, want 3 — the window, operation or service leaked", got)
	}
}

func TestDeleteOlderThanRemovesOnlyExpiredRows(t *testing.T) {
	st, _ := newTestStore(t)
	ctx := context.Background()

	for range 4 {
		mustAdd(t, st, errorAt("svc-retention", "op", time.Now().Add(-60*24*time.Hour)))
	}
	keep := mustAdd(t, st, errorAt("svc-retention", "op", time.Now()))

	deleted, err := st.DeleteOlderThan(ctx, 30, 5, 1000)
	if err != nil {
		t.Fatalf("DeleteOlderThan: %v", err)
	}
	if deleted != 4 {
		t.Fatalf("deleted %d rows, want 4", deleted)
	}

	// The fresh row survived, and the second sweep has nothing left to do.
	if err := st.BackdateAlert(ctx, keep.ID, 0); err != nil {
		t.Errorf("the row inside the window was deleted: %v", err)
	}
	again, err := st.DeleteOlderThan(ctx, 30, 5, 1000)
	if err != nil {
		t.Fatalf("second DeleteOlderThan: %v", err)
	}
	if again != 0 {
		t.Errorf("second sweep deleted %d rows, want 0", again)
	}
}

func TestDeleteOlderThanRespectsTheBatch(t *testing.T) {
	st, _ := newTestStore(t)
	ctx := context.Background()

	for range 5 {
		mustAdd(t, st, errorAt("svc-batch", "op", time.Now().Add(-60*24*time.Hour)))
	}

	deleted, err := st.DeleteOlderThan(ctx, 30, 5, 2)
	if err != nil {
		t.Fatalf("DeleteOlderThan: %v", err)
	}
	if deleted != 2 {
		t.Errorf("deleted %d rows, want the batch size of 2", deleted)
	}
}

// The sweep must never queue: a replica that loses the race has to return at
// once rather than hold a connection waiting for its turn.
func TestDeleteOlderThanDoesNotWaitForTheLock(t *testing.T) {
	st, _ := newTestStore(t)
	pool := st.pool

	holder, err := pool.Begin(context.Background())
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer holder.Rollback(context.Background()) //nolint:errcheck // test cleanup

	_, err = holder.Exec(context.Background(),
		`SELECT pg_advisory_xact_lock($1, $2);`, lockClass, lockKeySwep)
	if err != nil {
		t.Fatalf("hold the sweep lock: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	start := time.Now()
	deleted, err := st.DeleteOlderThan(ctx, 30, 5, 1000)
	if err != nil {
		t.Fatalf("DeleteOlderThan: %v", err)
	}
	if deleted != 0 {
		t.Errorf("deleted %d rows while another replica held the lock, want 0", deleted)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("waited %v for the lock, want an immediate return", elapsed)
	}
}
