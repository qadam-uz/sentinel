package store

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/qadam-uz/sentinel/entity"
)

// Postgres has two separate advisory-lock spaces: one keyed by a single
// bigint, one by a pair of int32s. The per-service+operation cooldown lock
// uses the bigint space (see hashServiceOperation), so these fixed locks in
// the pair space can never collide with a live cooldown, whatever the hash
// produces.
const (
	lockClass   = 0x5E17 // "SE17", for sentinel
	lockKeyDDL  = 1      // held while the schema is created or upgraded
	lockKeySwep = 2      // held by whichever replica is deleting old rows
)

// NewPgStore connects sentinel's tables to pool, creating or upgrading them if
// needed. ctx bounds that setup — and, because pgxpool connects lazily, the
// first connection to Postgres along with it.
func NewPgStore(ctx context.Context, pool *pgxpool.Pool, schema string) (*pgStore, error) {
	store := &pgStore{
		pool:  pool,
		table: pgx.Identifier{schema, "errors"}.Sanitize(),
	}

	err := store.initDB(ctx, schema)
	if err != nil {
		return nil, fmt.Errorf("NewPgStore: %w", err)
	}

	return store, nil
}

type pgStore struct {
	pool  *pgxpool.Pool
	table string
}

func (r *pgStore) Add(ctx context.Context, e entity.ErrorInfo) error {
	_, err := r.pool.Exec(ctx, fmt.Sprintf(`
		INSERT INTO %s (id, code, message, details, service, operation, created_at, alerted)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8);
	`, r.table), e.ID, e.Code, e.Message, e.Details, e.Service, e.Operation, e.CreatedAt, e.Alerted)
	if err != nil {
		return fmt.Errorf("pgStore.Add: %w", err)
	}
	return nil
}

func (r *pgStore) BackdateAlert(ctx context.Context, id string, minutes int) error {
	tag, err := r.pool.Exec(ctx, fmt.Sprintf(`
		UPDATE %s
		SET alerted_at = COALESCE(alerted_at, created_at) - INTERVAL '1 minute' * $2
		WHERE id = $1;
	`, r.table), id, minutes)
	if err != nil {
		return fmt.Errorf("pgStore.BackdateAlert: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("pgStore.BackdateAlert: %w", ErrNotFound)
	}
	return nil
}

func (r *pgStore) CheckAndMarkAlerted(ctx context.Context, e entity.ErrorInfo, cooldownMinutes int) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("pgStore.CheckAndMarkAlerted: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // rollback after commit is a no-op

	// 🔒 ADVISORY LOCK - based on service+operation hash
	// This ensures only ONE transaction can check/mark for this service+operation at a time
	// even when no rows exist yet
	lockKey := hashServiceOperation(e.Service, e.Operation)
	_, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1);`, lockKey)
	if err != nil {
		return fmt.Errorf("pgStore.CheckAndMarkAlerted: failed to acquire lock: %w", err)
	}

	// The window is measured from when the alert was SENT (alerted_at), by
	// Postgres' clock. Measuring from created_at would open the window early
	// for an error that sat in a queue, and measuring in Go would let two
	// replicas with drifting clocks disagree about whose turn it is. Rows
	// alerted before the column existed fall back to created_at, which is
	// what the old code compared against anyway.
	var inCooldown bool
	err = tx.QueryRow(ctx, fmt.Sprintf(`
		SELECT EXISTS (
			SELECT 1
			FROM %s
			WHERE service = $1 AND operation = $2 AND alerted
			  AND COALESCE(alerted_at, created_at) > NOW() - INTERVAL '1 minute' * $3
		);
	`, r.table), e.Service, e.Operation, cooldownMinutes).Scan(&inCooldown)
	if err != nil {
		return fmt.Errorf("pgStore.CheckAndMarkAlerted: %w", err)
	}
	if inCooldown {
		return ErrAlertCooldown
	}

	// Cooldown passed, claim the window
	tag, err := tx.Exec(ctx, fmt.Sprintf(`
		UPDATE %s
		SET alerted = true, alerted_at = NOW()
		WHERE id = $1;
	`, r.table), e.ID)
	if err != nil {
		return fmt.Errorf("pgStore.CheckAndMarkAlerted: %w", err)
	}
	// Without this the claim silently succeeds against a row that retention
	// already deleted, and the alert goes out with nothing recording it.
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("pgStore.CheckAndMarkAlerted: %w", ErrNotFound)
	}

	err = tx.Commit(ctx)
	if err != nil {
		return fmt.Errorf("pgStore.CheckAndMarkAlerted: %w", err)
	}

	return nil
}

func (r *pgStore) GetErrorFrequency(ctx context.Context, service, operation string, minutesBack int) (int, error) {
	var count int

	row := r.pool.QueryRow(ctx, fmt.Sprintf(`
		SELECT COUNT(*)
		FROM %s
		WHERE service = $1 AND operation = $2 AND created_at > NOW() - INTERVAL '1 minute' * $3;
	`, r.table), service, operation, minutesBack)

	err := row.Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("pgStore.GetErrorFrequency: %w", err)
	}

	return count, nil
}

func (r *pgStore) DeleteOlderThan(ctx context.Context, retentionDays, cooldownMinutes, batch int) (int64, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("pgStore.DeleteOlderThan: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // rollback after commit is a no-op

	// try, not wait: a replica that loses the race has nothing useful to do
	// with the connection it would otherwise hold for the whole sweep.
	var acquired bool
	err = tx.QueryRow(ctx, `SELECT pg_try_advisory_xact_lock($1, $2);`, lockClass, lockKeySwep).Scan(&acquired)
	if err != nil {
		return 0, fmt.Errorf("pgStore.DeleteOlderThan: failed to acquire lock: %w", err)
	}
	if !acquired {
		return 0, nil
	}

	// Deleting by ctid keeps one statement's work bounded: the subquery picks
	// a batch using idx_errors_created_at and the delete touches only those
	// rows, instead of one statement scanning years of them.
	//
	// The row holding a live claim is spared. Retention is measured in days
	// and the cooldown in minutes, so this only bites on a wild
	// configuration — but the cost of getting it wrong is that deleting a row
	// silently ends the window it was holding open.
	tag, err := tx.Exec(ctx, fmt.Sprintf(`
		DELETE FROM %s
		WHERE ctid IN (
			SELECT ctid
			FROM %s
			WHERE created_at < NOW() - INTERVAL '1 day' * $1
			  AND NOT (alerted AND COALESCE(alerted_at, created_at) > NOW() - INTERVAL '1 minute' * $2)
			LIMIT $3
		);
	`, r.table, r.table), retentionDays, cooldownMinutes, batch)
	if err != nil {
		return 0, fmt.Errorf("pgStore.DeleteOlderThan: %w", err)
	}

	err = tx.Commit(ctx)
	if err != nil {
		return 0, fmt.Errorf("pgStore.DeleteOlderThan: %w", err)
	}

	return tag.RowsAffected(), nil
}

// hashServiceOperation creates a consistent hash for service+operation
// to use as advisory lock key.
//
// Every replica must compute the same value for the same pair or the lock
// stops serialising them, so this function's output is a wire format: do not
// change it without a coordinated rollout. A collision between two different
// pairs is harmless — they merely take turns.
func hashServiceOperation(service, operation string) int64 {
	// Simple hash function - combine service and operation into a stable int64
	combined := service + ":" + operation
	var hash int64
	for i, c := range combined {
		hash = hash*31 + int64(c) + int64(i)
	}
	// Ensure positive value
	if hash < 0 {
		hash = -hash
	}
	return hash
}

// initDB creates the schema, table and indexes if they are missing, and
// applies the one column added since the first release. Every statement is
// idempotent, so it runs on every start and needs no migration tool.
func (r *pgStore) initDB(ctx context.Context, schema string) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("pgStore.initDB: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // rollback after commit is a no-op

	// IF NOT EXISTS is idempotent but not atomic: two processes starting
	// together both see "not exists" and one fails with a duplicate key on a
	// catalog index. Serialise the whole setup instead.
	_, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1, $2);`, lockClass, lockKeyDDL)
	if err != nil {
		return fmt.Errorf("pgStore.initDB: failed to acquire lock: %w", err)
	}

	_, err = tx.Exec(ctx, fmt.Sprintf(`
		CREATE SCHEMA IF NOT EXISTS %s;

		CREATE TABLE IF NOT EXISTS %s (
			id UUID PRIMARY KEY,
			code TEXT NOT NULL,
			message TEXT NOT NULL,
			details JSONB NOT NULL,
			service TEXT NOT NULL,
			operation TEXT NOT NULL,
			created_at TIMESTAMPTZ NOT NULL,
			alerted BOOLEAN NOT NULL
		);

		-- When the alert was sent, as opposed to when the error happened.
		-- Rows written before this column existed keep NULL and are read
		-- through COALESCE below, so there is no backfill: rewriting a
		-- multi-million-row table under the ACCESS EXCLUSIVE lock this
		-- statement already holds would turn every start into a full scan,
		-- and the first one into a boot failure.
		ALTER TABLE %s ADD COLUMN IF NOT EXISTS alerted_at TIMESTAMPTZ;

		-- Answers the cooldown check in one index lookup. Partial, so it
		-- costs nothing on insert: every row is written with alerted = false.
		-- The expression has to match CheckAndMarkAlerted's predicate exactly
		-- for the index to be usable.
		CREATE INDEX IF NOT EXISTS idx_errors_alert_lookup
		ON %s (service, operation, (COALESCE(alerted_at, created_at)) DESC)
		WHERE alerted;

		CREATE INDEX IF NOT EXISTS idx_errors_service_operation_alerted
		ON %s (service, operation, alerted);

		-- Also what the retention sweep scans.
		CREATE INDEX IF NOT EXISTS idx_errors_created_at
		ON %s (created_at);
	`, pgx.Identifier{schema}.Sanitize(), r.table, r.table, r.table, r.table, r.table))
	if err != nil {
		return fmt.Errorf("pgStore.initDB: %w", err)
	}

	err = tx.Commit(ctx)
	if err != nil {
		return fmt.Errorf("pgStore.initDB: %w", err)
	}

	return nil
}
