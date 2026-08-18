package store

import (
	"context"
	"errors"

	"github.com/qadam-uz/sentinel/entity"
)

var ErrNotFound = errors.New("object not found")
var ErrAlertCooldown = errors.New("alert is in cooldown period")

type Store interface {
	// Add stores one error.
	Add(ctx context.Context, e entity.ErrorInfo) error

	// CheckAndMarkAlerted claims the right to alert for this error's
	// service+operation, returning ErrAlertCooldown when another alert for
	// the same pair is still inside the window. Claiming and checking are one
	// atomic step, so concurrent reporters — goroutines or replicas — cannot
	// both decide to alert.
	CheckAndMarkAlerted(ctx context.Context, e entity.ErrorInfo, cooldownMinutes int) error

	// BackdateAlert moves a claim's alert time back by minutes, shortening
	// the window it opened. It is what a failed delivery does: the next error
	// should get a chance soon, but not at once — clearing the window
	// outright turns a chat API that is rejecting messages into a loop, where
	// every rejection immediately licenses the next attempt.
	BackdateAlert(ctx context.Context, id string, minutes int) error

	// GetErrorFrequency counts errors for a service+operation in the last
	// minutesBack minutes, for the "N in last M minutes" line in the alert.
	GetErrorFrequency(ctx context.Context, service, operation string, minutesBack int) (int, error)

	// DeleteOlderThan removes at most batch rows past the retention window
	// and returns how many it removed, never touching a row whose alert claim
	// is still live. It takes a try-lock, so when several replicas sweep at
	// once one does the work and the others return 0 immediately rather than
	// queueing behind it.
	DeleteOlderThan(ctx context.Context, retentionDays, cooldownMinutes, batch int) (int64, error)
}
