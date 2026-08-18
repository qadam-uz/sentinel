package usecase

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/qadam-uz/sentinel/entity"
	"github.com/qadam-uz/sentinel/repository/notifier"
	"github.com/qadam-uz/sentinel/repository/store"
)

const (
	// alertTimeout bounds the asynchronous half of one report: the cooldown
	// check and its advisory lock, the frequency query, and the calls to the
	// chat API including retries.
	alertTimeout = 30 * time.Second

	// notifyAttempts is how many times one alert is posted before it is given
	// up on. The chat APIs fail transiently often enough that a single try
	// throws away alerts for no reason.
	notifyAttempts = 3
	notifyBackoff  = 500 * time.Millisecond

	// retryAfterMinutes is how soon the next error for the same
	// service+operation may try again after a delivery gave up. Short enough
	// that one transient failure does not cost the whole cooldown window in
	// silence; long enough that a chat API which is rejecting messages is not
	// asked again by every report that arrives.
	retryAfterMinutes = 1

	// releaseTimeout bounds shortening the window after a failed delivery.
	// It is deliberately short: this runs when something is already wrong.
	releaseTimeout = 2 * time.Second
)

func New(log *slog.Logger, store store.Store, notifier notifier.Notifier, alertCooldownMinutes int) usecase {
	return usecase{
		log:                  log,
		store:                store,
		notifier:             notifier,
		alertCooldownMinutes: alertCooldownMinutes,
		notifyAttempts:       notifyAttempts,
		notifyBackoff:        notifyBackoff,
	}
}

type usecase struct {
	log      *slog.Logger
	store    store.Store
	notifier notifier.Notifier

	alertCooldownMinutes int

	// Retry shape, fields rather than constants so tests do not have to spend
	// the real backoff. New fills them from the constants above.
	notifyAttempts int
	notifyBackoff  time.Duration
}

func (uc usecase) SendError(ctx context.Context, e entity.ErrorInfo) error {
	err := uc.store.Add(ctx, e)
	if err != nil {
		return fmt.Errorf("usecase.SendError: %w", err)
	}

	go uc.alert(e)

	return nil
}

// alert is the asynchronous half of SendError: decide whether this error gets
// an alert, and post it. It is a method rather than a closure so that it can
// be tested — everything sentinel is actually for happens in here.
//
// e is the caller's copy; nothing outside reads it again.
func (uc usecase) alert(e entity.ErrorInfo) {
	// Detached from the caller — the alert outlives the request that raised
	// the error — but bounded: when sentinel runs embedded it may be holding
	// one of the host application's own pool connections here.
	ctx, cancel := context.WithTimeout(context.Background(), alertTimeout)
	defer cancel()

	err := uc.store.CheckAndMarkAlerted(ctx, e, uc.alertCooldownMinutes)
	if err != nil {
		if errors.Is(err, store.ErrAlertCooldown) {
			return
		}
		uc.logFailure(ctx, "CheckAndMarkAlerted failed", err)
		return
	}

	// From here the window is claimed, and every path out that does not post
	// an alert has to give it back.
	frequency, err := uc.store.GetErrorFrequency(ctx, e.Service, e.Operation, uc.alertCooldownMinutes)
	if err != nil {
		uc.logFailure(ctx, "GetErrorFrequency failed", err)
		uc.shortenCooldown(e)
		return
	}

	e.Frequency = frequency
	e.FrequencyMinutes = uc.alertCooldownMinutes

	err = uc.notify(ctx, e)
	if err != nil {
		uc.logFailure(ctx, "notification failed", err)
		uc.shortenCooldown(e)
		return
	}
}

// notify posts one alert, retrying with backoff. The chat APIs return 5xx and
// time out often enough that giving up on the first failure loses alerts that
// a second attempt a moment later would have delivered.
func (uc usecase) notify(ctx context.Context, e entity.ErrorInfo) error {
	var err error

	// New always sets this, but a zero value here would post nothing at all
	// and then report a failure, which is the worst of both.
	attempts := max(uc.notifyAttempts, 1)

	for attempt := range attempts {
		if attempt > 0 {
			timer := time.NewTimer(uc.notifyBackoff << (attempt - 1))
			select {
			case <-timer.C:
			case <-ctx.Done():
				timer.Stop()
				return fmt.Errorf("%w (gave up after %d attempts: %w)", err, attempt, ctx.Err())
			}
			timer.Stop()
		}

		err = uc.notifier.Notify(ctx, e)
		if err == nil {
			return nil
		}
	}

	return fmt.Errorf("%w (gave up after %d attempts)", err, attempts)
}

// shortenCooldown cuts the window the failed claim opened down to
// retryAfterMinutes. The row is marked before the chat API is called, so
// leaving the mark untouched would suppress every follow-up for the rest of
// the window — one transient failure costing an outage its alerts.
//
// Cutting the window rather than clearing it is what keeps that from
// backfiring. A cleared window licenses the very next report to try again
// immediately, so a chat API that is rate-limiting sentinel gets asked again
// by every error it rejects, and the rejections sustain themselves.
//
// It runs on a fresh context, because in the case that matters the alert
// context is exactly what ran out.
func (uc usecase) shortenCooldown(e entity.ErrorInfo) {
	minutes := uc.alertCooldownMinutes - retryAfterMinutes
	if minutes <= 0 {
		// The window is already no longer than the retry delay.
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), releaseTimeout)
	defer cancel()

	err := uc.store.BackdateAlert(ctx, e.ID, minutes)
	if err != nil {
		uc.logFailure(ctx, "shortening the cooldown failed", err)
	}
}

// logFailure records a failure of the alerting path itself. It stays at Warn
// on purpose: an application that routes its error logs into sentinel would
// otherwise turn every failed alert into another error to alert about, and
// the loop would feed itself. Storage failures reach the caller through the
// error returned by SendError, which is where they are logged as errors.
func (uc usecase) logFailure(ctx context.Context, what string, err error) {
	uc.log.WarnContext(ctx, fmt.Sprintf("usecase.SendError: %s: %v", what, err))
}
