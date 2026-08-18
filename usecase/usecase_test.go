package usecase

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/qadam-uz/sentinel/entity"
	"github.com/qadam-uz/sentinel/repository/store"
)

// fakeStore keeps enough state to answer the cooldown honestly: a claim
// records when it was granted and backdating moves that moment, so a test can
// assert that the window really is shorter afterwards rather than merely that
// some method was called.
type fakeStore struct {
	mu sync.Mutex

	added     []entity.ErrorInfo
	backdated []backdateCall
	alertedAt map[string]time.Time // service+operation -> when it last alerted
	claimedBy map[string]string    // service+operation -> the report holding it

	addErr   error
	claimErr error
	freq     int
	freqErr  error
	setErr   error

	cooldown time.Duration
}

type backdateCall struct {
	id      string
	minutes int
}

func (f *fakeStore) Add(_ context.Context, e entity.ErrorInfo) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.added = append(f.added, e)
	return f.addErr
}

func (f *fakeStore) CheckAndMarkAlerted(_ context.Context, e entity.ErrorInfo, _ int) error {
	if f.claimErr != nil {
		return f.claimErr
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	key := e.Service + ":" + e.Operation
	if last, ok := f.alertedAt[key]; ok && time.Since(last) < f.cooldown {
		return store.ErrAlertCooldown
	}
	f.alertedAt[key] = time.Now()
	f.claimedBy[key] = e.ID
	return nil
}

func (f *fakeStore) BackdateAlert(_ context.Context, id string, minutes int) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.backdated = append(f.backdated, backdateCall{id, minutes})
	if f.setErr != nil {
		return f.setErr
	}
	for key, holder := range f.claimedBy {
		if holder == id {
			f.alertedAt[key] = f.alertedAt[key].Add(-time.Duration(minutes) * time.Minute)
		}
	}
	return nil
}

func (f *fakeStore) GetErrorFrequency(_ context.Context, _, _ string, _ int) (int, error) {
	return f.freq, f.freqErr
}

func (f *fakeStore) DeleteOlderThan(_ context.Context, _, _, _ int) (int64, error) {
	return 0, nil
}

func (f *fakeStore) shortened() []backdateCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]backdateCall(nil), f.backdated...)
}

// silenceLeft is how much longer this service+operation stays suppressed.
func (f *fakeStore) silenceLeft(service, operation string) time.Duration {
	f.mu.Lock()
	defer f.mu.Unlock()
	last, ok := f.alertedAt[service+":"+operation]
	if !ok {
		return 0
	}
	return f.cooldown - time.Since(last)
}

var _ store.Store = (*fakeStore)(nil)

type fakeNotifier struct {
	mu     sync.Mutex
	got    []entity.ErrorInfo
	errs   []error // consumed one per call; nil or exhausted means success
	called chan struct{}
}

func (f *fakeNotifier) Notify(_ context.Context, e entity.ErrorInfo) error {
	f.mu.Lock()
	f.got = append(f.got, e)
	var err error
	if len(f.errs) > 0 {
		err, f.errs = f.errs[0], f.errs[1:]
	}
	f.mu.Unlock()

	if f.called != nil {
		select {
		case f.called <- struct{}{}:
		default:
		}
	}
	return err
}

func (f *fakeNotifier) calls() []entity.ErrorInfo {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]entity.ErrorInfo(nil), f.got...)
}

type fixture struct {
	uc    usecase
	store *fakeStore
	notif *fakeNotifier
	logs  *bytes.Buffer
}

const testCooldownMinutes = 5

func newFixture(tune func(*fakeStore, *fakeNotifier)) *fixture {
	st := &fakeStore{
		freq:      1,
		alertedAt: map[string]time.Time{},
		claimedBy: map[string]string{},
		cooldown:  testCooldownMinutes * time.Minute,
	}
	nt := &fakeNotifier{}
	if tune != nil {
		tune(st, nt)
	}
	logs := new(bytes.Buffer)
	return &fixture{
		uc: usecase{
			log:                  slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug})),
			store:                st,
			notifier:             nt,
			alertCooldownMinutes: testCooldownMinutes,
			notifyAttempts:       3,
			notifyBackoff:        time.Millisecond,
		},
		store: st,
		notif: nt,
		logs:  logs,
	}
}

func report() entity.ErrorInfo {
	return entity.ErrorInfo{
		ID:        "11111111-1111-1111-1111-111111111111",
		Service:   "svc",
		Operation: "charge order",
		Message:   "card declined",
		CreatedAt: time.Now(),
	}
}

// The alerting path must never log at Error: an application that routes its
// error logs into sentinel would alert about its own failing alerts.
func (f *fixture) assertNoErrorLogs(t *testing.T) {
	t.Helper()
	if strings.Contains(f.logs.String(), "level=ERROR") {
		t.Errorf("the alert path logged at ERROR:\n%s", f.logs.String())
	}
}

func TestSendErrorStoresThenAlerts(t *testing.T) {
	f := newFixture(func(_ *fakeStore, nt *fakeNotifier) { nt.called = make(chan struct{}, 1) })

	e := report()
	if err := f.uc.SendError(context.Background(), e); err != nil {
		t.Fatalf("SendError: %v", err)
	}

	// The report is stored on the caller's goroutine and the alert follows on
	// sentinel's own, so SendError returning is not the end of the story.
	select {
	case <-f.notif.called:
	case <-time.After(5 * time.Second):
		t.Fatal("SendError never alerted")
	}

	if got := f.store.added; len(got) != 1 || got[0].ID != e.ID {
		t.Errorf("stored %v, want the report", got)
	}
	if got := f.notif.calls(); len(got) != 1 || got[0].ID != e.ID {
		t.Errorf("alerted on %v, want the report", got)
	}
}

func TestSendErrorReturnsTheStorageError(t *testing.T) {
	boom := errors.New("connection refused")
	f := newFixture(func(st *fakeStore, _ *fakeNotifier) { st.addErr = boom })

	err := f.uc.SendError(context.Background(), report())
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v, want it to wrap %v", err, boom)
	}
	// Storage failures are the caller's to log and act on; alerting stays
	// quiet so it cannot alert about itself.
	f.assertNoErrorLogs(t)
}

func TestSecondErrorInTheWindowIsSuppressed(t *testing.T) {
	f := newFixture(nil)

	f.uc.alert(report())
	second := report()
	second.ID = "22222222-2222-2222-2222-222222222222"
	f.uc.alert(second)

	if got := f.notif.calls(); len(got) != 1 {
		t.Fatalf("posted %d alerts, want 1 — the cooldown did not hold", len(got))
	}
	// A delivered alert owns the whole window.
	if left := f.store.silenceLeft("svc", "charge order"); left < 4*time.Minute {
		t.Errorf("silence left = %v, want the full %d minutes", left, testCooldownMinutes)
	}
}

// The sentinel is compared with errors.Is, so a store that wraps it still
// suppresses instead of being treated as a failure.
func TestAlertRespectsAWrappedCooldown(t *testing.T) {
	f := newFixture(func(st *fakeStore, _ *fakeNotifier) {
		st.claimErr = fmt.Errorf("pgStore.CheckAndMarkAlerted: %w", store.ErrAlertCooldown)
	})

	f.uc.alert(report())

	if got := f.notif.calls(); len(got) != 0 {
		t.Errorf("posted %d alerts during the cooldown, want none", len(got))
	}
	// A suppressed alert is the normal case, not a problem worth logging.
	if f.logs.Len() != 0 {
		t.Errorf("the cooldown logged:\n%s", f.logs.String())
	}
}

func TestAlertCarriesTheFrequency(t *testing.T) {
	f := newFixture(func(st *fakeStore, _ *fakeNotifier) { st.freq = 7 })

	e := report()
	f.uc.alert(e)

	got := f.notif.calls()
	if len(got) != 1 {
		t.Fatalf("posted %d alerts, want 1", len(got))
	}
	if got[0].ID != e.ID {
		t.Errorf("alerted on %q, want the stored report %q", got[0].ID, e.ID)
	}
	if got[0].Frequency != 7 || got[0].FrequencyMinutes != testCooldownMinutes {
		t.Errorf("frequency = %d in %d minutes, want 7 in %d",
			got[0].Frequency, got[0].FrequencyMinutes, testCooldownMinutes)
	}
	if got := f.store.shortened(); len(got) != 0 {
		t.Errorf("shortened the window after a successful alert: %v", got)
	}
	f.assertNoErrorLogs(t)
}

func TestAlertDoesNotTouchTheWindowWhenTheClaimFails(t *testing.T) {
	f := newFixture(func(st *fakeStore, _ *fakeNotifier) {
		st.claimErr = errors.New("deadlock detected")
	})

	f.uc.alert(report())

	if got := f.notif.calls(); len(got) != 0 {
		t.Errorf("posted %d alerts without claiming the window, want none", len(got))
	}
	// Nothing was claimed, so there is nothing to give back.
	if got := f.store.shortened(); len(got) != 0 {
		t.Errorf("shortened a window that was never claimed: %v", got)
	}
	if !strings.Contains(f.logs.String(), "CheckAndMarkAlerted failed") {
		t.Errorf("the failure was not logged:\n%s", f.logs.String())
	}
	f.assertNoErrorLogs(t)
}

func TestFrequencyFailureShortensTheWindow(t *testing.T) {
	f := newFixture(func(st *fakeStore, _ *fakeNotifier) {
		st.freqErr = errors.New("statement timeout")
	})

	f.uc.alert(report())

	// Better no alert than one claiming "0 in the last 5 minutes"...
	if got := f.notif.calls(); len(got) != 0 {
		t.Errorf("posted %d alerts without a frequency, want none", len(got))
	}
	// ...but the window must not stay shut for its full length either.
	if left := f.store.silenceLeft("svc", "charge order"); left > 2*time.Minute {
		t.Errorf("silence left = %v, want about %d minute(s)", left, retryAfterMinutes)
	}
	f.assertNoErrorLogs(t)
}

// The row is marked alerted before the chat API is called, so a failed
// delivery has to give most of the window back — but not all of it, or every
// rejection would immediately license the next attempt.
func TestFailedDeliveryShortensTheWindowWithoutClearingIt(t *testing.T) {
	down := errors.New("502 bad gateway")
	f := newFixture(func(_ *fakeStore, nt *fakeNotifier) {
		nt.errs = []error{down, down, down}
	})

	e := report()
	f.uc.alert(e)

	if got := f.notif.calls(); len(got) != 3 {
		t.Errorf("tried %d times, want 3", len(got))
	}
	want := backdateCall{e.ID, testCooldownMinutes - retryAfterMinutes}
	if got := f.store.shortened(); len(got) != 1 || got[0] != want {
		t.Fatalf("shortened = %v, want %v", got, want)
	}

	left := f.store.silenceLeft("svc", "charge order")
	if left <= 0 {
		t.Errorf("silence left = %v, want the window shortened, not cleared — "+
			"a cleared window lets every rejected report ask again at once", left)
	}
	if left > 2*time.Minute {
		t.Errorf("silence left = %v, want about %d minute(s)", left, retryAfterMinutes)
	}
	if !strings.Contains(f.logs.String(), "notification failed") {
		t.Errorf("the failure was not logged:\n%s", f.logs.String())
	}
	f.assertNoErrorLogs(t)
}

func TestNotifyRetriesUntilItSucceeds(t *testing.T) {
	f := newFixture(func(_ *fakeStore, nt *fakeNotifier) {
		nt.errs = []error{errors.New("timeout"), errors.New("502")}
	})

	f.uc.alert(report())

	if got := f.notif.calls(); len(got) != 3 {
		t.Fatalf("tried %d times, want 3 (two failures then success)", len(got))
	}
	if got := f.store.shortened(); len(got) != 0 {
		t.Errorf("shortened the window after eventually delivering: %v", got)
	}
	if strings.Contains(f.logs.String(), "notification failed") {
		t.Errorf("a recovered delivery was reported as a failure:\n%s", f.logs.String())
	}
}

// A cooldown no longer than the retry delay has nothing to give back.
func TestShortWindowsAreLeftAlone(t *testing.T) {
	f := newFixture(func(_ *fakeStore, nt *fakeNotifier) {
		nt.errs = []error{errors.New("502"), errors.New("502"), errors.New("502")}
	})
	f.uc.alertCooldownMinutes = retryAfterMinutes
	f.store.cooldown = retryAfterMinutes * time.Minute

	f.uc.alert(report())

	if got := f.store.shortened(); len(got) != 0 {
		t.Errorf("backdated a window that is already at the retry delay: %v", got)
	}
}

func TestShorteningFailureIsLoggedNotPanicked(t *testing.T) {
	f := newFixture(func(st *fakeStore, _ *fakeNotifier) {
		st.freqErr = errors.New("statement timeout")
		st.setErr = store.ErrNotFound // retention already deleted the row
	})

	f.uc.alert(report())

	if !strings.Contains(f.logs.String(), "shortening the cooldown failed") {
		t.Errorf("the failure was not logged:\n%s", f.logs.String())
	}
	f.assertNoErrorLogs(t)
}
