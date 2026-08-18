package sentinel

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"testing"
	"testing/slogtest"

	"github.com/qadam-uz/sentinel/entity"
)

// slogFixture wires a logger through the handler onto a fake usecase, and
// returns what reached sentinel once the reporter is flushed.
type slogFixture struct {
	logger *slog.Logger
	uc     *fakeUC
	rep    *Reporter
}

func newSlogFixture(t *testing.T, opts *SlogOptions) *slogFixture {
	t.Helper()
	uc := &fakeUC{}
	rep := testReporter(uc, nil)
	inner := slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelDebug})
	return &slogFixture{
		logger: slog.New(NewSlogHandler(inner, rep, opts)),
		uc:     uc,
		rep:    rep,
	}
}

func (f *slogFixture) reported(t *testing.T) []entity.ErrorInfo {
	t.Helper()
	closeReporter(t, f.rep)
	return f.uc.delivered()
}

func TestSlogHandlerReportsErrors(t *testing.T) {
	f := newSlogFixture(t, nil)

	f.logger.Info("not an error")
	f.logger.Warn("also not an error")
	f.logger.Error("charge order", "err", "card declined", "code", "PAYMENT", "order_id", 42)

	got := f.reported(t)
	if len(got) != 1 {
		t.Fatalf("reported %d records, want only the error", len(got))
	}
	e := got[0]
	// The log message is the operation: it is the stable identifier the
	// alert cooldown groups by.
	if e.Operation != "charge order" {
		t.Errorf("Operation = %q, want %q", e.Operation, "charge order")
	}
	if e.Message != "card declined" {
		t.Errorf("Message = %q, want the err attribute", e.Message)
	}
	if e.Code != "PAYMENT" {
		t.Errorf("Code = %q, want %q", e.Code, "PAYMENT")
	}
	if e.Details["order_id"] != "42" {
		t.Errorf("details[order_id] = %q, want %q", e.Details["order_id"], "42")
	}
}

func TestSlogHandlerFallsBackToTheRecordMessage(t *testing.T) {
	f := newSlogFixture(t, nil)
	f.logger.Error("database unreachable")

	got := f.reported(t)
	if len(got) != 1 {
		t.Fatalf("reported %d records, want 1", len(got))
	}
	if got[0].Message != "database unreachable" {
		t.Errorf("Message = %q, want the record message", got[0].Message)
	}
}

func TestSlogHandlerFlattensAttrsAndGroups(t *testing.T) {
	f := newSlogFixture(t, nil)

	f.logger.With("bound", "yes").
		WithGroup("http").
		With("method", "POST").
		Error("call upstream", "status", 502, slog.Group("body", "len", 12))

	got := f.reported(t)
	if len(got) != 1 {
		t.Fatalf("reported %d records, want 1", len(got))
	}
	want := map[string]string{
		"bound":         "yes", // bound before the group: no prefix
		"http.method":   "POST",
		"http.status":   "502",
		"http.body.len": "12",
	}
	for k, v := range want {
		if got[0].Details[k] != v {
			t.Errorf("details[%q] = %q, want %q (all: %v)", k, got[0].Details[k], v, got[0].Details)
		}
	}
}

func TestSlogHandlerCustomOptions(t *testing.T) {
	f := newSlogFixture(t, &SlogOptions{
		Level:      slog.LevelWarn,
		CodeKey:    "kind",
		MessageKey: "reason",
	})

	f.logger.Warn("retry exhausted", "kind", "TIMEOUT", "reason", "upstream did not answer")

	got := f.reported(t)
	if len(got) != 1 {
		t.Fatalf("reported %d records, want the warning", len(got))
	}
	if got[0].Code != "TIMEOUT" {
		t.Errorf("Code = %q, want %q", got[0].Code, "TIMEOUT")
	}
	if got[0].Message != "upstream did not answer" {
		t.Errorf("Message = %q, want the reason attribute", got[0].Message)
	}
}

func TestSlogHandlerReportsBelowTheWrappedHandlersLevel(t *testing.T) {
	// The application logs at Error and above but wants Warn and above to
	// alert. Delegating Enabled to the wrapped handler alone would silently
	// report nothing.
	var buf bytes.Buffer
	uc := &fakeUC{}
	rep := testReporter(uc, nil)
	inner := slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelError})
	logger := slog.New(NewSlogHandler(inner, rep, &SlogOptions{Level: slog.LevelWarn}))

	logger.Warn("retry exhausted")
	logger.Info("ignored by both")

	closeReporter(t, rep)

	got := uc.delivered()
	if len(got) != 1 || got[0].Operation != "retry exhausted" {
		t.Fatalf("reported %v, want the warning", got)
	}
	// ...and the wrapped handler must not be handed a record it declined.
	if bytes.Contains(buf.Bytes(), []byte("retry exhausted")) {
		t.Errorf("the warning leaked into a handler set to Error:\n%s", buf.String())
	}
	if bytes.Contains(buf.Bytes(), []byte("ignored by both")) {
		t.Errorf("an Info record leaked into a handler set to Error:\n%s", buf.String())
	}
}

func TestSlogHandlerFindsCodeAndMessageUnderAnOpenGroup(t *testing.T) {
	f := newSlogFixture(t, nil)

	// Under WithGroup the attrs flatten to "http.code" / "http.err"; the
	// well-known keys must still be found.
	f.logger.WithGroup("http").Error("call upstream", "err", "connection reset", "code", "EOF")

	got := f.reported(t)
	if len(got) != 1 {
		t.Fatalf("reported %d records, want 1", len(got))
	}
	if got[0].Code != "EOF" {
		t.Errorf("Code = %q, want %q (details: %v)", got[0].Code, "EOF", got[0].Details)
	}
	if got[0].Message != "connection reset" {
		t.Errorf("Message = %q, want %q", got[0].Message, "connection reset")
	}
}

func TestSlogHandlerNilReporterIsPassthrough(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(NewSlogHandler(slog.NewTextHandler(&buf, nil), nil, nil))

	// Alerting is optional; call sites should not have to branch on it.
	logger.Error("still logged")

	if !bytes.Contains(buf.Bytes(), []byte("still logged")) {
		t.Fatalf("record did not reach the wrapped handler: %q", buf.String())
	}
}

// TestSlogHandlerContract runs the standard library's own handler conformance
// suite against the wrapped output — the middleware must not disturb how
// attributes, groups and empty keys are rendered.
func TestSlogHandlerContract(t *testing.T) {
	var buf bytes.Buffer
	uc := &fakeUC{}
	rep := testReporter(uc, nil)
	defer closeReporter(t, rep)

	h := NewSlogHandler(slog.NewJSONHandler(&buf, nil), rep, nil)

	results := func() []map[string]any {
		var out []map[string]any
		for _, line := range bytes.Split(bytes.TrimSpace(buf.Bytes()), []byte("\n")) {
			if len(line) == 0 {
				continue
			}
			var m map[string]any
			if err := json.Unmarshal(line, &m); err != nil {
				t.Fatalf("unmarshal %q: %v", line, err)
			}
			out = append(out, m)
		}
		return out
	}

	if err := slogtest.TestHandler(h, results); err != nil {
		t.Fatal(err)
	}
}

func TestFlattenAttr(t *testing.T) {
	tests := []struct {
		name   string
		prefix string
		attr   slog.Attr
		want   map[string]string
	}{
		{"plain", "", slog.String("k", "v"), map[string]string{"k": "v"}},
		{"prefixed", "g", slog.Int("k", 1), map[string]string{"g.k": "1"}},
		{"empty key is skipped", "", slog.String("", "v"), map[string]string{}},
		{"empty group is skipped", "", slog.Group("g"), map[string]string{}},
		{
			"nested group",
			"a",
			slog.Group("b", slog.String("c", "v")),
			map[string]string{"a.b.c": "v"},
		},
		{
			// The slog contract inlines a group with an empty key.
			"empty-key group is inlined",
			"a",
			slog.Attr{Key: "", Value: slog.GroupValue(slog.String("c", "v"))},
			map[string]string{"a.c": "v"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := map[string]string{}
			flattenAttr(got, tt.prefix, tt.attr)
			if len(got) != len(tt.want) {
				t.Fatalf("got %v, want %v", got, tt.want)
			}
			for k, v := range tt.want {
				if got[k] != v {
					t.Errorf("got[%q] = %q, want %q", k, got[k], v)
				}
			}
		})
	}
}
