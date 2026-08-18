package sentinel

import (
	"context"
	"fmt"
	"log/slog"
)

// Default keys used by SlogHandler to pull an error code and message out of a
// record's attributes.
const (
	DefaultCodeKey    = "code"
	DefaultMessageKey = "err"
)

// SlogOptions tunes NewSlogHandler. A nil *SlogOptions means all defaults.
type SlogOptions struct {
	// Level is the lowest level that is reported to sentinel. Records below
	// it are only passed through. Default slog.LevelError.
	Level slog.Leveler

	// CodeKey is the attribute whose value becomes ErrorInfo.Code.
	// Default DefaultCodeKey.
	CodeKey string

	// MessageKey is the attribute whose value becomes ErrorInfo.Message —
	// with Go's convention of logging the failure as an "err" attribute, the
	// record's own message is the operation and this is the detail. Falls
	// back to the record message when the attribute is absent.
	// Default DefaultMessageKey.
	//
	// Like CodeKey, it is looked up at the top level and under the group the
	// logger currently has open.
	MessageKey string
}

// SlogHandler is slog middleware: every record reaches the wrapped handler
// unchanged, and records at Options.Level and above are also reported to
// sentinel. It is the whole integration for most applications —
//
//	base := slog.New(slog.NewJSONHandler(os.Stdout, nil))
//	rep, err := sentinel.New(ctx, sentinel.Config{Service: "my-app", ...,
//		Logger: base}) // base, NOT the wrapped logger below
//	logger := slog.New(sentinel.NewSlogHandler(base.Handler(), rep, nil))
//
// — after which every logger.Error call alerts.
//
// The record's message becomes sentinel's operation, which is the key the
// alert cooldown groups by, so log messages should be stable identifiers
// ("charge order", "sync catalog") with the varying parts in attributes.
// Attributes become the alert's details, groups flattened to "group.key".
//
// Reporting resolves a record's slog.LogValuers a second time, after the
// wrapped handler; only for records that alert, but a LogValue with side
// effects will run twice.
type SlogHandler struct {
	inner slog.Handler
	rep   *Reporter

	level   slog.Leveler
	codeKey string
	msgKey  string

	// pre holds attrs bound via WithAttrs, already flattened under the group
	// prefix that was open AT THAT TIME — the slog contract says a group
	// qualifies only attrs added after it, so prefixing cannot wait until
	// Handle. group is the currently open prefix for record-level attrs.
	pre   map[string]string
	group string
}

var _ slog.Handler = (*SlogHandler)(nil)

// NewSlogHandler wraps inner so that error-level records are reported to rep.
// A nil rep yields a pure passthrough, which keeps alerting optional without
// branching at every call site.
func NewSlogHandler(inner slog.Handler, rep *Reporter, opts *SlogOptions) *SlogHandler {
	h := &SlogHandler{
		inner:   inner,
		rep:     rep,
		level:   slog.LevelError,
		codeKey: DefaultCodeKey,
		msgKey:  DefaultMessageKey,
	}
	if opts != nil {
		if opts.Level != nil {
			h.level = opts.Level
		}
		if opts.CodeKey != "" {
			h.codeKey = opts.CodeKey
		}
		if opts.MessageKey != "" {
			h.msgKey = opts.MessageKey
		}
	}
	return h
}

// Enabled is true when either side wants the record: the wrapped handler, or
// alerting. Without the second half, setting Options.Level below the wrapped
// handler's own level would silently report nothing.
func (h *SlogHandler) Enabled(ctx context.Context, l slog.Level) bool {
	return h.inner.Enabled(ctx, l) || (h.rep != nil && l >= h.level.Level())
}

func (h *SlogHandler) Handle(ctx context.Context, rec slog.Record) error {
	if h.rep != nil && rec.Level >= h.level.Level() {
		details := make(map[string]string, len(h.pre)+rec.NumAttrs())
		for k, v := range h.pre {
			details[k] = v
		}
		rec.Attrs(func(a slog.Attr) bool {
			flattenAttr(details, h.group, a)
			return true
		})
		msg := h.lookup(details, h.msgKey)
		if msg == "" {
			msg = rec.Message
		}
		h.rep.Report(ErrorInfo{
			Operation: rec.Message,
			Message:   msg,
			Code:      h.lookup(details, h.codeKey),
			Details:   details,
		})
	}
	// Enabled may have said yes on alerting's behalf alone; do not push a
	// record into a handler that has already declined it.
	if !h.inner.Enabled(ctx, rec.Level) {
		return nil
	}
	return h.inner.Handle(ctx, rec)
}

// lookup finds a well-known key whether it was logged at the top level or
// under an open group — a logger derived with WithGroup("http") flattens its
// "code" attribute to "http.code", and the code would otherwise go missing
// exactly where the context is richest.
func (h *SlogHandler) lookup(details map[string]string, key string) string {
	if v := details[key]; v != "" {
		return v
	}
	if h.group != "" {
		return details[h.group+"."+key]
	}
	return ""
}

func (h *SlogHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	nh := *h
	nh.inner = h.inner.WithAttrs(attrs)
	nh.pre = make(map[string]string, len(h.pre)+len(attrs))
	for k, v := range h.pre {
		nh.pre[k] = v
	}
	for _, a := range attrs {
		flattenAttr(nh.pre, h.group, a)
	}
	return &nh
}

func (h *SlogHandler) WithGroup(name string) slog.Handler {
	nh := *h
	nh.inner = h.inner.WithGroup(name)
	if name != "" {
		if nh.group != "" {
			nh.group += "."
		}
		nh.group += name
	}
	return &nh
}

// flattenAttr renders one attr into details. It follows the slog.Handler
// contract: LogValuers are resolved first (one may resolve to a group), empty
// groups and empty-key attrs are ignored, and an empty-key group is inlined.
func flattenAttr(details map[string]string, prefix string, a slog.Attr) {
	v := a.Value.Resolve()
	if v.Kind() == slog.KindGroup {
		group := v.Group()
		if len(group) == 0 {
			return
		}
		p := prefix
		if a.Key != "" { // empty-key group: inline without a prefix segment
			if p != "" {
				p += "."
			}
			p += a.Key
		}
		for _, ga := range group {
			flattenAttr(details, p, ga)
		}
		return
	}
	if a.Key == "" {
		return
	}
	key := a.Key
	if prefix != "" {
		key = prefix + "." + key
	}
	details[key] = fmt.Sprintf("%v", v.Any())
}
