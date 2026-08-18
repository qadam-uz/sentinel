package notifier

import (
	"strings"
	"testing"

	"github.com/qadam-uz/sentinel/entity"
)

func TestCapRendered(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{"under the limit", "short", "short"},
		{"at the limit", strings.Repeat("a", maxDetailLen), strings.Repeat("a", maxDetailLen)},
		{"over the limit", strings.Repeat("a", maxDetailLen+5), strings.Repeat("a", maxDetailLen) + "..."},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := capRendered(tt.in); got != tt.want {
				t.Errorf("capRendered(%d bytes) = %d bytes, want %d", len(tt.in), len(got), len(tt.want))
			}
		})
	}

	// A multibyte rune straddling the cut is dropped whole rather than split
	// into a replacement character.
	long := strings.Repeat("a", maxDetailLen-1) + "ыы"
	got := capRendered(long)
	if !strings.HasSuffix(got, "...") {
		t.Fatalf("capRendered did not truncate: %q", got[len(got)-5:])
	}
	for _, r := range got {
		if r == '�' {
			t.Fatal("capRendered split a rune")
		}
	}

	// An HTML entity straddling the cut is dropped whole: half an entity
	// would make Telegram reject the message.
	entity := capRendered(strings.Repeat("a", maxDetailLen-2) + "&amp;" + "bbbb")
	if strings.HasSuffix(strings.TrimSuffix(entity, "..."), "&") ||
		strings.Contains(entity, "&am...") || strings.Contains(entity, "&a...") {
		t.Fatalf("capRendered split an entity: %q", entity[len(entity)-10:])
	}
}

// Escaping inflates: "&" becomes "&amp;". Capping before escaping would let a
// value of ampersands leave the cap five times behind.
func TestDetailsAreCappedAfterEscaping(t *testing.T) {
	inflating := strings.Repeat("&", maxDetailLen)

	html := htmlField(inflating)
	if len(html) > maxDetailLen+len("...") {
		t.Errorf("escapeHtmlDetail produced %d bytes, want at most %d", len(html), maxDetailLen+3)
	}
	if strings.Contains(strings.TrimSuffix(html, "..."), "&am") &&
		!strings.HasSuffix(strings.TrimSuffix(html, "..."), "&amp;") {
		t.Errorf("escapeHtmlDetail left a partial entity: %q", html[len(html)-10:])
	}

	fenced := codeField(strings.Repeat("`", maxDetailLen))
	if len(fenced) > maxDetailLen+len("...") {
		t.Errorf("fenceSafeDetail produced %d bytes, want at most %d", len(fenced), maxDetailLen+3)
	}
	if strings.Contains(fenced, "```") {
		t.Errorf("fenceSafeDetail still closes a fence: %q", fenced)
	}
}

func TestTelegramEscapesDetailValues(t *testing.T) {
	tn := &telegramNotifier{environment: "prod"}

	// Telegram parses the message as HTML: an unescaped "<" in a value — an
	// error page quoted back from an upstream service, say — makes it reject
	// the whole alert, and the cooldown then swallows the retries.
	body := tn.buildMsgBody(entity.ErrorInfo{
		Service:   "svc",
		Operation: "call upstream",
		Message:   "bad gateway",
		Details:   map[string]string{"response": "<html>502 & counting</html>"},
	})

	if strings.Contains(body, "<html>") {
		t.Errorf("detail value was not escaped:\n%s", body)
	}
	if !strings.Contains(body, "&lt;html&gt;502 &amp; counting&lt;/html&gt;") {
		t.Errorf("escaped value missing from the body:\n%s", body)
	}
	// The tags the notifier itself writes must survive.
	if !strings.Contains(body, "<code>") {
		t.Errorf("notifier markup was escaped away:\n%s", body)
	}
}

func TestTelegramSkipsEmptyDetails(t *testing.T) {
	tn := &telegramNotifier{environment: "prod"}
	body := tn.buildMsgBody(entity.ErrorInfo{
		Operation: "op",
		Details:   map[string]string{"empty": "", "set": "v"},
	})
	if strings.Contains(body, "empty") {
		t.Errorf("empty detail was rendered:\n%s", body)
	}
	if !strings.Contains(body, "set") {
		t.Errorf("non-empty detail is missing:\n%s", body)
	}
}

func TestDiscordDetailCannotCloseTheCodeFence(t *testing.T) {
	dn := &discordNotifier{environment: "prod"}

	body := dn.buildMsgBody(entity.ErrorInfo{
		Service:   "svc",
		Operation: "op",
		Details:   map[string]string{"payload": "before ``` after"},
	})

	// Exactly two fences: the ones the notifier opened and closed itself.
	if got := strings.Count(body, "```"); got != 2 {
		t.Errorf("found %d code fences, want 2 — the value broke out:\n%s", got, body)
	}
}

func TestFenceSafe(t *testing.T) {
	tests := []struct {
		name string
		in   string
	}{
		{"no backticks", "plain text"},
		{"one backtick", "a `b"},
		{"two backticks", "a ``b"},
		{"three backticks", "a ```b"},
		{"four backticks", "a ````b"},
		{"trailing backtick", "a`"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := fenceSafe(tt.in)
			if strings.Contains(got, "```") {
				t.Errorf("fenceSafe(%q) = %q, still closes a fence", tt.in, got)
			}
			// Nothing is lost: only zero-width spaces are inserted.
			if strings.ReplaceAll(got, "​", "") != tt.in {
				t.Errorf("fenceSafe(%q) = %q, want the value preserved", tt.in, got)
			}
		})
	}

	// The closing fence follows the value directly, so a value ending in a
	// backtick must not run into it.
	if got := fenceSafe("a`") + "```"; strings.Count(got, "```") != 1 {
		t.Errorf("a trailing backtick merged with the closing fence: %q", got)
	}
}

func TestFrequencyIsShownOnlyWhenKnown(t *testing.T) {
	tn := &telegramNotifier{environment: "prod"}

	quiet := tn.buildMsgBody(entity.ErrorInfo{Operation: "op"})
	if strings.Contains(quiet, "Frequency") {
		t.Errorf("frequency shown without a count:\n%s", quiet)
	}

	loud := tn.buildMsgBody(entity.ErrorInfo{Operation: "op", Frequency: 7, FrequencyMinutes: 5})
	if !strings.Contains(loud, "7 in last 5 minutes") {
		t.Errorf("frequency missing:\n%s", loud)
	}
}
