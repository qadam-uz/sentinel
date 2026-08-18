package notifier

import (
	"html"
	"strings"
	"unicode/utf8"
)

// maxDetailLen caps one rendered detail value in a chat message. Alerts are
// meant to be read on a phone, and both Telegram and Discord reject an
// oversized post outright — which, with the alert cooldown, would swallow the
// retries too.
const maxDetailLen = 1000

// htmlField renders one field for Telegram's HTML mode: escaped
// first, then capped, so the cap applies to what is actually sent. Escaping
// can inflate a value fivefold ("&" becomes "&amp;"), and capping beforehand
// would let a value of ampersands sail past the limit.
func htmlField(in string) string {
	return capRendered(escapeHtml(in))
}

// markdownField renders one field for Discord: escaped first, then capped,
// for the same reason as htmlField.
func markdownField(in string) string {
	return capRendered(escapeMarkdown(in))
}

// codeField renders one detail value for a Discord code block: kept
// from closing the block, then capped, for the same reason.
func codeField(in string) string {
	return capRendered(fenceSafe(in))
}

// capRendered cuts s to maxDetailLen without leaving a broken fragment
// behind: not half a UTF-8 rune, which renders as a replacement character,
// and not half an HTML entity, which Telegram would reject the message over.
func capRendered(s string) string {
	if len(s) <= maxDetailLen {
		return s
	}
	cut := maxDetailLen

	// An entity is at most "&#x10FFFF;" — 10 bytes, all ASCII, so this window
	// can never overlap a multi-byte rune.
	if amp := strings.LastIndexByte(s[:cut], '&'); amp >= 0 && cut-amp <= 10 &&
		!strings.Contains(s[amp:cut], ";") {
		cut = amp
	}
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut] + "..."
}

// escapeMarkdown escapes Discord Markdown special characters.
func escapeMarkdown(in string) string {
	// Escape special characters used in Markdown: *, _, `, ~, |
	replacer := strings.NewReplacer(
		"*", "\\*",
		"_", "\\_",
		"`", "\\`",
		"~", "\\~",
		"|", "\\|",
	)
	return replacer.Replace(replaceNewlines(in))
}

func escapeHtml(in string) string {
	return html.EscapeString(replaceNewlines(in))
}

// fenceSafe keeps a value from closing the Markdown code fence it is placed
// in: a run of three backticks would end the block early and spill the rest
// of the alert out as formatting. Runs are broken with a zero-width space,
// which Discord does not render, so no character of the value is lost.
func fenceSafe(in string) string {
	out := strings.ReplaceAll(in, "``", "`​`")
	if strings.HasSuffix(out, "`") {
		out += "​" // do not run into the closing fence
	}
	return out
}

func replaceNewlines(in string) string {
	return strings.ReplaceAll(in, "\n", "\\n")
}
