package notifier

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"time"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api"
	"github.com/nikoksr/notify"
	"github.com/nikoksr/notify/service/telegram"
	"github.com/qadam-uz/sentinel/entity"
)

// telegramTimeout bounds one call to the Bot API. The library's own client has
// no timeout and its send takes no context, so without this both the getMe at
// construction and every alert afterwards can hang indefinitely — startup
// would block, and alerting would stop with nothing in the logs to say so.
const telegramTimeout = 10 * time.Second

type telegramNotifier struct {
	notifier    notify.Notifier
	environment string
}

func NewTelegramNotifier(token string, chatIDs []int64, environment string) (*telegramNotifier, error) {
	api, err := tgbotapi.NewBotAPIWithClient(token, &http.Client{Timeout: telegramTimeout})
	if err != nil {
		return nil, fmt.Errorf("NewTelegramNotifier: %w", err)
	}

	tg := &telegram.Telegram{}
	tg.SetClient(api)
	tg.AddReceivers(chatIDs...)

	n := notify.New()
	n.UseServices(tg)

	return &telegramNotifier{
		notifier:    n,
		environment: environment,
	}, nil
}

func (tn *telegramNotifier) Notify(ctx context.Context, e entity.ErrorInfo) error {
	// Build the message title
	msgTitle := tn.buildMsgTitle()

	// Build the message body
	msgBody := tn.buildMsgBody(e)

	// Send the message
	err := tn.notifier.Send(ctx, msgTitle, msgBody)
	if err != nil {
		return fmt.Errorf("telegramNotifier.Notify: %w", err)
	}

	return nil
}

func (tn *telegramNotifier) buildMsgTitle() string {
	return "<b>❗ Error from Sentinel</b>\n"
}

func (tn *telegramNotifier) buildMsgBody(e entity.ErrorInfo) string {
	var buffer bytes.Buffer

	// Main error information
	buffer.WriteString(fmt.Sprintf("<b>🔍 Environment:</b> %s\n", escapeHtml(tn.environment)))
	buffer.WriteString(fmt.Sprintf("<b>🛠️ Service:</b> %s\n", escapeHtml(e.Service)))
	buffer.WriteString(fmt.Sprintf("<b>🔄 Operation:</b> %s\n", escapeHtml(e.Operation)))
	buffer.WriteString(fmt.Sprintf("<b>🏷️ Code:</b> %s\n", escapeHtml(e.Code)))
	// Capped like the details: the standalone service stores whatever a
	// client sends, so the message is the one field that arrives unbounded.
	buffer.WriteString(fmt.Sprintf("<b>💬 Message:</b> %s\n", htmlField(e.Message)))

	// Separator for Details section
	buffer.WriteString("\n<b>📋 <i>Additional details</i></b>\n")

	// Details section with only visible details. The value is escaped like
	// every other field: the message is sent in HTML mode, so a value holding
	// a "<" — an HTML error page quoted back from an upstream service, say —
	// would make Telegram reject the whole alert, and the cooldown would then
	// swallow the retries.
	for k, v := range e.Details {
		if v != "" {
			buffer.WriteString(fmt.Sprintf("<i>%s</i>: <code>%s</code>\n",
				escapeHtml(k), htmlField(v)))
		}
	}

	if e.Frequency > 0 {
		buffer.WriteString(fmt.Sprintf("\n<b>📊 Frequency:</b> %d in last %d minutes", e.Frequency, e.FrequencyMinutes))
	}

	return buffer.String()
}
