package notifier

import (
	"bytes"
	"context"
	"fmt"

	"github.com/nikoksr/notify"
	"github.com/nikoksr/notify/service/discord"
	"github.com/qadam-uz/sentinel/entity"
)

type discordNotifier struct {
	notifier    notify.Notifier
	environment string
}

func NewDiscordNotifier(token string, channelIDs []string, environment string) (*discordNotifier, error) {
	d := discord.New()
	err := d.AuthenticateWithBotToken(token)
	if err != nil {
		return nil, fmt.Errorf("NewDiscordNotifier: %w", err)
	}
	d.AddReceivers(channelIDs...)

	n := notify.New()
	n.UseServices(d)

	return &discordNotifier{
		notifier:    n,
		environment: environment,
	}, nil
}

func (dn *discordNotifier) Notify(ctx context.Context, e entity.ErrorInfo) error {
	// Build the message title
	msgTitle := dn.buildMsgTitle()

	// Build the message body
	msgBody := dn.buildMsgBody(e)

	// Send the message
	err := dn.notifier.Send(ctx, msgTitle, msgBody)
	if err != nil {
		return fmt.Errorf("discordNotifier.Notify: %w", err)
	}

	return nil
}

func (dn *discordNotifier) buildMsgTitle() string {
	return "**❗ Error from Sentinel**\n"
}

func (dn *discordNotifier) buildMsgBody(e entity.ErrorInfo) string {
	var buffer bytes.Buffer

	// Main error information
	buffer.WriteString(fmt.Sprintf("**🔍 Environment:** %s\n", escapeMarkdown(dn.environment)))
	buffer.WriteString(fmt.Sprintf("**🛠️ Service:** %s\n", escapeMarkdown(e.Service)))
	buffer.WriteString(fmt.Sprintf("**🔄 Operation:** %s\n", escapeMarkdown(e.Operation)))
	buffer.WriteString(fmt.Sprintf("**🏷️ Code:** %s\n", escapeMarkdown(e.Code)))
	// Capped like the details: the standalone service stores whatever a
	// client sends, so the message is the one field that arrives unbounded.
	buffer.WriteString(fmt.Sprintf("**💬 Message:** %s\n", markdownField(e.Message)))

	// Separator for Details section
	buffer.WriteString("\n**📋 _Additional details_**\n")

	// Details section with only visible details. The value keeps its code
	// block — it is usually a payload or a stack — so it is not Markdown
	// escaped, only kept from closing the block early.
	for k, v := range e.Details {
		if v != "" {
			buffer.WriteString(fmt.Sprintf("_%s_: ```%s```\n",
				escapeMarkdown(k), codeField(v)))
		}
	}

	if e.Frequency > 0 {
		buffer.WriteString(fmt.Sprintf("\n**📊 Frequency:** %d in last %d minutes", e.Frequency, e.FrequencyMinutes))
	}

	return buffer.String()
}
