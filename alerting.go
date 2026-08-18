package sentinel

import (
	"errors"

	"github.com/qadam-uz/sentinel/repository/notifier"
)

// Alerting selects where alerts are delivered. Build one with Telegram,
// Discord, CustomNotifier or NoAlerts and put it in Config.Alert.
//
// The zero value alerts nowhere: errors are still stored in Postgres, which
// is what you want in dev and test.
type Alerting struct {
	build func(env string) (notifier.Notifier, error)
}

// Telegram posts alerts to the given chats. The bot must be a member of every
// chat (an administrator, for a channel).
func Telegram(botToken string, chatIDs ...int64) Alerting {
	return Alerting{build: func(env string) (notifier.Notifier, error) {
		if botToken == "" {
			return nil, errors.New("telegram: bot token is empty")
		}
		if len(chatIDs) == 0 {
			return nil, errors.New("telegram: no chat ids")
		}
		return notifier.NewTelegramNotifier(botToken, chatIDs, env)
	}}
}

// Discord posts alerts to the given channels.
func Discord(botToken string, channelIDs ...string) Alerting {
	return Alerting{build: func(env string) (notifier.Notifier, error) {
		if botToken == "" {
			return nil, errors.New("discord: bot token is empty")
		}
		if len(channelIDs) == 0 {
			return nil, errors.New("discord: no channel ids")
		}
		return notifier.NewDiscordNotifier(botToken, channelIDs, env)
	}}
}

// NoAlerts stores errors without posting them anywhere.
func NoAlerts() Alerting {
	return Alerting{build: func(string) (notifier.Notifier, error) {
		return notifier.NewNoopNotifier(), nil
	}}
}

// CustomNotifier delivers alerts through n — a Slack notifier, a fake in
// tests, a fan-out to several destinations.
func CustomNotifier(n notifier.Notifier) Alerting {
	return Alerting{build: func(string) (notifier.Notifier, error) {
		if n == nil {
			return nil, errors.New("custom notifier is nil")
		}
		return n, nil
	}}
}
