package app

import (
	"testing"

	"github.com/qadam-uz/sentinel/config"
)

// The step from environment config to an alert destination is the one place
// in this package that decides something rather than just wiring it, and it
// fails quietly: nothing goes wrong until an alert does not arrive, in a
// channel nobody is watching yet.
//
// What is checked here is which branch is taken. Whether the notifier itself
// works is not: building a Telegram one calls the Bot API.
func TestAlertingFrom(t *testing.T) {
	tests := []struct {
		name    string
		cfg     config.Config
		wantErr bool
	}{
		{
			"telegram",
			config.Config{
				AlertProvider:    config.AlertProviderTelegram,
				TelegramBotToken: "token",
				TelegramsChatIDs: []int64{1},
			},
			false,
		},
		{
			"discord",
			config.Config{
				AlertProvider:     config.AlertProviderDiscord,
				DiscordBotToken:   "token",
				DiscordChannelIDs: []string{"1"},
			},
			false,
		},
		{
			"unknown provider",
			config.Config{AlertProvider: "slack"},
			true,
		},
		{
			// ALERT_DISABLE wins: a service told not to alert has to boot even
			// when the provider settings are nonsense or missing entirely.
			"disabled beats an unknown provider",
			config.Config{AlertDisable: true, AlertProvider: "slack"},
			false,
		},
		{
			"disabled with nothing configured",
			config.Config{AlertDisable: true},
			false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := alertingFrom(tt.cfg)
			if (err != nil) != tt.wantErr {
				t.Errorf("alertingFrom error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
