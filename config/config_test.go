package config

import (
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestDSNPrefersTheExplicitOne(t *testing.T) {
	cfg := Config{
		PostgresDSN:  "postgres://user:pw@db.example.com:5432/sentinel?sslmode=require",
		PostgresHost: "ignored",
	}
	if got := cfg.DSN(); got != cfg.PostgresDSN {
		t.Errorf("DSN() = %q, want the explicit POSTGRES_DSN", got)
	}
}

func TestDSNCarriesTheSSLMode(t *testing.T) {
	cfg := Config{PostgresPassword: "pw", PostgresSSLMode: "require"}
	if got := cfg.DSN(); !strings.Contains(got, "sslmode='require'") {
		t.Errorf("DSN() = %q, want sslmode=require", got)
	}
}

// In keyword/value form an unquoted space ends the value, so a password
// containing one used to swallow dbname and sslmode with no parse error at
// all — you simply connected to a different database.
func TestDSNSurvivesAwkwardPasswords(t *testing.T) {
	tests := []struct {
		name     string
		password string
	}{
		{"with a space", "hunter 2"},
		{"with a quote", "hunter'2"},
		{"with a backslash", `hunter\2`},
		{"looking like another setting", "pw dbname=elsewhere"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := Config{
				PostgresHost:     "localhost",
				PostgresPort:     "5432",
				PostgresUser:     "postgres",
				PostgresPassword: tt.password,
				PostgresDatabase: "sentinel",
				PostgresSSLMode:  "disable",
			}

			// Assert against the parser that will actually read this, not
			// against the string: the point is that pgx recovers the values.
			parsed, err := pgconn.ParseConfig(cfg.DSN())
			if err != nil {
				t.Fatalf("pgx could not parse %q: %v", cfg.DSN(), err)
			}
			if parsed.Password != tt.password {
				t.Errorf("password = %q, want %q", parsed.Password, tt.password)
			}
			if parsed.Database != "sentinel" {
				t.Errorf("database = %q, want %q — the password swallowed it", parsed.Database, "sentinel")
			}
			if parsed.Host != "localhost" || parsed.User != "postgres" {
				t.Errorf("host/user = %q/%q, want localhost/postgres", parsed.Host, parsed.User)
			}
		})
	}
}

func TestValidateRequiresADatabase(t *testing.T) {
	base := Config{
		Environment:      "test",
		AlertProvider:    AlertProviderTelegram,
		TelegramBotToken: "token",
		TelegramsChatIDs: []int64{1},
	}

	if err := base.validate(); err == nil {
		t.Error("validate accepted a config with neither POSTGRES_DSN nor a password")
	}

	withPassword := base
	withPassword.PostgresPassword = "pw"
	if err := withPassword.validate(); err != nil {
		t.Errorf("validate rejected a password-configured database: %v", err)
	}

	withDSN := base
	withDSN.PostgresDSN = "postgres://localhost/sentinel"
	if err := withDSN.validate(); err != nil {
		t.Errorf("validate rejected a DSN-configured database: %v", err)
	}
}

func TestValidateChecksTheAlertProvider(t *testing.T) {
	tests := []struct {
		name    string
		cfg     Config
		wantErr bool
	}{
		{"unknown provider", Config{AlertProvider: "slack"}, true},
		{"telegram without a token", Config{AlertProvider: AlertProviderTelegram, TelegramsChatIDs: []int64{1}}, true},
		{"telegram without chats", Config{AlertProvider: AlertProviderTelegram, TelegramBotToken: "t"}, true},
		{"discord without a token", Config{AlertProvider: AlertProviderDiscord, DiscordChannelIDs: []string{"1"}}, true},
		{"discord without channels", Config{AlertProvider: AlertProviderDiscord, DiscordBotToken: "t"}, true},
		{
			"complete telegram",
			Config{AlertProvider: AlertProviderTelegram, TelegramBotToken: "t", TelegramsChatIDs: []int64{1}},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := tt.cfg
			cfg.PostgresPassword = "pw" // isolate the provider check
			if err := cfg.validate(); (err != nil) != tt.wantErr {
				t.Errorf("validate error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
