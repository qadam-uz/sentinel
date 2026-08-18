package config

import (
	"fmt"
	"strings"

	"github.com/ilyakaznacheev/cleanenv"
)

const (
	AlertProviderDiscord  = "discord"
	AlertProviderTelegram = "telegram"
)

type Config struct {
	Environment string `env:"ENVIRONMENT" env-required:"true"`

	GrpcHost string `env:"GRPC_HOST"    env-default:"localhost"`
	GrpcPort string `env:"GRPC_PORT"    env-default:"5001"`

	// PostgresDSN, when set, is used as-is and every POSTGRES_* setting below
	// is ignored. It is the only way to reach a managed database that
	// requires TLS, or to pass pgx pool options.
	PostgresDSN string `env:"POSTGRES_DSN"`

	PostgresHost     string `env:"POSTGRES_HOST"     env-default:"localhost"`
	PostgresPort     string `env:"POSTGRES_PORT"     env-default:"5432"`
	PostgresUser     string `env:"POSTGRES_USER"     env-default:"postgres"`
	PostgresPassword string `env:"POSTGRES_PASSWORD"`
	PostgresDatabase string `env:"POSTGRES_DATABASE" env-default:"sentinel"`
	PostgresSchema   string `env:"POSTGRES_SCHEMA"   env-default:"public"`
	PostgresSSLMode  string `env:"POSTGRES_SSLMODE"  env-default:"disable"`

	AlertProvider        string `env:"ALERT_PROVIDER"         env-required:"true"`
	AlertDisable         bool   `env:"ALERT_DISABLE"          env-default:"false"`
	AlertCooldownMinutes int    `env:"ALERT_COOLDOWN_MINUTES" env-default:"5"`
	// Off by default, unlike the embedded reporter's 30 days: this service
	// has existing deployments with existing tables, and a version bump must
	// not start deleting their history on its own. Set it to turn the sweep
	// on.
	AlertRetentionDays int `env:"ALERT_RETENTION_DAYS" env-default:"-1"`

	TelegramBotToken string  `env:"TELEGRAM_BOT_TOKEN"`
	TelegramsChatIDs []int64 `env:"TELEGRAM_CHAT_IDS"`

	DiscordBotToken   string   `env:"DISCORD_BOT_TOKEN"`
	DiscordChannelIDs []string `env:"DISCORD_CHANNEL_IDS"`
}

func LoadConfig() (Config, error) {
	var cfg Config

	err := cleanenv.ReadEnv(&cfg)
	if err != nil {
		return cfg, fmt.Errorf("LoadConfig: %w", err)
	}

	// ALERT_RETENTION_DAYS=0 is the obvious way to write "keep everything",
	// and the embedded reporter reads zero as "apply my default" — say it in
	// the way it understands before the two disagree about deleting data.
	if cfg.AlertRetentionDays == 0 {
		cfg.AlertRetentionDays = -1
	}

	err = cfg.validate()
	if err != nil {
		return cfg, fmt.Errorf("LoadConfig: %w", err)
	}

	return cfg, nil
}

// DSN returns the connection string: POSTGRES_DSN when it is set, otherwise
// one assembled from the individual variables.
func (cfg Config) DSN() string {
	if cfg.PostgresDSN != "" {
		return cfg.PostgresDSN
	}

	return fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=%s",
		quoteDSNValue(cfg.PostgresHost),
		quoteDSNValue(cfg.PostgresPort),
		quoteDSNValue(cfg.PostgresUser),
		quoteDSNValue(cfg.PostgresPassword),
		quoteDSNValue(cfg.PostgresDatabase),
		quoteDSNValue(cfg.PostgresSSLMode),
	)
}

// quoteDSNValue makes one value safe to interpolate into a keyword/value
// connection string. Unquoted, a space ends the value — so a password
// containing one silently swallows the settings after it, and you connect to
// a different database than you configured, with no parse error anywhere.
func quoteDSNValue(in string) string {
	escaper := strings.NewReplacer(`\`, `\\`, `'`, `\'`)
	return "'" + escaper.Replace(in) + "'"
}

func (cfg Config) validate() error {
	// Either form of database configuration, but a password cannot be
	// defaulted away.
	if cfg.PostgresDSN == "" && cfg.PostgresPassword == "" {
		return fmt.Errorf("Config.validate: set POSTGRES_DSN, or POSTGRES_PASSWORD and the other POSTGRES_* variables")
	}

	// Validate provider type
	if cfg.AlertProvider != AlertProviderDiscord && cfg.AlertProvider != AlertProviderTelegram {
		return fmt.Errorf("Config.validate: invalid alert provider: %q. Choices are: %q, %q",
			cfg.AlertProvider, AlertProviderDiscord, AlertProviderTelegram)
	}

	// Validate token and chat/channel IDs based on provider
	if cfg.AlertProvider == AlertProviderTelegram {
		if cfg.TelegramBotToken == "" {
			return fmt.Errorf("Config.validate: TELEGRAM_BOT_TOKEN is required for Telegram alert provider")
		}
		if len(cfg.TelegramsChatIDs) == 0 {
			return fmt.Errorf("Config.validate: TELEGRAM_CHAT_IDS is required for Telegram alert provider")
		}
	}
	if cfg.AlertProvider == AlertProviderDiscord {
		if cfg.DiscordBotToken == "" {
			return fmt.Errorf("Config.validate: DISCORD_BOT_TOKEN is required for Discord alert provider")
		}
		if len(cfg.DiscordChannelIDs) == 0 {
			return fmt.Errorf("Config.validate: DISCORD_CHANNEL_IDS is required for Discord alert provider")
		}
	}

	return nil
}
