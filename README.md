# sentinel

Error collection and alerting for Go services: errors go to Postgres, are
de-duplicated per service+operation with a cooldown, and are posted to Telegram
or Discord.

It runs in two shapes, on the same core:

| | Embedded | Standalone |
|---|---|---|
| Deployment | none — a library in your process | one gRPC service |
| Wire hop | none | gRPC |
| Setup | `sentinel.New` | `app.NewServer` + `cmd/sentinel-stand` |
| Use when | one application, or each owns its database | many applications report to one place |

Both write the same rows through the same code, so an application can start
embedded and move to a central service later without changing what it reports.
(The schema differs by default — `sentinel` embedded, `POSTGRES_SCHEMA`/`public`
standalone — so set it explicitly if the two must share tables.)

## Embedded

```go
rep, err := sentinel.New(ctx, sentinel.Config{
	Service: "my-app",
	Env:     "prod",
	DSN:     dsn,                                 // or Pool: existing pool
	Alert:   sentinel.Telegram(botToken, chatID),
	Logger:  logger,
})
if err != nil {
	return err
}
defer rep.Close(ctx)

if err := chargeOrder(ctx, id); err != nil {
	rep.Report(sentinel.ErrorInfo{
		Operation: "charge order",
		Message:   err.Error(),
		Details:   map[string]string{"order_id": id},
	})
}
```

Sentinel creates its own schema and tables on `New`; there is nothing to
migrate.

### Through slog

`NewSlogHandler` turns every `logger.Error` into an alert, so no call site has
to know sentinel exists:

```go
base := slog.New(slog.NewJSONHandler(os.Stdout, nil))

rep, err := sentinel.New(ctx, sentinel.Config{..., Logger: base})
// ...
logger := slog.New(sentinel.NewSlogHandler(base.Handler(), rep, nil))
slog.SetDefault(logger)

logger.Error("charge order", "err", err, "order_id", id)
```

The record's **message becomes the operation** — the key the cooldown groups by
— so log messages should be stable identifiers with the varying parts in
attributes. Attributes become the alert's details, groups flattened to
`group.key`; `err` becomes the message and `code` the error code (both
configurable through `SlogOptions`).

`Config.Logger` **must be the base logger**, never one wrapped with
`NewSlogHandler`: sentinel logs its own failures there, and feeding those back
into alerting would loop. Passing `nil` for the reporter makes the handler a
pure passthrough, which keeps alerting optional without branching.

### Behaviour worth knowing

- **`Report` never blocks and never fails.** It hands the error to a bounded
  queue drained by one goroutine. When the queue is full — an error storm — or
  the reporter is closed, the report is counted and dropped; the first reports
  of a storm already rang the bell, and the cooldown would have suppressed the
  rest. `Stats()` returns `Sent`/`Dropped`/`Failed`, and `Close` warns if
  anything was lost.
- **`SendError` is the synchronous alternative**, storing the report on the
  calling goroutine and returning the storage error. Use it in a background job
  or a handler that owes a client an answer. The alert itself is posted
  asynchronously either way, so a nil error means stored, not yet delivered.
- **`Close(ctx)` late in shutdown**, so errors raised by the shutdown itself
  still alert. It stops intake, flushes the queue within `ctx`, waits
  `CloseGrace` for the last alert (sentinel posts it asynchronously after the
  error is stored), and closes the pool if sentinel opened it. A second `Close`
  is a no-op, and a stray `Report` afterwards is counted, not a panic.
- **The database is separated by schema**, `sentinel` by default. Pass `Pool`
  to share the application's pool, or `DSN` to have sentinel open a small pool
  of its own (`MaxConns`, 2 by default) that never competes for the
  application's connections. `New`'s `ctx` bounds the whole setup, including
  the first connection — give it a deadline.
- **Old rows are deleted**, `RetentionDays` (30) past their `created_at`,
  hourly and in batches, by one replica at a time. Nothing ever reads a row
  older than the cooldown window, so this only reclaims space — but sentinel
  writes into a database it does not own, so it cleans up after itself. Set it
  negative to keep everything.
- **A failed alert does not cost the whole window.** The cooldown is claimed
  before the chat API is called, so if delivery fails after retries the window
  is cut to about a minute instead of running its full length — one transient
  502 no longer buys five minutes of silence. Cut, not cleared: clearing it
  would let every rejected report ask again at once, and a rate-limiting chat
  API would keep itself rate-limited.

> **Keep log messages constant.** The alert cooldown groups by
> service+operation, and `SlogHandler` uses the record's message as the
> operation. `slog.Error(fmt.Sprintf("order %d failed", id))` mints a new key
> per order: no de-duplication, and a row per error forever. Put the varying
> part in an attribute.

### Config

Everything except the database — exactly one of `Pool` or `DSN` — has a usable
default.

| Field | Default | |
|---|---|---|
| `Service` | — | name in stored rows and alerts; per-report `ErrorInfo.Service` overrides it |
| `Env` | `""` | shown in alerts, to tell prod from staging |
| `Pool` / `DSN` | — | set exactly one; a borrowed `Pool` is not closed |
| `Schema` | `sentinel` | created on `New` if missing |
| `Alert` | `NoAlerts()` | `Telegram`, `Discord`, `CustomNotifier`, `NoAlerts` |
| `Logger` | `slog.Default()` | sentinel's own failures, at Warn |
| `CooldownMinutes` | 5 | one alert per service+operation per window |
| `QueueSize` | 256 | pending reports before `Report` drops |
| `SendTimeout` | 5s | storing one report; the alert has its own timeout |
| `CloseGrace` | 2s | wait for the last asynchronous alert |
| `RetentionDays` | 30 | rows older than this are swept hourly; negative keeps everything |
| `MaxDetailLen` | 500 | per-value cap, on a UTF-8 boundary; negative disables |
| `MaxConns` | 2 | size of the pool opened for `DSN`; unset defers to `pool_max_conns` |

A nil `*Reporter` is valid and every method on it does nothing, so alerting can
be switched off by leaving the variable nil rather than branching at each call
site.

## Standalone

`cmd/sentinel-stand` is the gRPC service; clients call `pb.SentinelServiceClient`.
It is configured from the environment:

| Variable | Default | |
|---|---|---|
| `ENVIRONMENT` | required | |
| `GRPC_HOST` / `GRPC_PORT` | `localhost` / `5001` | **set `GRPC_HOST=0.0.0.0` in a container**, or it binds its own loopback |
| `POSTGRES_DSN` | | when set, replaces the connection settings below (host, port, user, password, sslmode) — the only way to reach a database that requires TLS. `POSTGRES_SCHEMA` still applies |
| `POSTGRES_HOST` / `POSTGRES_PORT` | `localhost` / `5432` | |
| `POSTGRES_USER` / `POSTGRES_PASSWORD` | `postgres` / required unless `POSTGRES_DSN` is set | |
| `POSTGRES_DATABASE` / `POSTGRES_SCHEMA` | `sentinel` / `public` | |
| `POSTGRES_SSLMODE` | `disable` | |
| `ALERT_PROVIDER` | required | `telegram` or `discord` |
| `ALERT_DISABLE` | `false` | store without alerting |
| `ALERT_COOLDOWN_MINUTES` | `5` | |
| `ALERT_RETENTION_DAYS` | off | set it to sweep rows older than N days; off by default so an upgrade never deletes an existing deployment's history |
| `TELEGRAM_BOT_TOKEN` / `TELEGRAM_CHAT_IDS` | | required for `telegram` |
| `DISCORD_BOT_TOKEN` / `DISCORD_CHANNEL_IDS` | | required for `discord` |

The `Dockerfile` builds it on `distroless/static` and sets `GRPC_HOST` for you.

To run it inside another process — sharing a lifecycle but keeping the gRPC
API — `app.NewServer` returns a server whose `Start` blocks and whose `Stop` is
graceful:

```go
srv, err := app.NewServer(logger, cfg)
if err != nil {
	return err
}
defer srv.Stop()

go func() {
	if err := srv.Start(); err != nil {
		logger.Error("sentinel", "err", err)
	}
}()
```

## Tests

`go test ./...` runs everywhere. The store tests need a real Postgres — the
cooldown transaction is made of `NOW()`, advisory locks and catalog races, and
a fake can tell you nothing about any of it — so they skip unless you point
them at one:

```sh
docker compose -f docker-compose.test.yml up -d
SENTINEL_TEST_DSN='postgres://postgres:postgres@localhost:5433/sentinel_test?sslmode=disable' \
  go test -race ./...
```

They create and drop their own schema per test, so the database is disposable.
