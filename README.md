# RTK Cloud Logger

Shared logging package for RTK cloud Go services.

This module owns the common zap logger configuration used by service and worker
entrypoints across the RTK cloud repositories. It emits single-line JSON logs to
stdout so deployment agents can collect service logs from journald, Docker, or
file-based sinks.

## Defaults

- backend: `go.uber.org/zap`
- style: typed `*zap.Logger` fields, not `SugaredLogger`
- encoding: zap production JSON
- static fields: `service`, `env`, `version`
- level names: `debug`, `info`, `warn`, `warning`, `error`, `dpanic`,
  `panic`, `fatal`
- output: application logs to stdout, zap internal errors to stderr

Applications should write logs to stdout/stderr only. Host or deployment agents
collect logs from journald, container logs, nginx log files, or similar sources.
This module does not push logs directly to Loki, CloudWatch, or any remote
backend.

## Public API

```go
type Config struct {
	Service     string
	Env         string
	Version     string
	Level       string
	Development bool
}

func New(Config) (*zap.Logger, error)
func MustNew(Config) *zap.Logger
func Nop() *zap.Logger
func ParseLevel(string) zapcore.Level
func HTTPMiddleware(*zap.Logger) func(http.Handler) http.Handler
func SanitizePath(string) string
```

`New` returns the configured root logger. `MustNew` panics on construction
errors for entrypoints that intentionally fail fast. `Nop` is for tests and
disabled logging paths. `ParseLevel` accepts the standard level names listed
above and defaults unknown values to `info`.

## Server Entrypoint

```go
logger, err := cloudlogger.New(cloudlogger.Config{
	Service: "video-cloud-api",
	Env:     "staging",
	Version: "v0.1.0",
	Level:   "info",
})
if err != nil {
	panic(err)
}
defer logger.Sync()

logger.Info("starting service", zap.String("addr", "127.0.0.1:18080"))
```

Wrap HTTP handlers with the request logger at the service boundary:

```go
mux := http.NewServeMux()
mux.HandleFunc("/v1/health", healthHandler)

handler := cloudlogger.HTTPMiddleware(logger)(mux)
server := &http.Server{
	Addr:    "127.0.0.1:18080",
	Handler: handler,
}
```

The middleware emits one `"http request"` event after each completed request.
It records `method`, sanitized `path`, `status`, `duration_ms`, `remote_addr`,
and `request_id` when `X-Request-Id` is present. It does not read request
bodies and does not perform panic recovery.

## Worker Entrypoint

```go
logger := cloudlogger.MustNew(cloudlogger.Config{
	Service: "video-cloud-worker",
	Env:     "prod",
	Version: version,
	Level:   logLevel,
})
defer logger.Sync()

logger.Info("starting worker", zap.String("component", "scheduler"))
```

Libraries and internal packages should accept `*zap.Logger` from callers rather
than constructing root loggers themselves. Tests should use `cloudlogger.Nop()`
or `zaptest/observer`.

## Field Names

Use stable event messages and typed zap fields. Variable data belongs in fields,
not interpolated messages.

Common field names:

- `component`: internal component or subsystem
- `addr`: listen or remote address
- `method`: HTTP method
- `path`: sanitized HTTP route/path
- `status`: HTTP status code
- `duration_ms`: request duration in milliseconds
- `remote_addr`: peer address after normal proxy handling
- `request_id`: request correlation id
- `operation_id`: idempotent operation id
- `device_id`: device id in the log body only
- `org_id`: organization id in the log body only
- `error`: error value via `zap.Error(err)`
- `error_category`: stable error class

Keep high-cardinality values such as device ids, user ids, organization ids,
request ids, operation ids, raw paths, and IP addresses in log bodies. Do not
promote them to default Loki labels.

## Redaction

Do not log authorization headers, bearer tokens, refresh tokens, account
passwords, OIDC client secrets, database DSNs with credentials, TURN shared
secrets, cloud provider credentials, SMTP credentials, private keys, or
certificate private material.

`SanitizePath` redacts sensitive query parameter values for `token`,
`access_token`, `refresh_token`, `api_key`, `apikey`, `password`, and
`client_secret`. Where sensitive values are useful for correlation, log a stable
hash or a redacted marker rather than the raw value.

## Deployment Boundary

This module owns application log formatting and shared Go helpers. It does not
own business metrics, device runtime log persistence, service-specific audit
databases, VM provisioning, Loki, Grafana, Vector, Alloy, Fluent Bit, or direct
remote log shipping.

Recommended Loki labels are low-cardinality values such as `env`, `host`,
`service`, `unit`, `component`, and `level`. Do not use `device_id`, `user_id`,
`org_id`, `request_id`, `operation_id`, raw `path`, or IP addresses as default
labels.

## Service Migration Checklist

- Construct root loggers through `rtk_cloud_logger` in server and worker
  operational entrypoints.
- Emit service and worker logs as JSON.
- Use typed zap fields and stable event messages.
- Wrap service HTTP handlers with structured request logging.
- Add tests for representative JSON log output.
- Add tests proving sensitive query parameters and headers are not logged.
- Document the `service` name that appears in deployment logs.
- Preserve HTTP response shapes, API behavior, Prometheus metric names,
  database schemas, device runtime log ingestion semantics, and audit event
  persistence semantics during migration.
