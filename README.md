# RTK Cloud Logger

Shared logging package and central logging owner for RTK cloud Go services.

This module owns the common zap logger configuration used by service and worker
entrypoints across the RTK cloud repositories. It also owns the central logging
architecture: journald forwarder, ingest API, storage/query backend contract,
redaction policy, and operational runbook.

## Defaults

- backend: `go.uber.org/zap`
- style: typed `*zap.Logger` fields, not `SugaredLogger`
- encoding: zap production JSON
- static fields: `service`, `env`, `version`
- level names: `debug`, `info`, `warn`, `warning`, `error`, `dpanic`,
  `panic`, `fatal`
- output: application logs to stdout, zap internal errors to stderr

Applications should write logs to stdout/stderr only. The `rtk_cloud_logger`
forwarder collects logs from journald, container logs, nginx log files, or
similar host-level sources and sends them to the logger backend.

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
func NewMemoryStore() *MemoryStore
func NewIngestServer(*MemoryStore, IngestServerConfig) *IngestServer
func NewHTTPDelivery(endpoint, token string, client *http.Client) *HTTPDelivery
func NewForwarder(ForwarderConfig) *Forwarder
func NewFileCursorStore(path string) *FileCursorStore
func NewDiskSpool(path string, maxRecords int) *DiskSpool
func NewFileRecordSource(path string) *FileRecordSource
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

This module owns application log formatting, shared Go helpers, the central
logger backend contract, and the Go journald forwarder. Application services do
not push logs directly to the backend; the forwarder handles remote delivery.

The detailed backend and forwarder handoff is documented in
[`docs/CENTRAL_LOGGING_ARCHITECTURE.md`](docs/CENTRAL_LOGGING_ARCHITECTURE.md).
The low-level package policy is documented in [`docs/SPEC.md`](docs/SPEC.md).

Recommended Loki labels are low-cardinality values such as `env`, `host`,
`service`, `unit`, `component`, and `level`. Do not use `device_id`, `user_id`,
`org_id`, `request_id`, `operation_id`, raw `path`, or IP addresses as default
labels.

## Central Logging Backend

The first central backend API is intentionally small and embeddable:

- `LogEvent` is the normalized service-log record shape.
- `MemoryStore` inserts events idempotently by `event_id` and supports queries
  by time, env, service, host, unit, level, trace id, request id, operation id,
  device id, org id, and user id.
- `IngestServer` exposes `/healthz`, `/readyz`, `/v1/logs/ingest`, and
  `/v1/logs/query`.
- `IngestServerConfig.Token` enables deployment-provisioned bearer-token auth.
  mTLS can be added at the deployment/server layer without changing event
  shape.
- Events are redacted before persistence. Sensitive fields such as auth
  headers, tokens, cookies, passwords, credential-bearing DSNs, cloud
  credentials, private keys, and certificate key material are stored as
  `[REDACTED]`.

Example ingest server:

```go
store := cloudlogger.NewMemoryStore()
handler := cloudlogger.NewIngestServer(store, cloudlogger.IngestServerConfig{
	Token: ingestToken,
})

server := &http.Server{
	Addr:    "127.0.0.1:19090",
	Handler: handler,
}
```

## Forwarder

The forwarder reads host log records, generates stable ids, persists cursor
state, spools records on disk, retries delivery with backoff, and reports local
status. Cursor advancement happens only after backend acknowledgement.

Example wiring:

```go
forwarder := cloudlogger.NewForwarder(cloudlogger.ForwarderConfig{
	Source:      cloudlogger.NewFileRecordSource("/var/log/rtk-cloud/records.jsonl"),
	CursorStore: cloudlogger.NewFileCursorStore("/var/lib/rtk-cloud-logger/cursor.json"),
	Spool:       cloudlogger.NewDiskSpool("/var/lib/rtk-cloud-logger/spool.jsonl", 10000),
	Delivery:    cloudlogger.NewHTTPDelivery(ingestURL, ingestToken, nil),
	MaxAttempts: 5,
})

if err := forwarder.ProcessOnce(context.Background()); err != nil {
	status, _ := forwarder.StatusJSON()
	_ = status
}
```

When the logger backend is unavailable, the forwarder reports degraded status
and keeps unacknowledged records in the bounded spool. Application services keep
writing stdout/stderr logs and do not need to stop.

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
