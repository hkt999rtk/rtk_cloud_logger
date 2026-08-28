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
func IngestHandler(EventStore, IngestConfig) http.Handler
func NewForwarder(EventSource, EventSink, CursorStore, ForwarderConfig) *Forwarder
func EventIDFromJournalMetadata(hostID, bootID, unit, cursor string) string
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
- `actor_id`: user, service, device, or admin actor id for audit drilldown
- `actor_type`: actor class such as `cloud_admin`, `service`, or `device`
- `outcome`: stable result such as `success` or `failure`
- `status_code`: HTTP or operation status code as a string
- `status_class`: status class such as `2xx`, `4xx`, or `5xx`
- `error`: error value via `zap.Error(err)`
- `error_category`: stable error class

Keep high-cardinality values such as device ids, user ids, organization ids,
actor ids, request ids, operation ids, raw paths, and IP addresses in log
bodies. Do not promote them to default Loki labels.

The journald forwarder parses JSON object `MESSAGE` payloads emitted by zap
services. Known fields such as `trace_id`, `request_id`, `operation_id`,
`device_id`, `org_id`, `user_id`, `component`, `error_category`, `actor_id`,
`actor_type`, `outcome`, `status_code`, and `status_class` are promoted to the
top-level `LogEvent` body so `/v1/logs` queries can match them. Other
non-sensitive JSON fields, such as `method`, `path`, `status`, `duration_ms`,
`remote_addr`, and `caller_identity`, remain in `fields`.

## Redaction

Do not log authorization headers, bearer tokens, refresh tokens, account
passwords, OIDC client secrets, database DSNs with credentials, TURN shared
secrets, cloud provider credentials, email delivery credentials, private keys, or
certificate private material.

`SanitizePath` redacts sensitive query parameter values for `token`,
`access_token`, `refresh_token`, `api_key`, `apikey`, `password`, and
`client_secret`. Where sensitive values are useful for correlation, log a stable
hash or a redacted marker rather than the raw value. Ingest and forwarder
parsing redacts sensitive fields in nested maps and arrays before persistence.

## Deployment Boundary

This module owns application log formatting, shared Go helpers, the central
logger backend contract, and the Go journald forwarder. Application services do
not push logs directly to the backend; the forwarder handles remote delivery.

Production-like private-cloud deployments use Loki as the central log
storage/query backend. The v1 operator dashboard does not require Grafana:
Cloud Admin owns the UI and queries Loki, or a workspace/logger query adapter,
over the private network. Grafana can be added later for an observability
profile, but it is not part of the v1 dashboard requirement.

Billing and usage metering remain Prometheus/usage-meter responsibilities.
Logger records provide admin audit, support correlation, and billing dispute
evidence only. Do not store prices, invoice ids, charge amounts, SKU/plan
decisions, or billing state in logger events.

The detailed backend and forwarder handoff is documented in
[`docs/CENTRAL_LOGGING_ARCHITECTURE.md`](docs/CENTRAL_LOGGING_ARCHITECTURE.md).
The low-level package policy is documented in [`docs/SPEC.md`](docs/SPEC.md).
The logger backend HTTP contract is documented in
[`docs/openapi.yaml`](docs/openapi.yaml).

Recommended Loki labels are low-cardinality values such as `env`, `host`,
`service`, `unit`, `component`, and `level`. Do not use `device_id`, `user_id`,
`org_id`, `request_id`, `operation_id`, raw `path`, or IP addresses as default
labels.

## Reference Backend And Forwarder

The first implementation includes reference binaries for integration and
provisioning work:

```sh
go run ./cmd/rtk-cloud-logger \
  -addr :18090 \
  -token "$RTK_CLOUD_LOGGER_TOKEN"
```

The reference backend exposes:

- `POST /v1/logs/ingest`
- `GET /v1/logs`
- `GET /healthz`

The CLI backend always uses Loki through `RTK_CLOUD_LOGGER_LOKI_URL` or
`-loki-url`. Service deployments must not fall back to in-process memory
storage. The in-process store remains available only to unit tests and embedded
package tests that exercise the `EventStore` interface directly.

The query API contract is intentionally admin-console friendly: callers should
page with `limit`, choose `order=desc` for newest-first lists or `order=asc`
for replay-style inspection, and receive `400` for invalid query parameters
such as malformed timestamps.

Forwarder example:

```sh
go run ./cmd/rtk-cloud-log-forwarder \
  -endpoint http://127.0.0.1:18090/v1/logs/ingest \
  -token "$RTK_CLOUD_LOGGER_TOKEN" \
  -units rtk-account-manager.service,video_cloud-api.service \
  -status-addr 127.0.0.1:18190 \
  -service account-manager \
  -env staging \
  -version "$VERSION"
```

The forwarder reads `journalctl -o json`, creates stable `event_id` values from
host, boot id, systemd unit, and journal cursor, persists the cursor only after
backend acknowledgement, and uses a bounded disk spool when upload fails.
Forwarders should expose local status for readiness and operations tooling,
including current cursor, last upload result, degraded state, and spool backlog
file/byte counts. Corrupt spool files should be quarantined so one bad local
file does not block later valid batches.

The ingest API should bound request body size and maximum events per batch.
These limits protect the logger backend from accidental oversized batches while
keeping normal forwarder traffic simple. The default reference handler limits
request bodies to 10 MiB and batches to 1000 events.

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
