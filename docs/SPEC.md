# RTK Cloud Logger Specification

## Purpose

`rtk_cloud_logger` is the shared logging module for RTK cloud Go services and
the source of truth for application log format policy, central service-log
ingestion, and forwarder behavior.

The module exists so service repositories do not each invent their own logger
configuration, field names, redaction rules, or operational assumptions. It
standardizes service log emission first while also owning the central logger
backend and journald forwarder contracts. Deployment tooling provisions those
components and wires service hosts to them.

## Scope

This repository owns:

- shared Go logging package built on `go.uber.org/zap`
- JSON service log format conventions
- common logger construction helpers for server and worker entrypoints
- HTTP request logging helper behavior for Go services
- redaction policy for application-emitted logs
- Go journald/file forwarder behavior
- ingest API contract and idempotent `event_id` handling
- logger backend storage/query requirements
- operator documentation for how services should emit, collect, and query logs

This repository does not own:

- business metrics; those remain in Prometheus-facing code
- device runtime log persistence; those remain device-originated diagnostic
  records owned by Video Cloud runtime log ingestion
- service-specific audit databases
- direct log shipping from application code
- unrelated VM provisioning implementation outside the logger dependency shape

Applications must write logs to stdout/stderr. Agents collect logs from
journald, Docker, nginx files, or other host-level sources.

## Logging Backend

The Go package must use `go.uber.org/zap`.

Service code should use `*zap.Logger` with typed fields. `SugaredLogger` is not
part of the service logging standard.

The default logger must:

- emit single-line JSON logs to stdout
- emit logger internal errors to stderr
- use zap production encoder defaults: `ts`, `level`, `msg`, `caller`,
  `stacktrace`
- include static service fields when provided:
  - `service`
  - `env`
  - `version`
- support level parsing for:
  - `debug`
  - `info`
  - `warn` and `warning`
  - `error`
  - zap fatal/panic levels for explicit process-fatal paths

The module should expose a small public API:

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
```

Additional helpers may be added when they remove duplicated service code, but
they must preserve typed zap fields.

The central logging implementation also exposes backend/forwarder primitives:

```go
type LogEvent struct { /* central service log record */ }
type LogQuery struct { /* supported query filters */ }

func NewMemoryStore() *MemoryStore
func NewIngestServer(*MemoryStore, IngestServerConfig) *IngestServer
func NewHTTPDelivery(endpoint, token string, client *http.Client) *HTTPDelivery
func NewForwarder(ForwarderConfig) *Forwarder
func NewFileCursorStore(path string) *FileCursorStore
func NewDiskSpool(path string, maxRecords int) *DiskSpool
func NewFileRecordSource(path string) *FileRecordSource
```

## Required Fields

Every service log line should contain these fields after logger construction:

| Field | Meaning |
| --- | --- |
| `ts` | Zap timestamp. |
| `level` | Zap level. |
| `msg` | Stable event message. |
| `service` | Low-cardinality service name, for example `video-cloud-api`. |
| `env` | Deployment environment, for example `staging` or `prod`. |
| `version` | Release or build version when known. |

Service log messages should be stable event names, not interpolated sentences.
Variable data belongs in typed fields.

Good:

```go
logger.Info("starting service",
	zap.String("addr", addr),
	zap.String("component", "http"),
)
```

Avoid:

```go
logger.Infof("starting service on %s", addr)
```

## Common Field Names

Services should prefer these field names where applicable:

| Field | Usage |
| --- | --- |
| `component` | Internal component or subsystem. |
| `addr` | Listen or remote address. |
| `method` | HTTP method. |
| `path` | Sanitized HTTP route/path. |
| `status` | HTTP status code. |
| `duration_ms` | Request duration in milliseconds. |
| `remote_addr` | Peer address after normal proxy handling. |
| `request_id` | Request correlation id when available. |
| `operation_id` | Idempotent operation id. |
| `device_id` | Device id in log body only; do not use as Loki label. |
| `org_id` | Organization id in log body only; do not use as Loki label. |
| `error` | Error value via `zap.Error(err)`. |
| `error_category` | Stable error class useful for dashboards or alert context. |

High-cardinality values such as `device_id`, `user_id`, `org_id`,
`request_id`, paths with dynamic ids, IP addresses, and operation ids must stay
in the log body and must not be promoted to Loki labels by default.

## HTTP Request Logging

HTTP middleware provided by or implemented according to this module should emit
one event per completed request:

```json
{
  "level": "info",
  "msg": "http request",
  "service": "account-manager",
  "method": "GET",
  "path": "/v1/health",
  "status": 200,
  "duration_ms": 3.4,
  "remote_addr": "203.0.113.10"
}
```

Request logging must:

- log after response completion
- record status code and latency
- sanitize sensitive query parameters
- avoid logging request bodies by default
- avoid logging `Authorization`, cookies, access tokens, refresh tokens, API
  keys, passwords, and raw OIDC secrets
- recover and log panics only through service-approved recovery middleware

Sensitive query parameter names include at least:

- `token`
- `access_token`
- `refresh_token`
- `api_key`
- `apikey`
- `password`
- `client_secret`

## Redaction Policy

Application logs must not contain runtime secrets unless an explicit local/eval
adapter is intentionally designed for that purpose.

Never log:

- authorization headers
- bearer tokens
- refresh tokens
- account passwords
- OIDC client secrets
- database DSNs with credentials
- TURN shared secrets
- Linode, GoDaddy, S3, SMTP, or CloudWatch credentials
- private keys or certificate private material

Evaluation-only token delivery logs are allowed for local development when a
service explicitly configures a `log` delivery adapter. Such events must be
clearly named and documented by the service repository, and production-like
deployments must not use that adapter.

Where a value is useful for correlation but sensitive, services should log a
stable hash or redacted marker instead of the raw value.

## Central Logging Backend

The backend accepts one `LogEvent` per service log record. Required fields are:

- `event_id`
- `ts`
- `level`
- `msg`
- `service`
- `env`
- `version`
- `host`
- `unit`
- `source`

The backend supports optional correlation fields:

- `trace_id`
- `request_id`
- `operation_id`
- `device_id`
- `org_id`
- `user_id`
- `component`
- `error_category`

Ingest API behavior:

- authenticate forwarders with deployment-provisioned bearer token or
  `X-Logger-Token`; mTLS can be enforced by deployment TLS configuration
- expose `/healthz`, `/readyz`, `/v1/logs/ingest`, and `/v1/logs/query`
- validate required fields
- redact sensitive values before persistence
- insert idempotently by `event_id`
- return per-record ingest status

Query behavior must support filters for time range, `env`, `service`, `host`,
`unit`, `level`, `trace_id`, `request_id`, `operation_id`, `device_id`,
`org_id`, and `user_id`.

## Forwarder Model

The forwarder reads selected journald/file/container records, creates stable
`event_id` values from `host_id + boot_id + unit + cursor`, persists cursor
state, writes unacknowledged events to a bounded disk spool, retries delivery
with backoff, and reports local status for readiness scripts.

Forwarder requirements:

- advance cursor only after backend acknowledgement
- preserve unacknowledged events when the backend is unavailable
- report cursor, last upload time, degraded status, spool count, dropped record
  count, and last error
- never require application services to stop when the logging backend is down
- never delete journald data directly

## Deployment Model

The expected production-like log architecture is:

```text
Go services -> stdout/stderr -> journald or container logs
nginx       -> access/error log files
Docker      -> container logs
forwarder   -> rtk_cloud_logger ingest API
backend     -> service-log storage/query
```

Recommended deployment roles:

- each application VM runs the RTK Cloud logger forwarder
- a dedicated logging service hosts ingest API and storage/query backend
- ingest API is reachable only on the private network unless explicitly proxied
- query UI/API may be exposed through the edge gateway with authentication
- long retention should use object storage rather than only local disk

Application code must not push logs directly to Loki, CloudWatch, or any remote
backend. Remote service-log delivery is handled by the forwarder.

## Loki Label Guidance

Default Loki labels should be low-cardinality:

- `env`
- `host`
- `service`
- `unit`
- `component`
- `level`

Do not use these as default labels:

- `device_id`
- `user_id`
- `org_id`
- `request_id`
- `operation_id`
- raw `path`
- IP addresses

Those values can remain searchable in log bodies.

## Integration Expectations

Service repositories should import this module and construct a root logger in
their operational entrypoints.

Recommended service startup pattern:

```go
logger, err := cloudlogger.New(cloudlogger.Config{
	Service: serviceName,
	Env:     env,
	Version: version,
	Level:   logLevel,
})
if err != nil {
	return err
}
defer logger.Sync()
```

Libraries and internal packages should accept `*zap.Logger` from callers rather
than constructing their own root logger. Tests should use `cloudlogger.Nop()` or
`zaptest/observer` when asserting output.

## Compatibility

The first migration from existing service logs to this module is allowed to
change log format from plain text or `slog` text to JSON.

The migration must not change:

- HTTP response shapes
- API behavior
- Prometheus metric names
- database schemas
- device runtime log ingestion semantics
- audit event persistence semantics

## Acceptance Criteria

A service repository has completed logger integration when:

- operational entrypoints construct loggers through `rtk_cloud_logger`
- service and worker logs are JSON
- request logs are structured and sanitized
- tests cover representative JSON log output
- tests prove sensitive query parameters or headers are not logged
- existing business tests still pass
- deployment docs identify which service name appears in logs

This repository itself is healthy when:

- `go test ./...` passes
- exported APIs are documented through README and this specification
- examples compile or are kept as documentation-only snippets
