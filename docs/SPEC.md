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
func IngestHandler(EventStore, IngestConfig) http.Handler
func NewForwarder(EventSource, EventSink, CursorStore, ForwarderConfig) *Forwarder
```

Additional helpers may be added when they remove duplicated service code, but
they must preserve typed zap fields.

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
| `actor_id` | Actor id for admin audit/support drilldown; do not use as Loki label. |
| `actor_type` | Actor class such as `cloud_admin`, `service`, or `device`. |
| `outcome` | Stable operation result such as `success` or `failure`. |
| `status_code` | HTTP or operation status code represented as a string. |
| `status_class` | Status class such as `2xx`, `4xx`, or `5xx`. |
| `error` | Error value via `zap.Error(err)`. |
| `error_category` | Stable error class useful for dashboards or alert context. |

High-cardinality values such as `device_id`, `user_id`, `org_id`,
`actor_id`, `request_id`, paths with dynamic ids, IP addresses, and operation
ids must stay in the log body and must not be promoted to Loki labels by
default.

## Journald JSON Message Promotion

The forwarder must parse journald records whose `MESSAGE` value is a JSON
object. It must promote known correlation fields from that JSON object into the
top-level `LogEvent` body:

- `trace_id`
- `request_id`
- `operation_id`
- `device_id`
- `org_id`
- `user_id`
- `component`
- `error_category`
- `actor_id`
- `actor_type`
- `outcome`
- `status_code`
- `status_class`

The JSON `msg` value becomes the event message. Unknown non-sensitive fields
remain in `fields` so support queries can inspect HTTP details and service
outcomes without adding high-cardinality Loki labels. If `MESSAGE` is not JSON,
the forwarder keeps the existing plain-text behavior.

Billing and usage metering are owned by Prometheus-facing code or dedicated
usage meters. Logger events are for admin management, support correlation, and
billing dispute investigation only. Do not add price, invoice, charge, SKU/plan,
or billing-state fields to logger events.

## Query API Behavior

`GET /v1/logs` is the Cloud Admin and support query surface. It must support the
same low-cardinality and post-filter query fields as `EventQuery` while keeping
high-cardinality values out of Loki labels.

The query API should support:

- `limit` for bounded result pages, defaulting to a conservative page size
- `order=desc` for newest-first admin lists
- `order=asc` for replay or timeline inspection
- `400` responses for invalid query parameters, including malformed timestamps,
  invalid `limit`, and unsupported `order`

Storage implementations should apply filters first, then sort, then apply the
limit. Loki-backed implementations must keep high-cardinality fields in
post-filtering rather than promoting them into selector labels.

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

## Deployment Model

The expected production-like log architecture is:

```text
Go services -> stdout/stderr -> journald or container logs
nginx       -> access/error log files
Docker      -> container logs
agents      -> Loki-compatible backend
Cloud Admin -> Loki query API for the v1 operator dashboard
```

Recommended deployment roles:

- each application VM runs Vector, Grafana Alloy, or Fluent Bit as a log agent
- a dedicated `logs` VM hosts Loki as the central log storage/query backend
- Loki is reachable only on the private network unless explicitly proxied
- the v1 operator dashboard is implemented by Cloud Admin and queries Loki, or
  a workspace/logger query adapter, from the private network
- Grafana is optional and is not required for the v1 dashboard; if deployed in a
  later profile, expose it only through the edge gateway with authentication
- long retention should use object storage rather than only local disk

This Go module must not push logs directly to Loki, CloudWatch, or any remote
backend. Shipping is an infrastructure concern.

## Forwarder And Ingest Operations

Forwarders should expose local status for readiness scripts and deployment
monitoring. The status surface should report current cursor, last upload time,
last uploaded count, last error, degraded state, and bounded spool backlog
statistics such as file count, total bytes, and oldest queued age.

The bounded spool exists to absorb backend outages without blocking
applications. A corrupt local spool file must not block later valid batches; it
should be quarantined for operator inspection while the forwarder continues
flushing other valid spool files.

The ingest API should reject oversized request bodies and batches with too many
events. These limits protect the backend from accidental oversized forwarder
uploads while keeping normal batch processing idempotent by `event_id`.

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
