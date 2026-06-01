# Central Logging Architecture

Status: implementation handoff.

Owner: `rtk_cloud_logger`.

## Scope

`rtk_cloud_logger` is the owner for RTK Cloud service logging:

- shared zap SDK and HTTP middleware
- service log schema and redaction policy
- journald forwarder binary
- ingest API
- storage/query backend
- operator runbook for logger backend and forwarders

Application code must use the zap SDK but must not synchronously push logs to
the backend. Remote delivery is handled by the forwarder.

## Components

| Component | Responsibility |
| --- | --- |
| Zap SDK | Build root `*zap.Logger` values with stable service fields and JSON stdout/stderr behavior. |
| HTTP middleware | Emit one request event after each request with status, latency, sanitized path, and request id. |
| Forwarder | Read journald/file/container sources, persist cursor, batch, retry, and send to ingest API. |
| Ingest API | Authenticate forwarders, validate schema, dedupe by `event_id`, and write accepted records. |
| Storage/query backend | Store service logs and support support/debug queries by time, service, host, unit, trace, request, operation, org, user, and device fields. |

## Log Event Shape

The backend should accept one event per service log record.

Required fields:

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

Optional correlation fields:

- `trace_id`
- `request_id`
- `operation_id`
- `device_id`
- `org_id`
- `user_id`
- `component`
- `error_category`

Payload-specific fields may be preserved under the event body, but unknown
fields must not bypass redaction rules.

## Forwarder Design

The forwarder is a Go binary installed as a systemd service on every VM that
runs RTK Cloud services.

The package now provides the reusable forwarder primitives:

- `JournalRecord` for normalized journald/file records
- `FileRecordSource` for JSON-lines file/container handoff
- `FileCursorStore` for cursor state under `/var/lib/rtk-cloud-logger/`
- `DiskSpool` for bounded local retry state
- `HTTPDelivery` for token-authenticated ingest API delivery
- `Forwarder.Status` and `Forwarder.StatusJSON` for local readiness scripts

Required behavior:

- collect selected journald units with their `_BOOT_ID`, `_HOSTNAME`,
  `_SYSTEMD_UNIT`, and cursor metadata
- generate stable `event_id` values from `host_id + boot_id + unit + cursor`
- persist cursor and spool state under `/var/lib/rtk-cloud-logger/`
- use bounded disk spool and report when records are dropped due to local
  limits
- retry failed batches with exponential backoff and jitter
- advance cursor only after backend acknowledgement
- expose local status for readiness scripts
- never delete journald data directly

Delivery is at-least-once. Duplicate sends are expected during crash/retry
windows and must be handled by backend idempotency.

## Ingest API Requirements

Forwarders authenticate with a deployment-provisioned token or mTLS identity.
The first implementation may use token auth; mTLS remains an accepted hardening
path.

The package now provides `IngestServer`, which exposes:

- `GET /healthz`
- `GET /readyz`
- `POST /v1/logs/ingest`
- `GET /v1/logs/query`

`IngestServerConfig.Token` enables bearer token or `X-Logger-Token`
authentication. Deployments that require mTLS should terminate or enforce mTLS
at the server/TLS layer while preserving the same ingest/query contract.

The ingest API must:

- reject unauthenticated writes
- validate required fields
- apply redaction policy before persistence where server-side parsing is used
- insert idempotently by `event_id`
- return per-record status for partial batch failures
- expose health and readiness endpoints

## Storage And Query Requirements

The backend must support operational queries for:

- time range
- `env`
- `service`
- `host`
- `unit`
- `level`
- `trace_id`
- `request_id`
- `operation_id`
- `device_id`
- `org_id`
- `user_id`

High-cardinality fields may be indexed if the selected backend supports it, but
they must not be treated as Loki-style low-cardinality labels by default.

The in-package `MemoryStore` is the first storage implementation and test
contract. It stores events idempotently by `event_id`, applies redaction before
persistence, and supports the query dimensions listed above.

## Security

Never store raw auth headers, bearer tokens, refresh tokens, cookies, passwords,
database DSNs with credentials, OIDC client secrets, TURN shared secrets, Linode
tokens, Object Storage credentials, SMTP credentials, private keys, or
certificate private material.

When a field is useful for correlation but sensitive, store a stable hash or a
redacted marker.

## Readiness

The logger deployment is ready when:

- backend health returns healthy
- ingest API accepts a test event
- duplicate test event does not create a second stored record
- forwarder reports current cursor and last successful upload time
- staging readiness can query a sample `trace_id`

Application services remain healthy when the logger backend is unavailable; the
readiness report should mark logging as degraded rather than failing unrelated
service health checks.
