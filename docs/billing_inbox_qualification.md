# Billing inbox qualification

## 2026-09-01 local retained inbox

Runtime/docs commit `92b5612` implements a separate synchronous bbolt inbox for
the `billing_usage` stream. The inbox commits immutable financial-content
binding, stored event and monotonic receipt sequence in one transaction. It
preserves a random store identity, freezes each paginated high-water horizon,
collects late events in the next horizon and rejects sequence gaps, foreign or
ahead cursors, missing/replaced/unsafe storage, corrupt receipts and conflicting
replays. It never initializes missing storage unless explicitly requested.
Structured producer fields are normalized to an exact JSON-number map before
hashing, so a restart does not change their digest or round large integers.

The dedicated billing credential cannot read or write operational logs, and
operational support queries exclude historical billing-stream records. Ingest
keeps integer precision with `json.Number`, rejects trailing JSON or financial
fields requiring redaction, limits stored events to 1 MiB and pages to 8 MiB.
Operational logs remain in Loki; PostgreSQL remains off the per-log path.

Focused race tests and vet/build passed. PR-profile run
`local-logger-billing-inbox-final` passed in **11.368s**, governed coverage
**76.76% >= 72%** and root package **78.73% >= 73.5%**. Artifact redaction and all
configured package ratchets passed. Coverage artifact SHA-256:
`965734c8c01d633b163e69ab90e7c05f688b502169f608b7940f6d3795a995cb`.
Evidence is in the unpublished qualification checkout at
`.artifacts/test-runs/local-logger-billing-inbox-final/coverage/test_report.md`.

The workspace catalog has 267 cases. Registering the complete service spec and
five service OpenAPI operations produced 41 features, 404 requirements, 669
operations, 67 workflows and zero blocking inventory findings. No normative
clause was removed or downgraded to obtain that result.

The gate did not use a base ref, so it is not default pre-PR, differential or
Linux CI evidence. This checkpoint also does not establish a consumer cursor/fact
transaction, reconciliation of old Loki data, producer-drain completeness,
volume restore/archival, HA routing, capacity or staging behavior. The new inbox
and consumer are a coordinated cutover; neither leaf change is independently
deployment-ready. No shared runtime or staging state was changed.
