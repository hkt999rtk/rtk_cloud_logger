# Billing raw-data lifecycle

Status: implemented opt-in protocol; production capacity, crash recovery, 24-hour
protection and four-hour recovery objectives still require environment qualification.
This is not an automatic cloud deletion policy or a legal retention decision.

The existing collection response, event bytes, financial content digest, random
StoreID and global receipt sequence remain unchanged. Every accepted v2 receipt
atomically writes its internal UTC `received_at`, bidirectional receipt identity
and self-contained dedupe binding. The latter also binds the original stored
record bytes, including deployment metadata excluded from financial dedupe.

## Enabling and migration

Go 1.25 or later is required. Configure an absolute private JSON file via
`RTK_CLOUD_LOGGER_BILLING_LIFECYCLE_CONFIG`. Its structure is:

```json
{
  "lifecycle": {
    "environment": "staging", "stack": "rtk",
    "source_event_environments": ["staging"],
    "scratch_dir": "/var/lib/cloud-logger-backup",
    "scratch_capacity_bytes": 34359738368,
    "encryption_key_id": "billing-backup-v1",
    "recipients": ["<age X25519 public recipient>"],
    "verifier_keys": {"verifier-v1": "<base64 Ed25519 public key>"},
    "recovery_keys": {"recovery-v1": "<independent base64 Ed25519 public key>"},
    "authority_url": "https://billing-private.example",
    "retention_days": 90, "part_bytes": 268435456, "cache_max_bytes": 1073741824,
    "backup_enabled": false, "retirement_enabled": false,
    "compaction_enabled": false
  },
  "object_store": {
    "environment": "staging", "region": "us-sea", "signing_region": "us-east-1",
    "endpoint": "https://us-sea-1.linodeobjects.com",
    "bucket": "rtk-cloud-staging-billing-backup-us-sea"
  }
}
```

Public-only configuration and scratch directories must be private (0700
directories / 0600 files). Scratch must be a separate backup PVC, not the hot PVC;
`scratch_capacity_bytes` is the explicit allocated budget, not the entire host
filesystem capacity. The writer preflights the full snapshot plus worst-case
bounded ciphertext parts and reserves at
least 2 GiB or 20% of that budget. Configuration has no private age identity or
signing key. `source_event_environments` defaults to the lifecycle environment;
any historical producer label exception needs explicit operator authorization
and an identically pinned verifier policy. Never rewrite archived event labels.

Credentials are distinct and never interchangeable:

- `RTK_CLOUD_LOGGER_BILLING_LIFECYCLE_TOKEN`: internal controller mutations.
- `RTK_CLOUD_LOGGER_BILLING_LIFECYCLE_READ_TOKEN`: only retirement terminal GET.
- `RTK_CLOUD_LOGGER_BILLING_RETENTION_AUTHORITY_TOKEN`: Logger's live Billing GET.
- `RTK_CLOUD_LOGGER_BILLING_BACKUP_ACCESS_KEY_ID` and
  `RTK_CLOUD_LOGGER_BILLING_BACKUP_SECRET_ACCESS_KEY`: dedicated bucket writer.

The existing operational and billing-intake credentials authorize none of these
control routes. Backup, retirement and compaction default disabled independently.
Backup runs every 12 hours by default; set
`RTK_CLOUD_LOGGER_BILLING_BACKUP_INTERVAL` (1 minute through 24 hours) explicitly
when qualified. This scheduling interval is not a promise of verified coverage.

Opening a v1 database does not migrate it. An authenticated `POST migrate`
advances at most 1000 legacy records per transaction and is resumable while
ingestion continues. New records receive real acceptance timestamps immediately;
legacy records use a shared conservative age floor set at migration completion.
Their original bytes and digests are never changed. No retirement or compaction
is possible before completion. Old binaries reject the migrating/v2 version;
an old pre-migration copy is not a rollback after any new commit.

## Capture, verification and archived reads

One pending set contains a consistent bbolt snapshot plus incremental raw NDJSON.
The live snapshot transaction ends after copying; compression/export operate on
that private copy. Parts are independently zstd-compressed and age-encrypted to
public X25519 recipients only. Plain parts default to 256 MiB, ciphertext is
bounded to 272 MiB, the complete set to 64 GiB and 1024 parts. Larger-than-4-GiB
snapshots are split, never sent as a single object PUT. Raw parts end on record
boundaries. Memory, free-space and workload limits still need load qualification.

Ciphertext, hashes and the exact manifest are durably sealed before upload.
Interrupted pre-seal IDs are abandoned; sealed bytes are never re-encrypted on
retry. PUT uses `If-None-Match: *`; uncertain publication or an existing object
requires full GET length/SHA-256 equality, never ETag equality. The plaintext
snapshot is removed after sealing. An unverified pending set blocks the next set
and raises coverage age; it does not evict hot records.

Verified completion publication removes only generated local ciphertext parts;
small manifest/completion metadata stays. Interrupted pre-seal sets are durably
marked abandoned and their generated scratch files cleaned on retry. Cloud
originals, hot bodies and dedupe evidence are not deleted by scratch cleanup.

Keys follow the workspace Object Storage policy. Manifest and completion reside
under `billing-inbox-snapshots/<stack>/<store-id>/<YYYY>/<MM>/<DD>/<set-id>/`.
Snapshot parts use that prefix; raw parts use the corresponding
`billing-raw/<stack>/<store-id>/<YYYY>/<MM>/<DD>/<set-id>/` prefix.

Only the independent off-cluster verifier decrypts. It checks both ciphertext
and plaintext part hashes, the full snapshot hash and complete v2 schema,
allocation frontier, both receipt indexes, retained dedupe bindings, archive
floor/catalog dependencies, terminal receipts and mutation journal. It must call
`ValidateBillingSnapshotExports` before signing: every raw record's original
bytes and trusted receipt time must match the snapshot. This prevents an older
raw timestamp being substituted under an unchanged financial digest.

The Ed25519 completion signature is domain-separated and binds the exact manifest
bytes and object list. Bounded (at most 1000 record) range proofs have a separate
domain and bind the independently derived ordered consumer reconciliation digest.
Neither proof is a financial retirement authorization. Logger accepts only its
local immutable set and explicitly approved verification public keys.

Logical collection always starts at receipt 1. A retired range absent from the
private archive cache returns retryable 503 with `X-Billing-Inbox-State:
archive-unavailable`, not an empty page or a changed cursor. Ordinary hot gaps
remain corruption and fail health. The off-cluster controller rehydrates original
bytes covered by the signed catalog and retained byte/receipt bindings, in
bounded batches. Cache eviction changes neither dedupe nor the global sequence.
The cache has an explicit byte budget (default 1 GiB); inserts beyond it fail
closed until the controller evicts older cache bodies. A request is additionally
bounded to 16 MiB, so batches must honor bytes as well as the 1000-record limit.

## Retirement and online compaction

A retirement operation is at most 1000 contiguous receipts starting immediately
after the durable archive floor. Coverage must be independently verified; every
trusted receipt must be at least 90 days old. Immediately before applying, Logger
reads the exact immutable operation from Billing over authenticated HTTPS (or an
explicitly qualified loopback/full `.svc.cluster.local` HTTP origin) and
requires ACTIVE plus exact store/environment/range/plan hash. The financial
authority maintains its durable fence until it directly reads Logger's terminal
receipt. No TTL or caller-supplied terminal JSON releases that fence.

Deletion, archive-floor advancement and the terminal receipt commit together.
IDs and trusted receipt times are retained. Completed retries return the same
receipt. Abort requires live ABORT_REQUESTED and persists a permanent tombstone,
including for a plan never previously received by Logger; late apply cannot cross
it. Disabling retirement prevents new plans and payload deletion, but leaves
authorized abort and terminal reads available so durable financial fences can
still be safely resolved. The cloud archive has no automatic expiration.

All domain mutations pass through the same journalled Update gateway. A
consistent copy is compacted into a private same-filesystem generation while
live ingestion continues. Deterministic Put/Delete/SetSequence write-sets catch
up by monotonic generation. Non-converging or oversized journals abort compaction
without rejecting live ingestion. A bounded final gate queues requests, syncs
the prevalidated candidate and atomically publishes a private active manifest.
No Pod stop, HTTP restart or open-inode rename is used. Existing read handles
drain on the old file. Only then is that exact previous generation unlinked.
The first legacy anchor becomes a tiny private fail-closed marker, reclaiming
its pages while preventing stale reopening or initialization if the manifest
is lost. Cleanup failure raises worker status without rolling back the active
generation. The durable manifest is the commit point: restart follows
it and never guesses the newest generation. No old-generation rollback is allowed
after new commits. The measured queue target is at most one second, not a fsync
guarantee; actual storage/load/crash qualification remains required.

Each compaction durably marks its private `compact-<generation>` scratch with
the exact environment, StoreID, anchor and generation before writing plaintext.
After all snapshot handles close, every normal success, failure or cancellation
unlinks only `snapshot.db`, its ownership marker and the empty operation directory.
Cleanup errors are returned and exposed in worker status; they never roll back an
already-published active generation. The next serialized compaction also removes
only strictly matching marked interrupted scratch before capacity checks. Backup
sets, foreign scopes, active generations, symlinks and unexpected files are never
swept. Legacy unmarked compaction scratch or mismatched contents require explicit
operator inspection and block further compaction until safely resolved. Unlink is
not secure erase of a PVC, provider snapshots or other backing-storage copies.
An exact owned failed shadow is closed and unlinked only before publication was
attempted. Uncertain manifest publication retains the candidate, fences admission
and reports the error; plaintext scratch is unlinked but its nonplaintext owner
marker remains. Restart cleanup requires a valid current active manifest matching
the live StoreID/path; it never unlinks that active generation. An inactive shadow
must additionally prove its StoreID and matching compaction/storage generation
before Close and exact-inode unlink. Partial, locked or unverifiable shadows and
missing/ambiguous manifests require manual inspection rather than guessed cleanup.

## Restore admission

Before opening any restored copy, set
`RTK_CLOUD_LOGGER_BILLING_INBOX_RECOVERY_MODE=true`. This durably fences admission
and survives clearing the environment flag. Queries, writes and lifecycle
mutations stay unavailable until independently signed recovery admission.

`GET recovery` exposes the restored frontier, floor, operation-history and archive
dependency digests. An independent recovery custodian (not the archive-verifier
signing role) must qualify external allocation history, financial and recovery
approvals, all referenced archives and PostgreSQL consumer checkpoints. Its
15-minute maximum-TTL signature binds these digests, exact restored StoreID and
frontier, an equal external allocation frontier and approval references. Logger
requires a separate registered recovery public key and exact state equality.
There is no automatic external-frontier discovery and no cursor reset. Clearing
the fence is not evidence that the operator performed the required qualification;
the qualified custodian is the explicit authority and audit boundary.

## Internal API

The lifecycle mux is served only on the separate private listener
`RTK_CLOUD_LOGGER_BILLING_LIFECYCLE_LISTEN_ADDR` (default `:8081`), never the public
ingest mux or public ingress. All lifecycle paths on the public listener are 404.
ClusterIP routing and network policy must restrict this listener to authorized
Billing and operator/controller access.

All paths below are relative to `/v1/internal/billing-lifecycle/`, `no-store`,
strict bounded JSON and control-token-only unless stated otherwise.

| Method/path | Input or result |
| --- | --- |
| GET migration / POST migrate | migration state / `{ "batch": 1000 }` |
| POST capture / GET captures | `{}` / sealed and uploaded set metadata |
| POST verify | `{ "set_id": "…", "completion": SignedCompletion }` |
| GET status | scoped frontier, floor, protected snapshot time and overdue state |
| POST retire/plan | immutable `RetirementPlan` |
| POST retire/apply | `{ "operation_id": "…" }` |
| POST retire/abort | full immutable `RetirementPlan` (supports unknown tombstone) |
| GET retire/{id} | immutable status; also permits read-only terminal token |
| POST rehydrate | `{ "set_id": "…", "records": [ExportRecord] }` (≤1000) |
| POST cache/evict | `{ "through_sequence": "…" }` (≤1000 removals) |
| POST compact | `{}` / cutover measurement |
| GET recovery / POST recovery/admit | restored state / `SignedRecoveryApproval` |

Unknown retirement status is 404, precondition failure 409 and unavailable
storage/authority 503. A 404 is never proof that the financial fence may release.
