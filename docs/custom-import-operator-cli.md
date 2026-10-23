# Custom-import operator CLI

Run these commands from an installed application environment with the existing
database configuration. They use the standalone engine; no external control
service is required. IDs below are synthetic examples, not default targets.

| Entry point | Commands | Purpose |
| --- | --- | --- |
| `custom_import_cli` | `validate` | Validate a definition from stdin without database access. |
| `custom_import_cli` | `register` | Register an immutable definition from stdin. |
| `custom_import_cli` | `execute`, `resume` with `--source https`, `s3` or `upload` | Run a bounded object import or replay its exact retained capture. |
| `custom_import_cli` | `status`, `captures`, `rejections` | Read exact retained lifecycle evidence or one bounded rejection page. |
| `custom_import_cli` | `cancel`, `activate`, `rollback` | Request cancellation or perform guarded publication. |
| `custom_import_snowflake_operator` | `register`, `register-authority` | Register a canonical binding using local database authority or a fixed mounted capability. |
| `custom_import_snowflake_operator` | `execute`, `resume` | Acquire an approved source or resume a retained capture. |
| `custom_import_snowflake_operator` | `discover`, `estimate`, `preflight` | Inspect selected metadata, count approved source streams, or preview bounded families. |

Validate a bounded definition from standard input without opening the database:

```sh
python -m custom_import_cli validate --format json < definition.json
```

Register a canonical JSON envelope containing exactly `dataset_key`,
`definition`, and `source_binding`, then execute its returned retained IDs:

```sh
python -m custom_import_snowflake_operator register < registration.json
python -m custom_import_snowflake_operator execute \
  --definition-revision-id 12 --source-binding-revision-id 14 \
  --idempotency-key synthetic-import
```

`register-authority` also reads stdin, using its separately prepared fixed
registration capability and authority mounts. It accepts no command-line
authority, credential, SQL, path, or environment overrides. Registration does
not itself acquire source records. `execute` uses the fixed local key-pair
credential directory, the retained role/warehouse, and disabled secondary roles.

Inspect only the selected fields of the approved relations:

```sh
python -m custom_import_snowflake_operator discover \
  --definition-revision-id 12 --source-binding-revision-id 14
python -m custom_import_snowflake_operator estimate \
  --definition-revision-id 12 --source-binding-revision-id 14 \
  --maximum-total-bytes 65536 --maximum-elapsed-seconds 30
```

`discover` runs a generated selected-column `LIMIT 0` query per stream. Its
receipt includes approved logical field IDs, recognized source types, and
capture-runtime type compatibility, including explicit decimal conversions.
Unrecognized or unavailable types are
`unknown` with compatibility false; this is not proof of data validity,
nullability, root-key uniqueness, or family completeness. No records are fetched.
The separate metadata queries do not constitute a single source snapshot.

`estimate` runs one generated statement containing `COUNT(*)` for each approved
stream relation with its approved row filters. Source row counts are exact
for that statement, including duplicates and unmatched child rows.
Counts are not retained import snapshots
or admitted family counts. Import yield, storage bytes, and warehouse credits
are explicitly `unknown`. Counts from repeated use of a relation are separate
stream observations, not additive distinct records.

Both commands load retained bindings read-only and emit no source samples,
physical relation names, SQL, or driver diagnostics. Their only optional
arguments are `--maximum-total-bytes` (default 1 MiB, cap 16 MiB) and
`--maximum-elapsed-seconds` (default 30, cap 120). The byte limit applies to the
serialized successful receipt; a fixed redacted error envelope is separate.
Errors discard the result. Elapsed time is shared across source queries,
with driver timeouts rounded up to whole seconds and a deadline check after
each call. A stalled driver or cleanup may exceed the deadline before returning.
These are not process-memory or warehouse-scan limits: an exact count may
scan the entire source relation and incur warehouse cost.

For bounded family preview, use the existing [`preflight` command](./custom-import-snowflake-preflight.md).
Samples require its explicit `--include-sample` option; discovery and estimate
do not accept that option.

Inspect an exact execution or retained generation:

```sh
python -m custom_import_cli status --dataset-id 5 --execution-id 17
python -m custom_import_cli status --dataset-id 5 --generation-id 19
```

Inspect the retained capture bound to one exact dataset and execution:

```sh
python -m custom_import_cli captures --dataset-id 5 --execution-id 17
```

This read-only command uses the retained operator evidence reader and returns
the capture bundle ID and manifest SHA-256, plus the execution's dataset,
definition and schema revision IDs and state. An existing execution with no
bound capture returns `"capture":null`; a missing execution returns `not_found`.
Mismatched or ambiguous retained evidence fails with a redacted `failed` receipt.
It does not acquire source data or inspect replay payloads, manifest contents,
per-stream metadata, storage paths, credentials or samples. A retained manifest
digest alone does not establish that capture payloads are still replayable.

Read one page of retained rejection evidence for an exact terminal build:

```sh
python -m custom_import_cli rejections --dataset-id 5 --execution-id 17 --build-id 23
```

This operator command uses the configured database identity; it does not
authorize a client or create a download endpoint. That identity must already
have SELECT access to the exact retained metadata, collection and snapshot
registries and rejection leaf, namespace USAGE, and EXECUTE on the existing
materialization-storage read-lock helper. The command never changes roles or
grants. Missing privileges, nonterminal executions, mismatched pins and
unavailable storage return the fixed `failed` error without partial stdout.
The full retained identity is checked before selecting rejection rows. Each
page owns a fresh repeatable-read, read-only transaction and reacquires the
registered relation locks and statement-bound relation identity checks.

The JSON page contains at most 100 events and 65,536 UTF-8 bytes, followed by a
newline. `--limit` may lower the event limit to any value from 1 through 100;
the byte bound may produce fewer events. Logical keys preserve their configured
types and leading zeros. Non-retained offending values remain explicitly
unavailable; raw rejection evidence and source credentials are never returned.

When `complete` is false, repeat the same command with `--after-ordinal` set to
the returned `next_after_ordinal` and `--scope-sha256` set to the returned
`scope_sha256`. Both continuation values must come from that same page. The
digest binds the retained scope; it is not an authorization token. A genuine
empty page has `complete:true`. One page is not a complete export: a caller must
consume every page until `complete:true` and treat any nonzero exit as an
incomplete result. This command provides neither an automatic export nor
client-owner authorization or signed cursors. Existing activation-evidence
HTTP authorization is unchanged.

### Dedicated rejection-page control

`POST /control/v1/custom-import/rejection-page` exposes the same retained reader
through a separate read-only service capability. Configure a distinct
`HLTHPRT_CUSTOM_IMPORT_REJECTION_READ_TOKEN` and send it as a Bearer token. The
route fails closed if this token is absent or reused as a control, registry,
activation-evidence, execution-stop or build-continuation token. It never grants
worker launch, import mutation or client-owner authorization. A calling service
must authorize its caller independently before requesting a page.

Send UTF-8 JSON with exactly these fields and no query parameters:

```json
{
  "dataset_id": 5,
  "definition_revision_id": 7,
  "idempotency_key": "synthetic-run",
  "build_id": null,
  "after_ordinal": null,
  "scope_sha256": null,
  "limit": 100
}
```

The body limit is 4,096 bytes. Trusted dataset, immutable definition and original
request pins must identify one terminal execution. A null `build_id` requires
exactly one retained build; ambiguity fails rather than choosing the newest
attempt. An explicit build must belong to that same execution. Continuations
must repeat the returned build ID, last ordinal and scope digest. IDs in the
response are decimal strings; request IDs and ordinals are JSON integers.

Each successful response is the reader page itself, bounded to 61,440 UTF-8
bytes and at most 100 events. This reserves space for a calling service's
authorization/cursor envelope without changing the CLI's larger page budget.
No-store applies to route-boundary failures too. Missing or foreign execution
locators share `404 not_found`; nonterminal evidence returns
`409 evidence_not_final`; unavailable or ambiguous storage returns the fixed
`503 evidence_unavailable` without retained values. Invalid requests return
`400 invalid_request`, and absent or incorrect credentials return
`403 forbidden`. The configured database role needs the same SELECT, USAGE and
existing read-lock-helper privileges as the CLI; this endpoint never changes
roles, privileges or storage. One HTTP page is not a complete download.

Request cooperative cancellation; a terminal execution can return an unchanged
receipt. This does not guarantee that an active worker has already stopped:

```sh
python -m custom_import_cli cancel --execution-id 17
```

Resume an existing retained Snowflake capture with its same definition, source
binding, and idempotency key:

```sh
python -m custom_import_snowflake_operator resume \
  --definition-revision-id 12 --source-binding-revision-id 14 \
  --idempotency-key synthetic-recovery
```

This source-specific command rebuilds the retained request and statement but
does not open source credentials or acquire the source again. It accepts only
an existing capture-bound execution in `running` or `canceling` state with an
expired database lease. Queued, terminal, live-lease, unbound, and mismatched
executions fail with a redacted receipt. A `canceling` execution is only
acknowledged as canceled. The command never automatically retries.

## Bounded segmented Snowflake execution

The [bulk-loading design requirement](./imports/bulk-loading.md) applies to
changes to this pipeline: set-based validation must replace old per-row database
validation, and snapshot/index preparation must not interrupt serving reads.
The current retained page limits below describe the existing execution contract,
not a claim that larger COPY transport alone meets that requirement. Immutable
binding limits are not changed implicitly by a writer migration.

An immutable `custom-import/source-binding/v2` binding opts into segmented
capture and bounded family builds. It retains the existing binding fields and
requires a complete `processing_policy` object. A v1 binding keeps its existing
execution path and identity; it does not inherit current operator settings.

| Policy member | Required declaration |
| --- | --- |
| `capture` | `custom-import/segmented-capture-policy/v1`: per-part limits, per-stream and whole-bundle budgets, decoded Arrow and manifest bounds, retained-storage limit, acquisition deadline |
| `driver_timeout_seconds` | Positive integer, at most 120 seconds per driver operation |
| `build` | `page_row_limit`, `page_byte_limit`, `statement_timeout_ms`, `lease_seconds`, `build_deadline_seconds` |

All limits are explicit positive integers; unknown, missing, incoherent, or
over-ceiling values fail validation. Logical build pages admit at most 256 records
and 256 MiB. Physical SOURCE, trusted admission and projection batches may combine logical
pages within a 100,000-record/256-MiB ceiling without changing their identity or
validation rules. Declaration ceilings are not recommended sizing or throughput claims.
Changing any retained limit requires a new binding revision, not a retry of an
old request with different settings. The complete capture-policy fields and
validation are defined in
[`segmented_capture_policy.py`](../process/custom_import/segmented_capture_policy.py).

Use the existing revision-based command and fixed local credential provider:

```sh
python -m custom_import_snowflake_operator execute \
  --definition-revision-id 12 --source-binding-revision-id 14 \
  --idempotency-key synthetic-segmented-run
```

Acquisition seals only after every stream reaches verified EOF with matching
snapshot evidence. Source replay, global duplicate detection, family replacement,
winner materialization and final sealing continue under the same execution claim.
An invalid child still rejects its whole root family. Engine activation uses the
original current-pointer compare-and-swap; a conflict retains a sealed but
unpublished candidate. Identical effective output records `no_change` only while
the original base and pointer version remain current.

### Explicit retained-build continuation

Ordinary `resume` replays retained captures under a new claim; it does not adopt
an expired build's admitted tables or partial GRAPH checkpoint. For an explicitly
authorized stopped build, the separate continuation control can retain that exact
snapshot and remove its processing wall-clock cap without editing the historical
deadline or producer identity. This is not an automatic retry policy.

`POST /control/v1/custom-import/build-continuation-status` reads bounded progress.
`POST /control/v1/custom-import/build-continuation-authorize` records the transfer.
Both require a distinct `HLTHPRT_CUSTOM_IMPORT_CONTINUATION_TOKEN`, the exact
dataset, definition revision, source-binding revision and idempotency key. The
authorization additionally binds the build, snapshot, capture, original producer,
base pointer, previous authority, replacement worker identity and nonce digest.
The caller must establish that the previous worker has stopped. A live lease,
cancellation, changed source identity or changed base pointer rejects transfer.

The processing worker consumes a single-use launch authorization and a separate
32-byte nonce from fixed protected files:

```sh
python -m process.custom_import.continuation_cli run \
  --authorization-file /var/run/custom-import-processing/authorization.json \
  --nonce-file /var/run/custom-import-processing/nonce --lease-seconds 3600
```

The first authorization requires a stopped, admitted, unsealed GRAPH build with
no opened generation. That continuation can progress normally through GRAPH,
OUTPUT and verification on the retained snapshot without another authorization;
it never replays SOURCE or admission. A later authorization may resume OUTPUT or
verification only through an existing continuation chain, with the same
stopped-worker and expired-lease requirements. The short launch window is not a
processing timer. Finite renewable leases, statement and memory bounds,
cancellation, complete validation and normal publication compare-and-swap still
apply. Old bearers and legacy write paths are
fenced after opt-in; stopped or expired continuation workers need a fresh explicit
authorization. Stop requests after opt-in must include the current
`expected_continuation_id`. Credentials and the raw nonce never belong in control
request bodies or diagnostic output.

For configured query projections under a restricted worker database role, mount
the approved TLS CA at `/var/run/custom-import-processing/writer-ca.pem` and pin
`HLTHPRT_CUSTOM_IMPORT_CONTINUATION_PROJECTION_ORIGIN` to the trusted HTTPS control
origin. The worker sends one nonce-authenticated, cursor-bound request to
`/control/v1/custom-import/build-continuation-projection` per bounded projection
step. The engine owner performs only the existing projection operation after
rechecking the current epoch; no snapshot ownership or general control credential
is given to the worker. Uncertain delivery permits one retained progress read,
not an implicit resend.

Local, signed-permit and continuation projection writers share the same physical
step. Request authority covers this bounded physical operation; the retained
logical limits and wire contract do not change. Existing signed scope or current
continuation authority and the supplied exact progress are checked under the
same locks, with fresh authority verified before commit.
Root, child and semantic-digest steps may coalesce up to 32 logical pages in one
transaction. Each iteration reserves its full retained row and byte
allowance, including empty windows, within the 100,000-row/256-MiB physical
envelope. Payloads and metadata remain bounded to one logical page at a time;
the byte allowance does not measure Python or database executor memory. No next
page starts after the one-second soft coalescing budget or half the remaining
fenced window. Existing statement, lease and permit deadlines still apply.
Coalescing stops at a phase change, and an index step builds at most one index.
All rows and the final exact checkpoint commit together; any failure rolls back
that physical transaction without authorizing an automatic resend.

### Frozen materialization verification

Bounded builds use `custom-import/materialization/v2` inside the existing
`custom-import-generation-seal/v1` envelope. They commit every selected canonical
family and winner identity, verify complete projection presence/null states and
relationships set-wise, and audit derived scalar values and complete winner
rankings on a bounded reproducible sample. They do not reread every scalar value
or rehash every large family after the protected snapshot freezes.
Protected SOURCE page writes close exact projection presence/null-state coverage;
finalization checks retained copies against canonical payloads in bounded batches,
including unexpected scalar rows. Complete selected-revision accounting and
relationship checks still cover both origins.

Operator status includes `materialization_contract` and `verification_evidence`.
The evidence records the seed, selected-key digest and actual root, child and
winner-group coverage. Child key probes are not a uniform row sample. Every
sampled revision receives a complete typed-value comparison. Whole-family
rehashing is limited to one sampled family with at most 128 children per selection
origin (source or retained), at most two families total. Oversized families receive
bounded revision checks rather than whole-family rehashing; oversized winner
groups may have zero independently recomputed rankings. Reported sample coverage
is not exhaustive proof. Isolated incorrect derived values or rankings outside
the sample can escape this redundant audit.
Authorization, candidate freeze, exact completeness and atomic publication are
not sampled.

Retained exhaustive `custom-import/materialization/v1` seals remain readable and
rollback-safe. Hashes from different materialization contracts are never used as
equivalent no-change evidence; the first compact rebuild of a legacy generation
may therefore produce a new generation even when its served content is unchanged.

The build deadline is the capture's database `sealed_at` plus the retained build
duration. Resume uses the same capture and deadline, without accessing source
credentials or fetching source rows. An expired deadline cannot be extended by
retrying. Run acquisition in a supervised worker process: driver timeouts and
cooperative cancellation alone cannot guarantee hard termination of a blocked
driver. No policy declaration grants database or source authorization.

### Optional trusted batch writer

Restricted workers use fixed internal operations for SOURCE loading, family
admission and configured derived-query projections. The application derives bytes and configuration from the sealed
capture; requests contain only retained execution/build/fence and cursor identities.
SOURCE authorization, bounded COPY and ordinary set-based finalization share one
transaction. Existing standalone operation remains available without this handoff.

The launcher must authenticate and mount the complete immutable set of
purpose-separated permits, `admission-launch.json` and the fixed `.writer-ca.pem`
certificate-only bundle before either `execute` or `resume`. The launch descriptor
pins the exact CA bytes; all clients verify certificates and hostnames using only
that bundle, without environment trust. A partial or invalid stage fails before
source credentials or a claim; it never silently selects the local writer.
All permits retain their original
scope and expiry. Workers receive no signing keys or additional database authority.
The engine endpoints are disabled without a dedicated keyring and fixed HTTPS
origin, and public proxy listeners deny these operations.

`custom-import/admission-launch/v2` adds the fixed `.projection-permit.json`
file and its digest to the launch descriptor. The projection permit authorizes
only `/control/v1/custom-import/query-projection-batch`; SOURCE and admission
permits cannot authorize it. New restricted launches with enabled numeric derived
query fields require v2 before claiming an execution. Definitions without those
capabilities may keep v1. Retained executions keep their original authenticated
files, expiry and compatible worker image; a software upgrade does not reissue
their authority. Deploy compatible endpoints, worker and launcher together before
selecting v2 for new launches.

An uncertain response permits one read of retained progress, not automatic resend
or local fallback. Continue only from verified forward progress under the same
authority. Source part boundaries do not define physical COPY batch size; small
parts are combined within the row and byte ceilings after verified reader close.
Measure capture, replay, validation, indexes and usable reads together before
claiming a throughput gain. The previous published snapshot continues serving
until the replacement is verified and atomically activated.

## Related child collections

A definition may include up to eight `child_memberships` constraints when two
declared child collections must refer to the same logical item within a root:

```json
{
  "child_memberships": [
    {
      "outer_collection": "items",
      "inner_collection": "observations",
      "key_mapping": [
        {"outer_field": "item_id", "inner_field": "observation_item_id"}
      ]
    }
  ]
}
```

Each mapping must cover the outer collection's complete child key, in order,
using distinct required fields of the inner child key. Corresponding fields
must share the supported `string` or `integer` type. Both collections still
belong directly to the same root; this does not enable cross-child query joins.

Admission checks all constraints after duplicate handling, across capture parts.
A missing referenced child rejects the whole source root family with
`child_membership_missing` evidence. An upsert may retain a compatible prior
family, but every family retained in the new candidate must also satisfy the
new constraints. Incompatible retained data fails the candidate without changing
the active generation; it is not silently removed.

Changing these constraints requires a new definition revision and import.
It does not by itself change the schema revision. Omitting `child_memberships`
preserves existing definition behavior and canonical identity.

## Explicit publication and rollback

Activate an initial generation only while the current pointer is absent:

```sh
python -m custom_import_cli activate --dataset-id 5 \
  --target-generation-id 19 --expected-pointer-version 0
```

For an existing pointer, supply both the observed generation and pointer version.
Rollback selects an exact retained generation, using the same compare-and-swap
precondition:

```sh
python -m custom_import_cli activate --dataset-id 5 \
  --target-generation-id 20 --expected-generation-id 19 --expected-pointer-version 1
python -m custom_import_cli rollback --dataset-id 5 \
  --target-generation-id 19 --expected-generation-id 20 --expected-pointer-version 2
```

Publication still applies the engine's existing seal, ownership, and retention
checks. A stale pointer fails with a redacted conflict receipt; inspect current
state before choosing new preconditions. Commands never automatically retry.

Success emits a compact JSON receipt after transaction commit and connection
cleanup. Errors omit raw database diagnostics and record payloads. A connection
cleanup failure can occur after commit: a failed command does not prove rollback.
Reconcile exact state with `status` before another mutation.

Only `custom_import_snowflake_operator execute` and `resume` add a `diagnostic`
object to their existing error envelope. Its `contract_version` is
`custom-import-failure/v1`; `phase`, `category`, and `database_reason` are closed
labels, and `execution_id` and `build_id` are known positive identifiers or
`null`. Phases identify operation boundaries such as `connect`, `capture`,
`source`, `admission`, `graph`, `output`, `activate`, and `disconnect`.
Database reasons use allowlisted PostgreSQL SQLSTATE names or
`connection_pool_timeout`, `unknown`, and `not_applicable`. In particular,
`query_canceled` does not establish that a statement timed out; pool acquisition
timeout is reported separately. Categories are `canceled`, `memory`, `timeout`,
`database`, `io`, `invalid_state`, `runtime`, and `unexpected`.
No exception text, SQL, row text, credentials,
or traceback is emitted. Identifiers are diagnostic context, not authorization
or evidence of successful commit. Registration, inspection, argument errors,
and successful receipts retain their existing formats.

Use `--help` on the entry point or any command for its arguments. Discovery,
preview, and estimate remain source-specific. Generic execution and resume
support the bounded HTTPS, S3-compatible and staged-upload paths below.

## Standalone object execution

Register the definition separately, then use its returned dataset, definition
and schema revision IDs:

```sh
python -m custom_import_cli register --dataset-key example_dataset --format json < definition.json
python -m custom_import_cli execute --source https \
  --dataset-id 5 --definition-revision-id 7 --schema-revision-id 11 < request.json
```

The bounded JSON request must contain `definition`, `objects`,
`source_snapshot_token`, `idempotency_key`, `lease_token`, `limits`,
`timeout_seconds`, and `complete_scope`. The definition must match the registered
immutable revision. The only optional request member is
`maximum_dataset_retained_bytes`, described below. Each object contains exactly
`stream_id`, `url`, `expected_sha256`, and `expected_bytes`. Supply exactly one
complete object for each declared stream; unknown,
duplicate or missing streams are rejected. URLs must satisfy the existing
public-address HTTPS transport checks. Source URLs and lease tokens belong in
stdin, not command-line arguments or emitted receipts.

Streams may declare CSV, TSV, JSON, NDJSON, XML or Parquet, with outer compression
`none` or `gzip`. The original compressed bytes are retained without converting
their format. XML uses the definition's declared record path and existing entity
denial and parser limits. Empty originals are permitted only when the declared
decoder accepts them; for example, an empty NDJSON stream is not a missing stream.

If any stream is not uncompressed Parquet, supply a non-boolean integer
`maximum_dataset_retained_bytes` between zero and `2**63 - 1`. There is no implicit
retention allowance for this path. The limit checks original bytes, retained
Parquet payload and part manifests, and prepared-source accounting across all
revisions and pending or abandoned attempts in the dataset. Exact retained
replay does not charge again.
This is an admission limit for the requested attempt, not a persistent dataset
policy. Changing it changes the execution request identity.

A mixed-format HTTPS bundle retains every stream through the same original-byte
transaction, including any Parquet streams. A wholly uncompressed-Parquet HTTPS bundle
keeps its existing storage and request identity; omit the optional cap or set it
to `null` for that legacy path. A non-null cap is rejected there rather than
pretending the legacy registrar enforces it. Existing Parquet captures remain
readable without rewriting their bytes or hashes.

### Capture database authority

Original-byte acquisition is a trusted standalone registrar operation, not a
direct-SQL worker capability. It requires effective ownership of the five
capture relations: bundle, stream header, Parquet part, canonical usage and raw
object. The pre-acquisition check refuses source access when any of those
relations retains a non-owner table or column write grant, including grants to
`PUBLIC` or an inherited group role. Explicit write grants to any non-owner are
refused, even for superusers or owner-assumable roles: their grants can be
inherited by less-privileged members. Registration checks again before inserting
originals. The application never grants privileges or changes its role.
Exact bound replay still reads the retained bytes without reacquiring a source.

The raw-object migration is additive. It does not revoke existing canonical
writer grants or enable hosted raw acquisition. A schema with restricted legacy
canonical writers can keep those writers and all old guards; raw acquisition
remains unavailable there until the write-authority cutover is complete.

Before upgrading a restricted segmented writer to the mixed-retention check,
the database administrator must explicitly provision its existing quota-reader
role with these accounting columns, in addition to its existing canonical
metadata reads. Substitute the actual schema and already-authorized role:

```sql
GRANT SELECT (dataset_id, retained_bytes)
    ON application_schema.custom_import_capture_usage TO capture_quota_reader;
GRANT SELECT (dataset_id, byte_count)
    ON application_schema.custom_import_capture_raw_object TO capture_quota_reader;
GRANT SELECT (dataset_id, retained_byte_count)
    ON application_schema.custom_import_prepared_source TO capture_quota_reader;
```

All three grants are needed even when the raw and prepared tables are empty.
Apply the schema migration and provision these columns before starting updated
restricted writers, including legacy Snowflake acquisition. Do not grant raw or
prepared payload reads, writes, counter writes or owner membership to satisfy
the quota query. These grants do not make the role a trusted registrar or enable
raw acquisition. They are explicit deployment provisioning, not automatic
migration grants.
Fresh segmented acquisition checks these metadata privileges without reading rows
before opening the source; bound replay does not require this acquisition preflight.

To enable originals, first upgrade every trusted capture writer, stop old writers
and drain their open transactions. Then close non-owner write grants at every
table and column grant root, preserving metadata-only reads. Inherited privileges
must not recover a removed write grant. Preserve existing canonical counter,
immutability and lease guards; do not disable them or rewrite their counters.
Existing ordinary-SQL entry points or owner-role escalation must not bypass this
boundary. Only then start trusted registrar acquisition. The check does not
revoke a statement that was already authorized before the cutover.

This is an authority boundary, not a format, revision or dataset restriction:
trusted writers retain and replay mixed historical v1, v2 and raw captures in the
same dataset. Legacy v1 writes retain their uncapped contract, and their bytes
remain counted by subsequent bounded raw/v2 admission. Turning off new raw
acquisition does not remove retained originals and is not permission to reopen
legacy direct writes that cannot account for them. Hosted raw acquisition stays
disabled until a separately supported bounded trusted capture handoff exists.

### Acquisition and replay bounds

Set `complete_scope` to `true` only for a complete declared source scope. Every
stream uses the same nonempty `source_snapshot_token`. That token is the
operator's upstream snapshot assertion: content hashes verify the selected
objects, not the atomicity of an upstream export.

`limits` is a mapping of `CaptureLimits` fields; `{}` uses the defaults. Limits
may be tightened, not raised: the entire bundle is bounded by 64 MiB of captured
content, 256 MiB of decoded content, and one million records. Arrow expansion
and retained mapped-record content are also accounted. These logical limits
are not a process RSS guarantee or high-volume throughput claim.
The CLI accepts a positive finite `timeout_seconds` of at most 3600 seconds,
matching the maximum lease window. This preparation deadline is shared across
acquisition, durable readback, replay and initial family admission; it does not
reset per object. Larger values are rejected before database access or execution
reservation. Subsequent graph materialization uses the existing claimed runner
and publication safeguards.

A separate fenced heartbeat renews the lease while acquisition and replay yield.
It is stopped and joined before the final admission renewal and claimed-runner
handoff. Synchronous validation is not preemptible and must still pass the
unchanged deadline and lease checks before publication.

Acquisition is bound transactionally only after the complete bundle verifies.
The runner reloads it in a fresh session and uses the existing family rejection,
immutable generation and compare-and-swap publication behavior. Errors return
fixed redacted receipts rather than source data or driver diagnostics.

Resume an existing capture-bound active execution after its lease expires:

```sh
python -m custom_import_cli resume --source https \
  --dataset-id 5 --definition-revision-id 7 --schema-revision-id 11 < request.json
```

Keep the same definition, object identities, snapshot token, limits, timeout and
idempotency key; use a new lease token. A live lease returns `not_claimed`.
Explicit resume rejects queued, terminal, unbound or mismatched executions and
never reacquires the source. An interrupted unbound acquisition needs a new
idempotency key. The same execution cannot silently refetch changing objects.
With a new idempotency key, refreshed URLs or transport timeouts can reuse the
same sealed content and snapshot token. Changed content for that token is rejected.

### Native S3-compatible objects

Use the same request envelope and revision IDs with `--source s3`. Each object
requires `stream_id`, `endpoint`, `region`, `bucket`, `key`, `expected_sha256`
and `expected_bytes`. Optional `version_id` selects an exact version;
`addressing` may be omitted or set to `path`. The HTTPS endpoint must be an
origin only, without a path, query, user information or fragment. No bucket
listing, region discovery, SDK fetcher or ambient credential lookup occurs.
ETag is not a substitute for the required SHA-256 content pin.

The operator provisions `/run/custom-import-operator/s3-access-key.json` inside
an owner-only directory. The owner-read-only regular file contains
`access_key_id`, `secret_access_key` and optionally `session_token`.
Credentials and credential paths are not request fields. Signing occurs only
after a new execution claim; encoded signed path/query bytes reach the existing
guarded HTTPS transport unchanged, with redirects forbidden. Credentials,
signatures and signing timestamps are excluded from the stable request identity.

### Staged uploads

Use `--source upload`. Each object contains exactly `stream_id`, `object_id`,
`expected_sha256` and `expected_bytes`. The operator stages an unchanged regular
file named by `object_id` under `/var/lib/custom-import-operator/uploads`.
IDs contain 1–128 lowercase letters, numbers, underscores or hyphens and start
with a letter or number. Requests cannot supply paths, choose a staging root,
follow symlinks or read special files. Acquisition checks descriptor identity,
directory replacement and in-place changes; it never removes the staged input.
Bounded local reads yield between chunks but cannot preempt a blocked kernel read.

Both native sources support all six formats and gzip. They always retain original
bytes, including plain Parquet, and require the explicit dataset retention cap.
They share the complete-bundle registration, exact-fence binding, fresh readback,
family validation and publication path described above. Execute or resume with
the same source kind and stable descriptors. A bound resume does not open staged
files, load credentials, sign URLs or access a source network.

These standalone operator paths require no external control service. They do not
enable hosted HTTPS, S3-compatible or upload endpoints or credential submission.

### Optional authenticated capture handoff

Application callers may supply `CaptureHandoffTransport` to
`run_https_candidate` or `run_object_candidate`. This replaces capture preflight,
registration and retained readback with three fixed authenticated POSTs through
the existing application pool. It does not replace execution claiming or the
claimed runner, enable a hosted source, or provision source credentials.
The ordinary CLI remains standalone and unchanged.

The server capability is off by default. Explicit
`HLTHPRT_CUSTOM_IMPORT_CAPTURE_HANDOFF_ENABLED=1` requires the existing startup-
pinned admission keyring and origin; partial or invalid configuration fails
startup. A separate capture signature domain prevents admission, SOURCE or
projection permits from authorizing capture writes. Only the issuer holds
signing keys; launch bootstrap must independently authenticate the signed permit
and approved TLS origin before constructing the worker transport.

The permit binds the immutable definition, request/content digests, retention
ceiling and tightened capture limits. Preflight checks the exact execution,
lease and registrar authority before source access. The binary bundle contains
a bounded canonical manifest header followed by exact original bytes; transport
compression, duplicate streams, truncation and trailing bytes are rejected.
Registration rechecks authority after transfer and uses the existing raw
registrar and execution binding in one transaction. Raw originals are explicit
for this path, including plain HTTPS Parquet; legacy standalone storage and
request identities remain unchanged.

Acquisition expires at the earlier of permit expiry and the execution's first
start plus the signed timeout. A new HTTP request never renews that budget.
Uncertain completion does not prove rollback and is never retried or followed
by a source refetch. A later bound resume reads the exact retained bundle over
the authenticated path, without falling back to local owner access. Retained-
only observation requires valid current authority but cannot reopen acquisition.
Managed configuration/issuer wiring, effective database roles and source-specific
runtime/security/capacity qualification are separate enablement requirements.

### Owner-only preparation of retained object sources

The Python integration also accepts immutable `custom-import/source-binding/v3`
bindings for HTTPS, S3-compatible objects and uploads. A binding pins the
definition, schema, approved stream locations and a
`custom-import/object-processing-policy/v1` policy. It has no credential fields,
arbitrary SQL, environment references or caller-selected filesystem paths.
Existing Snowflake v1/v2 bindings and operator commands keep their behavior once
the accounting-column permissions above are provisioned for restricted writers.

The existing `custom_import_snowflake_operator register` and `register-authority`
commands also accept v3 object bindings in the same canonical envelope containing
exactly `dataset_key`, `definition` and `source_binding`. They return the existing
numeric-ID/digest receipt; `register-authority` uses the same fixed mounted
capability and exact input pins. Registration only retains the definition and
binding. That entry point's execution, resume and inspection commands remain
Snowflake-specific. Registration does not enable managed issuer or worker-launch
integration and makes no database grants.

After acquisition has sealed and bound the original bundle, a trusted owner may
call `prepare_retained_source` with the exact execution, live fence/token and
SOURCE permit. Preparation reads retained originals, never reacquires the
source. All six supported formats and gzip use the same bounded decoders.
Prepared parts use the internal `custom-import/prepared-source-rows/v1`
representation, preserving source ordinals, field names, missing/null states
and decoded scalar values. They are not relabelled as Parquet.

Preparation progresses from pending to closed only after every declared stream
reaches decoder EOF and cleanup succeeds. Sealing verifies the bounded part
metadata and commits its original-bundle and processing-policy identity. Exact
committed prefixes are observed on resume; an uncertain write is not blindly
retried. Resume keeps the execution's original deadline. Dataset retention
accounting includes originals, prepared payloads, manifests and control metadata,
including pending or abandoned attempts.

`prepared_source_build.build_request` creates the value-only request for the
existing bounded SOURCE pipeline. Each write transaction independently rechecks
ownership, immutable pins, current authority and the seal. SOURCE reads one
prepared part at a time and uses native COPY, the existing set-based admission
and the ordinary graph/output checks. The original capture bundle remains the
provenance identity. Preparing or sealing input does not publish or activate a
generation; the normal verified publication and rollback lifecycle still applies.

This path requires effective database ownership at its application-owned write
seams. An EXECUTE-only worker is insufficient. Installing its schema does not
grant worker access, issue permits or enable hosted object sources.
