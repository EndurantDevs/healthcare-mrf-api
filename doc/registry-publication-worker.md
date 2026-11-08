# Run-once publication worker

Run the command in the normal application image:

```sh
python main.py start registry-publication-worker
```

It consumes at most one durable publication-family request, then exits. It uses
the existing publication consumer, authority client, approval, composition and
publication engines. It starts no HTTP service and changes no API pool.

The operator provides one JSON object through `HLTHPRT_REGISTRY_PUBLICATION_WORKER`.
The UTF-8 value is limited to 16 KiB. Duplicate keys at any level, unknown fields,
invalid types and unsupported connection options refuse before any connection or
HTTP client is opened. The required top-level fields are `lifecycle_dsn`,
`execution_dsn`, `app_origin`, `service_token`, and `consumer`.
`initial_address_sources` is optional and may be null. The optional `ptg` object
configures genuine retained-office composition. The optional `operation_seconds`
field is an integer from 1 through 86400, defaulting to 3600. Booleans and other
types are rejected before resources open.

```json
{
  "lifecycle_dsn": "postgresql://worker_lifecycle@db.example.test/registry",
  "execution_dsn": "postgresql://worker_execution@db.example.test/registry",
  "app_origin": "https://authority.example.test",
  "service_token": "example-service-token",
  "consumer": {
    "control_schema": "registry",
    "lifecycle_role": "worker_lifecycle",
    "execution_role": "worker_execution",
    "api_role": "registry_api",
    "queue_owner_role": "registry_queue_owner",
    "physical_owner_role": "registry_snapshot_owner",
    "loader_roles": [],
    "reader_roles": ["registry_reader"]
  },
  "initial_address_sources": null
}
```

These are synthetic examples. Supply dedicated credentials through the existing
protected configuration custody. Both DSNs must select the same host, port,
database and TLS mode, with different explicit users matching the consumer's
lifecycle and execution roles. PostgreSQL and `postgresql+asyncpg` URLs are
accepted. The only permitted query option is `ssl=require` or `ssl=verify-full`;
use the same option on both URLs. An omitted port means 5432. An omitted password
is passed as an explicit empty password, so the worker does not borrow an ambient
`PGPASSWORD` or password-file value. Existing native role and permission checks
remain required. No roles or grants are provisioned by this command.

The consumer object has exactly the six required schema and role fields shown
above; `loader_roles` and `reader_roles` are optional unique arrays of at most 32
role names each. The five principal roles must be distinct. The application
origin accepts only an HTTP or HTTPS origin without user information, path,
query or fragment. The service token is never printed.

For an ordinary first publication at serving head zero, provide an explicitly
admitted protected base-address artifact:

```json
{
  "base_source": {
    "schema_name": "retained_addresses",
    "table_name": "entity_address_unified",
    "generation_id": "retained-address-edition-one"
  },
  "npi_source": null
}
```

This object is the value of `initial_address_sources`. `base_source` is required;
`npi_source` is optional and accepts either null or the same three-field pin
shape. Configuration checks syntax only. The real preparation path must verify
protected native ownership, exact relation identity and retained provenance.
Never derive an immutable generation from a mutable live heap. Existing serving
heads resolve their retained source and address recipes through the consumer.
For retained PTG office recipes, supply a closed `ptg` object with `owner_role`,
`approval_role`, and `read_limits`. The limits object requires `maximum_bytes`,
`maximum_pages`, and `maximum_coordinates`; the existing graph reader validates
all limits. The worker uses its validated execution URL to open one additional
SQLAlchemy connection pool, with size one and no overflow. Its genuine session
acquires the existing company-approval fence before preparing a candidate and
revalidates the current company association, reviewed binding and retained
source. Historical capture receipts stay immutable. The worker disposes this
pool on every exit. No additional DSN or authority bypass is accepted.

For exact approved Source witness captures, the optional `source_witness` object
contains exactly `source_profiles`, `capture_owner_role`, and `runtime_roles`.
`source_profiles` is a nonempty array of validated native FHIR profiles with
unique schema names. Each profile uses the existing `schema_name`, `owner_role`,
`runtime_roles`, and `catalog_schema_name` fields, with an optional validated
`published_read_scope`. The capture census is a sorted, unique array of 1–32 role
names and must exclude the capture owner. It must include every actual original
and effective worker actor, queue owner, reader and custodian; a metadata-owner
label does not exempt a role that also acts at runtime. Incomplete census refuses
before the custodian pool is created.

This configuration requires `custody_dsn` and compact `initial_address_sources`.
The image must provide
`directory_membership_runtime.membership_source_export_runtime`, exposing the
original membership export context. The same module must re-export the original
`CatalogSnapshotRuntime`, `FHIRSourceMembershipInput` and
`open_membership_source_export` objects so exact type and function identity checks
remain intact. The metadata-only catalog factory cannot
substitute for it. The worker opens this context through its existing owned
lifecycle and closes it after its other pools, including on failure and
cancellation. Configuration does not accept factories, extra connections or
authority callbacks. A missing package refuses explicitly.

Each selected capture still requires protected approval of its whole digest,
the configured owner and census, independent complete Source replay, and native
candidate custody before ordinary publication. These settings do not enroll
successors or enable membership refresh. CMS, PTG and ACA source-notification
workers do not accept this new server configuration; their existing modes are
unchanged.

The worker owns a lifecycle pool with minimum 1 and maximum 2 connections, an
execution pool with minimum and maximum 1, and one authority client. Cleanup
runs after completion, startup failure, work failure and cancellation; a failed
pool close terminates that owned pool. The existing consumer fixes the lease at
300 seconds and heartbeat at 30 seconds. The operation deadline starts after
resource setup and bounds the consumer's work. Expiry cancels and drains the
existing execution and heartbeat tasks, then closes all owned resources; cleanup
is allowed to finish beyond the deadline. The request retains its running lease
and immutable command for an exact retry after lease expiry, while the prior
serving generation remains available. The command reports the usual constant
worker failure. No automatic retry flags are accepted.

Output is only `{"state":"no_work"}` or a canonical `request_id` and
`"state":"completed"`. Durable results, configuration, credentials and
exception messages are omitted. Exit status 2 reports constant invalid-input
errors, status 1 reports a constant worker failure, and status 130 reports
cancellation. Unsupported arguments are also redacted; `--help` is available.
Source qualification, trusted role bootstrap, deployed command registration and
runtime acceptance remain separate from this command's local checks.

For [initial inventory publication](../docs/network-initial-publication.md),
configure both retained bootstrap pins in `initial_address_sources` and provide
`custody_dsn`. This uses the same accepted URL format and database endpoint as
the execution connection, with a separately authenticated identity outside the
worker's role census. The existing native identity and ownership checks still
apply. Initial publication requires pre-existing direct, owner-issued,
nongrantable custody-ledger reader access; it refuses missing enrollment and
does not restore revoked grants. The worker consumes the initial request
through its ordinary queue and deadline lifecycle without approving saved drafts.

The custody ledger may have a protected owner separate from the registry schema
owner. The schema, source-admission records, approval controls, candidate metadata,
queue and serving head keep their existing owner. Only `network_candidate_custody`
and sealed candidate storage use the separate owner. The two owners must have no
membership relationship. Existing source receipts keep their original identities.

Complete the existing custody migration and adoption before this handoff. In a
caller-owned maintenance transaction, `split_registry_candidate_custody` validates
the exact native layout, foreign key and reader grants, then transfers only an
empty ledger. It requires the original LOGIN superuser, empty candidate and
manifest tables, and the original unset serving head. Its locks remain held until
the caller commits or rolls back. The new owner must already have registry
`USAGE` without `CREATE`; the handoff preserves the schema ACL. It does not attest
migration history or repair unexpected ownership. Repeating it after the split
refuses rather than reinterpreting retained state.

Ordinary custody continues through `custody_dsn`, never the maintenance login.
The custodian inherits the execution role's bounded reads and loading ownership,
and can assume the loading and custody owners. It must have no membership,
inherited privileges or role-assumption path to the registry schema owner.
Existing database `CREATE` prerequisites for candidate ownership transfer still
apply. Temporary candidate-schema access is removed before the final closure;
the worker installs no roles or memberships.

Split ownership emits revision-three seals binding both native owners. Revision
one and two seals retain their original single-owner meaning, so an existing
seal forbids this handoff. Older binaries reject the split. After activation,
rollback must retain a split-aware image and the retained serving snapshot;
transferring a populated ledger back is not a supported rollback.


The optional closed `sql_limits` object supplies six positive integer limits:
`work_mem_bytes` (default 16777216), `maintenance_work_mem_bytes` (67108864),
`temp_file_limit_bytes` (268435456), `statement_timeout_ms` (120000),
`lock_timeout_ms` (5000), and `connect_timeout_seconds` (5). A supplied object
must contain exactly these fields. Memory and temp values must be whole KiB
within PostgreSQL's native ranges; connection timeout is 1..60 seconds.
Parallel query and maintenance workers are fixed at zero. Arbitrary GUC names
and SQL values are rejected.

The worker sets USERSET bounds at connection startup and verifies effective
limits on its actual execution connection and optional genuine PTG backend
before claiming work. Every raw execution and adapted PTG checkout rechecks them. The operator
must configure a finite positive execution-role/database `temp_file_limit`
default no greater than its configured cap. The worker never grants parameter
privileges, changes role defaults, or sets this privileged parameter. An
unlimited or oversized effective temp limit refuses before claim or approval.
Connection and checkout waits use the configured connection timeout. The driver
query fallback allows that same interval beyond the server statement timeout
for its cancellation response. Existing
pool counts and the operation deadline/cleanup ordering remain unchanged.

These are per-backend memory targets, statement/spill limits and finite job
concurrency. They do not impose host CPU/RAM or I/O-rate quotas; deployment
owners retain container limits and worker concurrency. Temp-file limits exclude
explicit temporary tables. Cancellation still drains existing work and retains
the immutable request and prior serving generation for exact lease-expiry retry.
