# Provider Directory FHIR Import

`provider-directory-fhir` imports payer Provider Directory FHIR source metadata,
capability probes, and FHIR resources. It is meant to complement MRF/PTG price data with
FHIR directory data exposed by CMS-regulated payers.

Maintained endpoint support and configuration are listed in
[Provider Directory Endpoint Support](provider-directory-endpoint-support.md).
That generated matrix is deliberately separate from current runtime and import
status, which is recorded in the endpoint-acquisition campaign report.

## Source

The importer uses the public `provider-directory-db` SQLite catalog by default:

```text
https://raw.githubusercontent.com/hltiunn/provider-directory-db/main/data/provider_directory.db
```

Use `--seed-db-path` to run against a pinned copy during local or dev validation.
The source rows are stored in `provider_directory_source`, while live
CapabilityStatement probe results are stored in `provider_directory_capability`.
Probe parsing accepts UTF-8 BOM-prefixed JSON, because some payer endpoints
return valid FHIR JSON with that prefix. When the catalog stores a resource or
documentation path instead of the real FHIR base, probing also tries bounded
same-host parent bases for paths ending in `/metadata`, a FHIR resource type
such as `/Practitioner`, a resource query such as `/Organization?_format=json`,
a template such as `/fhir/{resource}` or `/providerDirectory/api/R4/[parameters]`,
or `/provider-directory`. If a source row carries an explicit confirmed
metadata URL, that exact URL is probed before guessed `/metadata` candidates.
If a candidate publishes a valid CapabilityStatement, that resolved base is
persisted back to `provider_directory_source.api_base` and used for the same
run's resource fetches.

The importer can also consume a pinned `provider-directory-db`
`retest_results.json` snapshot as a supplemental source list:
`--retest-results-path /path/to/retest_results.json` or
`--retest-results-url https://.../retest_results.json`. This is explicit rather
than enabled by default. It turns `valid`, `valid_non_fhir`, and
`auth_required` retest rows into seed rows, then dedupes after normal source
overrides so richer SQLite seed rows win when the same payer/base is already
represented. `auth_required` retest rows are seeded as credential-gated
Provider Directory candidates, not as immediately importable open endpoints.
Rows with a bad retest classification stay filtered unless a known payer
override or a generic FHIR resource-path normalization can recover a concrete
Provider Directory base. Those recovered rows are still gated by the normal
CapabilityStatement probe before resource import, so dead stale hosts can be
kept in the catalog for audit history without being selected for FHIR resource
fetches.

Source ids are calculated after source-specific URL overrides and generic
resource-path normalization. This keeps equivalent seed rows, such as a payer
portal URL and the confirmed FHIR base, from creating new logical source ids on
future imports.

`monthly-full` also enables supplemental official catalog pages that fill gaps
when `provider-directory-db` has no concrete FHIR base. Currently this includes
AmeriHealth Caritas plan-specific `provider-api` rows from its developer portal,
Contra Costa Health Plan's public Provider Directory API base from its APIs
for Developers page, Health Partners Plans' public Provider Directory FHIR
server, Aetna's credentialed Commercial/Medicare Provider Directory data base,
and the CMS State Medicaid Agency Endpoint Directory CSV. CMS SMA rows
are limited to public rows with a tracked Provider Directory status. Active
rows start with `last_validated_status='catalog_public'`; public rows marked
in development start with `last_validated_status='catalog_in_development'`.
Both forms store resource-specific endpoints when the CSV publishes
`/Practitioner`, `/Organization`, or `/Location` URLs, and remain out of
resource import until the normal live CapabilityStatement probe validates the
source. Source overrides also normalize
HAP/Health Alliance Plan rows to the production base published on HAP's
Provider Directory API page. Catalog parsing derives the FHIR base from the
official page/link instead of hardcoding only the resolved endpoint, then the
normal CapabilityStatement probe still gates resource import.
Contra Costa's page can return HTTP 403 to automated fetches, so the importer
retains the confirmed public metadata base as a fallback source row while still
recording the page-fetch error in supplemental catalog metrics.
Centene/WellCare seed rows can contain the stale generic
`https://fhir.centene.com/provider-directory` base. The importer records that
base as replaced by the official Centene Partner Portal Provider Directory base
`https://iopc-pd.api.centene.com/iopc/pd/fhir/providerdirectory`, while keeping
resource import probe-gated because live requests from non-allowlisted networks
can return CloudFront HTTP 403. The official Centene documentation also names
`https://iopc-provider.api.centene.com/iopc/provider/ca/` for California
provider details; it is retained as metadata for follow-up parsing rather than
used as the primary normalized base.
Aetna catalog rows from older public snapshots can point at stale portal or
patient-access bases. The importer normalizes those Medicaid Provider Directory
rows to `https://apif1.aetna.com/fhir/v1/providerdirectory`, then adds a
separate supplemental source for Commercial/Medicare data at
`https://apif1.aetna.com/fhir/v1/providerdirectorydata`. Both bases share the
same secret-backed Aetna OAuth2 credential fallback. When both exact API-base
rules are configured, the exact `providerdirectory` or `providerdirectorydata`
rule wins without merging the sibling path's rule. Resource credentials are
forwarded only below the selected base path, not to every path on
`apif1.aetna.com`.

The Commercial/Medicare source supports the seven Plan-Net resource types other
than `Endpoint`; the live `Endpoint` collection returns 404. Each paged search
starts as one unfiltered stream with `_count=30`. State and `name=aetna`
partitions are not used because they omit blank, non-US, or multi-state
resources. Aetna can return an HTTP-200 `OperationOutcome` above that page size,
so any HTTP-200 `OperationOutcome` is treated as a resource-search error.
Continuation `_page_token` links must remain on the same origin and resource
path, carry only the bounded count and one non-empty token, and cannot replay a
previous token. Commercial paging remains checkpoint-capable, but unbounded
acquisition can use the durable Bulk Data path described below. This is
important because the audited endpoint exposed about 1,414 `InsurancePlan`
rows and roughly 380 million `PractitionerRole` rows.

The Medicaid source supports six resources, excluding `HealthcareService` and
`Endpoint`. Its metadata declares
`provider_directory_coverage_mode=targeted` and an empty
`provider_directory_fully_enumerable_resources` list. A generic collection
search is stopped before the request with
`aetna_medicaid_targeted_query_required_npi_or_name_and_location`; an explicit
NPI search or name-plus-location search can still write source/canonical
evidence. Targeted evidence is never eligible to replace the current full
endpoint dataset.
Some retest rows have no concrete API base and no currently confirmed open FHIR
endpoint. Supplemental catalog discovery adds explicit `catalog_blocked` rows
for those cases when there is primary evidence that the endpoint is not
currently importable, for example Puerto Rico Medicaid FFS in the CMS SMA
Endpoint Directory or portal-only/token-gated Provider Directory pages. These
rows have no `api_base`, carry `metadata_json.provider_directory_blocked=true`,
and are skipped by resource import. They let the audit distinguish known
non-importable coverage blockers from unresolved endpoint-discovery gaps.

## Tables

- `provider_directory_api_endpoint`
- `provider_directory_endpoint_dataset`
- `provider_directory_dataset_resource`
- `provider_directory_bulk_acquisition_checkpoint`
- `provider_directory_bulk_output_checkpoint`
- `provider_directory_source`
- `provider_directory_capability`
- `provider_directory_insurance_plan`
- `provider_directory_practitioner`
- `provider_directory_organization`
- `provider_directory_location`
- `provider_directory_practitioner_role`
- `provider_directory_healthcare_service`
- `provider_directory_organization_affiliation`
- `provider_directory_endpoint`

### Endpoint dataset publication

`provider_directory_source` remains the plan/source alias catalog, but each
row with an importable API base also points to a deterministic
`provider_directory_api_endpoint`. The endpoint identity is the same identity
used to group resource acquisition: canonical API base, non-secret credential
descriptor, and the complete resource-endpoint signature. Endpoint rows are
upserted before their source aliases, so every alias in one acquisition group
has the same `endpoint_id` without making plan names part of transport
identity.

Each endpoint acquisition writes to a non-current
`provider_directory_endpoint_dataset` candidate. Normalized resources are
stored once per `(dataset_id, resource_type, resource_id)` in
`provider_directory_dataset_resource`; the existing source-specific resource
tables and `provider_directory_source_resource` edges are still written for
compatibility. One deterministic compatibility alias owns those physical rows:
the lexicographically smallest `source_id` in the endpoint/resource import
group. Typed rows, source-resource edges, seen rows, linked-resource rows, and
stale cleanup are restricted to that owner. Catalog, probe, endpoint identity,
and import-diagnostic metadata still update every logical alias. Counts report
remote rows once rather than multiplying them by the alias count. Existing
historical alias copies are intentionally not deleted by this change; their
cleanup is a separate migration.

Checkpointed acquisitions retain the candidate `dataset_id`. Candidate identity
uses `provider_directory_pagination_root_run_id` when import-control supplies
it, so pagination and partition/reverse-lookup retries across multiple hops
continue the same non-current dataset. Partition checkpoints are scoped to that
root lineage and a stable search-strategy version; changing strategy cannot
reuse cursors from the prior strategy. An unrelated fresh run cannot skip
completed partitions into a new empty candidate. Aetna Commercial and UHC are
included in checkpoint-capable source policy. An incomplete UHC partition scan
emits the same required-resume signal as an incomplete paginated scan, even
though its partition state lives in
`provider_directory_reverse_lookup_checkpoint`.

REST pagination checkpoints are keyed by canonical API base, resource type,
source scope, and acquisition root. A direct retry may adopt only its immediate
predecessor's row within the same root and candidate; a guarded fresh root gets
an independent row while failed-root evidence remains available for audit.
Failed-root checkpoint retention is intentionally conservative: cleanup must
first prove that the endpoint candidate is neither current nor
active/incomplete and that no import-control run in the root lineage is live.

Bulk acquisition checkpoints use the same endpoint candidate and acquisition
root. Their identity additionally binds the canonical API base, resource type,
source scope, Bulk strategy version, endpoint id, candidate `dataset_id`, and
export start request. The acquisition row stores encrypted status and manifest
capabilities, their immutable hashes, a URL-free manifest audit projection,
mutable owner/retry lineage, a lease, row count, lifecycle timestamps, state,
and error. Output rows store an encrypted URL capability and immutable hash plus
attempt, row-count, completion, timestamp, and error state for every selected
manifest output. Encryption derives a domain-separated Fernet key from
`HLTHPRT_PROVIDER_DIRECTORY_CHECKPOINT_KEY`, falling back to an existing
control/import-control token. Decryption also tries the fallback tokens and the
comma-separated or JSON-array
`HLTHPRT_PROVIDER_DIRECTORY_CHECKPOINT_PREVIOUS_KEYS` key ring, allowing a
dedicated key or control token to rotate while an accepted export is active.
Missing/wrong key configuration fails closed but remains retryable and does not
erase encrypted capabilities. Terminal completion or data-level failure clears
capability ciphertext while retaining hashes and audit data. A retry can adopt
an expired/retryable checkpoint only from its direct prior owner through the
same root and candidate identity; a fresh run receives a different checkpoint
and cannot adopt the old snapshot.

The candidate is published only when every selected resource reports a
complete, unbounded, error-free scan with no next page remaining. Publication
locks the endpoint identity and atomically marks the prior current dataset
superseded before making the candidate current. Bounded, failed, interrupted,
or still-resumable candidates remain non-current, leaving the prior current
dataset unchanged. The publication hash and row count are computed in
`(resource_type, resource_id)` order with an incremental SHA-256 over bounded
database batches, so promotion does not materialize a multi-million-row
dataset in worker memory.

Endpoint dataset selection first applies
`metadata_json.provider_directory_supported_resources`, then intersects an
explicit `provider_directory_fully_enumerable_resources` list when present.
An explicit empty list therefore allows typed/source/canonical evidence writes
without creating or promoting a full/current endpoint dataset. This is the
guard used for Aetna Medicaid's targeted coverage mode.

Provider, plan, network, and location references are retained as raw FHIR
references first. Address canonical linkage can be filled later through
`provider_directory_location.address_key`. Network references are resolved to
FHIR `Organization` names during PTG corroboration so served PTG
`network_names` can be matched without denormalizing network names into
PTG price rows. Raw Provider Directory network names are context only; serving
should treat them as network proof only after emitting concrete
`provider_directory_network_matches` objects with both `ptg_network_name` and
`provider_directory_network_name`.

Artifact publishing also stages keyable `Organization.address` rows into
`address_archive_v2`, not only `Location` rows. This lets
`entity-address-unified --refresh-mode provider-directory-partial` carry
Provider Directory organization-address evidence into `address_sources` with a
real `address_key`, so API search can find those providers by address key and
phone once the scoped unified refresh has run.

## Pricing Address Corroboration

Provider Directory FHIR can upgrade unified addresses from NPPES-only inference
to payer-directory corroboration. The helper
`provider_directory_address_corroboration_sql()` builds the query shape for
`provider_directory_address_corroboration`, which matches:

- `entity_address_unified.npi` / `inferred_npi` to `provider_directory_practitioner.npi` or
  `provider_directory_organization.npi`;
- FHIR roles/affiliations to referenced FHIR locations, including locations
  reached indirectly through referenced `HealthcareService.location` resources;
  and
- FHIR `provider_directory_location.address_key` to unified
  `entity_address_unified.address_key`.

The importer can publish `provider_directory_address_corroboration` as an
unlogged table during artifact publishing when `publish_corroboration=true`,
`--publish-corroboration`, or `HLTHPRT_PROVIDER_DIRECTORY_PUBLISH_CORROBORATION=1`
is set. This pays the expensive FHIR/unified-address join once after a full
import, adds lookup indexes for serving and audit checks, and keeps API-side PTG
address overlay fast. The disposable smoke helper can still build the same
relation as a view for small fixture schemas, but normal dev and scheduled
imports leave this disabled unless a PTG corroboration refresh is explicitly
needed.

Standalone/manual corroboration publishes refresh
`provider_directory_network_catalog` before building the corroboration table.
The monthly artifact chain already publishes the catalog as its own step, then
builds corroboration from that fresh catalog without running the catalog publish
twice.

Rows in this relation are Provider Directory evidence, not direct TiC location
evidence. Address-only rows remain public
`address_verification.address_network_binding=inferred_from_provider_identity`
with `address_verification.address_evidence_level=provider_directory_address`,
because they prove the provider/address relationship but not the PTG
plan/network relationship. Serving promotes rows to
`address_verification.address_network_binding=payer_directory_corroborated_location`
with `address_verification.address_evidence_level=payer_directory_network_location`
only when either the FHIR `InsurancePlan` identifier matches the PTG plan or
the served PTG `network_names` strictly match a referenced FHIR network
`Organization` name/alias. A served network-name match must be exposed as
`provider_directory_network_name_matched=true` with
`address_verification_evidence.matched_on` ending in `_network_name` and
concrete network-name match details. The database view's
`provider_directory_network_matches` column stores raw referenced FHIR network
organizations (`name`, `aliases`, `resource_id`, `ref`). The public API
`address_verification.provider_directory_network_matches` field is stricter:
serving emits only reconstructed PTG-to-directory matches that contain both
`ptg_network_name` and `provider_directory_network_name`.

## Usage

Seed the source catalog without network probes:

```bash
python main.py start provider-directory-fhir --seed-db-path /tmp/provider_directory.db --seed-only --no-probe
```

Seed from the SQLite catalog plus a pinned retest snapshot:

```bash
python main.py start provider-directory-fhir \
  --seed-db-path /tmp/provider_directory.db \
  --retest-results-path /tmp/retest_results.json \
  --seed-only \
  --no-probe
```

Probe every seed source and import full open-access resource pages:

```bash
python main.py start provider-directory-fhir \
  --seed-db-path /tmp/provider_directory.db \
  --import-resources \
  --full-refresh \
  --resource-limit 0 \
  --page-limit 0 \
  --page-count 100 \
  --stream-batch-size 5000 \
  --bulk-export \
  --source-concurrency 4 \
  --publish-artifacts \
  --stale-cleanup \
  --concurrency 12 \
  --timeout 10
```

Use `--source-query` and `--limit` for small payer-specific smoke runs. Resource
fetches default to sources marked open/none in the seed plus sources whose
unauthenticated metadata probe returned a valid FHIR CapabilityStatement. This
keeps stale or overly conservative seed `auth_type` values from hiding a
directory that is actually readable. Pass `--include-credentialed` or
`--include-auth-required` only for controlled tests.
An explicit `source_ids` resource-import request fails with
`provider_directory_requested_sources_not_selected_for_resource_import` when
validation and policy select none of those sources. The diagnostic includes
the requested ids and non-zero selection skip counts. Broad catalog runs retain
their existing skip-and-continue behavior; there is no implicit allow-empty
exception for an explicitly requested source import.
`--full-refresh` makes omitted `resource_limit` and `page_limit` default to `0`,
where `0` means unbounded. The importer still has a hard pagination loop guard
from `HLTHPRT_PROVIDER_DIRECTORY_MAX_FULL_PAGES` (default `10000`) so a broken
endpoint cannot run forever. `stream_batch_size` flushes long resource scans to
Postgres in batches so a large payer endpoint does not need to retain every row
in worker memory before the first upsert. `source_concurrency` controls how
many source resource imports can run at once; keep it low for full refreshes so
slow public FHIR servers are not overloaded. When multiple seed rows point to
the same canonical FHIR base with the same credential mode, the importer fetches
that directory once and fans out persisted rows under each source id, preserving
catalog/source provenance without doubling upstream API traffic. When
role/affiliation rows reference resources outside the fetched page slice,
`linked_resource_limit` bounds optional direct FHIR reads for those referenced
Practitioners, Organizations, Locations, HealthcareServices, InsurancePlans, and
Endpoints. A value of `0` disables that linked fallback; full refresh normally
relies on the direct resource collections. `linked_resource_deadline_seconds`
optionally bounds the linked fallback per source so a slow remote endpoint does
not hold the whole import chain after the direct resource collections have been
captured.

If resource rows were imported before canonical resource storage was enabled,
populate the canonical resource and source-edge tables from existing
source-level rows without refetching payer FHIR endpoints:

```bash
python main.py start provider-directory-fhir --canonical-backfill-only
```

Use `--resources Location,Practitioner` to backfill only selected resource
types during controlled validation. This backfill is additive/idempotent: it
upserts canonical rows keyed by `(canonical_api_base, resource_type,
resource_id)` and source edges keyed by `(source_id, resource_type,
resource_id)`.

Large resource batches use PostgreSQL `COPY` into temporary staging tables
before merging into the Provider Directory tables. Disable that path with
`HLTHPRT_PROVIDER_DIRECTORY_COPY_UPSERT=0` only for debugging. Tune the minimum
batch size with `HLTHPRT_PROVIDER_DIRECTORY_COPY_UPSERT_MIN_ROWS` (default
`100`). If the active driver cannot use `copy_records_to_table`, the importer
logs a short fallback message and continues with the values-based upsert path.

`--bulk-export` enables a safe FHIR Bulk Data fast path for resource reads. For
each selected resource type, the importer first tries `[base]/$export?_type=...`
with `Prefer: respond-async`, polls the returned status URL, streams matching
NDJSON output files, and writes rows through the same parser/COPY upsert path as
normal resource pages. If the payer returns a normal unsupported status such as
400, 404, 405, or 501, or returns a synchronous non-Bulk FHIR payload, the
importer falls back to the existing paginated FHIR search. Export attempts that
are accepted but later fail are recorded in the per-source resource diagnostics
as `fetch_mode=bulk_export` errors.
The Aetna Commercial/Medicare base is a known variant of this path: its
`$export` operation is accepted with `_type=...` and `Prefer: respond-async`,
but rejects the optional `_outputFormat` query parameter. That exception is
handled by source metadata so normal Bulk Data servers keep the standards-shaped
request while Aetna gets the gateway-compatible request. Row-bounded Aetna
validation keeps the compatibility Bulk path. Unbounded Aetna Bulk is enabled
only when the acquisition-only checkpoint mode has a run id, root lineage,
endpoint id, non-current candidate `dataset_id`, streaming writes, no resource
or page limit, `stale_cleanup=false`, and `publish_artifacts=false`. Otherwise
the importer keeps the existing paged behavior. Import metrics retain the compatibility
`bulk_export` flag and add `bulk_export_mode.requested`, `.effective`, and
`.effective_resource_fetches`, so a globally requested fast path is not
mistaken for the mode actually used by a source.

Before the export request, the importer reserves the root/candidate checkpoint.
Once a Bulk request returns HTTP 202, the accepted status URL is persisted
before polling. If a worker disappears while acceptance is in flight and leaves
an unresolved reservation, a retry fails closed rather than issuing a second
export with an unknown first-job outcome. A retry with a stored status URL polls
that same job until a manifest is accepted, or it uses the stored manifest
directly. The canonical manifest includes
`transactionTime`, `request`, `requiresAccessToken`, and the complete sorted
output identity. A changed manifest, unsafe status/output URL, expired status
job, or expired output fails closed. The importer never starts a replacement
export inside the same candidate after acceptance, and the prior current
endpoint dataset remains untouched.

Each checkpointed resource also holds a PostgreSQL session advisory lock for
the complete request/poll/stream lifecycle. A competing retry fails retryably
before checkpoint adoption while that worker is alive. Before every streamed
database batch and before output/checkpoint completion, the worker checks the
same guard connection and persisted owner. Losing either fails before the next
write, closing the lease-expiry window between a row batch and its progress
update.

Each completed NDJSON output is skipped on retry. If a worker stops while one
output is streaming, that output is requested again from byte/line zero and
rewritten through idempotent resource upserts; this is output-level restart,
not byte-level resume. Batch progress persists rows written during the attempt,
and the output is marked complete only after the final batch succeeds. Endpoint
dataset promotion remains blocked until every output for every selected
resource is complete and every resource diagnostic is unbounded and error-free.

Bulk start and status requests must be HTTPS and remain on the payer origin.
Payer credentials are resolved only for the payer request URL. Manifest outputs
must be absolute HTTPS URLs without userinfo or fragments; IP-literal hosts are
rejected before `aiohttp` can bypass DNS resolution. Same-base outputs can
receive payer credentials only when `requiresAccessToken=true`; each external
host must exactly match `provider_directory_bulk_export_output_hosts` source
metadata and is downloaded without auth. Aetna currently allows only the
observed `storage.googleapis.com` host. External output that claims it requires
the payer access token is rejected. A custom resolver rejects non-global DNS
answers on every connection, and redirects are not followed, preventing DNS
rebinding or moving credentials/snapshot identity to a URL not present in the
accepted manifest. Status and output capability paths/queries are never logged;
operational logs retain only the host and a short SHA-256 identity.

Bulk `transactionTime` values must be timezone-aware, no more than 15 minutes
in the future, and no older than 45 days by default. The bounds are configurable
with `HLTHPRT_PROVIDER_DIRECTORY_BULK_MANIFEST_FUTURE_SKEW_SECONDS` and
`HLTHPRT_PROVIDER_DIRECTORY_BULK_MANIFEST_MAX_AGE_SECONDS`. They are checked
when accepting the manifest and again before endpoint-dataset promotion, so an
old snapshot cannot replace a paged/pre-migration current dataset that lacks
Bulk lineage, and an excessive future timestamp cannot poison later freshness
comparisons.

Full-refresh stale cleanup also uses an append-only unlogged seen stage by
default (`HLTHPRT_PROVIDER_DIRECTORY_SEEN_STAGE=1`). During fetch, resource ids
are copied into the run stage without a primary-key/index maintenance cost. When
all source groups finish, the importer builds one lookup index, deletes stale
rows for completed source/resource scans, and drops the stage table. Set
`HLTHPRT_PROVIDER_DIRECTORY_SEEN_STAGE=0` to fall back to the persistent keyed
`provider_directory_import_seen` table for debugging.

Stale cleanup is conservative. The importer deletes rows missing from the
current run only for a source/resource pair whose pagination completed without
an HTTP error, row cap, page cap, hard full-page cap, or loop detection. Bounded
sample scans can upsert fresh rows but cannot delete rows for incomplete
resource scans.

Artifact publishing is meant for full default-resource refreshes. It stamps
`provider_directory_location.address_key`, publishes Provider Directory location
rows into the shared address archive, and can optionally republish the
table-backed PTG corroboration relation used by serving/search. The CLI defaults
`--publish-artifacts` on only when the selected resources equal the full
Provider Directory resource set. PTG corroboration publication defaults off
because it is a global join; use `--publish-corroboration` only when refreshing
that optional relation is part of the run.
Small payer or single-resource smoke runs should pass `--no-publish-artifacts`
or `publish_artifacts=false`; otherwise a bounded test can spend most of its
runtime rebuilding global address artifacts that are unrelated to the smoke.

Long-running imports report source-probe, resource-import, and artifact-publish
progress back to import-control when the running worker image includes progress
callbacks. Older active workers still show generic `process_data running`
progress until they complete.

The import-control default schedule plan runs the Provider Directory chain
monthly: full `provider-directory-fhir` resource import, latest-run
Provider Directory partial `entity-address-unified` refresh, then the optional
table-backed PTG corroboration publish once the unified-address dependency is
fresh. This is the production path for continuous reimports; ad hoc smoke runs
should avoid publishing global artifacts unless they intentionally refresh the
shared serving evidence.

The latest-run Provider Directory partial `entity-address-unified` refresh must
not patch the live API serving table in place. It materializes the affected FHIR
address groups, copies unaffected live `entity_address_unified` rows into a
replacement heap, appends the rebuilt affected rows, indexes that replacement,
and publishes through the same short table-rename swap used by full refreshes.
Live `DELETE FROM entity_address_unified ...` patch publishing is intentionally
disabled because those deletes can lock the serving table and block API reads.

## Credentialed API Access

CMS requires these Provider Directory APIs to be public at the user-data level,
but payer API servers can still require app registration or API keys. The
importer supports secret-backed credentials without storing secret values in the
database. Configure either:

```text
HLTHPRT_PROVIDER_DIRECTORY_CREDENTIALS_JSON
HLTHPRT_PROVIDER_DIRECTORY_CREDENTIALS_FILE
```

`credential_config_file` can also be passed as an import parameter or CLI
`--credential-config-file` for a single run. This is intended for secret-mounted
files, not inline secret values. In dev Kubernetes the worker config points
`HLTHPRT_PROVIDER_DIRECTORY_CREDENTIALS_FILE` at:

```text
/var/run/healthporta/provider-directory/credentials.json
```

Create or update the optional `provider-directory-credentials` secret with a
`credentials.json` key to enable credentialed scheduled imports without
changing the schedule payload.

The JSON shape is:

```json
{
  "hosts": {
    "api.payer.example": {
      "bearer_token": "env:PAYER_DIRECTORY_BEARER_TOKEN",
      "headers": {
        "X-API-Key": "env:PAYER_DIRECTORY_API_KEY"
      },
      "query_params": {
        "client_id": "env:PAYER_DIRECTORY_CLIENT_ID"
      },
      "oauth2": {
        "token_url": "env:PAYER_DIRECTORY_TOKEN_URL",
        "client_id": "env:PAYER_DIRECTORY_CLIENT_ID",
        "client_secret": "env:PAYER_DIRECTORY_CLIENT_SECRET",
        "scope": "system/*.read",
        "auth": "basic"
      }
    }
  },
  "sources": {
    "pdfhir_source_id": {
      "api_key": {
        "header": "Ocp-Apim-Subscription-Key",
        "value": "env:PAYER_DIRECTORY_SUBSCRIPTION_KEY"
      }
    }
  }
}
```

Credential entries can be matched by `sources`, `api_bases`, `hosts`, or
`org_names`; more specific entries override defaults. Values prefixed with
`env:` are resolved from environment variables. Request credentials are only
applied to the source host, so absolute linked FHIR references on another host
do not receive payer-specific headers or query parameters. Probe metadata stores
only non-secret descriptors such as matched rule and header/query parameter
names.

Import run metrics include non-secret source-selection counters:
`source_import_sources_considered`, `source_import_sources_selected`,
`source_import_skipped_probe_not_valid`, `source_import_skipped_open_only`,
`source_import_skipped_validation_status`,
`source_import_skipped_auth_required_policy`,
`source_import_sources_selected_declared_credentialed`, and
`source_import_sources_selected_auth_required_seed`. After installing a
credential secret, these counters show whether credentialed sources moved from
probe/auth skips into selected resource imports.

The coverage audit reads the same credential config environment/file and reports
how many auth/onboarding-gated sources match a configured credential rule. It
does not print secret values, header values, query parameter values, OAuth client
secrets, or resolved tokens. Treat the `missing config` count as the credential
onboarding backlog that still blocks full Provider Directory coverage. The
`configured` count means a source matched a credential rule; the
`credential secret readiness` line separately reports how many matched rules
have all referenced `env:` variables present in the running environment.
Use `--format credential-backlog-json` to export a smaller non-secret onboarding
artifact with host/auth groups, sample payer names, source-id/API-base samples,
market impact counts for Medicare Advantage, Medicaid MCO, CHIP, and QHP
sources, and placeholder credential-rule templates.
Use `--format credential-api-bases-json` to export the complete non-secret list
of blocked API-base targets, grouped with source counts, market counts, sample
payers/source ids, and path-scoped credential-rule templates.
Use `--format credential-priority-json` to aggregate those groups by host and
rank credential onboarding targets by missing-config impact, then regulated
market coverage.
Use `--format credential-config-template-json` to generate a host-level
`HLTHPRT_PROVIDER_DIRECTORY_CREDENTIALS_JSON` starting point from the same
backlog. The template intentionally uses `env:` placeholders and `_notes`;
confirm each payer portal's token URL, scope, API-key header, and registration
terms before enabling a rule.
When a host-level rule needs review, the template also includes sample
`api_bases` credential rules for the sampled FHIR bases in that backlog group.
Use those as the safer pattern for path-scoped onboarding; increase
`--sample-limit` for broader host-template samples, or use
`--format credential-api-bases-json` when you need every blocked API-base target
from a high-volume host. Markdown reports display only the first 50
credential/onboarding groups; the JSON exports are the machine-readable source
of truth for complete onboarding. The JSON exports include `api_base_count`,
`sample_api_base_count`, and `api_base_sample_complete` so operators can see
whether the generated path-scoped rules cover every known base in that group or
only the requested sample.
Use the market counts in the backlog/template summaries to prioritize
credentials that unblock the most regulated-market coverage first.
Template entries can also include `_review` when sampled API bases or source
hosts look like portal/documentation URLs, or when one host has several known or
sampled FHIR path variants. `_review` also flags mixed host-level auth schemes such as one host
having both OAuth2 and API-key groups. In those cases, keep the generated host
rule as a starting point only; prefer `api_bases` or `sources` credential rules
unless the payer portal confirms that the same credentials and auth scheme apply
to every path on that host.
Resolve those review notes before installing the rule so credentials are bound
to the real FHIR base and not only to a public landing page.

Known payer caveat: Aetna publishes an open CapabilityStatement at
`https://apif1.aetna.com/fhir/v1/providerdirectory/metadata`, but resource reads
require a subscribed OAuth client. The importer maps Medicaid developer-portal
seed rows to `https://apif1.aetna.com/fhir/v1/providerdirectory` and records
resource-level `http_401` diagnostics until credentials are configured. Aetna's
advertised token URL is
`https://apif1.aetna.com/fhir/v1/fhirserver_auth/oauth2/token`.
For Provider Directory production client-credentials tokens, Aetna's published
token-generation guide uses scope `Public NonPII` and Basic client
authentication. Once authorized, the Medicaid base still requires NPI or name
plus location targeting. Its metadata intentionally exposes no fully-enumerable
resources, and a generic full-refresh attempt fails closed before issuing the
HTTP-400-producing collection query. This targeted Medicaid contract is
separate from the Commercial/Medicare base's unfiltered collection streams.

ALOHR / Alabama One Health Record is mixed-mode. The original seed URL
`https://alohr.esante.us/public/providers` is a public React provider-search app,
not a FHIR REST base. Its real FHIR metadata base is
`https://fhir.alabamaonehealthrecord.com/csp/healthshare/hsods/fhir/r4`, where
`metadata` is open but resource reads are auth-gated. The importer maps ALOHR
seed rows to that FHIR base and uses a source-specific GraphQL connector at
`https://api.esante.us/graphql` with tenant `alohr` to stream public provider
and organization rows into the normal Provider Directory practitioner,
organization, location, role, and affiliation tables.

Cigna seed rows can appear through Availity non-FHIR/onboarding paths even
though Cigna publishes a public R4 Provider Directory at
`https://fhir.cigna.com/ProviderDirectory/v1`. The importer maps Cigna's
Availity `cigna/r4` seed rows to that public FHIR base so clean source refreshes
do not lose the directly importable Cigna endpoint.

Centene seed rows can point at the public partner portal catalog page
`https://partners.centene.com/apis` instead of a FHIR REST base. The portal's
public API catalog advertises `FHIR - Provider Directory` with authentication
`None`, production host `https://iopc-pd.api.centene.com/iopc/pd/`, and path
`/fhir/providerdirectory`. The importer maps those portal rows to
`https://iopc-pd.api.centene.com/iopc/pd/fhir/providerdirectory` and records the
catalog/doc URLs in metadata. Runtime probes remain the authority for whether
that base is reachable from the current environment; CloudFront/WAF 403s are
kept as probe diagnostics instead of treating the portal page as an importable
FHIR endpoint.

AmeriHealth Caritas seed rows can point at stale Availity paths or the stale
`https://fhir.amerihealthcaritas.com/provider-directory/` host. AmeriHealth's
developer portal publishes plan-specific Provider Directory bases such as
`https://api-ext.amerihealthcaritas.com/5400/provider-api`. The importer maps
only deterministic plan-name matches, for example `AmeriHealth Caritas DC`, to
those plan-code bases and replaces stale resource endpoint columns at the same
time. Ambiguous rows that only say `AmeriHealth Caritas` without plan/state
context stay unresolved for source-discovery review instead of guessing the
wrong plan code.
Scheduled `monthly-full` refreshes also enable supplemental catalog discovery.
That path parses AmeriHealth Caritas' official developer portal table directly
and adds each plan-specific `provider-api` base as a source row, so plans listed
there are included even when the upstream SQLite seed only has a stale generic
Availity row. Ad hoc runs can opt in with `include_supplemental_catalogs=true`
or `--include-supplemental-catalogs`.
The supplemental rows mark `https://fhir.amerihealthcaritas.com/provider-directory`
as a stale generic base replaced by the official plan-specific catalog. The
audit can therefore count those generic retest failures as recovered without
guessing which single plan-specific base an ambiguous upstream row should use
for resource import.

Molina seed rows can point at the developer portal
`https://developer.interop.molinahealthcare.com` or stale retest bases such as
`https://fhir.molinahealthcare.com/provider-directory/`. The portal exposes a
working Provider Directory metadata base at
`https://api.interop.molinahealthcare.com/providerdirectory`, so the importer
maps those rows to that concrete base and records the portal and metadata URLs
in source metadata. During discovery, broad live resource searches such as
`Practitioner?_count=1` returned HTTP 500 from Molina's API, so resource import
diagnostics remain the authority until Molina's supported query pattern is
confirmed or the upstream server behavior changes.

UnitedHealthcare seed rows can point at the UHC interoperability landing page
`https://www.uhc.com/legal/interoperability-apis` or stale retest bases such as
`https://fhir.uhc.com/v1/provider-directory/`. UHC publishes Provider Directory
Core metadata and open resource searches at
`https://flex.optum.com/fhirpublic/R4`, so the importer maps those rows to the
concrete FHIR base. Runtime probes still decide whether the base is reachable
from the import environment; connection timeouts stay as probe diagnostics
instead of converting a landing page into a resource import.

SCAN Health Plan seed rows can point at the developer portal
`https://developer.scanhealthplan.com`. The portal's embedded Provider Directory
OpenAPI spec advertises `https://providerdirectory.scanhealthplan.com` as the
FHIR server, and `/metadata` returns an InterSystems FHIR CapabilityStatement.
The importer maps SCAN portal rows to that FHIR base. Broad unfiltered resource
searches can return `SearchTooCostly`, so SCAN resource harvesting may require
source-specific paging/filter tuning even though endpoint discovery is resolved.
The importer partitions SCAN `Practitioner` and `Organization` searches by
two-letter name/family prefixes and `Location` searches by one-letter name
prefixes. `PractitionerRole` is not fetched through the broad paged fallback.
When SCAN roles are requested with practitioners, organizations, or locations,
the importer reverse-lookups roles from those fetched resource ids, such as
`PractitionerRole?practitioner=Practitioner/{id}`, and records
`scan_practitioner_role_requires_reverse_lookup` only when no seed resources are
available for that reverse lookup. The reverse-lookup path uses the same
streaming flush behavior as direct resource reads and honors the configured
`linked_resource_deadline_seconds`; when the deadline is reached, stale cleanup
for SCAN roles is skipped because the resource is incomplete.

## Self-Harness

Run the parser-only harness:

```bash
./venv314/bin/python scripts/research/provider_directory_fhir_harness.py
```

Run the disposable SQL typing harness before deploys that change artifact
publishing SQL. It creates a temporary schema, executes the generated
address-key batch SQL through SQLAlchemy's asyncpg dialect with null keyset
parameters, and drops the schema:

```bash
./venv314/bin/python scripts/research/provider_directory_fhir_harness.py \
  --sql-typing \
  --db-host 127.0.0.1 \
  --db-port 5440 \
  --db-database healthporta_test \
  --db-user nick
```

Run a bounded local DB-backed CLI case:

```bash
HLTHPRT_DB_HOST=127.0.0.1 \
HLTHPRT_DB_PORT=5440 \
HLTHPRT_DB_USER=nick \
HLTHPRT_DB_DATABASE=healthporta \
HLTHPRT_TEST_DATABASE_SUFFIX=_test \
./venv314/bin/python scripts/research/provider_directory_fhir_harness.py \
  --local-cli \
  --seed-db-path /tmp/provider-directory-db/data/provider_directory.db \
  --retest-results-path /tmp/provider-directory-db/data/retest_results.json \
  --limit 10 \
  --import-resources \
  --resources InsurancePlan,Practitioner \
  --include-credentialed \
  --resource-limit 10 \
  --linked-resource-limit 25 \
  --page-limit 1 \
  --no-publish-artifacts
```

Run the scheduled-shape monthly harness against a disposable database when
checking the monthly command surface. `--no-cli-test-mode` intentionally omits
the importer's `--test` flag, while the explicit limits keep the run bounded.
`--coverage-audit` runs the pod-safe coverage audit against the same
`HLTHPRT_DB_*` target after the import:

```bash
createdb -h 127.0.0.1 -p 5440 -U nick hp_pd_harness_monthly

HLTHPRT_DB_HOST=127.0.0.1 \
HLTHPRT_DB_PORT=5440 \
HLTHPRT_DB_USER=nick \
HLTHPRT_DB_DATABASE=hp_pd_harness_monthly \
./venv314/bin/python scripts/research/provider_directory_fhir_harness.py \
  --local-cli \
  --coverage-audit \
  --no-cli-test-mode \
  --refresh-preset monthly-full \
  --include-supplemental-catalogs \
  --import-resources \
  --no-stale-cleanup \
  --no-publish-artifacts \
  --source-query Cigna \
  --limit 5 \
  --resources InsurancePlan,PractitionerRole,Practitioner,Organization,Location \
  --resource-limit 10 \
  --page-limit 1 \
  --page-count 10 \
  --linked-resource-limit 20 \
  --linked-resource-deadline-seconds 30 \
  --stream-batch-size 100 \
  --source-concurrency 1 \
  --retest-results-url https://raw.githubusercontent.com/hltiunn/provider-directory-db/main/data/retest_results.json

dropdb -h 127.0.0.1 -p 5440 -U nick hp_pd_harness_monthly
```

When validating a completed artifact-publishing run rather than a bounded
resource smoke, replace the pod-safe audit with the serving-readiness gate:

```bash
./venv314/bin/python scripts/research/provider_directory_fhir_harness.py \
  --coverage-audit \
  --coverage-audit-full \
  --coverage-audit-require-serving-ready \
  --coverage-audit-fast-serving-readiness \
  --db-host 127.0.0.1 \
  --db-port 5440 \
  --db-database healthporta \
  --db-user nick
```

That command fails when `serving_readiness.status` is not `ready`. Use it only
after the Provider Directory resource import, address/unified refresh, network
catalog publish, and PTG corroboration steps have completed.

Reports are written to:

```text
reports/provider-directory-fhir/run-<timestamp>/report.json
reports/provider-directory-fhir/run-<timestamp>/report.md
```

`report.md` includes a **Coverage Audit Readiness** section for coverage-audit
cases. Use it as the first triage point after a monthly proof run: it lists the
serving-readiness status, failed required checks, and phone row coverage. A
`searchable_phone_overlay` failure means Provider Directory data imported, but
the serving artifacts still cannot support provider phone search.

## Coverage Audit

After a manual or scheduled import, run the coverage audit to verify source
coverage, resource yield, address-key usefulness, PTG overlap, and unresolved
network refs:

```bash
./venv314/bin/python scripts/research/provider_directory_coverage_audit.py \
  --host 127.0.0.1 \
  --port 5440 \
  --database healthporta \
  --schema mrf \
  --retest-results-path /tmp/retest_results.json \
  --format markdown
```

When `--retest-results-path` is present, the audit checks that every
`valid`, `valid_non_fhir`, and `auth_required` retest endpoint is covered by
the current normalized `provider_directory_source` catalog or by an intentional
source override redirect. Missing retest endpoints are emitted as gaps with
importable and credential-gated counts split out.
The audit also groups non-importable retest failures, such as `unreachable` or
`not_found`, into a Retest Source Discovery Backlog. Rows that were normalized
and deduped behind a richer seed source still count as recovered through
`provider_directory_equivalent_api_bases`; unresolved hosts stay visible as
endpoint-discovery work instead of disappearing during dedupe.

## Runtime Contract Preflight

Before building or accepting a dev image for the monthly Provider Directory
chain, run the no-DB runtime contract preflight:

```bash
./venv314/bin/python scripts/smoke/provider_directory_runtime_contract.py
```

It verifies the actual Click CLI exposes `--refresh-preset` and
`--include-supplemental-catalogs`, the coverage audit exposes
`--fast-serving-readiness` and `--require-serving-ready`, the control importer
registry publishes `refresh_preset=monthly-full`, and the monthly preset enables
supplemental catalogs. The JSON form is useful in deployment logs:

```bash
./venv314/bin/python scripts/smoke/provider_directory_runtime_contract.py --format json
```

Every audit report includes `serving_readiness`. This is the post-import gate:
it checks that the source catalog is seeded, FHIR resource rows were imported,
Provider Directory addresses are searchable by address key and phone with
retained FHIR source ids, network catalog rows are published when network refs exist, and PTG
corroboration/network-name overlap is present when the corresponding PTG data
exists. `--pod-safe` intentionally skips unified-address, PTG, and network
sections, so only the source/resource checks are required in that mode. Run a
full audit from the dev host or a dedicated worker before treating a monthly
refresh as serving-ready.

Use `--require-serving-ready` when the audit should fail the surrounding job or
schedule check instead of only reporting readiness:

```bash
./venv314/bin/python scripts/research/provider_directory_coverage_audit.py \
  --host 127.0.0.1 \
  --port 5440 \
  --database healthporta \
  --schema mrf \
  --format markdown \
  --require-serving-ready
```

When the gate runs near an API pod or while imports are active, add
`--fast-serving-readiness`. That mode uses bounded existence probes for
Provider Directory searchable-address readiness instead of exact
`entity_address_unified` aggregate counts, so it proves the serving path is
populated without scanning the whole serving table. Use the exact audit from a
worker/dev host when coverage totals are needed.

In dev Kubernetes, stream a pod-safe source/resource audit into the healthcare
pod so it uses the deployed DB connection secrets without printing them. Keep
the heavier unified-address, PTG, and network-resolution sections disabled in
the API pod; run the full audit from the dev host or a dedicated worker context.

```bash
sudo k3s kubectl -n healthporta-dev exec -i deploy/healthcare-mrf-api -- \
  venv/bin/python - --schema mrf --format markdown \
  --pod-safe \
  --statement-timeout-ms 30000 \
  < scripts/research/provider_directory_coverage_audit.py
```

Generate the non-secret credential onboarding artifact from the same deployed
environment:

```bash
sudo k3s kubectl -n healthporta-dev exec -i deploy/healthcare-mrf-api -- \
  venv/bin/python - --schema mrf --format credential-backlog-json \
  --pod-safe \
  --statement-timeout-ms 30000 \
  < scripts/research/provider_directory_coverage_audit.py \
  > provider-directory-credential-backlog.json
```

Generate a fill-in credential config template from the same backlog:

```bash
sudo k3s kubectl -n healthporta-dev exec -i deploy/healthcare-mrf-api -- \
  venv/bin/python - --schema mrf --format credential-config-template-json \
  --pod-safe \
  --statement-timeout-ms 30000 \
  < scripts/research/provider_directory_coverage_audit.py \
  > provider-directory-credentials.template.json
```

Generate a prioritized host-level onboarding list:

```bash
sudo k3s kubectl -n healthporta-dev exec -i deploy/healthcare-mrf-api -- \
  venv/bin/python - --schema mrf --format credential-priority-json \
  --pod-safe \
  --statement-timeout-ms 30000 \
  < scripts/research/provider_directory_coverage_audit.py \
  > provider-directory-credential-priority.json
```

Validate a candidate credential config file without changing the deployed
worker environment:

```bash
sudo k3s kubectl -n healthporta-dev exec -i deploy/healthcare-mrf-api -- \
  venv/bin/python - --schema mrf --format markdown \
  --pod-safe \
  --credential-config-file /path/in/pod/provider-directory-credentials.candidate.json \
  --statement-timeout-ms 30000 \
  < scripts/research/provider_directory_coverage_audit.py
```

The override is audit-only. When `--credential-config-file` is provided, the
audit evaluates that file by itself and does not merge in live
`HLTHPRT_PROVIDER_DIRECTORY_CREDENTIALS_JSON` or
`HLTHPRT_PROVIDER_DIRECTORY_CREDENTIALS_FILE` values.

The report is intentionally gap-oriented. Treat these as recurring operational
signals:

- auth-required sources need credential/registration handling before they can
  contribute full provider coverage. Sources that declare OAuth/API-key access
  and return non-FHIR onboarding HTML without configured credentials are
  classified here instead of as public `valid_non_fhir` endpoints.
  Known onboarding gateway hosts, such as Availity and Centene partner
  portals, are treated the same way when they return non-FHIR HTML without an
  applied credential.
- `credential config coverage` shows how many auth/onboarding-gated sources
  currently match secret-backed credential rules in the running environment.
  A high `missing config` count means the next coverage work is credential
  onboarding, not parser or source discovery work.
- `credential secret readiness` shows whether the matched credential rules are
  deploy-ready in the current environment. A rule that references missing
  `env:` variables is counted as configured but not secret-ready.
- `probe timeout backlog` groups source metadata probes that exceeded the
  per-source hard deadline by host/auth type. Treat large timeout groups as
  endpoint discovery or resolver tuning work. During ad hoc local/dev probes,
  `HLTHPRT_PROVIDER_DIRECTORY_SOURCE_PROBE_HARD_TIMEOUT` can cap one source's
  total metadata-probe time, while
  `HLTHPRT_PROVIDER_DIRECTORY_PROBE_FLUSH_EVERY` controls how often completed
  source/capability probe rows are persisted. Normal full probes flush
  periodically so one slow host does not hide all completed probe results.
- `valid_non_fhir` should be read together with the audit's
  `non-FHIR credential/gateway responses` line. If nearly all `valid_non_fhir`
  rows are credential/gateway responses, the next coverage work is credential
  onboarding rather than broad source URL discovery.
- `valid_non_fhir` sources should be reviewed as source-catalog cleanup or
  payer-specific endpoint discovery work.
- Provider Directory rows without `address_key` are not usable for canonical
  address search or pricing address corroboration.
- `plan/network context` counts sources with imported `InsurancePlan` rows,
  sources that expose network refs on plans/roles/affiliations, and how many of
  those refs resolve to FHIR network `Organization` names. Use this before PTG
  matching to tell whether missing overlap is caused by missing plan/network
  data or by name normalization/matching.
- unresolved network refs mean the payer exposed a network reference but the
  imported FHIR resources did not resolve it to an `Organization` name/alias.
- PTG rows with Provider Directory network refs but no resolved network-name
  match are address-corroborated only; they are not yet full network-location
  evidence.

## Schedule

`import-control` defines `default-provider-directory-fhir-monthly` at
`20 1 5 * *` America/Chicago. The default parameters probe the full seed
catalog, include the upstream `provider-directory-db` retest snapshot
supplement, include credentialed/auth-required sources when credentials are
configured, run a full resource refresh (`resource_limit=0`,
`resource_deadline_seconds=21600`, `linked_resource_limit=0`,
`linked_resource_deadline_seconds=1800`, `page_limit=0`, `page_count=100`,
`stream_batch_size=5000`, `bulk_export=false`, `source_concurrency=1`). The
acquisition schedule also sets `stale_cleanup=false`,
`publish_artifacts=false`, `publish_after_acquisition=false`, and
`publish_corroboration=false`. This keeps remote acquisition resumable and
separate from the later projection and artifact-publication schedules. The
documented schedule currently leaves `bulk_export=false`; setting it to true on
this acquisition shape enables durable unbounded Aetna Commercial Bulk Data.
The importer will not enable unbounded Aetna Bulk if stale deletion or artifact
publication is folded back into the acquisition run. Paged fallback remains
available before a Bulk job is accepted, and accepted jobs must resume from the
stored status/manifest checkpoint instead of creating a mixed snapshot.
The importer may cap `_count` below the schedule-level `page_count` for a
specific payer/resource when the live FHIR server is known to misbehave at
larger page sizes. Aetna Commercial caps all seven supported collections at 30.
Another confirmed case is Michigan InteropStation
`PractitionerRole`, where `_count>=50` returns an empty Bundle while `_count=25`
returns rows and a next link. Future source-specific caps can also be carried
in source `metadata_json.provider_directory_resource_page_count_caps` without
changing the monthly schedule.
`import-control` also schedules a follow-up artifact-only Provider Directory
run after the monthly `entity-address-unified` Provider Directory partial
projection. That run uses `publish_artifacts_only=true` and
`publish_corroboration=true`, and requires both the full FHIR refresh and the
projection to have succeeded within the 24-hour same-cycle window before
rebuilding the table-backed PTG corroboration relation. This keeps served PTG
network-name and `InsurancePlan` evidence aligned with the latest unified
address rows instead of publishing the expensive corroboration join before the
projection has caught up.
Full-refresh source catalog syncs also prune source rows that disappeared from
the current upstream catalog, but only for unfiltered `full_refresh` +
`stale_cleanup` runs that include the supplemental `retest_results` catalog.
Filtered payer smokes, limited runs, and seed DB-only refreshes never delete
source catalog rows. This keeps credential-gated retest aliases from being
removed just because an optional supplemental catalog was omitted from a manual
refresh. Seed-only catalog refreshes preserve existing live probe state until
the probe phase updates it.
During artifact publishing, Location address-key stamping uses bounded keyset
batches even when scoped through the full-refresh seen-stage table. Location
address archive publishing uses the same seen-stage/current-run scope when
building its temporary stage table, so the monthly run avoids broad
`provider_directory_location` publish scans for unchanged rows.

`import-control` also defines
`default-provider-directory-entity-address-unified-monthly` at `20 2 5 * *`
America/Chicago. It runs `entity-address-unified` with
`refresh_mode=provider-directory-partial`,
`provider_directory_partial_scope=latest-run`,
`serving_only_refresh=true`, and `publish=true` so Provider Directory FHIR rows
become visible in provider/address search without rebuilding the heavier
support/provenance tables on each Provider Directory refresh. The schedule
requires the latest `provider-directory-fhir` success before it can fire, so the
partial projection follows the refreshed source/run scope instead of running
against stale Provider Directory data.

The provider-directory partial refresh is scoped by default
(`provider_directory_partial_scope=latest-run`). It discovers the latest
completed Provider Directory source set from
`provider_directory_source.metadata_json.last_resource_import` and filters the
FHIR practitioner-role, organization-affiliation, and direct Organization-address
paths by source/run before building affected address groups. This keeps the monthly partial patch from
re-running the full Provider Directory relationship graph. For a deliberate
unscoped rebuild, pass `provider_directory_partial_scope=all`, or pass
`provider_directory_run_id` / `provider_directory_source_ids` explicitly for a
known run. Use `provider_directory_source_batch_size` (or
`--provider-directory-source-batch-size` for manual CLI launches) to tune source
batching; `0` disables source batching for deliberate one-off runs.
