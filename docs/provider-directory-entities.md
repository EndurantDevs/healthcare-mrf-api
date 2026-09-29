# Provider directory entity reads

The private Python upstream implements three GET routes beneath
`/api/v1/provider-directory/entities`: `/{kind}`, `/{kind}/{id}`, and
`/{kind}/{id}/relationships`. The gateway remains responsible for access control
and metering. Each request requires `source_id`; list and relationship requests
accept `limit` (1–100, default 25), `generation_id`, and `cursor`. Detail requests
accept only `source_id` and `generation_id`.

The CMS Doctors scope is `kind=medical-groups` and `source_id=cms-doctors`.
The CMS directory scope is described below. Other sources return
serving-unavailable. These routes do not use global readiness as evidence
of source publication.

Serving requires the accepted three-table CMS Doctors family, its tracked
`reference_family_result_generation` row, and exact stable group bindings in
`provider_directory_cms_doctors_group_binding`. All current relation OIDs must
match the ledger. Manual adoption without origin authority is unavailable.
Unpublished candidate bindings are excluded; any accepted nonblank group ID
without a binding makes the entire source unavailable. No joins to FHIR or
inferences from numeric identifiers are made.

Configure `HLTHPRT_PROVIDER_DIRECTORY_CURSOR_KEY` with a random 32-byte secret
encoded as canonical unpadded URL-safe base64 (43 characters). All workers must
share the key. Missing or malformed configuration fails closed. The key also
authenticates opaque generation, release, evidence and relationship keys.
Rotating it invalidates existing generations and cursors. Stable entity UUIDs
come from the source binding and are independent of this key.

Clients pin the returned generation for all subsequent requests. Cursors are
encrypted and authenticated, expire after 15 minutes, and bind the source,
kind, route shape, entity, generation and limit. Altered, expired or stale
pagination returns HTTP 409 and requires a restart. Entity pages seek by UUID.
Relationship pages seek by immutable source row number, then sort the bounded
page by opaque relationship key. Relationship keys are ordered within a page;
they are not seek tokens. No offset scan or whole-result in-memory sort is used.

Medical-group names are source assertions. Conflicting names produce a null
name and conflict status; activity and effective dates remain unknown. Explicit
group-site assertions have unresolved targets until an accepted site identity
link exists. Evidence exposes keyed record references, opaque releases and
observation timestamps; raw source identifiers and payloads are excluded.

Each response uses a read-only repeatable-read transaction, 2-second per-statement
timeout and 250-millisecond lock timeout. Access-share locks prevent a concurrent
source cutover from changing the family during a request. Errors and cancellation
release the transaction. Source binding coverage uses an anti-join against the
accepted group table and its indexed binding key. That check can scan the full
accepted source even for a small page. Full-scale latency must be measured before
activation; if it exceeds the timeout, introduce a generation-bound coverage
receipt rather than weakening completeness. For CMS Doctors, the timeouts bound individual
statements, not the complete HTTP request. Responses are private and uncacheable.
PostgreSQL operator working memory is limited to 4 MiB per operation; this is not
a total process-memory limit.

## Accepted CMS directory resources

`source_id=cms-npd` supports organizations, sites, plans, networks and
practitioner roles from the retained current publication. The existing
source-local selector verifies the compact admission digest. This slice also
requires generic admission, the exact eight resource types, all eight release
members, and agreement between the release's distinct counts and the sealed
content summary. Legacy unsealed rows and validated candidates are unavailable.
Generation keys bind the endpoint, dataset, acquisition root, content hash,
release vector and publication time. A rollback publication invalidates cursors.

Organizations and sites use durable entity bindings and exact release evidence.
Networks additionally require an explicit InsurancePlan network reference to the
same source Organization and matching retained plan evidence. Network evidence
always includes an InsurancePlan witness. Names, activity and date-only effective
periods are allowlisted; arbitrary resource JSON, identifiers, tax values and
contact details are never returned.

CMS semantic-content rows have no acquired raw-resource hash. Their raw entity
and plan evidence is tied to the sealed release, exact source and resource ID;
when a retained row does carry a raw hash, it must also match the evidence hash.

Plans and practitioner roles use `provider_directory_resource_identity`. Its
UUIDv5 identity names the exact source, resource type and FHIR ID using the fixed
namespace in `process/provider_directory_resource_identity.py`. UUIDs do not
change across releases or cursor-key rotation. The table has immutable bindings
and an index for source/type/UUID seeks. Applying its additive migration alone
does not populate identities or authorize a publication.

Admission integration must call `bind_resource_identity_batch` for each of
`InsurancePlan` and `PractitionerRole`, passing at most 100 exact resource IDs
from fully validated input. Commit each batch separately before cutover, then
verify complete distinct-ID coverage. Duplicate conflicting payloads must be
rejected by acquisition/admission before this identity-only helper is called.
Cancellation may leave idempotent unpublished bindings, which serving excludes.
The helper never commits, publishes, changes an existing identity or grants
release acceptance. Existing accepted datasets need the same validated bounded
backfill. Missing identity coverage returns 503.

Direct relationships expose only explicit source assertions: organization
part-of, site managing organization, plan owner/administrator/network/coverage
area, and role organization/site/network/plan. Network-to-plan relationships use
the exact plan witnesses. Only relative references resolving inside the same
accepted source are linked. External, missing and unbound network-role
references remain unresolved; a missing required durable binding fails closed.
No shared names, numeric IDs, tax values or addresses infer a relationship.
Practitioner, Endpoint and HealthcareService targets have no entity kind in this
contract and are not converted into another kind. Reverse affiliation expansion,
clinician/profile enrichment and search integration are outside this slice.

## Reviewed existing payers

Payer reads require the reviewed `provider_directory_mrf_payer_binding` and
immutable review-decision dependencies, plus an existing `mrf_payer` row. The
active approval must match the exact accepted Organization's release evidence
hash; a retained raw hash must also agree when present. Closed approvals and
changed source facts are excluded. Unreviewed CMS pay actors remain
Organizations. No payer is created, and no payer ownership of plans or networks
is inferred.

The gateway payer kind accepts its existing opaque ID (`[A-Za-z0-9_-]{1,64}`);
all other kinds retain UUID IDs. Payer labels come from accepted CMS assertions,
with conflicting labels reported explicitly. Payer-to-organization relationships
mean only the reviewed source binding. Payer generations additionally bind the
count of immutable source review decisions so approvals and closures invalidate
pagination. The review workflow must keep decisions and active pointers atomic.
The gateway's opaque payer-ID contract must be integrated before activation.

## Bounds and activation dependencies

CMS reads have a two-second budget for the entire database read, in addition to
the statement, lock and work-memory limits above. Entities seek indexed UUIDs
(or binary-ordered existing payer IDs). Direct references seek their retained
ordinal; network-plan and payer-organization pages seek target UUIDs. Only a
bounded page is projected into Python. Source evidence is HMAC-keyed and cursor
seek positions are encrypted. Missing schema dependencies return sanitized 503s.

CMS directory reads require an exact row in
`provider_directory_cms_serving_coverage` for the selected published dataset,
release vector, dataset hash and publication time. Without it, every CMS entity
kind returns 503. `build_cms_coverage` performs the existing five-kind binding
checks and reverse plan/network witness check once after publication, outside
the bounded cutover transaction. A per-release lock serializes network witness
writes while it builds the receipt; unrelated source writes can continue. A new
publication has no matching receipt and remains unavailable until its own proof
is built. Database guards keep published CMS
resource rows and CMS binding/evidence/network witness facts immutable; plan and
role identities already have their own immutability guard. The builder must run
after all identity and evidence backfill, and before enabling serving for that
generation. The acquisition/admission caller does not yet invoke it automatically.

The receipt makes completeness a primary-key lookup per request. Payer review
counts and aggregation, page seeks, and per-target relationship resolution still
need representative full-scale measurements against the two-second request
budget. The receipt builder itself scans the release and may take longer; run it
away from the short publication cutover.

The focused PostgreSQL fixtures exercise the real admission digest SQL and new
identity migration with synthetic dependency tables. They do not prove a full
CMS acquisition, the integrated migration chain, or production-scale latency.
Integrate the organization, network, admission and reviewed payer dependencies,
wire the identity binder and post-publication coverage builder, and run a
combined accepted-publication read proof before activation.
