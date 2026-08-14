# Provider-directory and formulary source acceptance

This ledger records the decisions and binary release gates for combining the
official provider files, public drug files, and the separate Flex FHIR
enrichment surface. It is part of the implementation contract so a context
reset cannot silently broaden or weaken the result.

## Fixed decisions

- The official IFP and Community & State provider-file dataset remains the
  authoritative exhaustive UHC Provider Directory source.
- The generic Flex source remains `probe_only`. Its capped broad collections
  are not publishable as endpoint-complete.
- Flex enrichment uses a distinct exact-cohort connector. Completion means
  every NPI in one immutable official Practitioner cohort has a terminal exact
  query result; it does not mean the Flex Practitioner collection was crawled.
- Official-file and Flex evidence retain separate source/dataset/resource
  provenance but share one authority identity. They must count as one
  independent payer source in Profile evidence.
- Drug identity is derived from the already-retained IFP and Community & State
  listing bytes. The adapter must not fetch either listing again.
- The advertised drug catalog remains exactly 24 IFP plus 24 Community & State
  identities. Each selected artifact must pass the unchanged transport, byte,
  and full JSON-array validation. An invalid, rejected, or temporarily
  unavailable source artifact is excluded from that run; it is never parsed or
  admitted. Local processing, configuration, claim/cancellation failure, or an
  empty validated selection still blocks admission. One
  normalized source URL cannot identify two files or appear in both families.
- One durable source-scoped acquisition claim covers the retained listing
  snapshot and every drug-file HTTP request. Its monotonic generation, random
  token, expiry, heartbeat, and fenced release admit only one live claimant to
  start new HTTP requests; the token is rechecked before every request.
  Cancellation drains in-flight work, and a stale or reclaimed owner cannot
  CAS-bind an artifact. A remote request already in flight at lease loss cannot
  be un-issued. A live owner returns a bounded busy error.
- Drug bytes use the existing content-addressed retained store. Independent
  normalization roots and retries reopen those bytes rather than redownload or
  duplicate them. A per-file failure does not cancel successful siblings: the
  bounded task set drains and verified artifacts are retained. Source rejection
  or temporary unavailability is represented by the run's exact private
  selection and public aggregate coverage; internal or local failure still
  aborts. The next attempt requests only identities that remain unresolved.
- Drug records publish only through the immutable `formulary_fhir` repository.
  The adapter must not invoke the legacy formulary importer or write legacy
  plan-drug relations.
- Admission and publication are separate default-off operations. Publication
  accepts only a durable receipt and revalidates retained bytes immediately
  before pointer movement.

## Binary implementation gates

The drug slice is acceptable only when all of the following are true:

1. The retained observation proves exactly 24+24 drug identities with 48
   distinct normalized source URLs.
2. Every selected retained file passes the real-source schema and
   resource-bound census; excluded identities contribute no bytes or records.
3. One fenced acquisition generation covers catalog snapshot and HTTP work.
   Acquisition, retry, and both roots use the same exact nonempty selected
   artifact set; failed siblings drain and retry performs zero network requests
   for already verified files.
4. Both repository roots match and PostgreSQL records one exact admission.
5. A durable receipt binds the complete listing observation, private selected
   artifact IDs, aggregate coverage, artifact set, spool proof, repository
   graph, and source configuration.
6. A fresh process can publish from the receipt alone; exact replay keeps one
   generation and the original publication timestamp.
7. Missing or corrupt retained bytes, source drift, admission drift, or pointer
   drift cause zero pointer movement.
8. Legacy plan-drug relations remain absent.

The Flex slice is acceptable only when all of the following are true:

1. The seed cohort binds the exact current official dataset, canonical content
   proof, Practitioner count, and bidirectionally equal member rows.
2. Every cohort NPI has exactly one terminal matched or unmatched query result;
   failed or missing results block completion.
3. Requests are exact identifier searches. Broad collection scans, inferred
   offsets, comma-OR batches, and the generic probe importer are not used.
4. Every admitted resource is a Practitioner carrying only the requested
   canonical NPI. A resource missing it, mixing it with another NPI, or carrying
   malformed same-system identifiers is quarantined before ID and duplicate
   processing. Invalid sibling entries and conflicting duplicate IDs are
   quarantined while unrelated clean rows continue; pagination, cap overflow,
   or invalid bundle structure still fail closed. If every resource is
   quarantined, the query records `unmatched` and retains no payload; twin proof
   intentionally compares admitted semantic output and does not distinguish
   that result from an empty searchset.
5. Two fresh complete cohort acquisitions produce the same immutable semantic
   dataset proof before any publication decision.
6. Dataset metadata states `cohort_complete=true`,
   `endpoint_collection_complete=false`, and `endpoint_complete=false`.
7. Profile delta processing covers the new cohort plus NPIs from the previous
   Flex dataset so removals are reflected.
8. Official and Flex evidence remain separately attributable while their
   independent-authority count remains one.

The rooted graph extension is acceptable only when all of the following are
true:

1. Its dormant source and endpoint registration match the reviewed connector
   signature and authority, and the root is exactly one locked, ready legacy
   or rooted Practitioner dataset. Dual, missing, or unrelated current
   datasets fail closed.
2. Baseline and candidate acquisitions use separate run identities and
   sessions but bind the same immutable root, query contract, endpoint
   signature, authority, and aggregate resource budgets.
3. PractitionerRole and OrganizationAffiliation are enumerated only by the
   reviewed exact parent searches. Referenced resources use exact direct
   reads, while InsurancePlan uses one finite, bounded census whose complete
   payload remains an admission witness. Any Reference-shaped object outside
   the reviewed structural field paths blocks the acquisition.
4. Pagination follows only bounded same-origin opaque links. Page, resource,
   byte, work-item, resource-row, edge-row, payload-byte, retry, lease, and
   per-root time limits are identity-bound and durably enforced.
5. A terminal root proves the complete Practitioner seed, completed direct
   and affiliation frontiers, the finite plan census, the locally intersected
   rooted plan set, zero errors, and a fixed point with no undiscovered work.
   A direct-read 404 or 410 is complete only with a bounded FHIR
   OperationOutcome witness.
6. Two sealed roots must match every terminal, resource, edge, census, and
   aggregate-budget proof before an immutable publication admission exists.
7. Publication atomically combines the exact root Practitioner rows with the
   admitted seven graph families, excludes census-only plans, rejects payload
   conflicts, materializes exact dataset-scoped relationships, and moves one
   logical current pointer across the legacy and rooted variants.
8. Profile selection admits exactly one ready variant and reads graph facts
   only from that selected dataset. A variant change refreshes affected NPIs,
   removes old-only evidence, and never falls back to source-wide typed rows.
9. Published metadata states `cohort_complete=true`,
   `rooted_graph_complete=true`, `endpoint_collection_complete=false`, and
   `endpoint_complete=false`; it never claims an exhaustive endpoint crawl.
10. The graph-aware Profile strategy and its signed capacity preflight must
    pass before any Profile materialization or serving-pointer change.

## PostgreSQL proof runbook

CI runs Flex registration, exact-cohort sealing, acquisition storage, twin
admission, dataset publication, and the rooted graph lifecycle once on the
`provider-directory` PostgreSQL shard with
`HLTHPRT_FHIR_FORMULARY_MIGRATION_POSTGRES_DSN`. The dataset-scoped
Profile replacement proof runs once on the `provider-profile` shard with
the shared test database also exposed as
`HLTHPRT_PROVIDER_DIRECTORY_PROFILE_POSTGRES_DSN`. The lifecycle DSN must name
an explicit disposable test database; each proof creates and removes its own
schema. That Profile shard also proves the guarded first-generation capacity
adoption path before any graph-aware Profile materialization can be admitted.

The workflow contract is covered by `tests/test_ci_test_sharding.py`. A skipped
local database proof means its required database configuration was absent and
is not successful evidence.

## Release proof ladder

Do not collapse these gates into one completion statement:

1. focused local unit and disposable-PostgreSQL proof;
2. exact pull-request and post-merge CI on the merged commit;
3. migration/image build and GitOps revision proof;
4. workload readiness and exact image digest;
5. full dev acquisition/admission evidence;
6. separately authorized publication and pointer proof;
7. API/Profile behavior and removal/replay proof;
8. task-owned worktree/branch/stash cleanup.

A blocked upstream file may leave code, migrations, and CI complete while the
full data-run and publication gates remain open. That state must be reported as
blocked activation, not feature completion.
