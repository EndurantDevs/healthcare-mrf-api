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
- The drug catalog is exactly 24 IFP plus 24 Community & State files. Missing,
  non-JSON, malformed, or conflicting files block admission; 47 of 48 is not a
  complete publication.
- Drug bytes use the existing content-addressed retained store. Independent
  normalization roots and retries reopen those bytes rather than redownload or
  duplicate them.
- Drug records publish only through the immutable `formulary_fhir` repository.
  The adapter must not invoke the legacy formulary importer or write legacy
  plan-drug relations.
- Admission and publication are separate default-off operations. Publication
  accepts only a durable receipt and revalidates retained bytes immediately
  before pointer movement.

## Binary implementation gates

The drug slice is acceptable only when all of the following are true:

1. The retained observation proves exactly 24+24 drug identities.
2. Every retained file passes the real-source schema and resource-bound census.
3. Acquisition, retry, and both roots use the same artifact set; retry performs
   zero network requests for already verified files.
4. Both full repository roots match and PostgreSQL records one exact admission.
5. A durable receipt binds the listing observation, artifact set, spool proof,
   repository graph, and source configuration.
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
4. Every returned resource is a Practitioner carrying the requested canonical
   NPI; pagination, cap overflow, cross-NPI rows, or conflicting duplicate IDs
   fail closed.
5. Two fresh complete cohort acquisitions produce the same immutable semantic
   dataset proof before any publication decision.
6. Dataset metadata states `cohort_complete=true`,
   `endpoint_collection_complete=false`, and `endpoint_complete=false`.
7. Profile delta processing covers the new cohort plus NPIs from the previous
   Flex dataset so removals are reflected.
8. Official and Flex evidence remain separately attributable while their
   independent-authority count remains one.

## PostgreSQL proof runbook

CI runs Flex registration, exact-cohort sealing, acquisition storage, twin
admission, and dataset publication once on the `provider-directory` PostgreSQL
shard with `HLTHPRT_FHIR_FORMULARY_MIGRATION_POSTGRES_DSN`. The dataset-scoped
Profile replacement proof runs once on the `provider-profile` shard with
the shared test database also exposed as
`HLTHPRT_PROVIDER_DIRECTORY_PROFILE_POSTGRES_DSN`. The lifecycle DSN must name
an explicit disposable test database; each proof creates and removes its own
schema.

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
