# NPI Import

## Purpose
Imports the National Plan and Provider Enumeration System (NPPES) provider directory and related address, taxonomy, and other-identifier structures.

## Source Website
- NPPES NPI files: <https://download.cms.gov/nppes/NPI_Files.html>

## Start Command
```bash
python main.py start npi
```

## Workers
```bash
python main.py worker process.NPI --burst
```

## Required Evidence Configuration
Every controlled NPI run fails closed unless all four settings are present:

- `HLTHPRT_NPPES_PUBLIC_EVIDENCE_MODE=required`
- `HLTHPRT_NPPES_RIGHTS_PROOF_SHA256` equals the SHA-256 of the shipped
  `specs/nppes-public-access-retention-review-v1.json` (currently
  `6bbb296fe4edb6764563ef01ccb6f264c795df594fe33dc5b7a6bcb74ac0eb40`)
- `HLTHPRT_NPPES_PUBLIC_EVIDENCE_ARTIFACT_ROOT` is an absolute, durable,
  owner-controlled directory outside the operating-system temporary tree
- `HLTHPRT_NPPES_PUBLIC_EVIDENCE_SCRATCH_ROOT` is an absolute, owner-only
  directory outside the operating-system temporary tree, backed by the import
  work-volume rather than pod ephemeral storage

The optional `HLTHPRT_NPPES_PUBLIC_EVIDENCE_BATCH_SIZE` accepts 100 through
100,000 rows and defaults to 20,000. The importer retains and verifies the
selected official listing and each source ZIP before either evidence admission
or canonical table publication. `HLTHPRT_NPI_JOB_TIMEOUT` defaults to 86,400
seconds; production must set it to the measured full-run budget and keep it no
greater than the Kubernetes worker Job deadline. Leave explicit deadline
headroom so ARQ can cancel, roll back, and release the import lease before
Kubernetes terminates the worker pod.

## PostgreSQL Full-Run Gate
A production full import must not start until PostgreSQL reports `fsync=on`,
`full_page_writes=on`, `synchronous_commit=on`, and
`current_setting('wal_compression')=pglz` on the writer session. The disposable
100,000- and 1,000,000-row admission benchmarks met their linearity and resource
gates with `pglz`; the uncompressed million-row run generated enough extra
full-page WAL to fail the normalized WAL gate. A different compression algorithm
requires a new accepted 100,000/1,000,000-row comparison before production use;
`HLTHPRT_NPPES_ADMISSION_SCALE_WAL_COMPRESSION` is test-only.

The import role must own, or have PostgreSQL 18 `MAINTAIN` on, the source-record,
member, common-record, source-link, and typed NPI-enumeration tables because the
atomic admission transaction runs `ANALYZE` before its deferred seal validator.
Before staging mutation, the importer requires enough scratch space for the
largest selected archive's four exact uncompressed legacy members plus 20%
reserve. Deployment telemetry must separately prove at least 20% reserve after
retained ZIPs and the projected PostgreSQL data/WAL peak, acceptable checkpoint
behavior, and a sufficient worker timeout. The synthetic benchmark covers the
six-field registry admission and validator; it does not qualify full-width CSV
parsing, canonical staging/rotation, retained-ZIP capacity, or the final source
census and post-publication API proof.

## Test Mode
Live queue test mode is deliberately unsupported because NPPES evidence and
canonical publication rows are immutable. Use the disposable PostgreSQL test
suites for bounded dry runs.

## Main Outputs
- `npi`
- `npi_address`
- `npi_taxonomy`
- `npi_taxonomy_group`
- `npi_other_identifier`
- `npi_phone_staffing`
- `npi_canonical_publication_receipt`
- `npi_canonical_publication_receipt_seal` (filled by the deferred database validator)
- `public_evidence_nppes_registry_admission`
- `public_evidence_nppes_registry_admission_seal`
- `public_evidence_nppes_registry_member`
- `public_evidence_nppes_registry_chain_admission`
- `public_evidence_nppes_registry_chain_admission_seal`
- `public_evidence_nppes_registry_chain_archive`

Each source archive also fills the existing immutable source identity, release,
source-record, common evidence, source-link, and typed NPI-enumeration tables.
Rows that the frozen typed-v1 contract cannot represent remain accounted for in
the member/admission exclusion census instead of being inferred or omitted.

## Post-Publication Proof

Retain the complete terminal control-run response. It must be `succeeded` with
phase `npi published`, a `snapshot_id` beginning `nppub1_`, required/admitted
NPPES metrics, identical evidence/publication `chain_ref` values, and canonical
row counts equal to the terminal stage counts.

For that exact `run_id`, require one joined row across
`import_run`, `npi_canonical_publication_receipt`, its seal, the referenced
NPPES chain admission, and its seal. The run snapshot must equal the publication
reference. Canonical state must be `canonical_api_published`; the evidence chain
must remain `verified_complete_disabled`, `serving_authority=none`, and
`publication_enabled=false`.

Verify each stored canonical relation OID still equals its live relation and
each stored census equals a fresh count:

```sql
\set run_id 'run_...'

SELECT
 p.npi_table_oid='mrf.npi'::regclass::oid
   AND p.npi_row_count=(SELECT count(*) FROM mrf.npi) AS npi_ok,
 p.npi_address_table_oid='mrf.npi_address'::regclass::oid
   AND p.npi_address_row_count=(SELECT count(*) FROM mrf.npi_address) AS address_ok,
 p.npi_taxonomy_table_oid='mrf.npi_taxonomy'::regclass::oid
   AND p.npi_taxonomy_row_count=(SELECT count(*) FROM mrf.npi_taxonomy) AS taxonomy_ok,
 p.npi_taxonomy_group_table_oid='mrf.npi_taxonomy_group'::regclass::oid
   AND p.npi_taxonomy_group_row_count=(SELECT count(*) FROM mrf.npi_taxonomy_group) AS taxonomy_group_ok,
 p.npi_other_identifier_table_oid='mrf.npi_other_identifier'::regclass::oid
   AND p.npi_other_identifier_row_count=(SELECT count(*) FROM mrf.npi_other_identifier) AS other_identifier_ok,
 p.npi_phone_staffing_table_oid='mrf.npi_phone_staffing'::regclass::oid
   AND p.npi_phone_staffing_row_count=(SELECT count(*) FROM mrf.npi_phone_staffing) AS phone_staffing_ok
FROM mrf.npi_canonical_publication_receipt p
WHERE p.run_id=:'run_id';
```

All six values must be true. For every chain archive, require its registry seal;
member and source-record counts equal `source_record_count`; common, link, and
typed counts equal `projected_record_count`; and independently rehash the
retained listing and ZIP bytes against the stored SHA-256 and byte counts.

Choose one published NPI with both address and taxonomy rows, then require HTTP
200 from `/api/v1/npi/`, `/api/v1/npi/all?npi=<NPI>&limit=1&include_total=1`,
`/api/v1/npi/id/<NPI>?include_profile=0&sync_geocode=0&lookup_stored_geocode=0`,
and `/api/v1/npi/id/<NPI>/full_taxonomy`. Root counts must match the receipt;
the list/detail/taxonomy payloads must contain the selected NPI and expected
canonical data; no response may contain `employer_identification_number` or
`parent_organization_tin`.

Archive the exact source/image SHA, deployed image digest, Flux revision,
migration head, worker exit/restart/OOM state, import duration, byte-exact
artifact/scratch/PostgreSQL/WAL free-space telemetry, checkpointer deltas, and
API readiness evidence with the run receipt.

## Notes
- This is the canonical provider directory import for names, addresses, phones, and NUCC-linked taxonomy declarations.
- Other imports such as provider enrichment add sidecar data, but do not replace NPPES as the core directory source.
- The shutdown phase also refreshes `npi_address.procedures_array` and `npi_address.medications_array` from canonical pricing tables.
- Publication, the immutable receipt, and the terminal control-run update occur
  in the same transaction; there is no separate NPI finish worker.
