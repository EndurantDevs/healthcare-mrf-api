# Address Canonical DevOps

This runbook covers the address-canonical foundation rollout. The feature is
safe to deploy with existing imports because canonical dual-write is disabled
until `HLTHPRT_ADDRESS_CANON_SOURCES` is set.

## Deploy Order

1. Deploy the code and Alembic migration together.
2. Run migrations before enabling canonical dual-write:

   ```bash
   ./venv314/bin/python -m alembic upgrade head
   ```

   If `alembic current` returns no revision on a populated database, do not run
   a blind `upgrade head`. First verify/stamp the existing schema state or run
   the missing recent migrations in a controlled maintenance window. The
   checksum widening migration rewrites the durable `address_archive` table
   (only — swap-published tables inherit `bigint` from the model at their next
   import). Lock-window estimate: `ALTER ... TYPE bigint` holds
   `ACCESS EXCLUSIVE` for the full rewrite + index rebuild; measured
   2026-06-11 on dev hardware at ~1.7 s per 1M narrow rows (synthetic, one
   unique index). The production archive (~3M wider rows, geocode columns)
   should be planned as **under one minute of exclusive lock — budget a
   5-minute window**. The API geocode upsert writes to this table, so run it
   when the API is idle or briefly drained.

   The SQL function names are stable, but their bodies define the current
   canonical key contract. Revision
   `20260611130000_reapply_address_canonical_functions` exists specifically to
   replay `CREATE OR REPLACE FUNCTION` on databases that already applied the
   original foundation revision. Revision
   `20260612193000_address_key_from_identity_function` adds the
   `addr_key_from_identity_v1(identity_key)` helper used by the set-based
   resolver/archive materialization path. Revision
   `20260612205000_reapply_address_canonical_nbsp_functions` replays the
   functions again so already-migrated databases receive the non-breaking-space
   normalization fix. Revision
   `20260618100000_address_canonical_current_rekey` replays the current
   functions and rewrites `address_archive_v2`, checksum maps, collision rows,
   and known source `address_key` columns to the current `v2` identity format.
   After applying a function-body fix to an environment with stamped dev data,
   prefer the rekey migration over manual reset/re-stamp cycles. If you must
   re-stamp a source manually before the migration, wipe nullable source
   `address_key` columns first:

   ```sql
   UPDATE mrf.npi_address SET address_key = NULL WHERE address_key IS NOT NULL;
   UPDATE mrf.doctor_clinician_address SET address_key = NULL WHERE address_key IS NOT NULL;
   UPDATE mrf.provider_enrollment_ffs SET address_key = NULL WHERE address_key IS NOT NULL;
   UPDATE mrf.provider_enrollment_ffs_address SET address_key = NULL WHERE address_key IS NOT NULL;
   UPDATE mrf.facility_anchor SET address_key = NULL WHERE address_key IS NOT NULL;
   UPDATE mrf.mrf_address SET address_key = NULL WHERE address_key IS NOT NULL;
   UPDATE mrf.mrf_address_evidence SET address_key = NULL WHERE address_key IS NOT NULL;
   UPDATE mrf.pharmacy_license_record_v1 SET address_key = NULL WHERE address_key IS NOT NULL;
   UPDATE mrf.pharmacy_license_record_history_v1 SET address_key = NULL WHERE address_key IS NOT NULL;
   UPDATE mrf.partd_pharmacy_activity_v2 SET address_key = NULL WHERE address_key IS NOT NULL;
   UPDATE mrf.entity_address_unified SET address_key = NULL WHERE address_key IS NOT NULL;
   ```

   Then rerun the enabled importer finish/stamp path, or rerun the importer, so
   `resolve_into_archive` sees stamps produced by the current contract. The
   resolver now aborts if a staged key disagrees with
   `sha256(identity_key)[:16]`.

3. Restart the Sanic API and all ARQ workers after the deploy. Dynamic staging
   classes are cached in worker processes; a restart is required so new staging
   tables include the nullable `address_key` column.
4. Keep canonical writes disabled for the first restart:

   ```bash
   unset HLTHPRT_ADDRESS_CANON_SOURCES
   ```

5. Enable one source family at a time after the migration is confirmed:

   ```bash
   export HLTHPRT_ADDRESS_CANON_SOURCES=cms_doctors
   ```

   Valid source names are `cms_doctors`, `facility_anchors`,
   `provider_enrichment`, `provider_enrollment_ffs`, `nppes`, `mrf`,
   `pharmacy_license`, `partd`, or `all`.

6. Leave canonical stamping at the code default unless a rebuild needs explicit
   throttling or tuning. `HLTHPRT_ADDRESS_CANON_STAMP_SHARDS` overrides the
   per-call shard count used by `stamp_address_keys` (default 8). Set it to a
   positive integer to reduce database pressure on small/disposable rebuilds or
   to increase parallel-sized batches only after measuring lock and CPU impact;
   invalid values fail fast during stamping. `HLTHPRT_ADDRESS_CANON_STAMP_CONCURRENCY`
   controls how many shard updates run at once (default 1). On the 24-core dev
   host, use 16 with `HLTHPRT_DB_POOL_MAX_SIZE=32` for full rebuilds; keep it at
   1 for small local databases.
   `HLTHPRT_ADDRESS_CANON_RUST_MATERIALIZE` defaults to enabled for the
   Rust-backed resolve materializer. Set it to `0`/`false` to force the SQL
   materializer. Python still owns the database
   transaction, temp table, collision checks, and archive writes; the
   `ptg2_scanner` binary only transforms exported raw address COPY rows into
   keyed PostgreSQL COPY rows. Before Rust is used, Python verifies the
   scanner identity version and embedded PUB28 hash match the Python runtime;
   stale scanners fall back to SQL with a warning. The Docker image ships the binary at
   `/opt/support/ptg2_scanner/target/release/ptg2_scanner`, also exposed through
   `HLTHPRT_PTG2_RUST_SCANNER_BIN`.

## Compatibility

- `entity_address_unified` checks for `addr_key_v1(...)` at runtime. If the
  migration is not present, it publishes with `NULL address_key` values and keeps
  legacy checksum grouping.
- Importer resolve helpers target `HLTHPRT_ADDRESS_ARCHIVE_TABLE`, defaulting to
  `address_archive_v2`.
- Import resolve materializes a keyed temp table by deduping staged
  `address_key` values before expensive normalization. By default, Python exports the deduped raw
  projection through asyncpg COPY, invokes `ptg2_scanner
  --address-canonicalize-copy`, and loads the keyed COPY file into the same
  transaction-local temp table. If the flag is disabled, or the scanner/COPY
  driver is unavailable, resolve falls back to the SQL materializer. Legacy
  archive migration still uses the parity-tested Python oracle because those
  rows do not carry pre-stamped `address_key` values.
- Python-side batch canonicalization can use the optional `ptg2_address_canon`
  PyO3 wheel built from `support/ptg2_scanner`. `process.ext.address_fast`
  verifies the same identity version and PUB28 hash before using the wheel; if
  it is missing or stale, callers use the pure-Python reference path.
- Resolve stats include Phase 2 gate counters (`eligible_key_rows`,
  `eligible_null_key_rows`, `gate_violations`, and `gate_sample_rows`). Any
  enabled importer that leaves a street+state+ZIP row without `address_key`
  emits a live warning event; `gate_sample_rows` carries bounded multi-source
  rows for the manual eyeball review.
- Resolve never infers a missing direction or suffix. Relaxed equivalence is
  applied only from active, reviewed `address_alias_v1` rows after canonical
  keys are materialized. Missing ZIP rows remain null-keyed unless an explicit
  coordinate-based ZIP restore step resolves them.
- Do not point `HLTHPRT_ADDRESS_ARCHIVE_TABLE` at the live legacy
  `address_archive` until the Phase 3 migration and verification steps are done.
- `HLTHPRT_ADDRESS_ARCHIVE_CUTOVER` (default **off**) gates the Phase 4 reader
  cutover: the API geocode read/write paths in `api/endpoint/npi.py` and the
  NPPES geo-backfill source in `process/npi.py` use the v2 archive ONLY when
  this flag is set. Leave it unset until the Phase 3 archive migration
  (`address-archive-v2-migrate`) has run AND its V1/V2/V3 verification queries
  pass on the target environment — enabling it earlier points readers at an
  archive without geocodes. With the flag off, behavior is identical to
  legacy even when the v2 table exists.

## Reviewed Address Aliases

The shared workflow supports two closed policy kinds:

- `numeric_grid_direction_v1` is the existing numeric-grid structural rule.
- `evidence_gated_address_match_v1` discovers exact address equivalences from
  current provider-serving evidence. It requires one valid shared NPI, country,
  state, exact ZIP5, one globally unique target key, and at least two strict
  target source bits. It permits candidate-confirmed bare/spaced units, unit
  punctuation, direction relocation, and a one-sided terminal suffix. Missing
  directions, descriptive second lines, route-number reinterpretation,
  conflicting markers, ambiguous targets, and `premise_only` results are never
  eligible.

Both policies preserve `identity_key`, `premise_key`, `address_key`, and the
canonical ruleset version. Rust remains the preferred import materializer;
reviewed aliases are projected only after Rust or PostgreSQL has produced the
same canonical keys.

Numeric-grid repair is an offline, generic structural rule. It is not fuzzy
matching and it is not state-specific. Optional state and ZIP scopes only bound
an operator run; they do not change the rule.

For example, `1234 E 5678, Suite 202` may alias to
`1234 E 5678 S, Suite 202` when that exact unit, state, ZIP, and country have
one structurally richer target backed by at least two independent source bits.
`Suite 203` is not the same unit and never matches. If both a north and south
target exist, the source remains ambiguous and no alias is eligible.

The operation is deliberately split into reviewable stages:

The alias run, candidate, and active-alias relations are controlled audit
tables. Use only the commands below; never issue direct `INSERT`, `UPDATE`,
`DELETE`, or `TRUNCATE` against them. Row triggers make sealed evidence and
terminal run state immutable and require revocation instead of deletion, but a
database owner can bypass row triggers (including with `TRUNCATE`). Database
owner access is therefore an explicit trusted operational boundary.

Before the first evidence-gated shadow after deploying the schema-2 migration,
run the full Provider Directory address-artifact rebuild and then the full
unified-address rebuild from step 5. The migration advances the alias
generation, so pre-deploy serving artifacts fail the evidence fence by design.
Numeric-grid shadows do not depend on this serving-evidence preflight.

1. Seal an initial shadow. The command prints JSON containing the immutable
   `run_id`, `candidate_digest`, counts, and bounded samples.

   ```bash
   python main.py start address-numeric-grid-alias \
     --mode shadow \
     --state-code TX \
     --zip-prefix 75
   ```

   For evidence-gated comparison, use the same workflow with the explicit
   policy kind:

   ```bash
   python main.py start address-numeric-grid-alias \
     --alias-kind evidence_gated_address_match_v1 \
     --mode shadow
   ```

   Evidence shadows fail closed unless every current unified-address row was
   built at the active alias generation. The schema-2 upgrade advances that
   generation once, so complete the two rebuilds in step 5 immediately after
   deployment and again after every activation or revocation before starting
   another shadow.

2. For numeric-grid shadows only, if target provenance is missing because the strict column was introduced
   after existing imports, attest only the candidate targets from that exact
   shadow. The stored source `address_key` is used solely as an indexed
   prefilter; both key and identity are recomputed from raw source components.
   The operation never copies historical `source_bits`, never certifies source
   keys, and never promotes an alias.

   ```bash
   python main.py start address-strict-source-backfill \
     --alias-run-id 00000000-0000-0000-0000-000000000001 \
     --expected-candidate-sha256 <exact-64-character-digest> \
     --reviewed-by <operator> \
     --max-targets 256
   ```

   Exact Marketplace evidence comes from `mrf_address_evidence`, not its
   independently aggregated summary. Historical Part D rows are excluded
   because missing addresses may have been filled from NPPES. New Part D rows
   set strict bit 64 only when their identity fields were present in the Part D
   source itself. Normal archive resolution never infers strict evidence from
   a post-processed row: callers must explicitly attest raw-source lineage.
   This prevents a ZIP restored from coordinates from counting as an exact
   source observation. Provider Directory archive stages can attest their rows
   because their input SQL requires a source-observed postal code; other
   historical evidence is recovered only by this exact-key backfill.
   Facility Anchor bit 8 is deliberately excluded from historical strict
   backfill: its import can restore a blank source ZIP from coordinates before
   the row is published, and the current live table does not retain a raw-ZIP
   lineage flag. Re-enable that source only after a source-observed postal-code
   marker is persisted before restoration.

3. Run a new shadow with the same scope and alias kind. Review this new
   run—not the first one—because its digest snapshots the attested target
   bitmasks and evidence fields.

   ```sql
   SELECT
       run_id,
       candidate_digest,
       source_count,
       candidate_source_count,
       eligible_count,
       ambiguous_count,
       insufficient_provenance_count,
       reason_buckets,
       sample_rows
   FROM mrf.address_alias_run_v1
   WHERE run_id = '<new-shadow-run-id>'::uuid;

   SELECT
       source_address_key,
       source_identity_key,
       target_address_key,
       target_identity_key,
       candidate_count,
       target_strict_source_bits,
       target_strict_source_count,
       match_rule,
       match_classification,
       evidence_npi,
       evidence_npi_count,
       decision
   FROM mrf.address_alias_candidate_v1
   WHERE run_id = '<new-shadow-run-id>'::uuid
   ORDER BY source_address_key, target_address_key;
   ```

4. Apply only the exact sealed digest after review. Apply re-derives the
   candidates under the alias lock and aborts if any input changed.

   ```bash
   python main.py start address-numeric-grid-alias \
     --mode apply \
     --alias-run-id <new-shadow-run-id> \
     --expected-candidate-sha256 <new-exact-digest> \
     --reviewed-by <operator>
   ```

   Include `--alias-kind evidence_gated_address_match_v1` when applying an
   evidence-gated shadow; apply rejects a kind mismatch.

   On dev, submit the shadow and apply through the deployment control API with importer
   `address-numeric-grid-alias`, node `local_mrf`, and the equivalent `params`
   (`alias_kind`, `mode`, then the sealed `alias_run_id`,
   `expected_candidate_sha256`, and `reviewed_by`). Preserve distinct
   idempotency keys and the exact terminal receipts.

5. An activation changes the alias generation. Rebuild the full Provider
   Directory address artifact set and then run a full unified-address rebuild.
   Partial overlay publication intentionally fails when its stored generation
   is stale, because copied rows could retain an old alias after activation or
   revocation.

   ```bash
   python main.py start provider-directory-fhir \
     --publish-artifacts-only \
     --publish-artifacts-targets addresses \
     --full-address-artifact-rebuild
   python main.py start entity-address-unified
   ```

   The explicit full-rebuild flag keeps every prerequisite bound to the same
   reviewed global dataset fence, while allowing the address overlay and
   corroboration artifacts to replace all source partitions at the new alias
   generation. The `addresses` target includes both network-catalog and
   corroboration artifacts; partial or source-scoped rebuilds fail closed.

   On dev, dispatch every mutation through the deployment control API; do not invoke the
   CLI directly in an application pod. Post to `http://127.0.0.1:8095/v1/runs`
   from the control deployment with its bearer token. The minimum
   post-activation sequence is:

   ```json
   {"importer":"provider-directory-fhir","node_id":"local_mrf","params":{"publish_artifacts_only":true,"publish_artifacts_targets":"addresses","full_address_artifact_rebuild":true},"idempotency_key":"address-alias-g<GEN>-provider-directory-addresses-attempt<N>","triggered_by":"manual","actor":"devops"}
   ```

   Wait for that exact run to reach terminal `succeeded`, then submit:

   ```json
   {"importer":"entity-address-unified","node_id":"local_mrf","params":{"refresh_mode":"full","publish":true,"serving_only_refresh":true},"idempotency_key":"address-alias-g<GEN>-entity-address-full-attempt<N>","triggered_by":"manual","actor":"devops"}
   ```

   Use `serving_only_refresh: false` when acceptance requires a comprehensive
   rebuild of unified support/provenance tables, not only the minimum serving
   refresh. Preserve each returned `run_id`; ensure its worker through
   `POST /v1/runs/<run_id>/workers/ensure`, and poll the exact run. Proceed only
   from terminal `succeeded`; `failed`, `canceled`, and `dead_letter` abort the
   chain. A `409` response or a completed Kubernetes Job is not proof of import
   success.

   A deliberately complete address-source re-import is broader than alias
   activation. Serialize `geo`, `nucc`, `cms-doctors`, `facility-anchors`,
   `provider-enrichment`, `npi`, `mrf`, `partd-formulary-network`, and
   `pharmacy-license`, each with `params: {}`, through the same control API;
   then run the two artifact jobs above with `serving_only_refresh: false`.
   Do not routinely append `address-archive-v2-migrate`: it is the one-time
   legacy-to-v2 copy, not a source refresh. A full external Provider Directory
   acquisition is also a separate freshness operation; artifact-only rebuilds
   use the currently published dataset without fetching it.

   Every canonical writer must report exactly one selected materializer in
   `address_resolve.reason_buckets` (or its captured worker log):
   `canonical_materializer_rust=1` and `canonical_materializer_sql=0` is the
   expected fast path, while `canonical_materialized_key_rows` records the row
   count. SQL fallback remains correct but must be explained in the release
   receipt before continuing.

   Verify that the overlay receipt equals the active generation and neither
   serving artifact retains an active source key:

   ```sql
   SELECT state.generation, artifact.generation AS overlay_generation
   FROM mrf.address_alias_state_v1 AS state
   JOIN mrf.address_alias_artifact_state_v1 AS artifact
     ON artifact.artifact_name = 'provider_directory_address_overlay'
   WHERE state.singleton = true;

   SELECT count(*) AS overlay_residual_source_keys
   FROM mrf.provider_directory_address_overlay AS overlay
   JOIN mrf.address_alias_v1 AS alias
     ON alias.source_address_key = overlay.address_key
    AND alias.revoked_at IS NULL;

   SELECT count(*) AS unified_residual_source_keys
   FROM mrf.entity_address_unified AS unified
   JOIN mrf.address_alias_v1 AS alias
     ON alias.source_address_key = unified.address_key
    AND alias.revoked_at IS NULL;

   SELECT count(*) AS bad_active_alias_targets
   FROM mrf.address_alias_v1 AS alias
   LEFT JOIN mrf.address_archive_v2 AS target
     ON target.address_key = alias.target_address_key
    AND target.merged_into IS NULL
   WHERE alias.revoked_at IS NULL
     AND target.address_key IS NULL;

   SELECT count(*) AS unified_stale_alias_generation
   FROM mrf.entity_address_unified AS unified
   CROSS JOIN mrf.address_alias_state_v1 AS state
   WHERE state.singleton
     AND unified.base_address_version IS DISTINCT FROM
         'address_archive_v2:v2+fmt-v2+alias-v1:g' || state.generation::text;
   ```

   All residual/staleness counts must be zero. Retain the publication receipts,
   active generation, deployed source SHA/image digest, migration head, run
   requests, terminal responses, worker exit/restart evidence, and materializer
   metrics.

   Finally, retain before/after canaries for three reviewed alias NPIs. Require
   `/api/v1/healthcheck/ready`, then verify `/api/v1/npi/all`,
   `/api/v1/npi/id/<NPI>`, `/api/v1/npi/id/<NPI>/full_taxonomy`, and
   `/api/v1/npi/match-candidates?address_key=<target>&include_sources=1&include_evidence=1`.
   The target key must serve the expected NPI and the active source key must not
   remain a distinct candidate. Also retain ZIP and national responses from
   `/api/v1/pricing/providers/<NPI>/score`. Public `address_key` fields are
   expected; internal alias rows, candidate digests, and source identity keys
   are not.

Revocation is one-way and also changes the generation. It requires the exact
active source and expected target keys, an operator, and a reason. A revoked
shadow cannot be reapplied; create a new shadow for any future decision. After
revocation, repeat both full artifact rebuilds above. Revocation stops future
alias projection, but it does not subtract ordinary `source_bits` already
projected into an archive target. Reconstruct the archive from raw source
lineage if exact historical provenance rollback is required; the two derived
artifact rebuilds do not perform that reconstruction.

```bash
python main.py start address-numeric-grid-alias-revoke \
  --alias-kind evidence_gated_address_match_v1 \
  --source-address-key <source-uuid> \
  --expected-target-address-key <target-uuid> \
  --reason "reviewed rollback" \
  --reviewed-by <operator>
```

Omit `--alias-kind` only for the default numeric-grid policy.

No public request joins `address_alias_v1`; active mappings are consumed only
while building archive, overlay, and unified artifacts. This preserves the
provider endpoint latency contract.

## Versioned Formatted Addresses

Revision `20260811110000_address_formatted_display` retains the historical v1
contract. Revision `20260815010000_address_formatted_display_v2` adds the
immutable, parallel-safe `addr_formatted_address_v2(...)` renderer. V2 renders
only from the row's structured components, applies human display casing,
expands common US directions and street suffixes, normalizes repeated unit
lines, formats ZIP+4, and omits the default US country. Both migrations are
schema-only and do not rewrite serving data while holding the migration lock.

After the migration and worker restart, materialize existing archive labels as
a bounded offline job:

```bash
python main.py start address-formatted-address --batch-size 10000
```

The job advances by `address_key`, is safe to retry, and reports scanned,
updated, and batch counts. It writes `formatted_address_version = 2` and
`formatted_address_source = 'canonical_v2'`; it performs no geocoding, fuzzy
matching, or network calls. `HLTHPRT_ADDRESS_ARCHIVE_TABLE` may select a
different unqualified archive table name. Invalid or schema-qualified values
fail closed.

Next rebuild the complete Provider Directory address artifacts and unified
addresses. Overlay and unified labels are rendered from each output row's own
components; archive premise and coordinate fields remain reusable:

```bash
python main.py start provider-directory-fhir \
  --publish-artifacts-only \
  --publish-artifacts-targets addresses \
  --full-address-artifact-rebuild
python main.py start entity-address-unified
```

Provider requests are stored-only by default. The public serializer rerenders
the label from the returned row as a final coherence guard. Explicit legacy
geocode flags remain compatibility-only and are disabled on the public provider
routes. When enabled, geocoders may provide coordinates and place IDs but their
display labels are never persisted or returned.

Verify completion before the serving canary:

```sql
SELECT count(*) AS archive_missing_display
FROM mrf.address_archive_v2
WHERE merged_into IS NULL
  AND formatted_address IS NULL;

SELECT count(*) AS archive_stale_display_version
FROM mrf.address_archive_v2
WHERE merged_into IS NULL
  AND (
      formatted_address IS DISTINCT FROM mrf.addr_formatted_address_v2(
          first_line, second_line, city_name, state_name, postal_code,
          country_code
      )
      OR formatted_address_version IS DISTINCT FROM 2
      OR formatted_address_source IS DISTINCT FROM 'canonical_v2'
  );

SELECT count(*) AS overlay_stale_or_incoherent_display
FROM mrf.provider_directory_address_overlay
WHERE formatted_address IS DISTINCT FROM mrf.addr_formatted_address_v2(
          first_line, second_line, city_name, state_name, postal_code,
          country_code
      )
   OR formatted_address_version IS DISTINCT FROM 2
   OR formatted_address_source IS DISTINCT FROM 'canonical_v2';

SELECT count(*) AS unified_stale_or_incoherent_display
FROM mrf.entity_address_unified
WHERE formatted_address IS DISTINCT FROM mrf.addr_formatted_address_v2(
          first_line, second_line, city_name, state_name, postal_code,
          country_code
      )
   OR formatted_address_version IS DISTINCT FROM 2
   OR formatted_address_source IS DISTINCT FROM 'canonical_v2';
```

Review any remaining rows before publication. Component-empty archive rows may
legitimately render `NULL`; they must not be filled from a fuzzy or geocoder
display string merely to make the count zero.

## Smoke Checks

Run these after migration and restart:

```bash
./venv314/bin/python -m pytest -q \
  tests/test_address_canonical_unit.py \
  tests/test_address_checksum_schema.py

cargo test --manifest-path support/ptg2_scanner/Cargo.toml
cargo build --release --manifest-path support/ptg2_scanner/Cargo.toml
```

Dev-server NPI shutdown optimization verification from 2026-06-13
(`ubuntu@dev-host.example`, PostgreSQL 18.4, 24 cores):

- Python 3.14.6 was provisioned with `uv`; focused parallel verification
  passed with 24 workers:
  `python -m pytest tests/test_address_canonical_unit.py tests/test_process_npi_unit.py tests/test_process_mrf_unit.py -n auto`
  -> `93 passed in 3.27s`.
- Published dev NPI address table had 19,707,579 rows, with 19,694,747
  populated `address_key` values and 12,832 null keys. The NPI shutdown path
  now skips full SQL restamp when load-time keys are present; for small missing
  sets it stamps one shard and bypasses the global
  `HLTHPRT_ADDRESS_CANON_STAMP_SHARDS` override to avoid repeated full scans.
  A temp-table run over the 12,832 null-key rows completed the canonical stamp
  computation in 2.922s and changed no rows, confirming those rows are
  ineligible for canonical keys rather than stale prekeys.
- `do_business_as` enrichment now avoids the unconditional 9.6M-row reset,
  updates only changed source NPIs, and clears only stale non-empty values.
  On published dev data, changed-row checks returned 0 update / 0 clear in
  2.282s and 0.374s. `do_business_as_text` is now non-null by default so fresh
  staging tables do not need a blanket reset before search indexes are built.
- NPI taxonomy arrays are now stamped onto primary and mailing addresses during
  row parsing from the in-memory NUCC code map. Shutdown keeps a deterministic
  changed-only SQL fallback for secondary/old rows. Published dev address
  counts were 9,260,504 primary, 9,260,501 mail, and 1,186,574 secondary; the
  old published data still has deterministic-array drift on 1,286,921 primary,
  1,286,921 mail, and 486,362 secondary rows, but fresh imports should avoid
  the primary/mail post-load rewrite.

For a disposable database on local Postgres:

```bash
env HLTHPRT_DB_HOST=127.0.0.1 \
    HLTHPRT_DB_PORT=5440 \
    HLTHPRT_DB_DATABASE=mrf_address_canon_test \
    HLTHPRT_DB_SCHEMA=mrf \
    HLTHPRT_DB_USER=nick \
    HLTHPRT_DB_PASSWORD= \
    HLTHPRT_ADDRESS_CANON_RUST_MATERIALIZE=true \
    ./venv314/bin/python -m pytest -q tests/test_address_canonical_db.py
```

Before enabling a production-sized source, check that the archive table exists:

```sql
SELECT to_regclass('mrf.address_archive_v2');
SELECT to_regprocedure('mrf.addr_key_v1(text,text,text,text,text,text)');
SELECT EXISTS (
  SELECT 1
  FROM information_schema.columns
  WHERE table_schema = 'mrf'
    AND table_name = 'npi_address'
    AND column_name = 'address_key'
);
```

Local verification snapshot from 2026-06-11:

- Disposable Postgres on `127.0.0.1:5440`, database
  `mrf_address_canon_test`, reached Alembic head
  `20260611130000_reapply_address_canonical_functions`.
- Dev `mrf_dev` read-only baseline is captured in
  `reports/address_canonical_baseline_2026-06-11.md`, including exact
  full-table unit-conflict numbers for the available `npi_address` and
  `address_archive` tables. Rerun it in the target production snapshot before
  Phase 3 because this local snapshot does not include every planned source
  table.
- SQL functions include static USPS Pub. 28 C1/C2/Appendix B/I4 normalization
  maps generated from the official Postal Explorer pages and are verified as
  `IMMUTABLE` / `PARALLEL SAFE`.
- Expanded impacted importer/API/control suite passed: `346 passed`.
- Full local suite passed: `942 passed`.
- Live local healthcare-mrf-api smoke on `127.0.0.1:8085` returned `200` for
  healthcheck, `npi/all`, `npi/id`, and pricing score against an unmigrated
  `mrf_dev` DB.
- Disposable-DB post-suite schema checks confirmed `address_archive_v2`,
  `address_checksum_map`, and
  `addr_key_v1(text,text,text,text,text,text)` remained installed. The local
  main `mrf_dev` DB is intentionally still on the legacy archive schema
  until Phase 3 cutover.
- PostGIS matrix passed in `mrf_address_canon_postgis_test` with
  PostGIS `3.6.1`; downgrade/upgrade recreated
  `address_archive_v2_geo_idx`.
- Phase 3.1 review-fix real-copy smoke passed in the disposable DB using
  `address-archive-v2-migrate`: 916,949 copied legacy rows collapsed to 780,372
  canonical rows in 88.644s, with 916,417 checksum bridge rows and zero
  represented/geocode misses. A 916,417-row real-copy
  `resolve_into_archive` performance smoke completed in 66.809s with 780,372
  distinct keys and zero gate violations. See
  `reports/address_archive_v2_migration_smoke_2026-06-11.md`.
- Disposable dev rebuild with canonical writes enabled for `all` sources passed
  through the ordered chain: geo, NUCC, CMS doctors, facility anchors, provider
  enrichment, NPPES, marketplace MRF, Part D, pharmacy license,
  `address-archive-v2-migrate`, and `entity-address-unified`. Final checks:
  `address_archive_v2` had 4,169 rows, `source_bits` OR was 63, checksum
  collisions were 0, and archive UUIDs matched `sha256(identity_key)[:16]` for
  every row. After the facility-coordinate archive refresh was added and
  `facility-anchors --test` reran, all 500 facility-source archive rows had
  `lat`/`long` with `geocode_source='facility_anchor'`. Part D did not set bit
  64 in this smoke because the sampled CMS Part D rows had no street line and
  no overlap with the small NPPES primary address sample; full or overlapping
  runs should produce Part D keys via the NPI-address fallback.
- Live API smoke against the rebuilt dev DB returned `200` for healthcheck,
  `npi/all`, `npi/id/1154324382`, pharmacy license coverage, pharmacy license
  detail for `1518379605`, Part D import status, Part D activity for
  `135672782`, NUCC, and control node health. Checked payloads did not expose
  internal `address_key` values. Pricing statistics was not included because
  the disposable rebuild did not run the claims-pricing import that creates
  `pricing_provider_procedure`.
- Hygiene passed: `git diff --check` and
  `python -m compileall -q api process db alembic tests`.
- Earlier post-rebuild local verification passed with `963 passed`,
  `git diff --check`, and
  `python -m compileall -q api process db alembic tests`.
- Current follow-up verification passed with `964 passed`; fresh API smoke on
  `127.0.0.1:8085` returned `200` for healthcheck, `npi/all`,
  `npi/id/1154324382`, pharmacy license detail for `1518379605`, Part D
  activity for `135672782`, and control node health, with no `address_key`
  string in checked payloads.
- Python 3.14 dev-server rebuild follow-up began on 2026-06-12 with
  `HLTHPRT_ADDRESS_CANON_SOURCES=all`,
  `HLTHPRT_ADDRESS_CANON_STAMP_SHARDS=24`,
  `HLTHPRT_ADDRESS_CANON_STAMP_CONCURRENCY=16`,
  `HLTHPRT_ADDRESS_CANON_RUST_MATERIALIZE=true`,
  `HLTHPRT_MAX_MRF_JOBS=16`, `HLTHPRT_MRF_QUEUE_READ_LIMIT=512`,
  `HLTHPRT_DB_DEADLOCK_RETRIES=20`, and
  `HLTHPRT_PARALLEL_DOWNLOAD_WORKERS=16`.
  CMS-doctors address stamping over 3,271,389 rows was the observed bottleneck
  when shards ran serially; SQL shard fan-out keyed 100% of rows and the helper
  now has explicit concurrency coverage.
- Rust materializer follow-up on image
  `ghcr.io/endurantdevs/healthcare-mrf-api:dev-address-canon-rust-20260612214227`
  fixed an actual NPI parity edge (`2ND FLOOR-PULMANARY`) and passed actual-data
  scratch resolves: 1M NPI rows in 9.752s wall time with 0 key mismatches and 0
  source-bit misses; multi-source checks also passed for NPI, marketplace MRF,
  CMS doctors, and facility anchors. Pharmacy-license was empty in the dev DB
  during this smoke.
- Dev deploy was advanced through
  `ghcr.io/endurantdevs/healthcare-mrf-api:dev-address-canon-rust-20260613020811`
  and finally to
  `ghcr.io/endurantdevs/healthcare-mrf-api:dev-address-canon-rust-20260613050521`
  after the full MRF/NPPES timing work. The newer deployment tags add
  `HLTHPRT_MAX_NPI_JOBS`, `HLTHPRT_NPI_QUEUE_READ_LIMIT`,
  `HLTHPRT_MAX_NPI_FINISH_JOBS`, conflict-safe formulary aggregate upserts,
  `HLTHPRT_MRF_QUEUE_READ_LIMIT` support, DB-pool sizing for parallel address
  stamping, BigInteger ORM coverage for MRF provider NPI/network-checksum
  staging plus NPPES NPI identifiers, and Rust parity fixes for actual NPI
  `2ND FLOOR-PULMANARY` and `STE T .` addresses. The high-parallel MRF
  follow-up found that a single very wide ARQ process can exhaust the
  per-process DB pool; use multiple normal `process.MRF --burst` processes with
  a larger queue read window instead.
- The controlled Python 3.14 MRF run for import
  `addrcanon_mrf_20260612215839` published successfully on 2026-06-13 after
  patched recovery/finish work. Final finish job
  `hp-mrf-finish-patched-20260613020811` completed in 3208.32s; total import
  delta was 5:51:04.280138. Published counts: 735 issuers, 15,500 plans,
  34,811 transparency rows, 29,507,745 drug rows, 6,929 plan drug stats,
  39,241 plan drug tier stats, 25,672,487 plan NPI rows, 10,583 network tiers,
  16,302,076 MRF address rows, 85,859,271 MRF address evidence rows, and
  15,500 plan search summary rows. `address_archive_v2` held 6,056,982 rows,
  with 1,322,413 carrying the MRF source bit. Only two published MRF address
  rows had null `address_key`; both came from malformed source ZIP values
  (`postal_code='.'`) in the Intermountain provider feed.
- The controlled Python 3.14 NPPES run for import
  `addrcanon_npi_20260613040046` loaded full June 2026 NPPES data in
  1369.91s. Its initial shutdown exposed two address-canonical parity bugs:
  Python-prekeyed rows skipped SQL repair, and Rust treated `STE T .` as unit
  `stet` while Python/SQL treat it as street text. After the source fixes,
  patched shutdown job `hp-npi-shutdown-patched-20260613050521` completed in
  26m12s. `canonical_address_resolve` took 780.512s, published 19,707,579
  `npi_address` rows, 9,606,683 `npi` rows, 12,019,685 taxonomy rows, and
  154,583 phone-staffing rows, with 5,351,601 NPPES keys represented in
  `address_archive_v2`. Runtime API smoke passed healthcheck, `npi/id`, and
  import summary, and Redis `arq:queue`, `arq:queue:NPI`, and
  `arq:queue:NPI_finish` were all 0. The remaining shutdown hotspots were SQL
  restamping (~13 minutes), `do_business_as` enrichment (225.619s),
  taxonomy-array enrichment (242.907s), and `npi_address` vacuum/analyze
  (146.505s).

## Rollback

1. Disable canonical writes by unsetting `HLTHPRT_ADDRESS_CANON_SOURCES`.
2. Restart API and ARQ workers.
3. Leave `address_archive_v2` in place unless a full migration rollback is
   explicitly requested. The new columns are nullable and do not affect legacy
   checksum readers.
