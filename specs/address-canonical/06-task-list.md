# 06 Task List

Sizes: S = hours, M = 1–2 days, L = several days. Every task lists its
acceptance criteria (AC). Tasks within a phase are ordered; phases gate each
other as described in 05.

## Phase 0 — standalone bug fixes (no dependency on the rest)

- [x] **0.1 (S)** Fix `_conflict_targets` ordering in `process/ext/utils.py:720-729`
      to prefer `__my_index_elements__`.
      AC: unit test proving an `MRFAddress` batch with two NPIs sharing one
      checksum keeps both rows; existing importer tests green.
- [x] **0.2 (S)** Stop street-line borrowing for FFS child rows in
      `process/entity_address_unified.py:439-443` (NULL parent street lines).
      AC: unit test: FFS-derived unified rows have NULL street lines; no
      chimera row in a rebuild over fixture data.
- [x] **0.3 (M)** Register `mrf_address` as unified source select #6
      (priority 5, marketplace evidence arrays mapped).
      AC: rebuild over fixtures shows marketplace addresses in
      `entity_address_unified` with `address_sources @> '{mrf}'`.
      Status: DB-backed fixture rebuild passes via
      `tests/test_address_canonical_db.py`; the fixture now uses the actual
      `mrf_address` contract, where taxonomy/procedure/plan/medication arrays
      are not present on the MRF address table and are borrowed from primary
      `npi_address` only when available.
- [x] **0.4 (S)** Raise unified publish min-rows gate to 1M
      (`HLTHPRT_ENTITY_ADDRESS_UNIFIED_MIN_ROWS`, default 1_000_000).
      AC: config test; gate blocks a small build in test mode unless the env
      override lowers it.
- [ ] **0.5 (M)** Production instrumentation report (read-only SQL, run before
      any identity change): actual CRC32 collisions present in `npi_address` /
      `address_archive` (same checksum, different address text); duplication
      factor per table (raw rows vs approx-normalized distinct, using the
      existing unified normalizer expressions); same normalized address seen
      across N sources; same address with conflicting units.
      AC: numbers in a committed report under `reports/`; gives the baseline
      that Phase 2–5 improvements are measured against.
      Status: local dev read-only baseline added at
      `reports/address_canonical_baseline_2026-06-11.md`; exact full-table
      unit-conflict audit completed for available local `npi_address` and
      `address_archive` tables with session-local temp tables. This dev snapshot
      does not include every source table (`mrf_address` is absent), so rerun the
      same baseline in the target production snapshot before Phase 3 cutover.
      Current verification: expanded impacted importer/API/control suite
      `346 passed`; full local suite `942 passed` on Postgres 5440 after the
      2026-06-11 review fixes.

## Phase 1 — foundations

- [x] **1.1 (M)** Alembic migration: `addr_*_v1` SQL functions (inlined USPS
      Pub. 28 suffix/unit/directional/state tables, no table reads).
      AC: `IMMUTABLE PARALLEL SAFE`; function-based smoke queries in migration
      test.
      Status: functions and DB smoke/parity tests are implemented. USPS Pub. 28
      C1 street suffix forms, C2 secondary unit designators, Appendix B
      state/possession/military codes, and I4 English/Spanish directionals are
      embedded in `process/ext/address_pub28.py` and compiled into SQL CASE
      expressions by the migration; runtime code performs no USPS fetches and
      no database lookup-table reads. Current verification: DB test asserts the
      generated SQL functions are `IMMUTABLE` / `PARALLEL SAFE` and smoke-tests
      expanded Pub. 28 examples.
- [x] **1.2 (M)** Golden-corpus fixture + parity test: Python oracle in
      `process/ext/address_canon.py` vs SQL, ≥200 curated cases (unicode,
      `<UNAVAIL>`, NULL vs `''`, ZIP+4, suite forms, `#` forms, PO Box, rural
      route, foreign country, `TEXAS`/`TX`, city punctuation; split-line vs
      joined-line unit placement pairs that MUST converge).
      AC: byte-identical keys across implementations, including the exact
      uuid derivation (`encode(substring(sha256(convert_to(k,'UTF8')) from 1
      for 16),'hex')::uuid` ≡ `uuid.UUID(bytes=sha256(k.encode()).digest()[:16])`);
      corpus frozen in `tests/fixtures/`.
      Status: `tests/fixtures/address_canonical_golden.json` now contains 279
      explicit frozen cases, with expected identity, address, and premise keys
      materialized into the cases themselves. DB parity test passes against
      Postgres, and the R8/R9 review gaps are fixed.
- [x] **1.3 (S)** Alembic migration: `address_archive_v2` shell +
      `address_checksum_map` + `address_checksum_collision` (03 DDL); geo
      index created conditionally on PostGIS presence.
      AC: migration up/down clean on empty and on production-like schema,
      with AND without PostGIS installed.
      Status: upgrade and downgrade/upgrade paths validated against a
      disposable Postgres database stamped at the prior revision; no-PostGIS
      path validated, checksum bridge PK fixed to `PRIMARY KEY(checksum)`.
      With-PostGIS matrix validated on
      `healthporta_address_canon_postgis_test` with PostGIS `3.6.1`; upgrade
      and downgrade/upgrade paths created `address_archive_v2_geo_idx`.
- [x] **1.4 (M)** `stamp_address_keys` + `resolve_into_archive` helpers with
      advisory lock, `SET LOCAL work_mem`/`statement_timeout`, ResolveStats
      (staged / distinct_keys / inserted / provenance_updates / exclusion
      reason buckets: missing_zip, missing_state, missing_street,
      unsupported_country, ambiguous_unit / unit_conflicts), live-progress
      events, cooperative cancellation (`raise_if_cancelled` between stamping
      shards and between statements), statement C collision tripwire (abort +
      alert on any hit, same transaction), and the target table read from
      `HLTHPRT_ADDRESS_ARCHIVE_TABLE` (default `address_archive_v2`) — never
      hardcoded.
      AC: unit tests over fixture staging tables: dedup, conflict no-op on
      re-run, provenance bits OR-ed, display priority election deterministic,
      cancel honored mid-stamp, statement C abort path exercised with a
      synthetic collision, reason buckets counted; premise_key populated
      alongside address_key.
      Status: helper, advisory lock, settings, stats, table targeting,
      phase-based live-progress events, DB-backed dedup/provenance test, reason
      buckets, synthetic collision guard, and failed progress alert on collision
      are implemented; cancel-path assertions are covered. `ResolveStats` also
      now carries Phase 2 gate counters for eligible street+state+ZIP rows,
      eligible rows still missing `address_key`, and normalized gate violations;
      bounded multi-source sample rows for eyeball review; and a live warning
      event when an eligible row remains unstamped. Stamp sharding now also has
      the `HLTHPRT_ADDRESS_CANON_STAMP_SHARDS` env override documented and
      covered by a focused unit test. Current verification:
      `tests/test_address_canonical_db.py` passes against disposable Postgres
      `5440`.
- [x] **1.5 (S)** Add nullable `address_key` to subclass models + per-importer
      index-list entries.
      AC: column appended LAST in each table's column order (assert in test —
      protects `api/endpoint/npi.py:2700-2711` positional unpack).
- [x] **1.6 (S)** Deploy runbook note + automation: restart arq workers after
      model deploy (staging-class cache).
      AC: documented in `docs/devops/`; control-plane restart hook if available.
      Status: documented in `docs/devops/address-canonical.md`; no worker
      restart hook is currently exposed by the control plane.

## Review findings R1–R17 (2026-06-11) — fix before production enablement

Source: adversarial multi-agent review of the Phase 0–3.1 implementation; every
finding below was independently verified against the code, several **live
against the deployed SQL functions**. Groups are in recommended execution
order. NOTE: Group 2 changes every computed `address_key` — after it lands,
wipe/re-stamp any dev data and re-run the 3.1 migration smoke (its 916,949-row
result was produced with the buggy identity).

### Group 1 — make the safety net real (do FIRST, makes later fixes provable)

- [x] **R8 (S)** Add address-canonical tests to CI. `.github/workflows/ci.yml`
      runs a hardcoded 7-file list; `test_address_canonical_unit.py`,
      `test_address_checksum_schema.py`, `test_address_canonical_db.py` are
      absent, and the DB suite silently skips (8 skipped) unless
      `HLTHPRT_DB_DATABASE` contains "test" (conftest defaults `healthporta`).
      AC: unit + schema tests run in CI now; DB suite runs against a CI
      Postgres service (or a follow-up task with an explicit date); a failing
      golden-corpus case fails CI.
      Status: unit/schema tests were added to the existing CI job; DB parity
      tests now run in a dedicated Postgres service job using the focused
      parent-revision stamp + canonical-foundation upgrade path.
- [x] **R9 (M)** Harden the golden corpus. Freeze EXPECTED `identity_key` /
      `address_key` per fixture case (generate once, hand-review, commit) so
      parity can't drift in lockstep via the shared `address_pub28.py` maps;
      assert convergence for every equivalence pair including NULL-vs-`''`
      (today: 3 groups / 8 of 264 cases asserted); add the adversarial cases
      this review surfaced: designator-prefix streets (`100 Florida Ave Fl 2`,
      `12 Steiner St Ste 5`, `1 United Nations Plz Unit 5`), units on BOTH
      lines (`'123 Main St Ste 200' / 'Apt 5'`, `'… Ste 200' / 'Suite'`),
      lowercase/mixed-case countries (`canada`, `Usa`), foreign provinces
      (`British Columbia` + `V6B 2W9`), `meadow`/`mdw`/`mdws`.
      AC: reverting any Group 2 fix makes the corpus fail (mutation-check each).
      Status: fixture now has 279 cases with frozen expected identity,
      address, and premise keys; unit and DB tests assert frozen values plus
      equivalence groups.

### Group 2 — identity correctness (false merges; verified live)

- [x] **R1 (M, CRITICAL)** SQL `addr_street_norm_v1` truncates the street at
      the FIRST substring occurrence of a unit designator
      (`strpos`, foundation migration :292): `'100 Florida Ave Fl 2'` →
      street `'100'`; `'100 Florida Ave Fl 2'` and `'100 Fleet St Fl 2'`
      produce THE SAME address_key. Python oracle (`tail.start()`) is correct —
      production SQL diverges from the oracle. Fix: strip the tail with the
      same end-anchored regex semantics as Python.
      AC: live SQL `addr_street_norm_v1('100 Florida Ave Fl 2','')` =
      `'100floridaave'`; Florida/Steiner/United-Nations keys distinct; R9
      corpus cases green in BOTH implementations.
- [x] **R2 (M, CRITICAL)** Unit extraction decided twice: when line1's tail
      AND line2 both look like units, the line1 suite is deleted from
      street_norm but line2 wins unit_norm — `('123 Main St Ste 200','Apt 5')`
      and `('… Ste 300','Apt 5')` MERGE (both Python and SQL,
      `address_canon.py:231-251` + migration :207-307). Variant: line2 a bare
      designator (`'Suite'`) still strips line1's `Ste 200` while
      unit_norm=''. Fix: ONE extraction decision shared by both functions —
      street_norm removes exactly (and only) the span unit_norm extracted;
      unit from line2 ⇒ line1 tail intact; ambiguous ⇒ strip nothing.
      AC: Ste-200-vs-300 cases distinct; ambiguous case keeps `ste200` text in
      street_norm; corpus cases green.
- [x] **R3 (S)** `addr_country_code_v1` strips lowercase BEFORE `upper()`
      (migration :142-146): `'canada'`→`'US'` (false-merge direction, escapes
      the unsupported_country bucket), `'Canada'`→`'C'`, `'Usa'`→`'U'`;
      diverges from the Python oracle. Fix order of operations.
      AC: `'canada'`→`'CANADA'`, `'Usa'`→`'US'`, parity with Python on R9 cases.
- [x] **R4 (M)** Enforce ONE non-US rule end-to-end: recommended — country ≠
      US ⇒ NULL identity (matches the exclusion-bucket semantics), in BOTH
      implementations; today foreign rows get keys (`'M5V 2T6'`→zip5 `'526'`),
      are inserted with garbage components, are counted `unsupported_country`
      anyway, and unmappable provinces (`'BRITISH COLUMBIA'`, 15 chars)
      overflow `state_code varchar(8)` ABORTING the whole NPPES resolve
      (spec caution 6). Belt: clamp state_code writes regardless.
      AC: foreign rows → NULL key, counted, never inserted; resolve completes
      with foreign rows staged; `stats.inserted` asserted in the reason-bucket
      test (today it asserts counts only while rows still insert).
- [x] **R5 (M)** Per-line unit extraction diverges from the spec's
      joined-line rule when line2 continues the street:
      `('123 Main St Ste 200','West Wing')` splits from the joined form. Safe
      direction, but breaks the MUST-converge guarantee between sources that
      wrap lines differently. Implement extraction on the joined string (line2
      first, else tail of the JOINED text).
      AC: both forms converge; corpus pair added.
- [x] **R6 (S)** Pub. 28 map idempotency: `'meadow'`→`'mdw'` but
      `'mdw'`→`'mdws'` — same street splits by spelling. Add a unit test
      asserting f(f(x)) = f(x) over every map entry; fix violators.
- [x] **R7 (S)** Remove (or parity-assert) the dead shadow normalization
      tables inside `address_canon.py:48-154` — drift hazard vs
      `address_pub28.py`.

### Group 3 — abort/guard semantics

- [x] **R10 (S)** Statement C runs BEFORE the insert and only joins the
      archive — a same-batch collision (two identities, one key, neither in
      archive) is silently dropped by `DISTINCT ON`; the
      `identity_collisions` gate input is hardcoded `0`
      (`address_canon.py:633-668` vs `:673-710`, `:815`). Add an
      intra-staging check (`GROUP BY address_key HAVING
      count(DISTINCT identity_key) > 1`), wire the stat, abort on hit.
      AC: forged same-batch collision test (pre-stamp two staged rows with one
      key, two identities) aborts; archive-vs-staging case still aborts.
- [x] **R11 (S)** `pharmacy_license` swallows resolve failures — INCLUDING
      the statement C abort — in its per-state try/except
      (`pharmacy_license.py:2582-2603`, `:3140-3263`); the run is marked
      succeeded while that state's records are dropped. Let resolve failures
      fail the run like every other importer.
      AC: test — induced resolve failure marks the run failed.
- [x] **R12 (M)** KEY_V2 hazards: (a) the spec-mandated row-count-delta abort
      guard (<80% of previous publish) is NOT implemented
      (`entity_address_unified.py:791-795`, `:2032-2041`); (b) when
      `addr_key_v1` is absent, the builder publishes NULL address_keys forever
      with only a `logger.info` — with KEY_V2 on, it silently degenerates to
      v1 checksum grouping. Implement the delta guard; emit a warning-status
      live event on canon-unavailability; refuse KEY_V2 when functions are
      absent.
      AC: tests for all three behaviors.
- [x] **R13 (S)** Display election tie-break is not total
      (`address_canon.py:503-515` ends at length comparisons): equal-length
      different texts pick nondeterministically; canonical display can flap
      between runs. Append full text columns as final ORDER BY keys.
- [x] **R14 (S)** Add the TRUE idempotent re-run test (same source_bit AND
      same priority ⇒ inserted=0, provenance_updates=0) — the current rerun
      test uses a different bit; document the accepted `last_seen_at`
      trade-off (guard skips same-source re-imports, so last_seen is NOT
      bumped on unchanged re-import — intentional, but write it down in 03).

### Group 4 — API/cutover gating (bites the moment production migrates)

- [x] **R15 (L)** v2 read/write paths activate on mere EXISTENCE
      (`to_regclass` + function probe, `api/endpoint/npi.py:3636-3647`) —
      Phase 4 executed during Phase 1: (a) API geocode write abandons the
      legacy archive immediately (`:3693-3738`); (b) NPPES geo backfill
      switches to the EMPTY v2 table (`process/npi.py:905-931`) — the ~3M
      legacy geocodes stop propagating until the unrun Phase 3 job;
      (c) v2 read returns geocode-less rows and blocks the legacy fallback
      (`:3649-3675`, no `lat IS NOT NULL`) ⇒ paid re-geocoding per request;
      (d) v2 upsert has no DISTINCT over the shared-checksum source ⇒
      `CardinalityViolation`, swallowed by fire-and-forget (`:3695-3737`);
      (e) `address_key` leaks into `/npi/id` + `/npi/all` payloads
      post-migration (spec 03 §6: internal); (f) zero test coverage — the
      `hasattr(db,'first')` guard routes every existing test to the legacy
      branch; (g) per-address uncached catalog probes on the hot path.
      Fix: explicit cutover flag (e.g. `HLTHPRT_ADDRESS_ARCHIVE_CUTOVER`) +
      geocode-coverage gate instead of existence; `lat IS NOT NULL`
      fall-through to legacy; DISTINCT/dedupe in the upsert source; strip
      `address_key` from payloads; cache the probes; FakeDB-with-`first`
      tests covering both v2 branches.
      AC: with the flag off and the migration applied, behavior is
      byte-identical to legacy; with it on, shared-checksum geocode write
      succeeds; payload snapshot test proves no `address_key` field.
- [x] **R16 (M)** Narrow `20260610143000_address_checksums_bigint`:
      `ALTER TYPE` rewrites huge live tables (+ all indexes) under
      `ACCESS EXCLUSIVE` in one transaction — but `npi_address`,
      `mrf_address`, `mrf_address_evidence`, `entity_address_unified` are
      swap-published and inherit the wider type from the model at their next
      import for free. Keep the ALTER only for durable tables
      (`address_archive`; `doctor_clinician_address` if not swapped before
      cutover), and verify the downgrade-comment claim that production
      contains out-of-range checksum values (record evidence in the
      migration docstring).
      AC: migration touches only durable tables; lock-window estimate in the
      runbook; out-of-range claim verified or the migration dropped.
      Status: migration now rewrites only `address_archive.checksum`; local
      production-like evidence recorded in the migration docstring
      (`address_archive`: 916,949 rows, 9 out-of-range signed-int checksums,
      max 4,150,343,390).

### Group 5 — performance protection

- [ ] **R17 (M)** `resolve_into_archive` re-runs full normalization over the
      staging table ~6× per call (keyed/coverage/reason CTEs + count + A + B)
      — threatens the NPPES ≤10-minute budget (task 2.4 AC). Materialize the
      keyed/dedup projection ONCE per resolve into a temp table
      (`ON COMMIT DROP`) and reuse it in all statements; measure on a
      production-size staging table.
      AC: ≤2 normalization passes per resolve; NPPES-scale timing recorded.
      Status: `resolve_into_archive` now dedupes by staged `address_key`,
      normalizes representative rows into one keyed projection, indexes the
      temp projection, and reuses it for coverage, reason buckets, collision
      checks, inserts, and provenance updates. A 916,417-row real-copy resolve
      smoke before the 2026-06-12 dedupe-first rewrite
      completed in 66.809s with 780,372 distinct keys, 0 gate violations, and 0
      identity collisions. A full local NPPES rebuild on 2026-06-11 published
      19,707,579 `npi_address` rows with 19,694,747 load-time address keys,
      resolved 5,386,849 canonical archive rows, and completed in 3:40:36; this
      proves correctness but also shows Phase 2.4 wall-time is not yet met. A
      full dev ordered run (`nucc` first, then `npi`) with
      `HLTHPRT_ADDRESS_CANON_SOURCES=nppes`,
      `HLTHPRT_ADDRESS_ARCHIVE_TABLE=address_archive_v2`, and
      `HLTHPRT_ADDRESS_ARCHIVE_CUTOVER=true` completed in 1:01:10, published
      the same 19,707,579 address rows with 19,694,747 address keys, 0
      normalized-eligible missing keys, 0 collisions, and 154,583
      `npi_phone_staffing` rows. A 2026-06-12 follow-up added an optional
      Rust-backed materialization path behind
      `HLTHPRT_ADDRESS_CANON_RUST_MATERIALIZE=true`; Rust transforms the
      deduped raw COPY projection into the keyed temp-table COPY file while
      Python keeps the database transaction, collision checks, and archive
      writes. Focused verification: Rust golden-corpus tests `37 passed`
      after adding the actual NPI `2ND FLOOR-PULMANARY` parity regression,
      Python address unit/schema tests `40 passed`, and DB-backed resolver
      tests `14 passed`, including SQL-vs-Rust temp materialization parity.
      Dev-server actual-data follow-up on image
      `dev-address-canon-rust-20260612214227` passed: 1M NPI rows resolved
      487,574 distinct keys in 9.752s wall time with Rust COPY transformation
      at 3.106s, 0 key mismatches, and 0 source-bit misses; source-specific
      scratch resolves also passed for NPI 250k, marketplace MRF 500k, CMS
      doctors 250k, and all keyed facility-anchor rows. Dev deploy was then
      advanced through `dev-address-canon-rust-20260613020811` and finally to
      `dev-address-canon-rust-20260613050521` for the NPPES timing follow-up
      and MRF recovery work. The newer images add configurable NPI worker
      concurrency, conflict-safe MRF formulary aggregate upserts, configurable
      `HLTHPRT_MRF_QUEUE_READ_LIMIT` support, DB-pool sizing for parallel
      address stamping, BigInteger ORM coverage for MRF provider
      NPI/network-checksum staging plus NPPES NPI identifiers, and Rust parity
      fixes for actual NPI `2ND FLOOR-PULMANARY` and `STE T .` addresses. The
      MRF parallelism follow-up found that multiple normal `process.MRF --burst`
      processes with a larger read window are safer than one very wide ARQ
      process, which can exhaust the per-process DB pool. The current dev
      profile pins `HLTHPRT_ADDRESS_CANON_STAMP_SHARDS=24`,
      `HLTHPRT_ADDRESS_CANON_STAMP_CONCURRENCY=16`, and
      `HLTHPRT_DB_POOL_MAX_SIZE=32` so canonical address stamping can use more
      of the 24-core dev host.
      Final MRF timing evidence for `addrcanon_mrf_20260612215839`: patched
      finish `hp-mrf-finish-patched-20260613020811` completed in 3208.32s, and
      the full import delta was 5:51:04.280138. Published counts were 15,500
      plans, 25,672,487 plan NPI rows, 16,302,076 MRF address rows,
      85,859,271 MRF address evidence rows, and 15,500 plan search summary
      rows; `address_archive_v2` had 1,322,413 rows with the MRF source bit.
      Final NPPES evidence for `addrcanon_npi_20260613040046`: full June 2026
      NPPES load completed in 1369.91s, then patched shutdown
      `hp-npi-shutdown-patched-20260613050521` completed in 26m12s with
      `canonical_address_resolve` at 780.512s. Published counts were 9,606,683
      NPI rows, 12,019,685 taxonomy rows, 19,707,579 address rows, and 154,583
      phone-staffing rows; `address_archive_v2` had 5,351,601 rows with the
      NPPES source bit. Healthcheck, `npi/id/1154324382`, and import API smoke
      passed, with NPI queues empty afterward.

### Full-scale NPPES performance follow-ups from 2026-06-11 dev run

These are not blockers for correctness, but they must be fixed before claiming
the Phase 2.4 NPPES wall-time AC. They were observed during a real local
`npi` rebuild on `healthporta` with `HLTHPRT_ADDRESS_CANON_SOURCES=nppes`,
`HLTHPRT_ADDRESS_ARCHIVE_TABLE=address_archive_v2`, and
`HLTHPRT_ADDRESS_ARCHIVE_CUTOVER=1`, and confirmed on the dev host during the
ordered `nucc` -> `npi` full import.

- [ ] **P1 (M)** Move full-scale `resolve_into_archive` materialization out of
      the Python per-row loop, or dedupe before Python normalization. Evidence:
      the load now precomputes `address_key`, but the resolver still spent
      roughly 20 minutes materializing 19.7M staged `npi_address` rows into
      `address_archive_resolve_keyed` before the SQL collision/upsert phase.
      AC: production-size NPPES resolve timing recorded; resolver emits
      durable progress at least every minute; materialization no longer hides
      progress inside one long CPU-bound Python loop.
      Status: source now uses staged-key dedupe before materialization and the
      Rust COPY materializer is deployed in dev. A 2026-06-12 actual-data
      1M-row NPI scratch resolve inserted 487,574 distinct keys in 9.752s wall
      time; Rust itself transformed 45.3 MB of COPY input to 143.8 MB in
      3.106s. A follow-up multi-source smoke passed NPI, marketplace MRF, CMS
      doctors, and facility anchors with 0 key mismatches, 0 source-bit misses,
      and no gate violations. Full staged NPPES shutdown later passed the same
      gate with Rust materialization after fixing the actual `STE T .` parity
      regression. Remaining before this item is fully closed: split archive
      resolve materialization vs archive upsert timing from the helper rather
      than only the combined `canonical_address_resolve` phase.
- [ ] **P2 (M)** Avoid full-table rewrites in NPI shutdown enrichment where the
      values can be loaded once or updated only for matching rows. Evidence:
      `UPDATE mrf.npi_20260611 SET do_business_as = ARRAY[]...` rewrote the
      9.6M-row staging table, then the DBA enrichment update rewrote rows again.
      AC: DBA fields are initialized during load or with a single targeted
      update; no unconditional whole-table reset remains in the NPI publish path.
      2026-06-13 dev timing: `do_business_as_enrichment` still took 225.619s
      during patched shutdown for `addrcanon_npi_20260613040046`.
- [ ] **P3 (L)** Replace the `npi_address.taxonomy_array` full-table update
      with load-time assignment, a side table, or a staged CTAS/merge strategy.
      Evidence: `UPDATE mrf.npi_address_20260611 SET taxonomy_array=...` over
      19.7M staged address rows ran for over an hour and grew the staged table
      from ~7.1 GiB to ~12 GiB. AC: taxonomy enrichment timing and table/WAL
      growth recorded; total added NPPES shutdown time is within the Phase 2.4
      budget.
      2026-06-13 dev timing: `taxonomy_array_enrichment` took 242.907s on the
      full 19.7M-row staged address table.
- [ ] **P4 (S)** Prevent autovacuum contention on transient staging tables
      during large import shutdown, without publishing a table that permanently
      disables autovacuum. Evidence: autovacuum repeatedly started on
      `mrf.npi_20260611` while NPI shutdown was doing large staged writes and
      index/enrichment work. AC: staging tables use temporary reloptions or the
      import explicitly analyzes at controlled points; final published tables
      keep normal autovacuum settings.
- [ ] **P5 (S)** Add a machine-readable shutdown phase timer to NPI logs/live
      progress for each heavy step: raw load, address resolve materialization,
      archive upsert, DBA enrichment, taxonomy array enrichment, index creation,
      vacuum/analyze, and publish swap. AC: a completed NPPES smoke report can
      be generated from the run output without manually sampling
      `pg_stat_activity`.
      Status (2026-06-12): partial. NPI shutdown now emits
      `NPI_SHUTDOWN_PHASE_*` log lines, live progress updates, and final
      `npi_shutdown_phase_timings` control-run metrics for canonical address
      resolve, DBA enrichment, taxonomy/geocode/plan/procedure/medication
      enrichment, phone staffing, index creation, vacuum/analyze, and publish
      swap. `ResolveStats` now includes `elapsed_seconds`. Remaining before
      this AC is closed: split archive resolve materialization vs archive
      upsert timings from the helper and include raw-load timing in the same
      generated smoke report.
- [ ] **P6 (M)** Avoid the full-table geocode enrichment rewrite on
      `npi_address` shutdown. Evidence: after taxonomy enrichment completed,
      NPI ran `UPDATE mrf.npi_address_20260611 ... FROM
      mrf.address_archive_v2` over all keyed rows with geocodes; the staged
      table was already ~13 GiB and the update remained active after 30+
      minutes. AC: geocodes are assigned during load, applied with a bounded
      keyed delta update, or merged through a CTAS/publish strategy with
      recorded timing and WAL/table-growth evidence.
- [ ] **P7 (M)** Revisit the published `npi_address` index set for required
      API/query coverage only. Evidence: obsolete historical geo/coverage
      indexes on the previously published table consumed tens of GiB and were
      not needed for the current import run; the import still builds many
      heavy indexes after enrichment. AC: every retained index maps to a live
      endpoint/query plan, duplicate/obsolete indexes are removed from the
      importer definition, and index build timings are included in the NPPES
      smoke report.
- [ ] **P8 (S)** Make NUCC and address-canonical prerequisites explicit in the
      NPI runbook/control-plane dependencies. Evidence: the dev server initially
      had `mrf.nucc_taxonomy` present but empty, so an NPI rebuild published
      correct core rows but 0 `npi_phone_staffing` rows. A later ordered run
      without `HLTHPRT_ADDRESS_CANON_SOURCES` populated staffing but published
      0 `address_key` values. AC: full NPI runs either validate prerequisites
      before starting or fail fast with an actionable message; import-control
      schedules `nucc` before `npi`; canonical validation requires
      `HLTHPRT_ADDRESS_CANON_SOURCES=nppes` and archive cutover flags when the
      address-canonical path is expected.
      Initial read-only audit flags for plan-backed review:
      `npi_address_idx_primary` may duplicate the table PK; the full
      `(type, state_name, city_name, npi)` index and partial primary
      `(state_name, city_name, npi)` index overlap; multicolumn GIN
      `(taxonomy_array, plans_network_array)` may be replaceable by bitmap-AND
      over the single-column GIN indexes; published `address_key` index has no
      known active published-table API/helper query; and callers should align
      ZIP expressions with the `LEFT(postal_code, 5)` index if that index stays.
- [ ] **P8 (M)** Move pricing-derived procedure/medication enrichment out of
      post-load full-table updates. Evidence: after geocode enrichment, local
      NPI shutdown ran full staged-address rewrites for `procedures_array`
      from `pricing_provider_procedure` and `medications_array` from
      `pricing_provider_prescription`, adding more long IO/WAL-bound work after
      the raw load had already completed. AC: procedure and medication arrays
      are populated during load, joined through compact side tables, or merged
      through a CTAS/publish strategy with timings captured in the NPPES smoke
      report.

## Re-review S1–S10 (2026-06-11, round 2) — verification of the R-fixes

Verdicts from the adversarial re-review: **R1, R2, R3, R4, R5, R7, R8(content),
R9, R13 genuinely FIXED** — R1/R2 verified LIVE against the deployed SQL
functions with the exact AC inputs, and R9's mutation check was executed for
real (installing the old strpos bug into the DB makes the corpus fail).
Remaining items below are the partial-fix remainders + new findings.

> **COORDINATION (2026-06-11, parallel review):**
> S2-S10 have local worktree fixes and focused test evidence. S1 remains open
> because its AC requires a committed/pushed tree and first green GitHub CI run;
> that source-control step is still owner-controlled.

- [ ] **S1 (S, BLOCKER)** `.github/workflows/ci.yml` is UNTRACKED — CI has
      never executed on GitHub (gh shows zero runs); the entire safety net is
      theoretical until committed. Also add
      `tests/test_process_pharmacy_license_unit.py` (R11's test) to the CI
      file list, and a `redis` service or drop the dangling
      `HLTHPRT_REDIS_ADDRESS` env. AC: first green CI run visible on GitHub.
      Status (2026-06-11): CONTENT DONE — pharmacy unit tests + the new
      S5 run-failure test added to the unit job; dangling Redis env dropped
      (DB suite never touches Redis without a run_id). REMAINING: the commit
      itself — must include the full feature tree, owner's call.
- [x] **S2 (M, MAJOR)** Resolve writes the STAMPED `address_key` next to a
      Python-RECOMPUTED `identity_key` with no consistency check
      (`address_canon.py:637-655`): a stale stamp (pre-R1/R2 keys persisted in
      `npi_address`, `pharmacy_license_record_v1`, `entity_address_unified`…)
      or any SQL↔Python divergence inserts archive rows where
      `address_key ≠ sha256(identity_key)[:16]` — unreachable by readers
      (geocode lookups recompute the key) and PERMANENTLY poisoning future
      imports of that address with unhandled `identity_key` UNIQUE
      IntegrityErrors. Fix (cheap): in the keyed batch loop, recompute
      `key_from_identity(identity_key)` and abort on mismatch with the stamp
      (one sha256/row). Ops half: the R-fixes were edited INTO the
      already-applied alembic revision — any DB that applied the pre-fix
      revision keeps the OLD function bodies (`alembic upgrade head` is a
      no-op); ship a `CREATE OR REPLACE` re-apply path (new revision or
      runbook command) + wipe/re-stamp stale `address_key` stamps everywhere.
      AC: mismatch test aborts resolve; runbook documents function re-apply +
      stamp wipe; dev DBs re-stamped.
      Status (2026-06-11): resolver aborts on stamped-key/identity
      mismatch, writes the recomputed key into the resolve projection, preserves
      missing-stamp gate counters with `staged_address_key`, and has DB
      regression coverage. Added
      `20260611130000_reapply_address_canonical_functions`; disposable Postgres
      `5440` upgraded through it and `tests/test_address_canonical_db.py`
      passed.
- [x] **S3 (S)** SPEC DECISION — Python is now a production identity producer
      (load-time stamping `process/npi.py:101`, resolve batch loop), but
      02/03 still say "SQL is the ONLY production implementation; Python never
      a producer" and `address_canon.py:3-7`'s docstring still claims
      tests-only. Either revert to SQL-producer or (recommended, given frozen
      corpus + CI parity + S2 guard) update spec 02 §SQL-only, 03 §2, and the
      module docstring to "dual implementation, byte-parity enforced by frozen
      corpus + CI + runtime stamp check". AC: spec and code claims agree.
      Status (2026-06-11): spec 02/03 and `address_canon.py` now
      describe the dual SQL/Python producer model and runtime stamp guard.
- [x] **S4 (S)** R6 remainder: idempotency test double-applies
      `_street_token_norm`, masking even-length cycles, and skips the UNIT map
      entirely. Assert `map.get(v, v) == v` for every value of all three RAW
      maps (suffix, directional, unit). AC: single-application idempotency
      pinned.
      Status (2026-06-11): unit test now checks suffix, directional, and
      unit raw maps.
- [x] **S5 (S)** R11 remainder: the run-failure path has no test (and
      `pharmacy_license_start` carries `# pragma: no cover`). Drive
      `PharmacyLicenseCanonicalAddressError` through the per-state loop and
      assert `_upsert_run`/`mark_control_run` record `failed` + re-raise.
      Status (2026-06-11): `tests/test_process_pharmacy_license_unit.py`
      and `tests/test_pharmacy_license_run_failure_unit.py` drive the typed
      error through `pharmacy_license_start`'s state loop with persistence
      stubbed; assert run/control/snapshot/coverage failed and the exception
      re-raised. Added to ci.yml unit job.
- [x] **S6 (S)** R12 remainder: the canon-unavailability warning event has
      zero test coverage, and it is gated on `if run_id:` — ad-hoc runs
      degrade with only a logger.info. Test the warning; decide whether
      non-control runs should also surface it.
      Status (2026-06-11): warning event is covered for controlled runs;
      ad-hoc degradation now logs at warning level.
- [x] **S7 (S)** R14 remainder: write the `last_seen_at` trade-off into 03
      (provenance UPDATE guard skips same-source re-imports, so last_seen is
      NOT bumped on unchanged re-import — intentional).
      Status (2026-06-11): documented in `03-data-model.md`.
- [x] **S8 (M)** R15 remainders: (f) zero flag-ON tests — cover v2 read hit,
      geocode-less miss→legacy fall-through, and the shared-checksum upsert
      (FakeDB with `first`); (g) fix the `_v2_archive_table` memoization race
      (`v2_archive_table_resolved = True` is set BEFORE the first await at
      `api/endpoint/npi.py:3663-3665`, so concurrent `_update_address` tasks
      in the same request silently fall to legacy with the flag ON) and
      document `HLTHPRT_ADDRESS_ARCHIVE_CUTOVER` in `.env.example` + runbook.
      Status (2026-06-11): `_v2_archive_table` uses an async lock and
      sets the cache only after probes complete; flag-on tests cover v2 read hit
      under concurrent addresses, geocode-less v2 fallback to legacy, and
      deduped v2 upsert. `.env.example` and runbook document the cutover flag.
- [x] **S9 (S)** R16 remainder: add the bigint ALTER lock-window estimate to
      the runbook (916,949-row archive ⇒ measure once on the dev copy); fix
      the stale plural "tables" wording post-narrowing.
      Status (2026-06-11): measured empirically on the disposable
      Postgres — synthetic 1M-row table + unique index, `ALTER ... TYPE
      bigint` = 1.68 s. Runbook now states the narrowed single-table scope and
      a "under one minute, budget a 5-minute window" production estimate for
      the ~3M-row archive, plus the API-write contention note.
- [x] **S10 (S)** Minor sweep: wire-or-remove the dead `identity_collisions`
      gate input (`address_canon.py:924`); clamp `addr_state_code_v1` in the
      API v2 upsert; geo-index guard probes unqualified `st_makepoint` but
      creates with `public.ST_MakePoint` (index silently skipped on non-public
      PostGIS — align the probe); trailing punctuation defeats unit
      extraction (`'Ste 200.'` splits from `'Ste 200'` — safe direction; add
      corpus case + allow trailing punct in the tail regex); unify the three
      advisory locks around the archive; delete dead `_index_name` (randomized
      `hash()`) in the foundation migration; assert `equivalence_groups`
      fixture cases are mirrored into `explicit_cases`; delete stray
      `tests/test_process_pharmacy_license_unit.pyc`.
      Status (2026-06-11): removed the dead `identity_collisions` gate
      input and `_index_name`; clamped API v2 `state_code`; qualified the
      PostGIS probe; unified archive advisory lock keys; added
      trailing-punctuation corpus cases and equivalence-group mirror assertion;
      deleted the stray `.pyc`.

R17/P1 note: the temp-table consolidation meets R17's literal AC (≤2 passes,
timing recorded) but the motivating Phase 2.4 budget fails — ~20 min Python
materialization of 19.7M rows; the honest unchecked status is correct and the
work continues under P1 (consider sharding the batch loop across workers, or
pre-deduplicating staged rows by `address_key` BEFORE Python normalization —
19.7M rows → ~5.4M distinct keys would cut the loop ~3.6×).

## Phase 2 — dual-write per importer (each task = same shape)

For each: stamp+resolve wired into the finish phase, behind
`HLTHPRT_ADDRESS_CANON_SOURCES` allow-list; AC = key-coverage gate, ResolveStats
sanity, archive growth plausibility, 20-row eyeball report (05 Phase 2 gates),
plus that importer's existing tests green.

- [ ] **2.1 (M)** cms_doctors (pilot) — bit 2, priority 1.
- [ ] **2.2 (S)** facility_anchors — bit 8, priority 4; geocode fill from CMS
      coordinates with `geocode_source='facility_anchor'`.
      Status (2026-06-12): source-coordinate archive geocode fill is
      implemented after facility-anchor archive resolve and only fills empty
      canonical archive geocodes. DB regression
      `test_facility_anchor_coordinates_refresh_archive_geocode_fields` proves
      `lat`, `long`, `geocode_source`, and `geocode_quality` are filled without
      overwriting existing geocodes. Broader Phase 2 pilot reports remain
      pending.
- [ ] **2.3 (M)** provider_enrichment + enrollment FFS — bits 4;
      `precision='city_zip'` for street-less rows. AC adds: no `city_zip` row
      shares a key with any `street` row (structural, but assert anyway).
- [ ] **2.4 (L)** NPPES — bit 1, priority 0; sharded stamping; resolve placed
      before geo backfill in shutdown. AC adds: total added wall-time ≤ 10 min
      on a production-size import.
- [ ] **2.5 (M)** marketplace MRF — bit 16, priority 5; `MRFAddress` +
      `MRFAddressEvidence` stamped; resolve after `_refresh_mrf_address_summary`.
- [ ] **2.6 (M)** pharmacy_license — bit 32, priority 6, set-based keys inside
      existing INSERT…SELECT.
- [ ] **2.7 (S)** partd — bit 64, priority 7.

Status: all Phase 2 importer hook points are wired behind
`HLTHPRT_ADDRESS_CANON_SOURCES`, existing importer tests are green, and the
shared `resolve_into_archive` helper now reports key-coverage/sanity gate
counters and bounded multi-source sample rows for every enabled importer.
Per-import pilot thresholds, archive-growth history, and recorded 20-row eyeball
acceptance reports are still pending. Current
verification: expanded impacted importer/API/control suite `346 passed`; full
local suite `942 passed` against the disposable Postgres `5440` test database.

## Phase 3 — archive v2 cutover (the 3M geocode step)

- [x] **3.1 (M)** One-time copy job (arq task `address-archive-v2-migrate`):
      v1 → v2 with deterministic collapse rule (prefer `place_id`, then latest
      `date_added`); bridge build routing colliding checksums to
      `address_checksum_collision` (map's PK(checksum) makes fan-out
      structurally impossible).
      AC: **V1/V2/V3 verification queries from 05 all pass**; collision table
      row count reported; runtime measured.
      Status: implemented as a controlled single-job importer and CLI command.
      Review-fix real-copy smoke on disposable Postgres copied 916,949 local
      legacy rows into 780,372 canonical rows in 88.644s, with 916,417 checksum
      bridge rows, 0 collision rows, 0 represented-key misses, and 0
      geocoded-key misses. The corrected run excludes 532 non-keyable rows
      under the post-review non-US/missing-key rules.
      Evidence: `reports/address_archive_v2_migration_smoke_2026-06-11.md`.
      The production target still needs this job rerun before Phase 3.2.
- [ ] **3.2 (S)** Rename swap v2 → `address_archive`, v1 → `address_archive_legacy`.
      AC: API geocode paths still green via bridge; legacy untouched
      (checksum-verified row count).
- [ ] **3.3 (M)** One-time backfill resolve over all live source tables.
      AC: per-source watermark report; archive size in expected range (~4–5M).
- [ ] **3.4 (S)** Soak checklist + owner sign-off gate before any legacy drop
      (drop itself is a separate, explicitly-approved task — not scheduled).

## Phase 4 — reader cutover (one PR each)

- [ ] **4.1 (S)** NPPES geo backfill → `address_key` join (bridge fallback kept).
      AC: backfilled geo counts ≥ previous import's, on a real import.
- [ ] **4.2 (M)** API geocode read+write (`api/endpoint/npi.py:3607-3640`,
      `:3704-3710`) → `address_key`.
      AC: `tests/test_npi_api.py` geocode tests updated + green; collision
      write path provably gone (no `WHERE checksum=` writes remain).
- [ ] **4.3 (S)** Remove bridge fallbacks; drop `address_checksum_map`.
      AC: grep + query-log check: zero checksum joins.

## Phase 5 — unified key v2

- [ ] **5.1 (M)** Stage carries `address_key`; GROUP BY flip behind
      `HLTHPRT_ENTITY_ADDRESS_UNIFIED_KEY_V2`; row-count-delta abort guard
      (<80% previous publish ⇒ abort+alert).
      AC: side-by-side rebuild report (v1 vs v2 row counts, top collapses);
      three readers + pinned tests updated in the same PR; flag-off path
      byte-identical to today.

## Phase 6 — satellites + steady-state audits

- [ ] **6.1 (M)** `address_observation` for marketplace MRF (change-only,
      gated by `raw_signature`).
- [ ] **6.2 (L)** Merge machinery: `address_match_candidate` queue,
      `address_merge_log`, `merged_into` resolution in archive reads; admin
      review endpoint in the control plane; PLUS the candidate-generation
      batch job (02 §queue-filling): blocked comparison (`zip5+house token` /
      `premise_key` / 25m GiST), component scoring with `pg_trgm` street
      similarity + geocode-distance buckets. Review-only — no auto-merge path
      exists in code.
      AC: golden pairs (`Saint Louis`/`St Louis`, `N Main`/`Main N`,
      PO Box vs street, campus multi-NPI) rank correctly; runtime bounded by
      blocking (measured); queue endpoint paginates by score.
- [ ] **6.3 (S)** Nightly audit cron: 0.1% re-key sample, per-source watermark
      reconciliation, identity-UNIQUE alert.
      AC: audits visible in import-status events; alert wiring tested.
- [ ] **6.4 (S)** Chain resolve refresh into control-plane registry
      (`api/control_imports.py` pattern).
- [ ] **6.5 (S)** Premise geocode sharing: backfill `lat/long` for rows whose
      `premise_key` sibling has a rooftop/parcel geocode
      (`geocode_source='premise_inherited'`, `geocode_quality='premise'`);
      tag existing geocodes with `geocode_quality` where derivable from the
      geocoder response.
      AC: no unit-specific geocode is ever overwritten; coverage report
      before/after.

## Phase 7 (optional, separate decision) — USPS CASS/DPV validation

- [ ] **7.1 (L)** Batch-validate DISTINCT archive addresses through a
      CASS-certified vendor (Smarty/Melissa/etc.) — never row-by-row from
      importers; results cached on the archive row
      (`postal_validation_status`, `zip4`, delivery-point data).
      Used as quality evidence and candidate-scorer input only; switching
      identity to delivery-point codes would be an explicit `v2` re-key
      decision with its own migration.
      AC: vendor cost model approved by owner first; validation status on
      ≥95% of street-precision rows; zero identity changes.

## Suggested PR slicing

Phase 0 = one PR (four commits). Phase 1 = two PRs (migrations+functions+tests,
then helpers+models). Each Phase 2 importer = one PR. Phases 3–5 = one PR per
numbered task. Nothing in any PR blocks ongoing import operations.
