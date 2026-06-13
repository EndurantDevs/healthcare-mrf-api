# 05 Migration Phases, Geocode Preservation, Rollback, Audits

Every phase ships alone, is verified alone, and can be reverted alone.

## Phase 0 — standalone bug fixes (valuable even if everything else stops)

| Fix | Where | Effect |
|---|---|---|
| B1 conflict-target order: prefer `__my_index_elements__` over `__my_initial_indexes__` | `process/ext/utils.py:720-729` | stops silent drop of cross-NPI shared addresses in `mrf_address` batches |
| B2 chimera: NULL parent street lines for FFS child rows | `process/entity_address_unified.py:439-443` | unified stops serving fabricated addresses |
| B3 register `mrf_address` as unified source #6 | `process/entity_address_unified.py` source selects | marketplace addresses reach unified |
| B4 raise publish gate 10k → ~1M (env-tunable) | `entity_address_unified` publish check | truncated builds can't publish |

Phase 0 also produces the **baseline instrumentation report** (task 0.5):
how many CRC32 collisions, duplicate addresses, and cross-source overlaps
exist in production *today*. Every later phase is measured against these
numbers, and they justify the project with hard data.

Rollback: plain revert; each fix is an independent commit.

## Phase 1 — foundations (no behavior change)

- Alembic migration: `address_archive_v2` shell + `addr_*_v1` functions +
  `address_checksum_map` / `address_checksum_collision` (03).
- Resolve helpers target `HLTHPRT_ADDRESS_ARCHIVE_TABLE` (default
  `address_archive_v2`): all Phase 2 dual-writes land in v2, which is the
  durable canonical store from day one. The Phase 3 swap only renames it and
  flips the config default — there is no period where canonical rows live in
  the legacy table.
- `process/ext/address_canon.py` + golden-corpus parity tests (SQL vs Python
  oracle).
- Nullable `address_key` columns on models (subclass-level).
- **Deploy step: restart all arq workers** (staging-class cache, 04 §cautions).

Rollback: drop the new objects; nothing reads or writes them yet.

## Phase 2 — dual-write, one importer per release

Order: cms_doctors → facility_anchors → provider_enrichment/FFS → NPPES →
marketplace MRF → pharmacy_license → partd.

Gate before enabling the next importer (run as SQL checks, recorded in the
import's live-progress events):

- zero staged rows with `address_key IS NULL` that have street+zip+state
  (key coverage);
- exclusion reason counts (`missing_zip`, `missing_state`, `missing_street`,
  `unsupported_country`, `ambiguous_unit`) within the per-import thresholds
  established during that importer's pilot run — a spike in any bucket blocks
  the gate (it means the source format changed, not that addresses got worse);
- `resolve` stats sane: `distinct_keys ≤ staged`, `inserted ≤ distinct_keys`;
- statement C (collision tripwire) returned zero rows — any hit already
  aborted the import;
- archive row count grew by a plausible amount (alert if an importer suddenly
  inserts > 2× its historical unique-address estimate);
- spot-check query: top 20 multi-source addresses look like real shared
  addresses (eyes on data once per importer).

Rollback per importer: stop calling the two helper functions (env flag
`HLTHPRT_ADDRESS_CANON_SOURCES` holds the enabled list); staged `address_key`
columns simply stay NULL. Raw tables never depended on the new layer.

## Phase 3 — archive v2 cutover + geocode preservation (THE 3M-ROW STEP)

The production `address_archive` holds ~3M geocoded addresses. Hard acceptance
criterion: **zero geocoded unique addresses lost, proven by query, before
anything legacy is touched.**

Why this is safe by construction: every v1 archive row is a self-consistent
full copy (the API upsert writes address text + geocode together,
`api/endpoint/npi.py:3629-3638`), so each row's new key is computed from its
OWN text — never joined through the collision-prone checksum.

Implementation note from the dev real-copy smoke and 2026-06-12 performance
check: the SQL `addr_street_norm_v1` path is correct but still too slow for
legacy rows that do not already carry `address_key`. The shipped
`address-archive-v2-migrate` job therefore materializes canonical keys in
Python batches using the parity-tested oracle, then uses SQL for the durable set
operations. The live import resolver takes the faster stamped-key path instead:
it dedupes by staged `address_key` before normalizing representative rows.

```sql
-- 1) copy everything, geocodes included, from a keyed temp table produced by
--    the Python oracle. Collapse rule is deterministic:
--    prefer place_id, then latest date_added.
INSERT INTO mrf.address_archive_v2 (address_key, identity_key, ..., lat, long,
       place_id, formatted_address, geocode_source, date_added, source_bits)
SELECT address_key, identity_key, ..., lat, long, place_id, formatted_address,
       'archive_backfill', date_added, 1
FROM address_archive_v2_keyed
WHERE address_key IS NOT NULL
ON CONFLICT (address_key) DO UPDATE SET
    source_bits = address_archive_v2.source_bits | 1,
    -- geocode/display fields use the same deterministic winner rule.
    ...
;

-- 2) build the bridge; collisions are separated STRUCTURALLY (map has
--    PRIMARY KEY (checksum), so a fan-out row cannot even be inserted)
WITH keyed AS (
    SELECT checksum, mrf.addr_key_v1(...) AS address_key
    FROM mrf.address_archive
    WHERE mrf.addr_key_v1(...) IS NOT NULL
), counted AS (
    SELECT checksum, count(DISTINCT address_key) AS n
    FROM keyed GROUP BY checksum
)
INSERT INTO mrf.address_checksum_map (checksum, address_key)
SELECT DISTINCT k.checksum, k.address_key
FROM keyed k JOIN counted c USING (checksum) WHERE c.n = 1;

INSERT INTO mrf.address_checksum_collision (checksum, address_key)
SELECT DISTINCT k.checksum, k.address_key
FROM keyed k JOIN counted c USING (checksum) WHERE c.n > 1;
-- collision rows: manual-review report; all bridging joins use ONLY the map
```

Verification — all must pass before the rename swap, and the legacy table is
kept (as `address_archive_legacy`) until a soak period the owner chooses:

```sql
-- V1: every keyable legacy row is represented
SELECT count(*) = 0 FROM address_archive a
WHERE mrf.addr_key_v1(...) IS NOT NULL
  AND NOT EXISTS (SELECT 1 FROM address_archive_v2 v
                  WHERE v.address_key = mrf.addr_key_v1(...));
-- V2: geocode coverage did not shrink
SELECT (SELECT count(DISTINCT mrf.addr_key_v1(...)) FROM address_archive
        WHERE lat IS NOT NULL AND mrf.addr_key_v1(...) IS NOT NULL)
    <= (SELECT count(*) FROM address_archive_v2 WHERE lat IS NOT NULL);
-- V3: non-keyable rows are counted and reported (expected: tiny fraction;
--     they remain served from legacy via the bridge until re-geocoded)
```

Then a one-time backfill resolve runs over each live source table (~5–10 min
total, dominated by `npi_address`), populating archive rows for addresses that
were never geocoded.

Rollback: rename swap back; legacy table was never modified.

## Phase 4 — reader cutover, one consumer per release

1. NPPES geo backfill (`process/npi.py:850-856`) → join archive by
   `address_key` (legacy checksum join via bridge as fallback, then removed).
2. API geocode read+write (`api/endpoint/npi.py:3607-3640, 3704-3710`) →
   `address_key`. **This ends the live collision blast radius.**
3. Remove bridge fallbacks; drop `address_checksum_map` when query logs show
   zero checksum joins.

Each step is a small PR; revert = restore the previous join.

## Phase 5 — unified key v2 (flag-gated)

`HLTHPRT_ENTITY_ADDRESS_UNIFIED_KEY_V2` flips the unified hard-dedup key to
`address_key`, protected by the raised min-rows gate AND the row-count-delta
guard (abort publish if rows < 80% of previous). The three unified readers and
pinned tests move in the same release. Revert = flip the flag.

## Phase 6 — optional satellites + steady-state audits

- `address_observation` for marketplace MRF; merge queue + merge log (03 §5).
- Nightly audits (arq cron):
  - **re-key sample**: 0.1% random archive rows — recompute
    `addr_key_v1(identity components)` and assert it equals the stored key
    (drift tripwire; should never fire while `_v1` exists);
  - **watermark reconciliation**: per source, distinct staged keys vs archive
    rows carrying that source bit — a skipped/failed resolve can't stay silent;
  - **collision tripwire**: statement C runs per import (aborts on hit); the
    nightly job additionally cross-checks
    `count(*) = count(DISTINCT identity_key)` over the archive and alerts on
    any `UNIQUE(identity_key)` violation from non-resolve write paths
    (expected: never).
- Chain a resolve/refresh after each address-writing import through the
  existing control-plane registry (`api/control_imports.py` pattern), so
  freshness never depends on operator memory.

## Performance expectations (summary)

| Operation | Cost | When |
|---|---|---|
| Key stamping, NPPES staging (~8M rows) | minutes (sharded set-based UPDATE) | per NPPES import |
| Resolve A+B, NPPES | ~30–60s strainer + insert probes | per NPPES import |
| Resolve, all other importers | seconds | per import |
| Re-import of unchanged data | ~0 writes (guarded UPDATE) | steady state |
| One-time archive v2 copy | minutes for ~3M rows | once |
| One-time live-table backfill | ~5–10 min total | once |
| Archive at steady state | ~4–5M rows, 4 indexes | — |
