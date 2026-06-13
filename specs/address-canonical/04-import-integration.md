# 04 Import Integration — what changes in each importer

## The shared helper (one new module)

`process/ext/address_canon.py`:

```python
async def stamp_address_keys(conn, staging_table, field_map, *, shards=8) -> int
    # UPDATE {staging} SET address_key = mrf.addr_key_v1(...field_map...)
    # WHERE address_key IS NULL — sharded by mod(hash, shards) for big tables.

async def resolve_into_archive(conn, staging_table, field_map, *,
                               source_bit, priority,
                               work_mem='256MB', timeout='10min') -> ResolveStats
    # Statements A (INSERT ... ON CONFLICT DO NOTHING) and B (guarded UPDATE)
    # from 02-algorithm, wrapped in:
    #   pg_advisory_xact_lock(hashtext('address_archive_resolve'))
    #   SET LOCAL work_mem / statement_timeout
    # Returns counts: staged, distinct_keys, inserted, provenance_updates.
    # Counts feed the watermark audit (05) and live-progress events.
```

Both are called from each importer's **finish/shutdown phase** — the same place
each importer already runs its enrichment UPDATEs and index builds — after the
staging table is fully loaded, before its rename swap. Parsing and COPY code is
not touched in any importer.

## Per importer

Rollout order = blast radius, smallest first. Each is its own task with its own
verification gate (06).

### 1. CMS Doctors (`process/cms_doctors.py`) — pilot, smallest risk
- Model: `address_key` on `DoctorClinicianAddress`.
- After the staged load, before the swap: `stamp_address_keys` +
  `resolve_into_archive(source_bit=2, priority=1)`.
- Field map: `(address_line1, address_line2, city, state, zip_code, 'US')`.
- The NPI+specialty-flavored `address_checksum` PK stays (row identity);
  `address_key` is the new cross-source identity.
- Volume ~2–3M rows ⇒ resolve well under a minute.

### 2. Facility anchors (`process/facility_anchors.py`)
- Same pattern, `source_bit=8, priority=4`. Facility addresses double as a
  high-trust geocode source (CMS publishes coordinates): statement B may fill
  missing `lat/long` with `geocode_source='facility_anchor'`.

### 3. Provider enrichment / Medicare enrollment FFS
- `provider_enrollment_ffs_address` rows have **no street lines** — they
  resolve with `precision='city_zip'` (`source_bit=4, priority=2`); structurally
  unmergeable with street rows, but the locality evidence stays addressable.
- The parent enrollment rows (which DO have streets) resolve as `'street'`.

### 4. NPPES (`process/npi.py`) — the big one
- `address_key` stamped on `npi_address_{date}` staging (sharded UPDATE), then
  resolve (`source_bit=1, priority=0` — NPPES is the most trusted display
  source).
- The geo backfill (`process/npi.py:850-856`) flips from
  `JOIN address_archive ON checksum` to `JOIN ... ON address_key` — this is the
  step that ends checksum-collision geo propagation. During transition the old
  join remains as fallback via `address_checksum_map`.
- The in-parse dict keyed `npi_checksum_type` (`npi.py:131,154`) is untouched.
- ~8M address rows ⇒ stamp+resolve ≈ a few minutes inside a multi-hour import.

### 5. Marketplace MRF (`process/initial.py`)
- `address_key` on `MRFAddress` + `MRFAddressEvidence` rows
  (`source_bit=16, priority=5`); resolve runs after `_refresh_mrf_address_summary`.
- Optional (Phase 6): write `address_observation` rows — MRF tables are dropped
  every import, so this is the only durable history MRF addresses will ever have.

### 6. Pharmacy license (`process/pharmacy_license.py`)
- Addresses live inside the md5-signed record (`license_key` /
  `record_signature` stay untouched). Resolve runs over the staged records with
  the address fields projected out (`source_bit=32, priority=6`). Set-based
  importer ⇒ keys computed inside its existing INSERT…SELECT for free.

### 7. Part D / partd pharmacy activity (`process/partd_formulary_network.py`)
- Same set-based pattern, `source_bit=64, priority=7`.

### 8. `entity_address_unified` (consumer upgrade, flag-gated — Phase 5)
- Its raw stage carries `address_key` (computed in the same INSERT…SELECT that
  already computes the md5-int32 checksum).
- Behind `HLTHPRT_ENTITY_ADDRESS_UNIFIED_KEY_V2`: the hard-dedup GROUP BY key
  flips from the int32 checksum to `address_key`. Guarded by the raised
  min-rows gate AND a row-count-delta guard (abort if v2 rows < 80% of the
  previous publish — catches normalizer bugs and truncated builds alike).
- `mrf_address` registered as source select #6 (fixes B3) — independent of the
  flag.

### 9. PTG / PTG2 (group-plan / pricing MRF data) — no changes required

Verified directly: the pricing files themselves contain **no postal addresses**.

- Transparency-in-Coverage payloads identify providers by NPI lists + TIN only
  (`provider_references` / provider-group identity hashing,
  `process/ptg.py:324-349`); the only "address" in `ptg.py` is
  `content_addressed_path` — artifact file addressing, not postal.
  `ptg_parts/allowed_amounts.py`, `ptg_parts/source_download.py`,
  `ptg_control.py`, `ms_drg.py`: zero postal-address handling.
- Addresses enter PTG2 only at serving time, by joining pricing NPIs to
  upstream address tables in priority order
  (`process/ptg_parts/serving_maintenance.py:236-309`):
  priority 1 `doctor_clinician_address`, priority 2 `entity_address_unified`,
  priority 3 `npi_address` — then one best row per NPI becomes
  `ptg2_provider_location` with
  `location_hash = md5(npi|source|zip5|city_norm|state)` (`:325`) and a
  denormalized `address_payload` JSON. Note the hash is deliberately
  city/zip-grained (no street component).
- Therefore PTG2 **inherits** address quality from upstream: every Phase 2/5
  improvement (deduped sources, unified key v2, the B2 chimera fix) flows into
  PTG2 location candidates automatically on its next precompute.
- Optional Phase 6 enhancement (not required for correctness): include
  `address_key` in `address_payload` (and in the
  `ptg2_provider_group_location_*` snapshots, which today copy
  `npi_address.checksum` verbatim) so pricing locations are traceable to the
  canonical address book.

### 10. MRF source discovery — no changes
- `mrf_source_discovery` handles URLs, not postal addresses.

### API read paths (cutover in Phase 4, one per release)
- Geocode write path `api/endpoint/npi.py:3607-3640`: look up / upsert the
  archive by `address_key` computed from the row's own fields (one
  `mrf.addr_key_v1(...)` call in the existing statement). Ends the live
  collision blast radius.
- Geocode read `api/endpoint/npi.py:3704-3710`: same swap.
- `pricing.py` / PTG2 location candidates: may later join the archive by
  `address_key` for geo data; not required for dedup correctness.

## Operational cautions (learned from design review — do not skip)

1. **Restart arq workers after the model deploy.** `make_class` caches dynamic
   staging classes (`_DYNAMIC_CLASS_CACHE`, `process/ext/utils.py:510`); a
   long-lived worker would keep creating staging tables WITHOUT `address_key`.
2. **Column placement**: `address_key` goes on subclasses, never on
   `AddressPrototype` (positional `c.*` unpack in `api/endpoint/npi.py:2700-2711`).
3. **Indexes on staging**: `make_class` copies only the index *lists*
   (`utils.py:533-539`) — add `(address_key)` to each importer's explicit index
   step or backfill joins seq-scan.
4. **`push_objects` rewrite fallback** builds its DO UPDATE SET from ALL model
   columns (`utils.py:799-815`): a batch dict missing `address_key` would null
   it on conflict. Guard: the helper stamps keys in SQL *after* load, so batch
   dicts never carry the column at all during COPY.
5. **Advisory lock** uses `pg_advisory_xact_lock(hashtext('...'))` — the
   function takes bigint, not text.
6. **State overflow**: `state_code` is varchar(32), not char(2) — NPPES foreign
   provinces must not abort a resolve statement.
