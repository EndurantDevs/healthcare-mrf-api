# 03 Data Model

All DDL ships as alembic migrations (the first address DDL ever under
`alembic/versions/` — today address tables are created imperatively at worker
startup, which is part of the drift problem).

## 1. `mrf.address_archive` v2 — the address book (center of the system)

Evolved in place from today's `address_archive` (built as `address_archive_v2`,
then swapped; old table kept as `address_archive_legacy` until verification —
see 05). One row = one unique physical address.

```sql
CREATE TABLE mrf.address_archive (
    -- identity -----------------------------------------------------------
    address_key       uuid PRIMARY KEY,       -- first 16 bytes of sha256(identity_key)
    identity_key      text NOT NULL UNIQUE,   -- 'v1|123mainst|ste200|miami|FL|33156|US|street'
    identity_version  smallint NOT NULL DEFAULT 1,
    precision         text NOT NULL DEFAULT 'street'
                      CHECK (precision IN ('street','city_zip')),
    premise_key       uuid,                   -- sha256 of identity_key with unit blanked:
                                              -- the BUILDING. Two suites share premise_key,
                                              -- never address_key. Grouping/geo-sharing only,
                                              -- NEVER a merge identity.

    -- normalized components (the exact hash inputs, stored for audit) ----
    line1_norm        text,                   -- street incl. nothing-stripped tail fallback
    unit_norm         text NOT NULL DEFAULT '',  -- 'ste200' | '' when none
    city_norm         text,
    state_code        varchar(32),            -- USPS code; longer only for unmappable foreign
    zip5              varchar(5),
    zip4              varchar(4),             -- stored, NOT identity
    country_code      varchar(8) NOT NULL DEFAULT 'US',

    -- best display variant (elected by source priority, deterministic) ---
    first_line        text,
    second_line       text,
    city_name         text,
    state_name        text,
    postal_code       text,
    telephone_number  text,                   -- display only, NEVER identity
    fax_number        text,
    display_priority  smallint NOT NULL DEFAULT 9,

    -- geocode enrichment (the archive's historic role, kept) -------------
    formatted_address text,
    lat               numeric(11,8),
    long              numeric(11,8),
    place_id          text,
    geocode_source    text,                   -- 'mapbox' | 'archive_backfill' |
                                              -- 'facility_anchor' | 'premise_inherited' ...
    geocode_quality   text,                   -- 'rooftop' | 'parcel' | 'street' |
                                              -- 'premise' | 'zip_centroid' | 'unknown'
    postal_validation_status text,            -- 'none' | 'cass_dpv_confirmed' | ... (optional Phase 7)
    geocoded_at       timestamptz,

    -- provenance / history -----------------------------------------------
    source_bits       integer NOT NULL DEFAULT 0,   -- nppes=1, cms_doctors=2, ffs=4,
                                                    -- facility_anchor=8, mrf=16,
                                                    -- pharmacy=32, partd=64,
                                                    -- ptg_overlay=128
    first_seen_at     timestamptz NOT NULL DEFAULT now(),
    last_seen_at      timestamptz NOT NULL DEFAULT now(),
    date_added        date,                   -- carried over from v1 rows

    -- supervised merge support -------------------------------------------
    merged_into       uuid REFERENCES mrf.address_archive (address_key)
) WITH (fillfactor = 90);   -- keep steady-state updates HOT
```

Indexes — deliberately few; the resolve insert touches each one:

```sql
-- PK (address_key) and UNIQUE (identity_key) come from constraints
CREATE INDEX ON mrf.address_archive (zip5, line1_norm);
CREATE INDEX ON mrf.address_archive (state_code, city_norm);
CREATE INDEX ON mrf.address_archive (premise_key) WHERE premise_key IS NOT NULL;
-- Created ONLY when PostGIS is installed (SELECT 1 FROM pg_extension WHERE
-- extname='postgis'), mirroring how the codebase already conditionally skips
-- geography indexes. Absence degrades geo queries only; writes never depend on it.
CREATE INDEX ON mrf.address_archive
    USING gist (Geography(ST_MakePoint((long)::float8, (lat)::float8)))
    WHERE lat IS NOT NULL;
```

Why `premise_key` earns its column (idea adopted from Google/Mapbox-style
multi-level identity — premise vs delivery point):

- **Geocode sharing**: a building gets rooftop-geocoded once; suites without
  their own geocode inherit it (`geocode_source='premise_inherited'`,
  `geocode_quality='premise'`) instead of each triggering a Mapbox call.
- **Blocking for the review queue**: candidate generation (Phase 6) compares
  only rows inside one premise/block, never all-pairs.
- **Analytics**: "all providers in this medical building" becomes one indexed
  lookup.
- It is **never** used to merge: dedup identity stays at delivery-point
  (suite-aware) level, period.

Notes:

- **Physical naming during rollout:** the table is created and lives as
  `address_archive_v2` through Phases 1–2; importer resolve helpers address it
  via `HLTHPRT_ADDRESS_ARCHIVE_TABLE` (default `address_archive_v2`), and the
  Phase 3 rename swap flips both the table name and the config default to
  `address_archive` in one deploy. Nothing ever writes canonical rows into the
  legacy table.
- Never dropped, never swapped after the one-time v2 cutover. This is the only
  address table whose keys are stable across import cycles.
- Every mutable-column UPDATE goes through `IS DISTINCT FROM` guards so an
  unchanged address costs zero WAL.
- A same-source re-import with the same display priority and unchanged display
  fields intentionally performs no provenance UPDATE. That means `last_seen_at`
  is not bumped for no-op re-imports; it records the last write that changed
  provenance/display state, not every time an unchanged source row was observed.
- `telephone_number` exists because the v1 archive carries it and the API
  returns it; it is display data only.

## 2. Normalization functions (same migration)

```sql
mrf.addr_street_norm_v1(line1 text, line2 text) RETURNS text
mrf.addr_unit_norm_v1(line1 text, line2 text)   RETURNS text
mrf.addr_state_code_v1(state text)              RETURNS text
mrf.addr_identity_key_v1(first_line, second_line, city, state, zip, country) RETURNS text
mrf.addr_key_v1(first_line, second_line, city, state, zip, country)          RETURNS uuid
```

All `IMMUTABLE PARALLEL SAFE`, all rules **inlined** (CASE expressions /
regexes). They must NOT read any table: an "immutable" function that reads a
mutable mapping table silently changes identity when the table changes — that
defect was explicitly rejected in design review. New rules ⇒ new `_v2`
functions ⇒ explicit re-key migration.

The Python implementation (`process/ext/address_canon.py`) is production code
for migration and high-volume batch materialization paths, while SQL remains
the set-based producer for live staging updates. Both implementations are bound
to the same v1 contract: a frozen golden corpus (unicode lines, `<UNAVAIL>`,
`''` vs NULL, ZIP+4, `Suite 200`/`STE #200`/`#200`, `TEXAS`/`TX`, PO Box forms,
rural routes) is run through both implementations and the keys must be
identical. Runtime resolve also recomputes `sha256(identity_key)[:16]` and
aborts if a staged `address_key` does not match the current identity text.

## 3. `mrf.address_checksum_map` — the transition bridge

Lets every existing checksum-based code path keep working during rollout.

```sql
-- One row per SAFE checksum. PRIMARY KEY (checksum) makes a fan-out join
-- impossible BY CONSTRUCTION: a transition join through this table can never
-- return two addresses for one checksum.
CREATE TABLE mrf.address_checksum_map (
    checksum     bigint PRIMARY KEY,          -- legacy CRC32, non-colliding only
    address_key  uuid NOT NULL REFERENCES mrf.address_archive (address_key)
);

-- CRC32 collisions land here instead — excluded from ALL bridging, reported:
CREATE TABLE mrf.address_checksum_collision (
    checksum     bigint NOT NULL,
    address_key  uuid    NOT NULL,
    detected_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (checksum, address_key)
);
```

- Many checksums → one key is expected and fine (raw spelling variants).
- One checksum → many keys is a CRC32 collision: the populating query routes
  those rows to `address_checksum_collision` (05 Phase 3), so the map's PK is
  satisfiable and quarantine is enforced by schema, not by discipline.
- Both dropped at the end of the rollout, when no reader joins by checksum.

## 4. Source tables — one added column each (nothing else changes)

`address_key uuid NULL` is added to: `NPIAddress`, `MRFAddress`,
`MRFAddressEvidence`, `DoctorClinicianAddress`, `ProviderEnrollmentFFSAddress`,
`FacilityAnchor`, the pharmacy/partd staged models, and the
`entity_address_unified` stage.

- Added on each **subclass**, NOT on `AddressPrototype`: an abstract-base
  column would change the physical column ORDER of `npi_address`, and
  `api/endpoint/npi.py:2700-2711` unpacks `c.*` rows positionally — a
  base-level column would silently shift npi/type values during the deploy
  window. Subclass columns append at the end, which is safe.
- Existing PKs, CRC32 checksum columns, unique indexes, COPY paths, swap
  publishing: all untouched. The legacy checksums remain as intra-table row
  identity; `address_key` is the cross-source identity.
- Staged/swapped tables pick the column up automatically at the next import
  (they are recreated from the models via `make_class`/`tometadata`,
  `process/ext/utils.py:507-521`). Durable tables get a plain
  `ALTER TABLE ... ADD COLUMN` (nullable ⇒ instant, catalog-only).
- `make_class` does NOT copy plain column indexes — each importer's existing
  "create additional indexes" step gains one `(address_key)` btree entry.

## 5. Optional satellites (Phase 6, can be deferred)

```sql
-- Append-only sighting history — ONLY for marketplace MRF, whose tables are
-- dropped every import and therefore have no durable history of their own.
CREATE TABLE mrf.address_observation (
    address_key   uuid NOT NULL,
    source        text NOT NULL,              -- 'mrf'
    import_id     text NOT NULL,
    observed_at   timestamptz NOT NULL,
    raw_signature integer NOT NULL,           -- write only when raw text changed
    first_line text, second_line text, city_name text,
    state_name text, postal_code text,
    PRIMARY KEY (address_key, source, import_id)
);

-- Human-reviewed merge machinery (no fuzzy merge ever runs unattended).
CREATE TABLE mrf.address_match_candidate (
    candidate_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    address_key_a uuid NOT NULL, address_key_b uuid NOT NULL,
    rule text NOT NULL, score real, created_at timestamptz DEFAULT now(),
    status text NOT NULL DEFAULT 'pending'    -- pending|approved|rejected
);
CREATE TABLE mrf.address_merge_log (
    merge_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    loser_key uuid NOT NULL, winner_key uuid NOT NULL,
    rule text, decided_by text, decided_at timestamptz DEFAULT now(),
    reverted_at timestamptz
);
```

## 6. What deliberately does NOT change

- `entity_address_unified` — table, PK, builder, and its three readers
  (`api/endpoint/pricing.py:2407-2440`,
  `process/ptg_parts/serving_maintenance.py:261-283`, cohort SQL) stay as they
  are through the whole rollout; it later *consumes* `address_key` behind a
  flag (see 05).
- PTG2 tables — consumers only; they inherit better dedup upstream for free.
- API payload shapes — `address_key` is internal until we decide to expose it.
