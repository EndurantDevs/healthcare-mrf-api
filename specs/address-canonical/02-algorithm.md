# 02 The Deduplication Algorithm

## The whole idea in five steps

Think of it as a library check-in desk for addresses:

```
   raw address from any import
            │
            ▼
   ① CLEAN it          (normalize: same spelling rules for everyone)
            │
            ▼
   ② BUILD the identity sentence   (one line of text that *defines* the address)
            │
            ▼
   ③ FINGERPRINT it    (sha256 → 128-bit key; practically collision-proof)
            │
            ▼
   ④ CHECK IN at the address book  (mrf.address_archive: insert if new, skip if known)
            │
            ▼
   ⑤ STAMP provenance  (remember which import saw it, keep the nicest spelling)
```

Steps ①–③ are pure functions (same input → same output, forever, no side
effects). Steps ④–⑤ are two SQL statements run **once per import run**, over the
whole staged table at once — never row by row.

---

## Step ① CLEAN — the normalization rules

All cleaning rules live in **one versioned contract**: SQL functions created by
the migration (`mrf.addr_*_v1`) and a Python implementation generated from the
same checked-in Pub. 28 constants. SQL is the set-based producer for live
staging tables; Python is production code for migration and large-batch
materialization paths. A frozen corpus, SQL/Python parity tests, and a runtime
stamp-vs-identity guard must keep both implementations byte-identical. The
rules, in plain words:

| Rule | Example before | After |
|---|---|---|
| Uppercase→lowercase, trim, collapse spaces | ` 123  MAIN  St. ` | `123 main st` |
| Join line 1 + line 2 before cleaning (sources disagree about which line holds the suite) | `123 Main St` / `Suite 200` | `123 main st suite 200` |
| USPS street-word abbreviations (full Pub. 28 table, not a hand-picked dozen) | `street, avenue, boulevard, parkway…` | `st, ave, blvd, pkwy…` |
| Directionals | `North, NorthEast` | `n, ne` |
| Unit designators | `Suite / STE. / #` → `ste`, `Apartment` → `apt`, `Floor` → `fl`, `Building` → `bldg`, `Room` → `rm` | one canonical token each |
| PO Box forms | `P O Box / PO Box / POB 12` | `pobox 12` |
| State: full name → USPS code | `TEXAS` | `TX` |
| ZIP: digits only, first 5 | `33156-2814` | `33156` (the `+4` is stored but is NOT identity) |
| City: letters+digits only | `St. Louis` | `stlouis` |
| Country: empty / `USA` / `UNITED STATES` | | `US` |
| Final pass: strip everything that is not a letter or digit | `123 main st ste 200` | `123mainstste200` |

This extends the *already proven* normalizer family in
`process/entity_address_unified.py:177-242` (`_alnum_norm_expr`,
`_zip5_norm_expr`, `_street_soft_norm_expr`) — v1 keys are a strict refinement
of the best cleaning the codebase already does, upgraded with full USPS tables.

**Unit/suite extraction — the patient-safety rule.** The suite number is PART of
the identity: two clinics in one building ("Ste 200" vs "Ste 310") must NEVER
merge. Extraction is deliberately conservative:

- extraction runs on the JOINED line1+line2 text (after basic cleaning),
  matching `STE|SUITE|APT|UNIT|RM|ROOM|FL|FLOOR|BLDG|DEPT|#` + number — in the
  line-2 portion first, otherwise only at the TAIL of the joined string;
- on a match, the matched span is **removed from the street text** and becomes
  `unit_norm` (`ste200`). `street_norm` NEVER contains the unit. This removal
  rule is what makes `123 Main St / Suite 200` (two lines) and
  `123 Main St Ste 200` (one line) converge to the same
  `street=123mainst, unit=ste200` — without it they would split;
- if anything is ambiguous (a `#` mid-sentence, fractional addresses, rural
  routes) — extract nothing: `unit_norm = ''` and the full text stays in
  `street_norm`, deterministically.

That fallback can only ever **split** (keep two rows for what might be one
place). It can never merge two different places. Splitting is the safe error;
merging is the dangerous one.

**Rows that can't be street-identified.** Medicare FFS enrollment addresses
have no street line at all (`db/models/_legacy.py:3403-3405`) — only
city/state/zip. They get identity too, but in a separate class:
`precision = 'city_zip'` instead of `'street'`. The class is part of the
identity sentence, so a city-level row can never merge with a street-level row.
Rows that fail even that (no usable state+zip) start with no key (`NULL`).
They are not guessed from broad geography. A missing ZIP may be repaired only
during resolve when the row has street+unit+city+state+country and exactly one
observed keyed sibling with the same normalized components and one ZIP. Every
remaining exclusion is **counted by reason** in ResolveStats — `missing_zip`,
`missing_state`, `missing_street` (⇒ `city_zip` class), `unsupported_country`,
`ambiguous_unit` — and each import has expected thresholds for these counts:
a sudden spike in one bucket means the source changed format, and the import
alerts instead of silently shrinking its address coverage.

---

## Step ② BUILD — the identity sentence

One human-readable line, with a version prefix:

```
v2|{street_norm}|{unit_norm}|{city_norm_or_blank}|{state}|{zip5}|{country}|{precision}
```

For `street` precision rows, `city_norm_or_blank` is intentionally blank:
`house/street + unit + state + ZIP5 + country` is the identity. City labels are
stored for display and search, but not for street-precision identity because
USPS preferred cities, vanity city names, abbreviations, and ZIP boundaries are
too noisy. For `city_zip` precision rows, city is still load-bearing and remains
in the identity.

Example — these five raw inputs from four different imports:

| Import | Raw |
|---|---|
| NPPES | `123 MAIN STREET / SUITE 200 / MIAMI / FL / 331562814` |
| NPPES (older file) | `123 Main St. / Ste 200 / Miami / FL / 33156` |
| CMS doctors | `123 MAIN ST STE 200 / / MIAMI / FL / 33156` |
| MRF | `123 Main St / #200 / Miami / Florida / 33156` |
| Pharmacy | `123 MAIN ST, SUITE 200 / / MIAMI / FL / 33156-2814` |

…all produce the **same sentence**:

```
v2|123mainst|ste200||FL|33156|US|street
```

Why a readable sentence and not just a hash? **Audit.** The sentence is stored
on the row. Anyone can look at it and see exactly *why* two addresses merged.
And a `UNIQUE` constraint on the sentence is our collision tripwire (below).

What is **deliberately NOT in** the identity: the doctor (NPI), the address
*type* (mail/practice), and the phone number. The same building must hash the
same no matter who works there or who reported it. (Phones change and are
recorded separately as display data.)

The `v2|` prefix is the current contract. Function names may keep their
historical `_v1` suffix for database compatibility, but existing keys are
rewritten in-place by Alembic migrations instead of carrying parallel runtime
key versions.

---

## Step ③ FINGERPRINT — the key

```
address_key uuid = first 16 bytes of sha256(identity sentence)
```

The derivation is specified to the byte, so independent implementations cannot
quietly diverge:

```sql
-- SQL:
encode(substring(sha256(convert_to(identity_key, 'UTF8')) from 1 for 16), 'hex')::uuid
```

```python
# Python:
uuid.UUID(bytes=hashlib.sha256(identity_key.encode("utf-8")).digest()[:16])
```

Both take the first 16 bytes of the digest in digest order (no endianness
games, no UUID-version bit fiddling); the golden corpus asserts byte equality
of the two expressions over every fixture.

- 128 bits: with ~10M addresses the chance of a random collision is about
  1 in 10²³ — effectively zero (versus *thousands* of expected collisions
  in today's 32-bit CRC32).
- sha256 is built into PostgreSQL ≥ 11 (no pgcrypto, no md5 — md5 breaks on
  FIPS-hardened servers).
- The key is **deterministic**: any environment, any rebuild, any year —
  the same address always computes the same key. The address book can be
  reconstructed from sources if disaster ever strikes.
- Belt-and-suspenders — but honestly wired: `UNIQUE (identity_key)` alone
  CANNOT catch a hash collision in the resolve path, because
  `ON CONFLICT (address_key) DO NOTHING` skips the colliding row before the
  identity constraint is ever consulted. The collision detector is therefore
  an **explicit statement C** in every resolve (below): staged keys joined to
  the archive where `address_key` matches but `identity_key` differs — any hit
  aborts the import loudly. The UNIQUE constraint remains as a backstop for
  any write path that bypasses the resolve helper.

---

## Step ④ CHECK IN — the resolve step (two SQL statements per import)

When an import finishes loading its staging table (its COPY fast path is
untouched), it runs the statements below. `{archive}` is the **configured**
physical target (`HLTHPRT_ADDRESS_ARCHIVE_TABLE`): `address_archive_v2` during
Phases 1–2, flipped to `address_archive` in the same deploy as the Phase 3
rename swap — the helper never hardcodes the table name.

```sql
-- 0) stamp keys onto the staged rows, set-based, sharded if large:
UPDATE {staging} SET address_key = mrf.addr_key_v1(first_line, second_line, city, state, zip, country);

-- 0b) repair only observed unique siblings:
-- - missing ZIP: same street+unit+city+state+country and one keyed sibling ZIP
-- - missing suffix/directional: same unit+state+ZIP+completion norm and one
--   higher-specificity sibling
-- Ambiguous clusters remain split/null-keyed and are counted.

-- A) register unknown addresses (the strainer: dedupe staging first, then one pass):
INSERT INTO {archive} (address_key, identity_key, ...normalized..., ...display..., source_bits)
SELECT DISTINCT ON (address_key) ...
FROM {staging} WHERE address_key IS NOT NULL
ORDER BY address_key, <deterministic tie-break>     -- stable display election
ON CONFLICT (address_key) DO NOTHING;

-- B) update provenance/display ONLY where something actually changed:
UPDATE {archive} c SET
    source_bits = c.source_bits | :bit,
    first_line  = CASE WHEN :prio < c.display_priority THEN s.first_line ELSE c.first_line END, ...
FROM (SELECT DISTINCT ON (address_key) ... FROM {staging}) s
WHERE c.address_key = s.address_key
  AND (c.source_bits & :bit = 0 OR :prio < c.display_priority);

-- C) collision tripwire — same transaction, expected 0 rows, ANY hit aborts the import:
SELECT s.address_key
FROM (SELECT DISTINCT address_key, identity_key FROM {staging}
      WHERE address_key IS NOT NULL) s
JOIN {archive} c USING (address_key)
WHERE c.identity_key <> s.identity_key;
```

Statement C exists because A's `ON CONFLICT ... DO NOTHING` would otherwise
*silently skip* a row whose 128-bit key collides while its identity text
differs — vanishingly unlikely, but "vanishingly unlikely" is not a healthcare
guarantee; this makes it observable and fatal instead.

Why this is fast (the "no slowdowns" requirement):

- **Zero per-row round trips.** A CSV importer never asks the database
  "have you seen this address?" while parsing. Everything happens in one
  set-based pass at the end.
- **`DISTINCT ON` before insert**: 8M staged NPPES rows shrink to ~4M unique
  keys *before* touching the archive, so the upsert machinery never fights
  itself over duplicates inside one statement.
- **`ON CONFLICT DO NOTHING`** is race-safe if two imports ever finish at
  once; an advisory lock (`pg_advisory_xact_lock(hashtext('addr_resolve'))`)
  additionally serializes only this final step — COPY phases stay parallel.
- **Statement B's guard** (`source_bits & bit = 0 OR better priority`) matches
  ~0 rows when an import re-runs over known data: a repeat import costs one
  index probe per unique key and writes nothing.
- `SET LOCAL work_mem = '256MB'` and a `statement_timeout` wrap every resolve
  so it can neither spill to disk silently nor hang an import.

Expected cost on the biggest import (NPPES, ~8M address rows inside a
multi-hour run): key stamping + strainer + check-in ≈ **a few minutes**, once.
Re-imports: seconds. Small imports: seconds.

---

## Step ⑤ STAMP — provenance, display, history

Each archive row remembers:

- `source_bits` — bitmask of which imports have seen this address
  (nppes=1, cms_doctors=2, ffs=4, facility_anchor=8, mrf=16, pharmacy=32, partd=64);
- `display_priority` + the raw spelling from the most trusted source
  (NPPES first — same priority ladder `entity_address_unified` already uses);
- `first_seen_at` / `last_seen_at`;
- geocodes (`lat`, `long`, `place_id`, `formatted_address`) — lazily filled,
  exactly like today, but keyed by the collision-proof fingerprint.

**Two levels of place, one level of identity.** Besides `address_key`
(the deliverable address, suite included), each row also stores
`premise_key` — the same identity sentence with the unit blanked, i.e. *the
building*. `123 Main St Ste 200` and `123 Main St Ste 310` have different
`address_key`s (they never merge) but the same `premise_key`. The premise
level is used for geocode sharing (geocode the building once), for grouping
("everyone in this medical building"), and for blocking in the review queue —
never for merging.

**Merging "almost equal" addresses is a human decision.** If analytics later
suggests `NYC` and `NEW YORK` rows are the same place, that lands in a review
queue (`address_match_candidate`). An approved merge sets `merged_into` on the
losing row and writes a reversible, attributed `address_merge_log` entry.
Nothing fuzzy ever touches the identity computation itself.

How the review queue gets filled (Phase 6, offline batch — adapted from
record-linkage practice: block, then score, never auto-merge):

1. **Block**: compare only rows that already share `zip5 + house-number token`
   or share a `premise_key` or sit within ~25m of each other (GiST) — never
   all-pairs.
2. **Score inside the block** with component evidence, not one fuzzy string:
   street trigram similarity (`pg_trgm`, built into Postgres), unit
   exact-or-missing, city exact-or-alias, ZIP/state exact, geocode distance
   bucket (rooftop within 25m = strong; ZIP-centroid proximity = worthless,
   ignored).
3. Pairs above a high threshold land in `address_match_candidate`, ranked by
   score, for a human. Below it: nothing happens. There is no auto-merge
   threshold by design.
