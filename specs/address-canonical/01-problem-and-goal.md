# 01 Problem and Goal

## The problem, explained simply

Imagine six friends who each keep their own notebook of doctors' addresses.
Each friend writes addresses their own way:

- one writes `123 Main Street, Suite 200`
- another writes `123 MAIN ST STE 200`
- a third writes `123 Main St. #200`

Same place, three spellings. Worse: each friend also invented their own secret
code to identify an address — and the codes are computed differently, so even
when two friends *did* write the address identically, their codes don't match.
One friend even mixes the doctor's name into the address code, so the *same
building* gets a different code for every doctor working in it.

Nobody can answer the simple question: **"how many unique addresses do we know,
and which doctors share one?"** And every notebook keeps growing with duplicates.

That is exactly our database today. The "friends" are our imports:

| Import ("friend") | Their notebook (table) | Their secret code |
|---|---|---|
| NPPES (national provider registry) | `npi_address` | CRC32 of the raw, uncleaned text — `process/npi.py:122` |
| CMS Doctors & Clinicians | `doctor_clinician_address` | CRC32 of **doctor's NPI + address + specialty** — `process/cms_doctors.py:205` |
| Marketplace MRF (insurance files) | `mrf_address` + `mrf_address_evidence` | CRC32, slightly different cleaning — `process/initial.py:282` |
| Medicare enrollment (FFS) | `provider_enrollment_ffs_address` | CRC32 of the whole record; rows have **no street line** at all |
| Pharmacy license | `pharmacy_license_record_v1` | md5 of the whole record, address inside |
| Unified pipeline | `entity_address_unified` | md5→int32 of *cleaned* text, but per-entity and rebuilt from scratch each run |

## The four concrete defects

### 1. Tiny spelling differences split one address into many rows

Almost no cleaning happens before hashing. `"123 Main St."` and `"123 MAIN ST"`
get different CRC32 codes, so they become two "different" addresses. The archive
cannot deduplicate even NPPES against itself.

### 2. The codes are mutually incomparable

Four different hash recipes over four different field combinations. There is no
join that says "this NPPES address IS this MRF address". The closest thing we
have (`entity_address_unified`) recomputes a *fifth* identity, per entity, in a
full batch rebuild — and marketplace `mrf_address` isn't even one of its sources.

### 3. 32-bit checksums collide — silently

All the CRC32 codes are 32-bit integers used as primary keys
(`db/models/_legacy.py:1121`). With ~4–5M unique addresses, the math of the
"birthday paradox" makes collisions a statistical certainty (thousands expected
at ~10M). A collision is invisible and destructive:

- in `npi_address`, the second address is **silently dropped** (ON CONFLICT DO
  NOTHING on the colliding key) — a real practice location disappears;
- in `address_archive`, a later address **overwrites** an earlier address's
  cached geocode (`api/endpoint/npi.py:3629-3638` does a full-row upsert by
  checksum), and the NPPES geo backfill (`process/npi.py:850-856`) then copies
  that wrong geocode around by the same colliding key.

In healthcare, attaching the wrong coordinates to a clinic, or losing one, is a
patient-facing error. 32 bits is simply too small a code space.

### 4. Bugs found during this analysis (independent of the redesign)

These are losing or corrupting data **today** and get fixed first (Phase 0):

- **B1 — conflict-target ordering** (`process/ext/utils.py:720-729`):
  `_conflict_targets` prefers `__my_initial_indexes__` over
  `__my_index_elements__`, so `MRFAddress` batches dedupe on the *non-unique*
  `checksum` column — addresses legitimately shared by several NPIs are
  silently dropped inside every 10k flush.
- **B2 — chimera addresses** (`process/entity_address_unified.py:439-443`):
  the unified builder borrows the parent enrollment's street lines onto
  street-less FFS rows, fabricating addresses that never existed, and serves
  them to pricing.
- **B3 — `mrf_address` missing from unified**: the marketplace addresses never
  reach `entity_address_unified` at all.
- **B4 — publish gate too low**: `entity_address_unified` publishes if it has
  ≥10,000 rows — a truncated build would still pass. Production floor should
  be ~1M.

## The goal

> Whatever import the data came from — NPPES, CMS doctors, MRF, PTG, pharmacy,
> enrollment — an address that is *the same place* is stored **once**, in
> `mrf.address_archive`, under **one stable fingerprint**, with a record of which
> imports saw it, full geocode data, and no slowdown of any import.

## Non-goals (explicitly out of scope)

- **No fuzzy auto-merge.** "St Marys Hospital, 1 Main St" vs "Saint Mary's
  Hospital, One Main Street" will NOT be merged automatically. Deterministic
  cleaning only; near-matches go to a human review queue (see 02, 03).
- **No rewriting of import file parsers.** Parsing, COPY loading, table-swap
  publishing all stay as they are.
- **No dropping per-source tables.** They remain the immutable audit layer;
  they just gain a pointer (the fingerprint) into the shared address book.
