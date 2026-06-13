# Address Canonical Baseline

Date: 2026-06-11  
Database: local `healthporta` on `127.0.0.1:5440`, schema `mrf`  
Mode: read-only SQL; no migrations or writes were applied to this database.

## Table Coverage

| Table | Rows | Notes |
|---|---:|---|
| `npi_address` | 19,221,939 | Legacy table, no `address_key` column yet |
| `address_archive` | 916,949 | Legacy checksum archive |
| `doctor_clinician_address` | 2,752,652 | CMS doctors address source |
| `provider_enrollment_ffs` | 2,956,287 | FFS parent address source |
| `provider_enrollment_ffs_address` | 917,578 | FFS city/ZIP address source |
| `facility_anchor` | 24,182 | Facility anchor source |
| `mrf_address` | missing | Not present in this local DB snapshot |
| `pharmacy_license_record_v1` | 9,796 | Pharmacy license source |
| `partd_pharmacy_activity_v2` | 469,654 | Part D pharmacy activity source |
| `entity_address_unified` | 25,786,507 | Existing unified output |

## CRC32 Collision Buckets

These counts group by the legacy checksum and count buckets where the same
checksum has more than one distinct address text.

| Table | Collision Buckets |
|---|---:|
| `npi_address` | 4,708 |
| `address_archive` | 0 |

This confirms the checksum bridge must be one-to-one and collision-aware:
`address_checksum_map` can only contain non-colliding checksums, and colliding
checksums must stay out of checksum-based fallback joins.

## Raw Rows vs Approx-Normalized Distinct

Normalization here is a read-only approximation of the existing lower/alnum
normalizer: line1, line2, city, state, ZIP5, country. It is not the new
`addr_*_v1` identity.

| Source | Raw Rows | Approx Distinct | Duplication Factor |
|---|---:|---:|---:|
| `npi_address` | 19,221,939 | 5,701,208 | 3.37 |
| `address_archive` | 916,949 | 854,025 | 1.07 |
| `doctor_clinician_address` | 2,752,652 | 391,119 | 7.04 |
| `provider_enrollment_ffs` | 2,956,287 | 56 | 52,790.84 |
| `provider_enrollment_ffs_address` | 917,578 | 32,226 | 28.47 |
| `facility_anchor` | 24,182 | 22,628 | 1.07 |
| `pharmacy_license_record_v1` | 9,796 | 8,898 | 1.10 |
| `partd_pharmacy_activity_v2` | 469,654 | 4,984 | 94.23 |
| `entity_address_unified` | 25,786,507 | 5,926,631 | 4.35 |

The FFS parent result is unusually collapsed in this local snapshot; it should
be rechecked in a production-sized report because many rows share a small number
of address values.

## Cross-Source Overlap

Using the same approximate normalized key across `npi_address`, CMS doctors,
FFS, FFS address, facility anchors, pharmacy license, and Part D:

| Metric | Count |
|---|---:|
| Keys seen in at least 2 source families | 217,071 |
| Keys seen in at least 3 source families | 5,795 |
| Keys seen in at least 4 source families | 30 |
| Maximum source families for one key | 4 |

This is the practical reason for a shared archive: a meaningful number of
addresses are already observed across independent imports.

## Unit Conflict Baseline

An exact full-table regex audit was rerun with session-local temporary tables
and no persistent writes. The audit extracts conservative suite/unit tokens,
normalizes the premise without the unit token, then counts premises with more
than one observed unit value.

| Table | Rows with Unit Token | Distinct Premise/Unit Pairs | Premises with Conflicting Units | Rows on Conflicting Premises | Max Units on One Premise |
|---|---:|---:|---:|---:|---:|
| `npi_address` | 6,890,982 | 1,740,942 | 246,319 | 4,992,788 | 335 |
| `address_archive` | 449,486 | 344,655 | 46,257 | 276,986 | 257 |

Largest conflicting-unit premises in `npi_address`:

| Premise Key | Unit Count | Rows |
|---|---:|---:|
| `21600oxnardst|woodlandhills|ca|91367|us` | 59 | 14,247 |
| `2000toweroaksblvd|rockville|md|20852|us` | 4 | 12,408 |
| `3500depauwblvd|indianapolis|in|46268|us` | 16 | 11,604 |
| `16782vonkaave|irvine|ca|92606|us` | 2 | 11,502 |
| `350fairwaydr|deerfieldbeach|fl|33441|us` | 27 | 10,214 |

Largest conflicting-unit premises in `address_archive`:

| Premise Key | Unit Count | Rows |
|---|---:|---:|
| `9500euclidave|cleveland|oh|44195|us` | 257 | 533 |
| `7777forestln|dallas|tx|75230|us` | 222 | 344 |
| `1001potreroave|sanfrancisco|ca|94110|us` | 139 | 329 |
| `55fruitst|boston|ma|02114|us` | 175 | 307 |
| `622w168thst|newyork|ny|10032|us` | 146 | 300 |

This supports the split-by-unit design: units must remain part of
`address_key`, while `premise_key` can group sibling suites for review/geocode
sharing.

## Follow-Up

- Re-run this baseline after `mrf_address` exists in the target environment.
- Re-run after Phase 3 archive migration to compare v1/v2 row counts and
  collision quarantine counts.
