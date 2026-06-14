# address_archive_v2 efficiency snapshot - 2026-06-13

Environment: `ns1033171` dev server, database `healthporta`, PostgreSQL 18.x, after the current MRF/NPI import optimization deploy.

## Executive summary

`address_archive_v2` is doing useful deduplication and the archive upsert itself is no longer the dominant wall-time cost.

Latest dev evidence:

- `mrf` staged 16,491,194 address rows, collapsed to 1,318,664 distinct address keys, and resolved the archive in 79.111 seconds.
- `npi` staged 19,707,579 address rows, collapsed to 5,351,601 distinct address keys, and resolved the archive in about 123-129 seconds depending on metric boundary.
- Current archive size is 6,062,888 rows and 3,092 MB total, with 1,675 MB heap and the rest mostly unique/key indexes.
- Current source memberships total 7,162,687, so cross-source archive dedup removes about 15.35% of current source memberships.

Hotfix rerun evidence:

- MRF hotfix import `addrcanon_hotfix_mrf_20260613204028` completed on dev at
  2026-06-13 23:03 UTC with run id `run_8ed53868692f442b814d600db33f1ec8`.
- The start phase used 16 `process.MRF --burst` pods. The queue finished in
  about 125 minutes; the last two 1.4 GiB BestLife provider files dominated the
  tail and each used roughly one CPU core.
- MRF finalization completed in about 13 minutes. The longest observed SQL was
  the `mrf_address` source-summary array update, which ran CPU-bound in
  PostgreSQL for roughly 11 minutes before the archive/publish steps completed.
- Published MRF counts after the hotfix rerun were 16,010,563 `mrf_address`
  rows, 85,303,436 `mrf_address_evidence` rows, and 25,490,323 `plan_npi_raw`
  rows.
- `address_archive_v2` grew to 6,064,287 rows after the MRF hotfix publish.
- NPI hotfix import `addrcanon_hotfix_npi_20260613230636` completed on dev at
  2026-06-13 23:41 UTC with run id `run_2de97e9c95d2403088d999f495699a06`.
  The control run reached `npi published` in 1,979 seconds; the worker had
  already staged 19,706,175 address rows by 1,389 seconds and then completed
  `VACUUM FULL ANALYZE` on the new address table before exiting.
- Published NPI counts after the hotfix rerun were 9,606,683 `npi` rows,
  19,707,579 `npi_address` rows, 12,019,685 `npi_taxonomy` rows, and
  3,477,937 `npi_other_identifier` rows.
- `address_archive_v2` grew to 6,064,687 rows after the NPI hotfix publish and
  still had 0 key mismatches, 0 null identities, and 0 non-null `merged_into`
  rows.

The next speedup should target MRF finalization write shape and NPI shutdown tails, not a Rust rewrite of archive dedup. Rust may still be useful later for raw address key materialization if profiling shows Python normalization is hot, but the measured PostgreSQL archive resolve phase is already handling 16-20M staged rows in roughly 1-2 minutes.

The highest-priority algorithm correctness bug was `city_zip` unit loss. When v1
could not derive a street but could derive a unit-like token such as
`DEPARTMENT 1234`, it fell back to `city_zip` precision and explicitly blanked
`unit_norm` in the identity. That over-merged distinct lockbox, department,
building, and military unit addresses into one city/state/ZIP key. The
2026-06-13 hotfix now preserves that unit token in the v1 `city_zip` identity
for new imports, left-pads 3-4 digit ZIPs, and rejects unmapped US state text
instead of storing long pseudo-state codes.

The largest deduplication improvement candidate, meaning fewer over-split keys, is city alias handling for street-precision addresses. A dev scan found 94,668 groups where `line1_norm`, `unit_norm`, `state_code`, and `zip5` match but `city_norm` differs, representing 198,930 current archive rows and about 104,262 extra city-specific keys. That is larger than the safe unit parsing and PO Box cleanup candidates.

## Current archive shape

Total rows:

| Metric | Value |
| --- | ---: |
| `address_archive_v2` rows | 6,062,888 |
| table + indexes | 3,092 MB |
| heap | 1,675 MB |

Precision:

| Precision | Rows |
| --- | ---: |
| `street` | 6,030,595 |
| `city_zip` | 32,293 |

Source memberships:

| Source bit | Source | Rows with bit |
| ---: | --- | ---: |
| 1 | NPPES | 5,351,601 |
| 2 | CMS doctors | 423,678 |
| 4 | provider enrichment | 32,126 |
| 8 | facility anchors | 21,166 |
| 16 | MRF | 1,334,116 |
| 32 | pharmacy license | 0 |
| 64 | Part D | 0 |

Source freshness from the latest archive:

| Source | Rows | First seen min | Last seen max |
| --- | ---: | --- | --- |
| NPPES | 5,351,601 | 2026-06-12 11:02:52 UTC | 2026-06-13 12:35:18 UTC |
| CMS doctors | 423,678 | 2026-06-12 11:02:52 UTC | 2026-06-13 12:35:18 UTC |
| provider enrichment | 32,126 | 2026-06-12 11:02:52 UTC | 2026-06-13 03:48:50 UTC |
| facility anchors | 21,166 | 2026-06-12 11:02:52 UTC | 2026-06-13 12:35:18 UTC |
| MRF | 1,334,116 | 2026-06-12 11:02:52 UTC | 2026-06-13 12:35:18 UTC |

## Dedup efficiency

Within-source reduction:

| Source | Staged rows | Distinct keys | Reduction | Rows per key |
| --- | ---: | ---: | ---: | ---: |
| NPI | 19,707,579 | 5,351,601 | 72.84% | 3.68 |
| MRF | 16,491,194 | 1,318,664 | 92.00% | 12.51 |

Cross-source overlap:

| Overlap | Rows |
| --- | ---: |
| NPPES and MRF | 752,035 |
| NPPES and CMS doctors | 309,643 |
| MRF and CMS doctors | 235,792 |
| MRF without NPPES | 582,081 |
| NPPES without MRF | 4,599,566 |
| rows with more than one source bit | 869,251 |

This is meaningful but not huge cross-source compression. The biggest win is source-local canonicalization: MRF collapses repeated facility/provider evidence heavily before archive membership.

## Quality checks

Current invariants look healthy:

| Check | Result |
| --- | ---: |
| null `identity_key` | 0 |
| null `address_key` | 0 |
| null `source_bits` | 0 |
| duplicate `identity_key` groups | 0 |
| `address_key != addr_key_from_identity_v1(identity_key)` | 0 |
| street precision rows with null `premise_key` | 0 |

Exceptions worth cleaning up:

- 22 rows have `state_code` longer than two characters.
- 32,293 rows fall back to `city_zip` precision. Samples are mostly APO/FPO, military, or foreign-looking addresses, so this is not a broad normalizer failure.
- NPI had 12,832 null-key rows during staging. Reason buckets show 12,800 unsupported-country rows plus a small number of missing-state/street/ZIP cases.
- MRF had 2 null-key rows during staging, both attributable to feed-quality edge cases.

Archive resolve reason buckets from the latest runs:

| Source | Ambiguous unit | Unit conflicts | Unsupported country | Missing street | Missing ZIP | Missing state |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| NPI | 363,911 | 248,182 | 12,800 | 265 | 189 | 32 |
| MRF | 98,261 | 100,911 | 0 | 137 | 2 | 0 |

## Missed dedup candidates

These scans looked for current archive rows that are deterministic under v1 but likely over-split in real data. They are candidates for a versioned v2 key experiment, not in-place v1 changes.

### city_zip unit loss

This was an over-merge bug, not an over-split. Before the 2026-06-13 hotfix,
`identity_key_v1` computed `unit_norm` first, but when `street_norm` was empty
and `city_norm` existed, it switched to `precision = 'city_zip'` and set
`unit = ''`.

Pre-hotfix live SQL confirmed the old deployed function behavior:

| Input | Identity | Unit | Street |
| --- | --- | --- | --- |
| `DEPARTMENT 1234`, Knoxville TN 37995 | `v1|||knoxville|TN|37995|US|city_zip` | `dept1234` | null |

Post-hotfix live SQL and in-pod Python/Rust probes confirm:

| Input | Identity |
| --- | --- |
| `DEPARTMENT 1234`, Knoxville TN 37995 | `v1||dept1234|knoxville|TN|37995|US|city_zip` |
| ZIP `2138` | `02138` |
| state `calif` | null |

Current source impact:

| Source table | city_zip keys | source rows | multi-first-line keys | rows in multi-first-line keys | distinct first lines in multi keys | distinct unit values |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `mrf.npi_address` | 158 | 2,263 | 54 | 2,041 | 366 | 420 |
| `mrf.mrf_address` | 137 | 333 | 28 | 166 | 71 | 156 |

Post-hotfix source impact after the fresh MRF/NPI dev reruns:

| Source table | Source rows | Simulated old no-unit keys | Current keys | Rows split from old key | Current unit identities |
| --- | ---: | ---: | ---: | ---: | ---: |
| `mrf.npi_address` | 2,263 | 158 | 420 | 2,263 | 395 |
| `mrf.mrf_address` | 331 | 136 | 154 | 331 | 113 |

Archive-level post-hotfix shape:

| Metric | Value |
| --- | ---: |
| `city_zip` archive keys | 32,846 |
| `city_zip` keys with unit identity | 553 |
| simulated old city_zip keys | 32,293 |

This confirms the fix did not broadly rewrite the archive. It added the 553
unit-specific city/ZIP keys that were previously collapsed into the old no-unit
city/ZIP identities.

The worst confirmed NPI group is `51862d43-93c4-1b20-eb75-d16406cc5997`, which has 189 source rows, 46 distinct `first_line` values, and 42 distinct unit values under Knoxville TN `37995`.

Archive display rows show the same pattern: 216 `city_zip` archive rows still produce a non-empty `addr_unit_norm_v1(first_line, second_line)` if recomputed. Samples include `# 12`, `BLDG 1`, `BLDG 100`, and other unit-like first lines.

Implemented hotfix:

- Stop blanking `unit_norm` for `city_zip` identities.
- Left-pad 3-4 digit numeric ZIPs before keying.
- Reject unknown US state text instead of accepting long pseudo-state codes.
- Reapply the SQL functions through an owner-safe Alembic migration and verify
  Python, Rust, and PostgreSQL parity before running fresh dev imports.

Remaining versioned-v2 option:

- Add a separate `lockbox`, `unit_zip`, or `city_zip_unit` precision label for
  rows with no street but a strong unit/department/PO-box/building token. The
  current hotfix preserves correctness in the existing v1 precision string; a
  future v2 can make the precision semantics more explicit.

### City aliases

Street-precision addresses currently include `city_norm` in the identity:

```
v1|{line1_norm}|{unit_norm}|{city_norm}|{state_code}|{zip5}|US|street
```

If grouped by `line1_norm`, `unit_norm`, `state_code`, and `zip5`, the archive has:

| Metric | Value |
| --- | ---: |
| groups split only by city | 94,668 |
| rows in those groups | 198,930 |
| extra city keys | 104,262 |

Distribution by number of city variants:

| City variants | Groups | Rows |
| ---: | ---: | ---: |
| 2 | 86,508 | 173,016 |
| 3 | 7,043 | 21,129 |
| 4 | 902 | 3,608 |
| 5+ | 215 | 1,177 |

Source mix:

| Source mix | Groups | Rows |
| --- | ---: | ---: |
| NPPES and MRF | 60,265 | 128,206 |
| NPPES without MRF | 25,967 | 53,249 |
| MRF without NPPES | 8,227 | 17,054 |
| other | 209 | 421 |

Samples included ordinary aliases and misspellings like `fortsamhouston` / `ftsamhouston`, `littleeggharbor` variants, `miramar` variants, and `bentonville` variants. This is the strongest evidence that city normalization is too literal.

Do not simply drop city from v1. Build an offline v2 candidate first:

- preserve `zip5`, `state_code`, `line1_norm`, and `unit_norm`;
- either remove city from street-precision identity or replace it with a canonical ZIP-city alias;
- validate top candidate merges manually and with geocode/CASS data when available;
- keep `city_norm` as display/provenance even if it leaves the identity.

### ZIP variants and placeholder addresses

The symmetric "same street/unit/city/state but different ZIP5" grouping found:

| Metric | Value |
| --- | ---: |
| ZIP-variant groups | 32,340 |
| rows in those groups | 70,091 |
| extra ZIP keys | 37,751 |

Top samples were dominated by non-physical address text such as `telemedicineprovider`, `virtualcounselingonly`, `homebasedservicesonly`, `mobileprovider`, and `callforappointment`. That does not justify dropping ZIP from physical street identity. Instead, add a classifier for non-physical/service-modality address strings and either exclude them from street-precision archive keys or put them in a separate low-precision class.

Current placeholder-like rows found by simple patterns:

| Placeholder bucket | Rows |
| --- | ---: |
| other `virtual` / `telemedicine` / `homebased` / `callforapp`-like | 3,380 |
| `virtualcounselingonly` | 2,216 |
| `telemedicineprovider` | 1,021 |
| `homebasedservicesonly` | 619 |
| `mobileprovider` | 518 |
| `callforappointment` | 377 |

### Unit parsing over-splits

Rows with raw suite/unit/floor/building tokens but blank `unit_norm`:

| Source | Rows |
| --- | ---: |
| NPPES | 106,139 |
| MRF | 40,754 |
| CMS doctors | 4,839 |
| other | 99 |
| total | 151,831 |

Bucketed samples:

| Bucket | Rows | Notes |
| --- | ---: | --- |
| other complex unit text | 111,009 | mixed safe and unsafe cases, including `GROUND FLOOR`, `SUITE E6.100`, `FL 4 PLAZA 1`, but also street names containing unit words |
| `second_line` ordinal floor, e.g. `3RD FLOOR` | 12,574 | safe v2 candidate: normalize to `fl3` |
| line1 tail `Suite/Ste E/N/S/W` | 7,203 | possible candidate, but must avoid stealing directionals from street text |
| line1 tail designator + single letter | 6,385 | possible candidate with stricter guards |
| `second_line` `Suite/Ste E/N/S/W` | 6,200 | safe v2 candidate when the whole second line is the unit |
| `second_line` slash/multi-unit | 2,928 | handle only with explicit multi-unit rules |
| line1 tail ordinal floor | 2,808 | candidate with stricter guards |
| line1 tail `Floor/Fl <number>` | 1,033 | candidate with stricter guards |
| line1 slash/multi-unit | 855 | handle only with explicit multi-unit rules |
| `second_line` designator + other single letter | 836 | likely safe when the whole second line is the unit |

The current invalid-unit guard rejects `E/N/S/W` values because they are directionals. That is correct for ambiguous line1 tail parsing, but too conservative for a whole-line unit like `Suite E`. v2 should allow directional single-letter unit values only when the designator occupies the whole `second_line` or another unambiguous unit field.

### PO Box parsing

`line1_norm = 'pobox'` has 5,345 rows, 3,240 distinct `unit_norm` values, and 4,928 rows with `ste*` unit prefixes. Plain `pobox<number>` rows account for 237,074 rows.

This points to a smaller but real split:

- `PO Box 123` becomes street `pobox123`, blank unit.
- `PO Box #123` can become street `pobox`, unit `ste123`.

v2 should detect PO Box forms before unit extraction and keep the box number in `line1_norm`, not `unit_norm`.

### ZIP and state normalization tails

Two small function probes confirmed minor fragmentation risks before the hotfix:

| Probe | Current result | Effect |
| --- | --- | --- |
| `addr_zip5_norm_v1('2138')` | `2138` | fragments 4-digit ZIPs that should be zero-padded, for example `02138` |
| `addr_state_code_v1('calif')` | `CALIF` | accepts unknown US state text instead of returning null or mapping to `CA` |

The 2026-06-13 hotfix fixed both in the current v1 functions for new imports:
left-padding is applied to 3-4 digit numeric ZIPs, and unmapped US state text
returns null so malformed rows stay out of the archive instead of creating long
pseudo-state keys.

### Re-key plumbing

Current archive versioning is still not enough for broad normalization changes:

- `identity_version` exists but current resolve paths insert hardcoded version 1.
- `merged_into` exists but has 0 non-null rows in the 6,062,888-row archive.
- The current repair path restamps staging keys, not archive identities.

A broad v2 rollout needs a real re-key path: compute v2 identities, insert v2
canonical rows, record old-to-new relationships or atomically swap, and update
source tables. The narrow 2026-06-13 v1 hotfix is being validated by fresh
MRF/NPI imports on dev; larger semantic changes such as city aliasing should
not be shipped without this re-key path.

### Geocode is not usable for merge validation yet

Current archive geocode coverage:

| Metric | Rows |
| --- | ---: |
| non-null `place_id` | 0 |
| non-null lat/long | 21,014 |

Because `place_id` is empty, geocode cannot currently validate same-place candidate merges at scale.

## Performance interpretation

The live process screenshot showing many PostgreSQL `UPDATE` workers at 100% CPU is consistent with a CPU-bound database update phase, not an I/O-saturated job. That can still be correct for this workload, but the important distinction is where the CPU is spent.

Measured archive resolve cost:

- MRF archive resolve: 79.111 seconds.
- NPI archive resolve: 122.613 seconds top-level metric; 129.385 seconds in shutdown phase timing.

Latest NPI shutdown top costs:

| Phase | Seconds |
| --- | ---: |
| `vacuum_analyze:npi_address_20260613` | 150.835 |
| `canonical_address_resolve` | 129.385 |
| `plan_network_array_enrichment` | 62.273 |
| `vacuum_analyze:npi_20260613` | 45.740 |
| `vacuum_analyze:npi_taxonomy_20260613` | 45.313 |
| `taxonomy_array_enrichment` | 31.539 |
| `index_creation:npi_address_20260613:type_state_city_npi` | 16.634 |
| `do_business_as_enrichment` | 16.039 |

For MRF, the multi-hour wall time is not explained by `address_archive_v2` resolve. The heavier areas remain finalization write shape: source summary arrays, evidence volume, address-key stamping/propagation, and duplicate provider URL fan-out. The latest code path already moved toward precomputed MRF address keys and provider URL dedupe; a fresh timed MRF run should verify that those changes reduced the DB tail.

The 2026-06-13 MRF hotfix rerun confirmed two separate bottlenecks:

- Bulk parallelism helps until the queue tail. 16 MRF pods kept the dev host
  around 80-85% busy during the middle of the run without material I/O wait, but
  the final two huge provider files were single-job CPU tails. More pods cannot
  speed that tail; the next speedup is per-file chunking or a compiled row
  extractor for very large provider JSON files.
- The source-summary array refresh is still a single PostgreSQL update over the
  staged `mrf_address` table. It is CPU-bound and should be redesigned or
  incrementally materialized if MRF finalization needs another wall-time cut.

## Recommendation

Do not start with Rust for archive deduplication.

The current archive strategy is a deterministic key plus PostgreSQL unique insert/update. It is resolving tens of millions of staged rows in 1-2 minutes on dev. Replacing that with Rust will not remove the database uniqueness/update cost, and the measured bottleneck is elsewhere.

Use Rust only if the next profiling run proves address normalization/key materialization itself is CPU-bound outside PostgreSQL. In that case, the Rust boundary should be narrow: batch-normalize raw address fields into `identity_key`, `address_key`, `premise_key`, `line1_norm`, `line2_norm`, and reason buckets, then let PostgreSQL keep owning the unique archive merge.

For dedup quality, start with a versioned `v2` key experiment. The expected upper-bound improvements from the current archive are roughly:

| Candidate | Approximate upper bound |
| --- | ---: |
| `city_zip` unit preservation | hotfixed in v1; 2,596 NPI/MRF source rows affected pre-hotfix |
| city alias handling for street addresses | 104,262 fewer keys |
| safe unit parsing buckets | tens of thousands of rows to reclassify, exact key reduction TBD |
| PO Box `#` handling | up to 5,345 rows affected |
| malformed state/country strictness | 22 rows affected |

`city_zip` unit preservation was the first correctness fix even though it can
increase key count. The city candidate remains the largest key-reduction
opportunity. Unit and PO Box changes are still worthwhile because they improve
identity correctness and cross-source matching.

## Next steps

1. Run one fresh MRF finish on dev after the latest pre-key/provider URL dedupe changes and record phase timings for:
   - MRF address staging/key stamping.
   - child-to-parent key propagation.
   - `mrf_address_evidence` writes.
   - source summary array refresh.
   - index/vacuum phases.
   - `resolve_into_archive`.

2. Add persistent MRF phase metrics matching the NPI shutdown metric style. MRF needs the same per-phase JSON timing evidence so future optimization work is not inferred from `htop`.

3. Keep `address_archive_v2` as the canonical dedup sink. The immediate useful SQL improvement is not `ON CONFLICT DO NOTHING`; current logic must also update `source_bits`, display-priority fields, and first/last seen timestamps for existing keys.

4. Decide whether `source_bits` means "ever seen" or "present in latest source snapshot". Current `source_bits` accumulates source membership and does not clear bits for disappeared source rows. If product semantics require current membership, add source-run membership tables or a per-source freshness table rather than trying to infer staleness from `last_seen_at`.

5. Add a repeatable audit command/report for:
   - archive row total and size,
   - source-bit memberships,
   - null keys,
   - duplicate identity keys,
   - key parity failures,
   - `city_zip` fallback count,
   - state-code anomalies,
   - staged/distinct/inserted/provenance-updated/elapsed metrics per importer.

6. Clean the small quality tail:
   - inspect the 22 long `state_code` values,
   - classify NPI unsupported-country rows separately from malformed US rows,
   - keep the MRF 2 null-key rows as explicit feed-quality exceptions unless they recur at scale.

7. Revisit NPI shutdown cost separately. `vacuum_analyze:npi_address_20260613` is currently slower than archive resolve. Check whether regular `ANALYZE`, changed-only enrichment, or a different table-build/publish strategy can replace the heavier maintenance tail.

8. Do not judge pharmacy-license or Part D archive dedup yet. Their source bits are still 0 in the current archive, so they have not contributed meaningful live archive data in this snapshot.

9. Build an offline `address_key_v2` experiment over `address_archive_v2` and latest NPI/MRF staging tables:
   - separate `lockbox` / `unit_zip` precision labeling for the now-preserved
     `city_zip` unit identities,
   - street identity without literal city, or with ZIP-city alias canonicalization,
   - whole-`second_line` directional single-letter units,
   - ordinal floor parsing,
   - PO Box parsing before unit extraction,
   - ZIP left-padding for 3-4 digit ZIPs,
   - strict state/country validation.

10. Compare v1 vs v2 before migrating:
    - total keys,
    - over-merged v1 keys that split under v2,
    - merge groups and largest merge groups,
    - candidate merges by source mix,
    - samples for every merge group with more than two current keys,
    - false-positive review on high-risk groups like hospitals, campuses, military bases, and multi-tenant buildings.

11. Keep v1 immutable. Any identity change should land as explicit `identity_version = 2` with a migration/backfill plan, dual-write metrics, and a rollback path.
