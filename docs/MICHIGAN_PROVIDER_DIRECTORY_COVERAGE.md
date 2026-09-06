# Michigan Provider Directory coverage

The public MDHHS/MHB directory is reachable, but the imported slice is not an
exhaustive directory. A successful cursor traversal does not establish complete
coverage for this source.

## Observed upstream behavior

Bounded checks from a US host on 2026-09-06 established:

- All five configured collections stop advertising continuation after ten pages.
  With `_count=1`, each traversal yields ten records. The configured page sizes
  yield 1,000 Locations, 1,000 Organizations, 1,000 OrganizationAffiliations,
  100 Practitioners, and 250 PractitionerRoles: 3,350 resources in total.
- References from the imported roles identify resources missing from the imported
  Practitioner and Location slices. Direct resource reads return those records.
- Bundles omit `total`; `_summary=count` and `_total=accurate` return HTTP 400.
- All five collections ignore an impossible future `_lastUpdated` filter.
  Practitioner also ignores an invalid date and a before-1900 filter.
  `meta.lastUpdated` changes between reads of the same record, so it cannot serve
  as a stable partition watermark.
- Exact `_id` lookup works, but tested ranges, prefixes, and comma-separated IDs
  do not provide enumeration. Practitioner family-prefix searches work for
  subsets, but missing-family partitions fail. Missing-name/reference controls
  fail or are ignored for other collections. No complete partition scheme was
  established.

Only exact advertised opaque next links are valid continuation evidence.
Synthesized offsets, larger page sizes, guessed IDs, or reference expansion do
not prove that the unobserved remainder is empty.

## Containment

The importer rejects acquisition and new publication for this source, including
stale source metadata that asserts full coverage. Existing published resources
remain available, with a coverage warning; their counts describe the imported
slice, not the directory population. Historical run success records are not
rewritten as evidence of a different execution.

The acquisition manifest and Profile source selection are intentionally unchanged
by this correction: their authority participates in coordinated global Profile
transitions. The runtime coverage block takes precedence over the configured
acquisition classification. This correction does not release a Profile hold or
rebuild the global evidence layer.

## Required upstream resolution

A complete import requires either corrected, exhaustive server-issued pagination
or a supported full directory extract with per-resource counts and a stable
snapshot identity. Any proposed search partitions must also account for missing
values and overflowing partitions; ordinary successful subset searches are not
sufficient.

The [MiHIN developer guide](https://mihin.org/wp-content/uploads/2022/08/InterOp-Station-Third-Party-Developer-Portal-User-Guide-v1-8-18-22.pdf)
identifies the MDHHS tenant and original public FHIR endpoints, but supplies no
exhaustive pagination or directory-export contract. The
[MHB directory documentation](https://sandbox.mhbapp.com/provider-directory.html)
describes the public R4 interface; its separate patient/member bulk-access API is
not a Provider Directory export. No export was initiated or vendor contacted as
part of these checks.
