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

Those observations do not rule out an alternative paging contract. They do show
that ordinary cursor exhaustion, larger page sizes, guessed IDs, or reference
expansion cannot prove that the unobserved remainder is empty.

## Offset-mode investigation

Further bounded checks found a route beyond the published window on all five
collections: `_count=1&_getpagesoffset=N&_offset=0` returns ten resources, with
exact five-ID overlap between windows starting at N and N+5. Successful later
windows start at 100 for Practitioner, 250 for PractitionerRole, and 1,000 for
Location, Organization, and OrganizationAffiliation. A repeated Practitioner
window returned the same ordered IDs.

The endpoint advertises HAPI FHIR 7.6.1. Its
[version-pinned response builder](https://github.com/hapifhir/hapi-fhir/blob/63b2df5750203ed940233c03f2ccdd56edc72822/hapi-fhir-server/src/main/java/ca/uhn/fhir/rest/server/method/ResponseBundleBuilder.java#L98)
uses a different path when `_offset` is present, including zero: it assumes the
resource provider has already applied database paging and does not slice the
returned list again. The live results are consistent with Michigan applying
`_getpagesoffset` in its provider, then HAPI applying it a second time when
`_offset` is absent. Michigan's internal implementation is not public evidence.

Generated next links retain `_getpagesoffset=N` and increment `_offset`, not the
backend window. They are not a verified continuation for this mode. A proposed
adapter must keep `_offset=0`, advance the backend offset by actual returned
resources, and reject duplicate, malformed, or drifting pages. The response may
contain ten times `_count`; byte limits must remain enforced. Old completed
cursor checkpoints must not be reused for a new strategy.

This route is not yet an exhaustive acquisition contract. Deeper unfiltered
requests at offset 10,000 returned HTTP 504 after about 29 seconds for tested
Practitioner, PractitionerRole, Organization, and Location collections. All five
collections returned 504 at offset 1,000,000. A valid empty filtered Organization
search is possible, but an unfiltered terminal boundary remains unverified.
Gateway failures are errors, never evidence that the remainder is empty.

Later checks confirmed Organization row-offset continuity at 2,000: one
twenty-resource response exactly matched adjacent ten-resource windows. These
requests took approximately 17–21 seconds. Practitioner windows at offsets 500
and 510 already hit the 29-second timeout with either `_count=1` or `_count=2`.
The failure is therefore not avoided simply by increasing the window size.

The documented InterOp Station route is not a verified fallback. Its corresponding
Organization offset 10,000 and Practitioner offsets 500/10,000 returned empty
HTTP 200 searchset Bundles after approximately 29 seconds. This timing is
consistent with the relay masking an upstream timeout; server logs are needed
to confirm the mechanism. These empty responses cannot establish completeness.
The relay also namespaces resource IDs differently, so endpoint substitution
must not silently rewrite resource or reference identities.

Count-only requests do not supply a census: `_count=0` returns empty Bundles with
no total on four collections and HTTP 404 on Practitioner. The absence of a
total is unknown coverage, not zero population.

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

A complete import still requires verified exhaustive traversal and a trustworthy
terminal condition, or a supported full directory extract with per-resource
counts and a stable snapshot identity. Any proposed search partitions must also
account for missing values and overflowing partitions; ordinary successful
subset searches are not sufficient.

The [MiHIN developer guide](https://mihin.org/wp-content/uploads/2022/08/InterOp-Station-Third-Party-Developer-Portal-User-Guide-v1-8-18-22.pdf)
identifies the MDHHS tenant and original public FHIR endpoints, but supplies no
exhaustive pagination or directory-export contract. The
[MHB directory documentation](https://sandbox.mhbapp.com/provider-directory.html)
describes the public R4 interface; its separate patient/member bulk-access API is
not a Provider Directory export. No export was initiated or vendor contacted as
part of these checks.
