# Imported fields in provider list, geo, and service reads

`POST /api/v1/extensions/custom-import/providers` composes a pinned custom-import
generation with the native provider list. Provider composition uses the
provider-only signed extension-read transport (v2), separate from the generic
`/extensions/custom-import/search` and `/extensions/custom-import/detail`
transport (v1). The two transports use distinct body-hash and signature domains;
a permit for one cannot authorize the other. The host must authorize the
extension scope before signing a request. These routes do not accept unsigned
requests or URL query parameters.

Public gateways use `ext_filter` to select filtering and ordering. GET routes
accept its JSON-encoded object; provider batch POST accepts an object. The
independent `include` string selects payload enrichment and may name the same
import, another authorized import, or multiple imports. Filtering alone does
not imply enrichment. A gateway signs `include_filter: true` only when the
included name selects the filter's same current attachment.

The canonical signed JSON body contains these properties:

```json
{
  "target": {
    "dataset_key": "synthetic_dataset",
    "generation_id": 101,
    "definition_revision_id": 11,
    "schema_revision_id": 11,
    "profile_id": "default"
  },
  "native_query": {"q": "Synthetic", "limit": "50"},
  "context": [{"field_id": "service_code", "operator": "eq", "value": "example"}],
  "filters": [{"field_id": "amount", "operator": "gt", "value": "12.50"}],
  "order": [{"field_id": "amount", "direction": "desc"}],
  "require_match": true,
  "include_filter": true
}
```

The example illustrates the document shape; the transport signer must produce
canonical JSON and bind its exact bytes, target, route, authorization scope, and
the provider-v2 signing domain. Fields and aliases must be declared by the
pinned definition. `context` contains only non-null equality selectors for
declared context dimensions. `filters` contains only non-null `eq`, `gt`, `gte`,
`lt`, or `lte` metric predicates and cannot name a context dimension. `gte` and
`lte` include values equal to the threshold; `gt` and `lt` exclude them. Ungrouped requests
accept up to three context and metric terms combined, and three order terms.
Imported ordering requires an equality selector for every declared
selection-context dimension.

- `require_match: false` retains native providers without a matching import.
  Metric `filters` are rejected in this mode; context selection remains allowed.
  Context equality selectors choose the imported context; they do not remove
  otherwise eligible native providers. Imported records sort first, including
  records with null or missing sort values. Absent imports sort last in both
  ascending and descending order.
- `require_match: true` restricts the native result to matching imported winners.
  Use this when applying imported metric predicates. `order: null` permits a
  filter-only query.
- Selection happens before metric filtering. A failing selected family is not
  replaced by a lower-priority family. Child predicates use the same selected
  child, not independent matches from sibling records.

Native query values are strings, except `name_like`, which may also be a bounded
list of strings. The signed route requires exact totals and a provider-page
response; count-only, sitemap, and alternate-format requests are not supported.
Filtering and imported ordering precede pagination over the complete eligible
native relation. Complete-family composition pages admit at most 50 returned
provider rows and retain the native NPI tie-break order. Generic search and
grouped query-projection reads keep their existing page limits.

The native response envelope and provider fields remain unchanged. With
`include_filter: false`, rows omit `custom_import` entirely and no imported
family payload is hydrated or rendered. An omitted flag defaults to true to preserve deployed signed-v2 callers; new gateways always send an explicit Boolean. With `include_filter: true`, the same prepared
query and pinned target supply `custom_import`, either null for an absent
order-only match or:

```json
{
  "target": {
    "dataset_key": "synthetic_dataset",
    "generation_id": 101,
    "definition_revision_id": 11,
    "schema_revision_id": 11,
    "profile_id": "default"
  },
  "root_fields": [],
  "context_fields": [
    {"field_id": "amount", "field_type": "decimal", "state": "value", "value": "12.50"}
  ],
  "children": [
    {
      "collection": "facts",
      "fields": [
        {"field_id": "amount", "field_type": "decimal", "state": "value", "value": "12.50"}
      ]
    }
  ]
}
```

Projected values distinguish `value`, `null`, and `missing`. Decimal values are
strings. In the signed provider query contract, a gateway preserves a caller's
fractional or exponent JSON number as an exact tagged value such as
`{"decimal":"25e-1"}`. Quoted JSON strings remain strings rather than becoming
numeric tags; schema validation still determines which values a field accepts.
Numeric envelopes are decoded against the declared field schema. Integer metrics
accept exact fractional comparison thresholds; stored integer values, grouped
selection keys, and complete child keys remain integral. Decimal comparisons
retain declared storage bounds without converting through binary floating point.
The returned fields come from the exact matching family and context;
filter-only queries matching multiple contexts use a deterministic winner
tie-break. `context_fields` describes that selected context; `children` contains
every child in every collection of the same selected family, with all declared
scalar projections. Child expansion does not change matching, ordering, totals,
or provider pagination. Repeated addresses of one provider share the same family.
Detail reads return all children of a selected family; opted-in grouped
full-family list and geo pages also return all children, as described below.

Child membership and scalar values are hydrated in batches for the returned page.
Each selected family may contain at most 1,000 children; all families are
preflighted before hydration, and each membership/scalar batch contains at most
1,000 children. Each complete family projection is bounded by 256 KiB before
further batches are retained. Complete ordinary provider list, geo, and service
pages use the same finite response policy as grouped full-family pages: at most
50 actual rows, at most 256 KiB per imported provider payload, and a final page
envelope of `(actual_rows + 1) * 256 KiB`. Each repeated geo-address occurrence
counts toward that envelope. Generic search and grouped query-projection pages
retain their existing 256 KiB response limit. Complete-family pages require a
compatible reader applying these bounds during transport, decoding, presentation,
and final response serialization; a legacy reader's aggregate bound is insufficient.

Provider-page signatures must originate from a host-authorized full-family
attachment. Generic summary/search reads retain their selected-context projection
and do not expand children. Authorization, native count/page queries, batched
field hydration, and finality checks run inside one bounded read snapshot.
Responses are private and no-store.
A missing required match, invalid native page, unavailable pinned generation, or
response exceeding the applicable per-provider or page bound fails closed; the
route does not return a partial or unextended fallback. Requests without extension
composition retain the ordinary provider endpoints and their existing behavior.

Included grouped list and geo requests with `include_filter: true` and
`family_entitlement: "full_family"`
return `custom_import` as a `custom-import/entity-family-set/v1` document with
`target`, `projection`, `selection`, `families`, and `missing_group_values`.
`projection` is `full_family`; each selected family includes its root fields and
complete child collections, not only the query-context child. Grouped requests
accept four metric predicates and four order terms, with up to seven combined
context and metric predicates when selecting a declared child. An implicit
default selection value counts as one predicate. Ungrouped reads retain their
existing three-term limits.

Included ordinary complete-family pages and included grouped full-family pages allow at
most 50 native rows. List pages accept `limit` or `page_size`; service pages accept
`limit`. A value above 50 is rejected with HTTP 400 before storage reads, even if
fewer providers would match. Each provider's complete
selected family set, including both groups, retains the 1,000-child and 256 KiB limits. Child
hydration uses batches of at most 1,000 children and checks each provider's
projected bytes before retaining further batches. The complete page is bounded
by `(returned row count + 1) * 256 KiB`: one allowance per row plus one for the
native envelope. Repeated geo addresses hydrate their shared provider once but
each serialized occurrence counts toward the page bound. Neither children nor
rows are truncated; exact totals, native ordering, and pagination are unchanged.
Legacy/query-projection pages and detail responses keep the 256 KiB bound.

## Exact native-page hydration

`POST /api/v1/extensions/custom-import/detail/batch` hydrates an already selected
native page without choosing providers, filtering membership, ordering results,
or creating a cursor. It uses the generic detail-v1 signing contract, bound to
the exact batch route, target, authorization scope, and canonical body bytes:

```json
{
  "target": {
    "dataset_key": "synthetic_dataset",
    "generation_id": 101,
    "definition_revision_id": 11,
    "schema_revision_id": 11,
    "profile_id": "default"
  },
  "entities": {"adapter_id": "npi", "values": ["1000000000", "1000000001"]},
  "family_entitlement": "full_family"
}
```

The body accepts 1–50 unique canonical ten-digit NPI strings. Grouped requests
use the same optional selection, child-query, and context descriptors as detail
reads. Native queries, metric filters, ordering, and URL query parameters are
rejected. Hosts deduplicate identities and split larger native pages into finite
batches, then attach each returned family to the original rows.

The response contains `target` and ordered `items`, each with the requested
`npi` and nullable `custom_import`. Every requested identity appears exactly
once, including absent families. Ordinary imports use the detail shape
`target`, `root_fields`, and complete `children`, without `context_fields`.
Grouped imports use the existing full-family set shape. Each family retains
the 256 KiB limit; the batch envelope is bounded by `(requested identities + 1)
* 256 KiB`. Reads use one bounded read-only snapshot and fail closed without
partial results when authorization, hydration, finality, or a limit fails.

## Filtered native batches

`POST /api/v1/extensions/custom-import/providers/batch` uses the same signed
provider-v2 body plus `native_batch`. This object has exactly `npis`,
`address_limit`, `address_offset`, `include_sources`, and `include_evidence`.
NPIs are 1–100 unique normalized ten-digit strings. Flat address pages retain
the native 1–20 limit and offset bound; premise grouping returns 1–5 groups
with at most five members per group. Normalized list eligibility and shaping
options live in `native_query`, independently of the import selectors.

One query is prepared for all requested identities. Native eligibility and
imported membership/order cover that complete finite set before successful
results are paged. Complete imported families are hydrated only when included,
in chunks of at most 50 using the same prepared query and target. The final
complete-family bound is `(successful returned rows + 1) * 256 KiB`.

The response retains native `items` with `npi`, `status`, and `provider` or
`error`, plus `requested`, `found`, `not_found`, and `meta`. Counts describe
all inputs before pagination; `pagination` carries successful `total`, `page`,
`offset`, `limit`, and `has_more`. Its default limit is the requested count.
Without effective ordering or explicit paging, items retain input order.
Configured default imported ordering counts as effective ordering. With
ordering or paging, successful page entries precede failures in input order.
Successful inputs outside the page are omitted and never become 404 entries.

The ordinary `POST /api/v1/npi/id/batch` retains its original five-field
request and summary response. An optional `native_query` uses the same normal
list eligibility, paging, card, and address-shaping path without an import pin.
An identity-only native provider remains a success when no native eligibility
filter requires an address. `extra_info`, card view, and premise grouping are
applied through the existing native projections; no per-NPI HTTP or query loop
is introduced. The native reader resolves all requested identities together.

## Geo pages

`POST /api/v1/extensions/custom-import/providers/geo` uses the same provider-v2
signed body with native geo parameters such as `lat`, `long`, `radius`, `limit`,
and `cursor`. Native values are strings; the page limit is 50. It returns
`items`, an exact `total_count`, `has_more`, `next_cursor`, and
`result_identity: ["npi", "address_key"]`. Full and card views both include the
same nullable `custom_import` field described above when `include_filter` is true;
otherwise the field is absent.

Native eligibility and provider-address deduplication precede imported ordering
and pagination. Absent imports remain last in either direction. Equal imported
values are ordered by unrounded native distance, NPI, and address identity.
Different addresses for the same NPI use the same selected imported family.

The opaque signed cursor binds the immutable imported generation, schema,
profile, effective native query, imported predicates/order, authorization scope,
and expiry. It carries only the last provider-address identity, not metric
values or an offset. Each continuation reconstructs that anchor's sort values
from the eligible relation; an absent or ambiguous anchor is rejected. The
original cursor expiry is retained across pages.

The native directory is live between requests, not frozen for the lifetime of
a cursor. Native updates can move results between pages under ordinary keyset
pagination semantics. Each individual request uses one read snapshot for its
exact count, page, and imported hydration; the imported generation remains
pinned across the traversal. Start a new query after cursor expiry or an
invalidated anchor.

## Service pages

`POST /api/v1/extensions/custom-import/providers/by-service` uses the same
provider-v2 body. Its `native_query` must include `code` and
`code_system`; other accepted native service parameters remain strings. The
route requires an exact declared context when the selected profile has context
dimensions, then applies imported membership and ordering before native claim
thresholds, count, ordering, and pagination. The native service response keeps
its `items`, `pagination`, and `query` envelope; each item receives the same
nullable `custom_import` field described above when `include_filter` is true;
otherwise the field is absent.

## Exact billing pages

`POST /api/v1/extensions/custom-import/providers/billing-search` composes the
provider-v2 query with exact billing-reference pricing. The body retains
`include_filter` and adds `billing_transport_context_sha256`; `native_query`
contains the canonical string parameters of the existing billing-search GET.
The request carries both existing signed header families. The provider-v2 proof
authenticates the complete POST body, and its context digest binds the separately
verified billing GET authorization. Neither proof alone authorizes this route.

Configured membership and typed ordering apply to the complete native
provider-location candidate scope before native price eligibility and
pagination. Missing optional imports sort last; configured null handling and
directions are preserved, with native identity breaking ties. The opaque cursor
binds the normalized imported query, verified authority and immutable import
generation together with the native request and generation. Fresh transport
timestamps and metering identifiers do not invalidate a continuation.

The direct response retains native `items` and `pagination`, including nested
rate occurrences. `include_filter: true` adds the normal internal
`custom_import` payload, or explicit null for an optional unmatched import, to
each item; false leaves that field absent. The existing 100-item billing page
remains supported. Native response budgets are checked before import hydration;
full-family hydration reuses the same prepared query in batches of at most 50
unique NPIs within the same read snapshot.
