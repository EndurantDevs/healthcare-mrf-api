# Imported fields in provider list, geo, and service reads

`POST /api/v1/extensions/custom-import/providers` composes a pinned custom-import
generation with the native provider list. Provider composition uses the
provider-only signed extension-read transport (v2), separate from the generic
`/extensions/custom-import/search` and `/extensions/custom-import/detail`
transport (v1). The two transports use distinct body-hash and signature domains;
a permit for one cannot authorize the other. The host must authorize the
extension scope before signing a request. These routes do not accept unsigned
requests or URL query parameters.

The canonical JSON body has exactly these six properties:

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
  "require_match": true
}
```

The example illustrates the document shape; the transport signer must produce
canonical JSON and bind its exact bytes, target, route, authorization scope, and
the provider-v2 signing domain. Fields and aliases must be declared by the
pinned definition. `context` contains only non-null equality selectors for
declared context dimensions. `filters` contains only non-null `eq`, `gt`, or
`lt` metric predicates and cannot name a context dimension. Ungrouped requests
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

The native response envelope and provider fields remain unchanged. Each returned
provider gains `custom_import`, either null for an absent order-only match or:

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

Opted-in grouped list and geo requests with `family_entitlement: "full_family"`
return `custom_import` as a `custom-import/entity-family-set/v1` document with
`target`, `projection`, `selection`, `families`, and `missing_group_values`.
`projection` is `full_family`; each selected family includes its root fields and
complete child collections, not only the query-context child. Grouped requests
accept four combined context and metric predicates, or five with the declared
grouped child query. An implicit default selection value counts as one predicate.
The limit of three order terms is unchanged.

Ordinary complete-family pages and opted-in grouped full-family pages allow at
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

## Geo pages

`POST /api/v1/extensions/custom-import/providers/geo` uses the same six-property
signed body with native geo parameters such as `lat`, `long`, `radius`, `limit`,
and `cursor`. Native values are strings; the page limit is 50. It returns
`items`, an exact `total_count`, `has_more`, `next_cursor`, and
`result_identity: ["npi", "address_key"]`. Full and card views both include the
same nullable `custom_import` field described above.

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
six-property provider-v2 body. Its `native_query` must include `code` and
`code_system`; other accepted native service parameters remain strings. The
route requires an exact declared context when the selected profile has context
dimensions, then applies imported membership and ordering before native claim
thresholds, count, ordering, and pagination. The native service response keeps
its `items`, `pagination`, and `query` envelope; each item receives the same
nullable `custom_import` field described above.
