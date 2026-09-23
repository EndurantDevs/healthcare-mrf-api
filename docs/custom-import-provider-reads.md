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
`lt` metric predicates and cannot name a context dimension. Up to three context
and metric terms combined, and three order terms, are accepted. Imported ordering
requires an equality selector for every declared selection-context dimension.

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
native relation. Pages retain the native 200-provider maximum and native NPI
tie-break order.

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
  ]
}
```

Projected values distinguish `value`, `null`, and `missing`. Decimal values are
strings. The returned fields come from the exact matching family and context;
filter-only queries matching multiple contexts use a deterministic winner
tie-break. Detail reads remain the route for all children of a selected family.

Authorization, native count/page queries, batched field hydration, and finality
checks run inside one bounded read snapshot. Responses are private and no-store.
A missing required match, invalid native page, unavailable pinned generation, or
response exceeding 256 KiB fails closed; the route does not return a partial or
unextended fallback. Requests without extension composition retain the ordinary
provider endpoints and their existing behavior.

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
