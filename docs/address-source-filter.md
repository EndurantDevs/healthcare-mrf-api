# Minimum address sources

Use `min_address_sources=3` to return addresses with a stored `source_count` of
at least three. The optional integer ranges from 0 through 2147483647. Omitted
or zero keeps the existing behavior. Native provider batches accept the same
optional field as a JSON integer.

The count belongs to the exact provider/address. It uses current office evidence
from the request's serving snapshot, including when the associated prices come
from a historical release. It does not count independent payers or establish
that a provider still practices at the address. `mrf_source_count` and profile
fact source counts retain their separate meanings.

Positive thresholds apply before office selection, candidate limits, counts and
pagination. Searches exclude providers without qualifying offices. Provider
details retain an authorized identity with empty addresses when the threshold
excludes every office. Start pagination again when changing the threshold.

Upstream clients can negotiate `X-HealthPorta-Address-Source-Filter: v1`.
Require that exact acknowledgement once on the same successful response to
avoid accepting an older server's unfiltered result during a mixed release.
Absent negotiation leaves response headers unchanged.

The OpenAPI operations document support for provider list, geo, match, detail,
profile and batch reads; canonical network offices; provider procedure and drug
searches; exact billing searches; pharmacy licenses and reports; and hospital
facility searches. Managed and custom-import request descriptors carry the same
filter. Addressless provider-level network memberships cannot use a positive
threshold.

Addresses without a known corresponding stored count do not qualify. This
includes coarse claims geography, raw profile postal contacts and hospital
metadata without an exact office correspondence. Other profile facts and
statistical cohort geography remain available. A source's own fact count does
not substitute for the unified address count.

```http
GET /api/v1/npi/all?zip_code=84001&min_address_sources=3
GET /api/v1/npi/id/1000000004?min_address_sources=3
```

```json
{"npis":["1000000004"],"min_address_sources":3}
```
