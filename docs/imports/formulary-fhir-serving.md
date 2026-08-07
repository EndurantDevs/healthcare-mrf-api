# FHIR formulary serving

The first FHIR formulary read surface is intentionally narrow and dormant by
default. It serves one current published CoveragePlan at
`GET /api/v1/formulary/fhir/{formulary_id}` only when
`HLTHPRT_FHIR_FORMULARY_SERVING_ENABLED` is explicitly enabled for that API
process.

The selector is the existing opaque `fhir_` public formulary identifier. The
response exposes only allowlisted plan fields and publication times. Source,
dataset, generation, run, alias, upstream, configuration, metadata, and hash
identities remain internal. Unknown, malformed, unpublished, and non-current
identifiers share the same not-found response.

Every read runs in one repeatable-read, read-only transaction and resolves the
current pointer through an exact published dataset. Source `enabled` state is
not a serving gate: it controls acquisition, while the published pointer is the
serving authority. All responses use `Cache-Control: private, no-store`.

There is no public source catalog yet because the storage schema has no
reviewed public source identity. Medication pagination is also deferred until
an indexable public coverage-context or serving-projection contract is approved.
