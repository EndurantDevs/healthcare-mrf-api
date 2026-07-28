# UnitedHealthcare official-file identity and logical scopes

This contract names UnitedHealthcare's corporate official provider-file surface without treating it as the existing Louisiana FHIR source.

## Source boundary

The maintained Provider Directory acquisition manifest entry `uhc` remains unchanged: it is the probe-only `unitedhealthcare-community-plan-louisiana` owner, bound to source `pdfhir_0b5cfd565c53364a73981dcb` at the canonical `flex.optum.com` FHIR base. That source ID must not identify nationwide official files.

The corporate source is registered independently from that Louisiana endpoint:

- entry: `uhc-provider-files`
- display name: `UnitedHealthcare Official Provider Files`
- owner: `unitedhealthcare`
- source kind: `official_provider_files`
- portal: the public UHC interoperability portal
- catalog: the `providermrf.uhc.com` official-file surface
- adapter: `uhc-provider-file-catalog`
- Provider Directory source ID: `pdfhir_2754e999dd691175821ec26e`
- acquisition manifest entry: `uhc-provider-files`

It is an acquisition-configured transport adapter in the normal
`provider-directory-fhir` lifecycle. Source resolution, probe accounting,
grouping, concurrency control, progress, failure isolation, completion
tracking, artifact publication, Profile follow-up, and cleanup are shared with
the other Provider Directory sources. The adapter replaces only the transport
and atomic candidate build: it reads official catalog files instead of FHIR
REST pages.

The normal CLI selects it with `--import-resources --source-id
pdfhir_2754e999dd691175821ec26e`. `--uhc-catalog-set-sha256` is an optional
exact-current-catalog assertion, not a separate execution mode.

Missing official files download concurrently with a default bound of four and
a hard cap of eight. Native retention/admission and independent semantic files
each use a default bound of two and a hard cap of four; every worker owns its
database connection, and each native encoder also uses bounded range workers.
Environment overrides are
`HLTHPRT_UHC_PROVIDER_FILE_DOWNLOAD_CONCURRENCY` and
`HLTHPRT_UHC_PROVIDER_FILE_ADMISSION_CONCURRENCY` and
`HLTHPRT_UHC_SEMANTIC_FILE_CONCURRENCY`. Admission, canonical merge, proof
sealing, and source-local cutover remain deterministic and fail closed.

The existing healthcare read-model wire field remains `entry_id: "uhc"` for downstream compatibility. Its additive `source_entry_id: "uhc-provider-files"` identifies this registry contract. The current downstream controller ignores extra fields but rejects any replacement of the legacy `entry_id`.

## Reviewed filename census

`specs/uhc_provider_file_census_v1.json` records the filename-only census verified on 2026-07-22. It contains no per-file URL or downloaded content:

- 53 Community & State provider-membership files: `JSON_Providers_<product>.json`
- 24 state IFP provider-membership files: `JSON_Providers_<state>IEX.json`
- one exceptional IFP provider-membership file: `JSON_Providers_UHCEX_HIX.json`
- 24 state IFP plan-reference files: `JSON_PLANS_<state>.json`

The 24 IFP states are explicitly enumerated and the provider and plan files normalize to one shared logical scope per state. `UHCEX_HIX` is an explicitly unpaired, retained-only provider scope with `jurisdiction: null`; its token alone does not prove national coverage. Community & State files never pair to IFP plan files.

The live census proved the 53 Community & State product tokens, but not authoritative jurisdiction metadata. Every Community & State logical scope therefore retains its exact product and carries `jurisdiction: null`. No state is inferred from a token prefix, and ambiguous tokens such as `HMOSNP` and `PPOSNP` are not asserted to be national. Publication remains gated until reviewed jurisdiction evidence exists.

Parsing is census-closed. The public API always loads the repository census and verifies its frozen semantic digest; callers cannot substitute a census object or alternate path. Unknown names, case drift, unsafe paths, duplicates, missing files, wrong family or collection assignments, and broken IFP pairs fail the contract.

## Semantic and publication identity

The identity module reserves deterministic keys so acquisition and
materialization cannot collapse unlike plans or networks:

- plan identity binds logical scope, family, market, plan ID type, plan ID, and plan year;
- network identity binds the same fields plus network tier;
- dataset-build identity binds adapter and transform versions, catalog-set hash, deterministically sorted logical scopes, one artifact digest per source file, as-of date, and a sorted unique set of plan years. It does not assume that one build contains only one year.

A plan key retains the complete canonical logical-scope value rather than copying caller-controlled scope strings. Every consumer revalidates that scope against the frozen census, validates plan dimensions independently, and then verifies the digest. Network keys retain that validated plan key. Explicit validators provide the same boundary for directly constructed plan, network, and dataset-build dataclasses; a matching hash alone is not treated as authority.

The implementation admits exact retained bytes, seals native semantic facts,
materializes the six registered resource families, stores content-proof shards,
and atomically promotes one source-local dataset. Runtime success and
downstream readiness remain separate evidence and must be verified after
deployment.
