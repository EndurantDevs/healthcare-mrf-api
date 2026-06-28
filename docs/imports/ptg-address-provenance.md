# PTG address provenance

PTG/TiC rate files reliably bind negotiated rates to provider identities by NPI/TIN/provider group and network name. They do not reliably bind a negotiated rate to a displayed street address.

Current serving behavior treats the street address as provider-identity evidence unless the source explicitly contains payer-confirmed location fields:

- Every PTG priced provider row must include `address_verification`, even when no displayable address is available.
- `network_names` identifies the priced network context emitted by the TiC artifact.
- `address_verification.rate_network_binding = tic_provider_group_npi_tin` means the rate was matched through the TiC provider group identity.
- `address_verification.address_sources` describes sources for the displayed address only. It must not include `ptg` unless PTG/TiC supplied address fields that were explicitly materialized as address evidence, and a bare `ptg`/`tic` source label alone is not enough to claim payer-confirmed location evidence.
- `address_verification.address_network_binding = inferred_from_provider_identity` means the displayed address came from NPPES/unified-address enrichment, not from the TiC rate file.
- `address_verification.address_evidence_level = provider_directory_address` means a payer Provider Directory FHIR record links the provider identity to this address, but no PTG plan/network context was proven.
- `address_verification.address_network_binding = payer_directory_corroborated_location` means a payer Provider Directory FHIR record links the provider identity to this address and a PTG plan or network context.
- `address_verification.provider_directory_plan_context_matched = true` means the corroboration found a matching FHIR `InsurancePlan` identifier for the PTG plan.
- `address_verification.provider_directory_network_name_matched = true` means the served PTG `network_names` matched a FHIR network `Organization` name or alias referenced by the Provider Directory role, affiliation, or matched plan. The concrete match must include a `ptg_network_name` that is present in the same row's served `network_names`.
- `address_verification.address_verification_evidence.matched_on` records the low-level join shape, for example `npi_address_key_role_location_plan` for a plan-context match or `npi_address_key_role_location_network_name` for a served-network-name match.
- A `payer_directory_corroborated_location` marker is not enough on its own; serving downgrades it to address-only evidence unless it has a plan-context match, or it has a network-name match backed by `_network_name` evidence and concrete network-name match details.
- `address_verification.displayed_address_present = true` means the row contains a usable displayed address, either a street line or a city paired with state or ZIP. `address_key` alone, or a lone city/state/ZIP field, is only partial provenance and does not count as a displayed address. If it is false, the row proves rate/provider identity but does not provide an address to trust. Serving strips sibling address/map/phone/distance/coordinates/location fields in that state and keeps `address_verification` minimal with `address_evidence_level=unknown`; nested address/location evidence fields such as `address_sources`, `location_source`, and Provider Directory match details are omitted. This is a valid schema/provenance state, but it must fail strict displayed-address assurance.
- `address_verification.network_bound_address = true` means the displayed address is tied to direct payer/PTG/TiC location evidence or payer Provider Directory plan/network context for the served PTG row. If it is false, the address is absent or is a best-known provider address from NPPES, unified-address, MRF-address, or address-only Provider Directory evidence and should be confirmed before member-facing exact-office guidance.
- `address_verification.address_evidence_level = unknown` means address certainty is unavailable and must not pass the assurance report for a displayed PTG address.
- `address_verification.address_network_binding = payer_confirmed_location` is reserved for sources that explicitly emit payer-confirmed provider locations, and the row must include a direct payer-location marker such as `location_source=payer_provider_group_location`, row-level materialized PTG/TiC source-record evidence such as `provider_group_id`, `provider_group_hash`, `provider_group_global_id_128`, `source_record_key`, or `json_pointer` in `address_verification_evidence`, and `source_trace.source_file_version_id` so the retained raw TiC artifact can be audited.

To audit a retained raw artifact for direct location fields, run:

```bash
rtk venv/bin/python scripts/research/audit_tic_provider_location_evidence.py /work/ptg2-artifacts/raw/path/to/file.json.gz
```

If `direct_location_fields_present` is false, the source artifact did not expose provider address/phone-like fields under provider references or inline provider groups. In that case, user-facing results must not claim that the street address is payer-confirmed; they should use `address_verification` and, when useful, `network_names`.

If `direct_location_fields_present` is true but `direct_displayable_location_fields_present` is false, the raw artifact exposed only partial location signals such as phone-only or city-only fields. That is useful audit evidence, but it is not enough to display a payer-confirmed address.

If `direct_displayable_location_fields_present` is true, treat that as an ingestion design signal, not proof that current serving rows already use those fields. The current provider-reference/provider-set import materializes NPI/TIN/provider-group identity and `network_names`; it does not promote arbitrary TiC provider-group address/phone fields into `ptg_address`. Before emitting `address_network_binding=payer_confirmed_location` from those fields, add an explicit schema/materialization path and tests that preserve the source record key, address fields, and phone fields in `address_verification_evidence`.

To inspect a served API response together with optional raw artifacts, run:

```bash
rtk venv/bin/python scripts/research/ptg_address_assurance_report.py \
  --api-payload /tmp/ptg-response.json \
  --raw-artifact /work/ptg2-artifacts/raw/path/to/file.json.gz \
  --strict
```

If the API response was fetched with source traces and contains `source_file_version_id`,
the report can resolve retained raw artifacts from the PTG source-file table:

```bash
rtk venv/bin/python scripts/research/ptg_address_assurance_report.py \
  --api-payload /tmp/ptg-response.json \
  --resolve-raw-artifacts-from-db \
  --require-resolved-raw-artifacts \
  --db-host 127.0.0.1 \
  --db-port 5440 \
  --database healthporta_test \
  --db-user nick
```

For an explicit source file version, use `--source-file-version-id <id>` with
`--resolve-raw-artifacts-from-db`. The report then audits the resolved local
`file://` raw artifacts and includes a `raw_artifact_resolution` section.
Add `--require-resolved-raw-artifacts` for deployment verification; with that
flag, any traced source id that is missing from `ptg2_source_file_version`,
points at a non-local URI, or points at a missing local file fails the report.
When an API row claims `address_network_binding=payer_confirmed_location`, it
must carry `source_trace.source_file_version_id`, and the resolved raw artifact
for that source id must contain direct displayable provider-location fields. If
it does not, the report fails even if the row-level `address_verification`
object is otherwise well formed.

After deployment, first verify the public dev API contract has the address
assurance field. Use the API ingress, not the app/docs host:

```bash
rtk bash -lc "curl -fsS https://api-dev.healthporta.com/openapi.yaml | rg -q 'network_bound_address'"
```

Then fetch api-layer directly:

```bash
rtk venv/bin/python scripts/research/ptg_address_assurance_report.py \
  --api-url "https://api-dev.healthporta.com/api/v1/provider-services-claims/services/CPT/29888/providers?plan_id=010854205&plan_market_type=group&zip5=62401&zip_radius_miles=50&include_sources=true&include_providers=true&limit=5" \
  --api-key "$HEALTHPORTA_API_KEY" \
  --require-network-names \
  --require-source-file-version-id \
  --require-network-bound-address \
  --strict
```

To make that deployed check prove retained raw-source auditability too, run it
from a node that can reach the dev database and retained artifact mount, and add:

```bash
  --resolve-raw-artifacts-from-db \
  --require-resolved-raw-artifacts
```

The assurance report fails when a priced row has no usable address, has no `address_verification`, uses `address_evidence_level=unknown`, claims `payer_confirmed_location` without direct payer/PTG/TiC location evidence plus row-level materialized source-record evidence and `source_trace.source_file_version_id`, claims `payer_confirmed_location` against a traced raw TiC artifact that lacks direct displayable provider-location fields, or mislabels address-only Provider Directory evidence as plan/network corroboration. With `--require-network-names` and `--require-source-file-version-id`, it also fails if any PTG price row lacks retained PTG network context or raw source-file traceability. Raw artifacts that are supplied without a `source_file_version_id` mapping are audit context only; traced raw artifacts are enforcement evidence.

For member-facing exact-office approval, add `--require-network-bound-address`. That mode rejects best-known NPPES, unified-address, MRF-address, and address-only Provider Directory rows even when they have usable street fields. It only passes displayed addresses with `network_bound_address=true`, which means the address is tied to direct payer/PTG location evidence or to payer Provider Directory plan/network context for the served PTG row. It also requires retained `network_names` and `source_trace.source_file_version_id`, so strict assurance proves the PTG rate/network context and raw source traceability together with the displayed-address claim. Missing `network_bound_address` is contract drift, not a legacy shape to infer from.

For a schema/provenance check that should accept explicit no-address provider-set rows, add `--allow-missing-displayed-address`. In that mode, a no-address row only passes when it has `displayed_address_present=false`, no usable address fields, `address_network_binding=inferred_from_provider_identity`, `address_evidence_level=unknown`, `requires_location_confirmation=true`, and no nested address/location evidence fields in `address_verification`. Do not use that mode for member-facing address display validation.

## Provider Directory Corroboration

Provider Directory FHIR is the preferred upgrade path for inferred PTG addresses. It can corroborate a PTG address when all of the following match:

- PTG `ptg_address.npi` equals a FHIR `Practitioner.npi` or `Organization.npi`.
- The FHIR role or affiliation references a FHIR `Location`.
- The FHIR location's canonical `address_key` equals the PTG `address_key`.
- The role, provider, and location are active or not explicitly inactive.

The SQL helper `provider_directory_ptg_address_corroboration_sql()` builds the `ptg_provider_directory_address_corroboration` view for this join. Rows from that view can promote serving output to `address_evidence_level=provider_directory_address`. They promote to `address_network_binding=payer_directory_corroborated_location` only when the FHIR `InsurancePlan` context matches the PTG plan identifier or the served PTG `network_names` strictly match a referenced FHIR network `Organization` name or alias. The view keeps raw FHIR network context, while the served API `address_verification.provider_directory_network_matches` field must contain concrete reconstructed matches with both the served `ptg_network_name` and matched `provider_directory_network_name`; raw Provider Directory network names alone are retained context, not proof. Assurance validation rejects network-name corroboration when the match's `ptg_network_name` is absent from that row's `network_names`.

To verify the generated SQL against a real PostgreSQL database with isolated fixture data, run:

```bash
rtk venv/bin/python scripts/smoke/provider_directory_ptg_address_corroboration_smoke.py
```

The smoke creates a disposable schema, builds the real corroboration view, and asserts both layers of behavior: the raw view marks a plan-matched Provider Directory role as `payer_directory_corroborated_location` and an address-only role as `provider_directory_address`; serving then promotes the address-only row only when the requested PTG `network_names` produce a concrete directory-network name match.

To check real database coverage for a specific PTG plan after imports or on dev,
run the Provider Directory coverage audit with a plan filter:

```bash
rtk venv/bin/python scripts/research/provider_directory_coverage_audit.py \
  --host 127.0.0.1 \
  --port 5440 \
  --database healthporta_test \
  --user nick \
  --schema mrf \
  --ptg-plan-id 010854205 \
  --format markdown
```

For a plan that is ready for Provider Directory address assurance, the report
should show `ptg_address_rows > 0`, plus non-zero PTG corroboration rows. If it
reports no `ptg_address` rows, that environment does not contain the plan's
served PTG address projection. If it reports PTG rows but no corroboration rows,
the exact-office trust decision must fall back to raw TiC direct-location
evidence or remain `network_bound_address=false`.
