# Provider Directory FHIR Import

`provider-directory-fhir` imports payer Provider Directory FHIR source metadata,
capability probes, and FHIR resources. It is meant to complement MRF/PTG price data with
FHIR directory data exposed by CMS-regulated payers.

## Source

The importer uses the public `provider-directory-db` SQLite catalog by default:

```text
https://raw.githubusercontent.com/hltiunn/provider-directory-db/main/data/provider_directory.db
```

Use `--seed-db-path` to run against a pinned copy during local or dev validation.
The source rows are stored in `provider_directory_source`, while live
CapabilityStatement probe results are stored in `provider_directory_capability`.
Probe parsing accepts UTF-8 BOM-prefixed JSON, because some payer endpoints
return valid FHIR JSON with that prefix. When the catalog stores a resource or
documentation path instead of the real FHIR base, probing also tries bounded
same-host parent bases for paths ending in `/metadata`, a FHIR resource type
such as `/Practitioner`, or `/provider-directory`. If a candidate publishes a
valid CapabilityStatement, that resolved base is persisted back to
`provider_directory_source.api_base` and used for the same run's resource fetches.

## Tables

- `provider_directory_source`
- `provider_directory_capability`
- `provider_directory_insurance_plan`
- `provider_directory_practitioner`
- `provider_directory_organization`
- `provider_directory_location`
- `provider_directory_practitioner_role`
- `provider_directory_healthcare_service`
- `provider_directory_organization_affiliation`

Provider, plan, network, and location references are retained as raw FHIR
references first. Address canonical linkage can be filled later through
`provider_directory_location.address_key`. Network references are resolved to
FHIR `Organization` names during PTG corroboration so served PTG
`network_names` can be matched without denormalizing network names into
`ptg_address`. Raw Provider Directory network names are context only; serving
should treat them as PTG network proof only after emitting concrete
`provider_directory_network_matches` objects with both `ptg_network_name` and
`provider_directory_network_name`.

## PTG Address Corroboration

Provider Directory FHIR can upgrade PTG addresses from NPPES-only inference to
payer-directory corroboration. The helper
`provider_directory_ptg_address_corroboration_sql()` builds the view
`ptg_provider_directory_address_corroboration`, which matches:

- `ptg_address.npi` to `provider_directory_practitioner.npi` or
  `provider_directory_organization.npi`;
- FHIR roles/affiliations to referenced FHIR locations; and
- FHIR `provider_directory_location.address_key` to PTG `ptg_address.address_key`.

Rows in this view are Provider Directory evidence, not direct TiC location
evidence. Address-only rows remain public
`address_verification.address_network_binding=inferred_from_provider_identity`
with `address_verification.address_evidence_level=provider_directory_address`,
because they prove the provider/address relationship but not the PTG
plan/network relationship. Serving promotes rows to
`address_verification.address_network_binding=payer_directory_corroborated_location`
with `address_verification.address_evidence_level=payer_directory_network_location`
only when either the FHIR `InsurancePlan` identifier matches the PTG plan or
the served PTG `network_names` strictly match a referenced FHIR network
`Organization` name/alias. A served network-name match must be exposed as
`provider_directory_network_name_matched=true` with
`address_verification_evidence.matched_on` ending in `_network_name` and
concrete network-name match details. The database view's
`provider_directory_network_matches` column stores raw referenced FHIR network
organizations (`name`, `aliases`, `resource_id`, `ref`). The public API
`address_verification.provider_directory_network_matches` field is stricter:
serving emits only reconstructed PTG-to-directory matches that contain both
`ptg_network_name` and `provider_directory_network_name`.

## Usage

Seed the source catalog without network probes:

```bash
python main.py start provider-directory-fhir --seed-db-path /tmp/provider_directory.db --seed-only --no-probe
```

Probe every seed source and import full open-access resource pages:

```bash
python main.py start provider-directory-fhir \
  --seed-db-path /tmp/provider_directory.db \
  --import-resources \
  --full-refresh \
  --resource-limit 0 \
  --page-limit 0 \
  --page-count 100 \
  --stream-batch-size 1000 \
  --source-concurrency 4 \
  --publish-artifacts \
  --stale-cleanup \
  --concurrency 12 \
  --timeout 10
```

Use `--source-query` and `--limit` for small payer-specific smoke runs. Resource
fetches default to sources marked open/none in the seed plus sources whose
unauthenticated metadata probe returned a valid FHIR CapabilityStatement. This
keeps stale or overly conservative seed `auth_type` values from hiding a
directory that is actually readable. Pass `--include-credentialed` or
`--include-auth-required` only for controlled tests.
`--full-refresh` makes omitted `resource_limit` and `page_limit` default to `0`,
where `0` means unbounded. The importer still has a hard pagination loop guard
from `HLTHPRT_PROVIDER_DIRECTORY_MAX_FULL_PAGES` (default `10000`) so a broken
endpoint cannot run forever. `stream_batch_size` flushes long resource scans to
Postgres in batches so a large payer endpoint does not need to retain every row
in worker memory before the first upsert. `source_concurrency` controls how
many source resource imports can run at once; keep it low for full refreshes so
slow public FHIR servers are not overloaded. When multiple seed rows point to
the same canonical FHIR base with the same credential mode, the importer fetches
that directory once and fans out persisted rows under each source id, preserving
catalog/source provenance without doubling upstream API traffic. When
role/affiliation rows reference resources outside the fetched page slice,
`linked_resource_limit` bounds optional direct FHIR reads for those referenced
Practitioners, Organizations, Locations, HealthcareServices, and InsurancePlans.
A value of `0` disables that linked fallback; full refresh normally relies on
the direct resource collections.

Stale cleanup is conservative. The importer deletes rows missing from the
current run only for a source/resource pair whose pagination completed without
an HTTP error, row cap, page cap, hard full-page cap, or loop detection. Bounded
sample scans can upsert fresh rows but cannot delete rows for incomplete
resource scans.

Artifact publishing is meant for full default-resource refreshes. It stamps
`provider_directory_location.address_key` and republishes the PTG corroboration
view used by serving/search. The CLI defaults `--publish-artifacts` on only
when the selected resources equal the full Provider Directory resource set.
Small payer or single-resource smoke runs should pass `--no-publish-artifacts`
or `publish_artifacts=false`; otherwise a bounded test can spend most of its
runtime rebuilding global address artifacts that are unrelated to the smoke.

Long-running imports report source-probe, resource-import, and artifact-publish
progress back to import-control when the running worker image includes progress
callbacks. Older active workers still show generic `process_data running`
progress until they complete.

## Credentialed API Access

CMS requires these Provider Directory APIs to be public at the user-data level,
but payer API servers can still require app registration or API keys. The
importer supports secret-backed credentials without storing secret values in the
database. Configure either:

```text
HLTHPRT_PROVIDER_DIRECTORY_CREDENTIALS_JSON
HLTHPRT_PROVIDER_DIRECTORY_CREDENTIALS_FILE
```

The JSON shape is:

```json
{
  "hosts": {
    "api.payer.example": {
      "bearer_token": "env:PAYER_DIRECTORY_BEARER_TOKEN",
      "headers": {
        "X-API-Key": "env:PAYER_DIRECTORY_API_KEY"
      },
      "query_params": {
        "client_id": "env:PAYER_DIRECTORY_CLIENT_ID"
      },
      "oauth2": {
        "token_url": "env:PAYER_DIRECTORY_TOKEN_URL",
        "client_id": "env:PAYER_DIRECTORY_CLIENT_ID",
        "client_secret": "env:PAYER_DIRECTORY_CLIENT_SECRET",
        "scope": "system/*.read",
        "auth": "basic"
      }
    }
  },
  "sources": {
    "pdfhir_source_id": {
      "api_key": {
        "header": "Ocp-Apim-Subscription-Key",
        "value": "env:PAYER_DIRECTORY_SUBSCRIPTION_KEY"
      }
    }
  }
}
```

Credential entries can be matched by `sources`, `api_bases`, `hosts`, or
`org_names`; more specific entries override defaults. Values prefixed with
`env:` are resolved from environment variables. Request credentials are only
applied to the source host, so absolute linked FHIR references on another host
do not receive payer-specific headers or query parameters. Probe metadata stores
only non-secret descriptors such as matched rule and header/query parameter
names.

Known payer caveat: Aetna publishes an open CapabilityStatement at
`https://apif1.aetna.com/fhir/v1/providerdirectory/metadata`, but resource reads
such as `Practitioner`, `PractitionerRole`, `Organization`, `Location`, and
`InsurancePlan` require a subscribed OAuth client. The importer maps Aetna
developer-portal seed rows to
`https://apif1.aetna.com/fhir/v1/providerdirectory` and records resource-level
`http_401` diagnostics until credentials are configured. Aetna's advertised
token URL is
`https://apif1.aetna.com/fhir/v1/fhirserver_auth/oauth2/token`.

ALOHR / Alabama One Health Record is mixed-mode. The original seed URL
`https://alohr.esante.us/public/providers` is a public React provider-search app,
not a FHIR REST base. Its real FHIR metadata base is
`https://fhir.alabamaonehealthrecord.com/csp/healthshare/hsods/fhir/r4`, where
`metadata` is open but resource reads are auth-gated. The importer maps ALOHR
seed rows to that FHIR base and uses a source-specific GraphQL connector at
`https://api.esante.us/graphql` with tenant `alohr` to stream public provider
and organization rows into the normal Provider Directory practitioner,
organization, location, role, and affiliation tables.

## Self-Harness

Run the parser-only harness:

```bash
./venv314/bin/python scripts/research/provider_directory_fhir_harness.py
```

Run a bounded local DB-backed CLI case:

```bash
HLTHPRT_DB_HOST=127.0.0.1 \
HLTHPRT_DB_PORT=5440 \
HLTHPRT_DB_USER=nick \
HLTHPRT_DB_DATABASE=healthporta \
HLTHPRT_TEST_DATABASE_SUFFIX=_test \
./venv314/bin/python scripts/research/provider_directory_fhir_harness.py \
  --local-cli \
  --seed-db-path /tmp/provider-directory-db/data/provider_directory.db \
  --limit 10 \
  --import-resources \
  --resources InsurancePlan,Practitioner \
  --include-credentialed \
  --resource-limit 10 \
  --linked-resource-limit 25 \
  --page-limit 1
```

Reports are written to:

```text
reports/provider-directory-fhir/run-<timestamp>/report.json
reports/provider-directory-fhir/run-<timestamp>/report.md
```

## Coverage Audit

After a manual or scheduled import, run the coverage audit to verify source
coverage, resource yield, address-key usefulness, PTG overlap, and unresolved
network refs:

```bash
./venv314/bin/python scripts/research/provider_directory_coverage_audit.py \
  --host 127.0.0.1 \
  --port 5440 \
  --database healthporta \
  --schema mrf \
  --format markdown
```

In dev Kubernetes, stream the script into the healthcare pod so it uses the
deployed DB connection secrets without printing them:

```bash
sudo k3s kubectl -n healthporta-dev exec -i deploy/healthcare-mrf-api -- \
  /opt/venv/bin/python - --schema mrf --format markdown \
  < scripts/research/provider_directory_coverage_audit.py
```

The report is intentionally gap-oriented. Treat these as recurring operational
signals:

- auth-required sources need credential/registration handling before they can
  contribute full provider coverage. Sources that declare OAuth/API-key access
  and return non-FHIR onboarding HTML without configured credentials are
  classified here instead of as public `valid_non_fhir` endpoints.
  Known onboarding gateway hosts, such as Availity and Centene partner
  portals, are treated the same way when they return non-FHIR HTML without an
  applied credential.
- `valid_non_fhir` sources should be reviewed as source-catalog cleanup or
  payer-specific endpoint discovery work.
- Provider Directory rows without `address_key` are not usable for canonical
  address search or PTG address corroboration.
- unresolved network refs mean the payer exposed a network reference but the
  imported FHIR resources did not resolve it to an `Organization` name/alias.
- PTG rows with Provider Directory network refs but no resolved network-name
  match are address-corroborated only; they are not yet full network-location
  evidence.

## Schedule

`import-control` defines `default-provider-directory-fhir-daily` at `20 1 * * *`
America/Chicago. The default parameters probe the full seed catalog, run a full
open-access resource refresh (`resource_limit=0`, `page_limit=0`,
`page_count=100`, `stream_batch_size=1000`, `source_concurrency=4`,
`publish_artifacts=true`), and delete stale rows only for completed
source/resource scans.

`import-control` also defines
`default-entity-address-unified-provider-directory-daily` at `20 2 * * *`
America/Chicago. It runs `entity-address-unified` with
`serving_only_refresh=true` so Provider Directory FHIR rows become visible in
provider/address search without waiting for the heavier monthly full support
table rebuild.
