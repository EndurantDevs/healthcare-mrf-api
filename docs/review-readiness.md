# Repository Review Readiness

This note records the checks used before sharing the repository for external review. It intentionally focuses on reproducible commands and known caveats rather than marketing claims.

## PTG2 Serving Storage Evidence

This section is historical local evidence for the first manifest-backed storage
reduction work. Current storage reviews should prefer live snapshot manifests
and deployment smoke output. PostgreSQL binary snapshots can use either the v1
direct provider-membership layout or the v2 normalized membership graph.

Measured on the local PostgreSQL instance at `127.0.0.1:5440`, database
`healthporta`, schema `mrf`.

Current source snapshots:

| Source | Snapshot | Serving table total | Support tables total |
| --- | --- | ---: | ---: |
| `example_source_a` | `ptg2:202605:2e95465b2025` | 41 GB | ~4.5 GB |
| `example_source_b` | `ptg2:202605:79060d12dfcf` | 41 GB | ~4.5 GB |

Support tables in this historical evidence are `price_atom`,
`provider_group_member`, and `code_count`. This was the hot database footprint
for the earlier manifest layout. For `postgres_binary_v2`, expect `price_atom`,
`code_count`, `provider_npi_scope`, the provider-set dictionary, and the binary
serving table. Review PostgreSQL artifact chunks for all four membership graph
directions and confirm that pod-local materialized artifact caches are absent.

Import timing records available locally:

| Import run | Status | Serving rows | Duration |
| --- | --- | ---: | ---: |
| `ptg2:example_source_a_202605_full_thin2` | `validated` | 301,445,112 | 14,631.8 s |
| Source B manifest-backed import | `validated` | 149,971,480 | 10,231.8 s |

The `example_source_a` manifest-backed run was validated from salvaged artifacts, so its recorded duration is not useful for throughput comparison.

## Artifact Hygiene

Legacy retained sidecar directories can contain stale files from interrupted or
superseded imports. Use the manifest-driven dry-run before deleting anything:

```bash
HLTHPRT_DB_HOST=127.0.0.1 \
HLTHPRT_DB_PORT=5440 \
HLTHPRT_DB_DATABASE=healthporta \
HLTHPRT_DB_USER=nick \
HLTHPRT_DB_SCHEMA=mrf \
HLTHPRT_PTG2_ARTIFACT_DIR=/Volumes/Data/data \
  ./venv314/bin/python -m process.ptg_parts.ptg2_artifact_cleanup --schema mrf
```

Historical dry-run result:

- referenced sidecars: 36.2 GB
- unreferenced sidecars: 61.1 GB
- missing manifest-referenced files: 0

Only add `--execute` after reviewing the dry-run output. The cleanup tool does not touch raw payer downloads.

## Verification Commands

```bash
./venv314/bin/python -m pytest -q
./venv314/bin/python -m py_compile \
  process/ptg_parts/ptg2_artifact_cleanup.py \
  process/ptg_parts/ptg2_legacy_cleanup.py \
  api/ptg2_serving.py \
  api/ptg2_serving_utils.py \
  process/ptg.py
git diff --check
```

Last local results:

- `743 passed in 21.26s`
- compile checks passed
- `git diff --check` passed
- guarded Source B UUID smoke import passed in `healthporta_test`: 2,641,583 serving rows, `id_storage=uuid`, 51.34 seconds
- guarded `postgres_binary_v2` full-file smoke passed in `healthporta_test`:
  65,536 serving rows and all original price/provider counts matched; forward
  p95 was 12.66 ms and reverse NPI p95 was 14.69 ms with in-process binary and
  sidecar caches disabled

## Security Hygiene

The tracked `specs/example_coding.txt` transcript was removed because it was unrelated to this repository and contained credential-like literal examples. A follow-up secret-pattern sweep should remain part of review preparation:

```bash
rg -n "consumer_secret|api_key=|Bearer [A-Za-z0-9._~+/=-]{12,}|PRIVATE KEY|AWS_SECRET|SECRET_KEY\\s*=|password\\s*=\\s*['\\\"][^'\\\"]+" \
  . -S --glob '!venv*/**' --glob '!restore/**' --glob '!data/**' --glob '!reports/**' --glob '!docs/review-readiness.md'
```

The current matches are code variables or request parameters, not hardcoded credentials.

## Known Caveats

- `pylint` is not installed in `venv314`, so the pylint baseline could not be captured from this environment.
- Local untracked data/report artifacts remain in the working tree and should not be committed unless explicitly reviewed.
- Existing internal module names may still include historical implementation labels; public docs and API response labels should avoid exposing those labels.
