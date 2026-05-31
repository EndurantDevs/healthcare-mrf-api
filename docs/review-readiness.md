# Repository Review Readiness

This note records the checks used before sharing the repository for external review. It intentionally focuses on reproducible commands and known caveats rather than marketing claims.

## PTG2 Serving Storage Evidence

Measured on the local PostgreSQL instance at `127.0.0.1:5440`, database `healthporta`, schema `mrf`.

Current source snapshots:

| Source | Snapshot | Serving table total | Support tables total |
| --- | --- | ---: | ---: |
| `asr_1208` | `ptg2:202605:2e95465b2025` | 41 GB | ~4.5 GB |
| `asr_1236` | `ptg2:202605:79060d12dfcf` | 41 GB | ~4.5 GB |

Support tables are `price_atom`, `provider_group_member`, and `code_count`. This is the hot database footprint used by serving. The older selected-table baseline was 212 GB, so the current serving layout is materially smaller while keeping API responses compatible.

Import timing records available locally:

| Import run | Status | Serving rows | Duration |
| --- | --- | ---: | ---: |
| `ptg2:asr_1208_202605_full_thin2` | `validated` | 301,445,112 | 14,631.8 s |
| ASR 1236 manifest-backed import | `validated` | 149,971,480 | 10,231.8 s |

The `asr_1208` manifest-backed run was validated from salvaged artifacts, so its recorded duration is not useful for throughput comparison.

## Artifact Hygiene

The retained sidecar directory can contain stale files from interrupted or superseded imports. Use the manifest-driven dry-run before deleting anything:

```bash
HLTHPRT_DB_HOST=127.0.0.1 \
HLTHPRT_DB_PORT=5440 \
HLTHPRT_DB_DATABASE=healthporta \
HLTHPRT_DB_USER=nick \
HLTHPRT_DB_SCHEMA=mrf \
HLTHPRT_PTG2_ARTIFACT_DIR=/Volumes/Data/data \
  ./venv314/bin/python -m process.ptg_parts.ptg2_artifact_cleanup --schema mrf
```

Current dry-run result:

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
- guarded ASR 1236 UUID smoke import passed in `healthporta_test`: 2,641,583 serving rows, `id_storage=uuid`, 51.34 seconds

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
