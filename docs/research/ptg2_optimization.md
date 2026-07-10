# PTG2 Optimization Research Harness

This workflow keeps PTG2 optimization work on `research/ptg2-optimization-harness`
until a measured subset is explicitly promoted to `main`.

## Branch Rules

- Create the branch from `origin/main`.
- Keep generated benchmark evidence under `reports/ptg2-experiments/`; that path
  is ignored by Git through `reports/*`.
- Do not change default deployed PTG2 scanner behavior from this branch unless a
  local fixture gate and a dev pilot gate pass.
- Keep the PTG2 DB publish dedupe fallback enabled.

## Local Usage

List the configured cases:

```bash
./venv314/bin/python scripts/research/ptg2_experiment.py list
```

Run a dry-run plan:

```bash
./venv314/bin/python scripts/research/ptg2_experiment.py run --dry-run
```

Run fixture benchmarks after building the scanner:

```bash
cargo build --release --manifest-path support/ptg2_scanner/Cargo.toml
./venv314/bin/python scripts/research/ptg2_experiment.py run \
  --case large-in-network-fixture \
  --case duplicate-serving-fixture
```

Compare default worker-side raw chunks with a memory-sensitive 1 MiB raw chunk
cap on a bulky synthetic file:

```bash
cargo build --release --manifest-path support/ptg2_scanner/Cargo.toml
./venv314/bin/python scripts/research/ptg2_experiment.py run \
  --case bulky-raw-chunk-fixture \
  --variant parse_in_workers \
  --variant raw_chunk_1m
```

Inspect `elapsed_seconds`, `peak_rss_kb`, `raw_chunk_count`,
`raw_chunk_max_bytes`, and matching COPY output digests. This case intentionally
skips the performance gate because smaller chunks can trade some throughput for
lower peak memory and more predictable huge-file scheduling.

Run the local DB-backed PTG smoke against PostgreSQL on `127.0.0.1:5440`.
This serves a tiny TOC and in-network file from a temporary localhost HTTP
server, enables `HLTHPRT_FETCH_ALLOW_LOCAL=true` only for the child process, and
targets `healthporta_test` via `HLTHPRT_DB_DATABASE=healthporta` plus
`HLTHPRT_TEST_DATABASE_SUFFIX=_test`:

```bash
HLTHPRT_DB_HOST=127.0.0.1 \
HLTHPRT_DB_PORT=5440 \
HLTHPRT_DB_USER=nick \
HLTHPRT_DB_DATABASE=healthporta \
HLTHPRT_TEST_DATABASE_SUFFIX=_test \
./venv314/bin/python scripts/research/ptg2_experiment.py run \
  --case local-db-smoke
```

Run a full-file local import and verify the published DB snapshot against the
original generated source file. Unlike `local-db-smoke`, this does not pass
`--max-items`; after `PTG2_IMPORT_DONE`, the harness reloads `rates.json.gz`,
computes independent source counts and a sorted price-atom digest, then compares
those values with the published serving, price atom, and provider-member tables:

```bash
HLTHPRT_DB_HOST=127.0.0.1 \
HLTHPRT_DB_PORT=5440 \
HLTHPRT_DB_USER=nick \
HLTHPRT_DB_DATABASE=healthporta \
HLTHPRT_TEST_DATABASE_SUFFIX=_test \
./venv314/bin/python scripts/research/ptg2_experiment.py run \
  --case local-full-file-verify
```

### Normalized Membership Graph A/B

Run the PostgreSQL-only v1/v2 membership comparison with:

```bash
HLTHPRT_DB_HOST=127.0.0.1 \
HLTHPRT_DB_PORT=5440 \
HLTHPRT_DB_USER=nick \
HLTHPRT_DB_DATABASE=healthporta \
HLTHPRT_TEST_DATABASE_SUFFIX=_test \
./venv314/bin/python scripts/research/ptg2_experiment.py run \
  --suite docs/research/ptg2_membership_graph_local_suite.json
```

The 2026-07-10 64K-rate control run validated every source price and NPI in both
architectures with API caches disabled:

| Metric | `postgres_binary_v1` | `postgres_binary_v2` |
| --- | ---: | ---: |
| Snapshot bytes | 396,480 | 372,984 |
| Import seconds | 0.80 | 0.66 |
| Code lookup p95 | 14.08 ms | 12.66 ms |
| NPI reverse p95 | 17.38 ms | 14.69 ms |
| ZIP + 100-price-set response p95 | 51.17 ms | 55.22 ms |

The small fixture has one NPI per provider set, so v2 is only about 6% smaller
there. It intentionally does not model the expensive payer shape that motivated
v2: many provider sets containing large shared groups. In that shape, v1 stores
the provider-group membership table plus a direct set-to-NPI expansion; v2
stores each group/NPI edge once in each direction and keeps only a distinct-NPI
scope table. Use a real high-fan-out source comparison before estimating fleet
storage savings from the control fixture.

The suite also includes a TIN-only v2 case. It verifies that a valid rate file
with zero NPIs still publishes all unique prices, creates an empty indexed NPI
scope, returns no reverse-NPI items, and leaves no scope stage tables behind.

Reports are written to:

```text
reports/ptg2-experiments/run-<timestamp>/report.json
reports/ptg2-experiments/run-<timestamp>/report.md
```

## Dev Pilot

The pilot case is skipped unless a control URL and token are supplied. Use the
engine control API URL in dev, or the equivalent import-control proxy if that is
the selected entry point:

```bash
HLTHPRT_CONTROL_URL=http://127.0.0.1:8080/control/v1 \
HLTHPRT_CONTROL_API_TOKEN=<token> \
./venv314/bin/python scripts/research/ptg2_experiment.py run \
  --case ptg-pilot-import \
  --variant parse_in_workers
```

The pilot payload uses existing PTG lane fields:

- `_expected_queue`
- `_expected_worker_class`
- `_scanner_rust_workers`
- `_scanner_parse_in_workers`
- `_scanner_work_queue`
- `_scanner_event_queue`

## Gates

- Correctness: sorted COPY row counts and SHA-256 digests must match baseline.
- Dedupe: negotiated-rate and serving-rate dedupe counters must match expected
  baseline behavior.
- Performance: candidate elapsed time must improve by at least 15 percent.
- Memory: candidate peak RSS must not grow more than 20 percent unless the
  report assigns it to a larger PTG resource lane.

Unknown memory is non-fatal for local macOS runs because `/proc` is unavailable;
dev-server runs should use pod/cgroup evidence before promotion.

## Dev Deploy Shape

When gates pass, build a research image:

```bash
TAG=ghcr.io/endurantdevs/healthcare-mrf-api:research-ptg2-$(git rev-parse --short HEAD)-$(date -u +%Y%m%d%H%M%S)
```

Load or publish that tag for `ns1033171`, update the dev deploy branch image
pins, reconcile Flux, and run the pilot import again. Roll back by restoring the
previous image pin in `healthporta-deploy`.
