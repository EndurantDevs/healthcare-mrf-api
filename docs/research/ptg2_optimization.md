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
