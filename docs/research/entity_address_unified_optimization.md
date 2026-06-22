# Entity Address Unified Optimization Harness

This workflow reuses the PTG2 experiment harness for entity-address-unified
pilots. It keeps the evidence under ignored `reports/` paths and captures the
final import-control run, progress samples, elapsed time, and published metrics
from the importer.

## Dev Pilot Usage

Run a dry-run plan:

```bash
./venv314/bin/python scripts/research/ptg2_experiment.py run \
  --suite docs/research/entity_address_unified_benchmark_suite.example.json \
  --dry-run
```

Run the 1k-per-source bounded dev smoke through import-control:

```bash
HLTHPRT_IMPORT_CONTROL_URL=http://127.0.0.1:8095 \
HLTHPRT_IMPORT_CONTROL_API_TOKEN=<token> \
./venv314/bin/python scripts/research/ptg2_experiment.py run \
  --suite docs/research/entity_address_unified_benchmark_suite.example.json \
  --case dev-bounded-smoke-1k
```

Bounded smoke cases pass `publish=false`, so they validate staging, support-table
builds, publish validation, and phase timings without replacing the live serving
tables.

Run the full dev pilot only when PTG/openaddress jobs are not competing for the
same database CPU and temp I/O:

```bash
HLTHPRT_IMPORT_CONTROL_URL=http://127.0.0.1:8095 \
HLTHPRT_IMPORT_CONTROL_API_TOKEN=<token> \
./venv314/bin/python scripts/research/ptg2_experiment.py run \
  --suite docs/research/entity_address_unified_benchmark_suite.example.json \
  --case dev-full-run
```

Reports are written to:

```text
reports/ptg2-experiments/run-<timestamp>/report.json
reports/ptg2-experiments/run-<timestamp>/report.md
```

Inspect `import_run.final_run.metrics.phase_timings` for the real phase costs.
The full-run target is under one hour on dev when the database is not already
saturated by another import.

The importer uses the same bounded-concurrency pattern as the PTG experiments
for the long independent phases. Dev can tune the final support tail with:

```text
HLTHPRT_ENTITY_ADDRESS_UNIFIED_SUPPORT_CONCURRENCY
HLTHPRT_ENTITY_ADDRESS_UNIFIED_SUPPORT_INDEX_CONCURRENCY
```
