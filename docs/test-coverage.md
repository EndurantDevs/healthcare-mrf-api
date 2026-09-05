# Test Coverage Ratchet

CI keeps the measured Python and Rust coverage ratios for each metric at or
above the exact base revision's ratios. It does not cap the absolute number of
uncovered lines, branches, functions, or regions, reserve artificial headroom,
or require unrelated debt paydown.

Changed executable product lines must be at least 85% covered for Python and
80% covered for Rust. The denominator comes from added and modified lines in
the Git diff intersected with Coverage.py's executable line sets or llvm-cov's
full JSON coverage segments. Comments, blank lines, tests, scripts, generated
files, and pure deletions do not count. Failures list every uncovered changed
`path:line`. New in-scope coverage suppression directives remain forbidden.

## Current baseline

<!-- coverage-baseline:start -->
### Python

| Metric | Covered / total | Coverage |
| --- | ---: | ---: |
| Branches | 40,735 / 44,708 | 91.11% |
| Lines | 129,861 / 135,903 | 95.55% |

### Rust

| Metric | Covered / total | Coverage |
| --- | ---: | ---: |
| Functions | 3,608 / 4,003 | 90.13% |
| Lines | 57,271 / 63,486 | 90.21% |
| Regions | 84,329 / 93,650 | 90.05% |

<!-- coverage-baseline:end -->

Python is measured with Coverage.py 7.16.0 across `main.py`, `api/`, `db/`,
`process/`, `public_evidence/`, and `service/`. The PTG2 scanner is measured
with cargo-llvm-cov 0.8.7 across all targets with its Python bridge enabled.
LLVM reports no instrumented branch metric for the scanner, so Rust retains
line, function, and region ratio floors.

The tracked table is generated from `test-coverage-baseline.json` and records
the policy transition snapshot. CI stages fresh Python and Rust measurements,
generates its live table beside the resulting baseline, and stores both
successful `main` outputs as
`healthcare-mrf-api-coverage-baseline-<source SHA>` for 90 days. Pull requests
download only the successful artifact for their exact base SHA. The transition
commit is the sole bootstrap from the tracked base because earlier runs did not
publish this artifact. Keeping the moving table in the artifact avoids a
protected-branch bot commit or a recursive baseline-only pull request.

## Checks

Generate or verify the table after producing both reports:

```bash
python scripts/coverage_reports.py --write-docs --check
python scripts/coverage_ratchet.py --self-test
```

The CI coverage job is canonical. It validates the exact Python shard set and
Rust report provenance, combines the measurements, compares ratios with the
exact-base artifact, checks changed-line coverage, generates and verifies the
live artifact table, and publishes the next machine baseline. The quality job
checks that this tracked transition table has not drifted from the tracked
policy document. Measurement scope and `test_deselections` remain protected
baseline fields.
