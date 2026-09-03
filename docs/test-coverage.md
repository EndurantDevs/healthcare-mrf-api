# Test Coverage Ratchet

CI keeps Python and Rust coverage separate. A change fails when any coverage
ratio falls or when any uncovered-code count rises, so growth in one language
cannot hide a regression in the other.

Production-source changes also pay down existing debt until the configured
target is reached. For every metric in the affected language report, CI requires
the smaller of:

- 1% of the base branch's uncovered units, rounded up; or
- 10% of the PR's changed in-scope source lines, rounded up, with a minimum of one.

Lines and statements target 95%. Branches, functions, and compiler regions
target 90%. Test-only, documentation, and tooling PRs keep the exact
no-regression check without an unrelated paydown requirement. The policy and
targets are versioned in `test-coverage-baseline.json`; CI rejects attempts to
weaken them.

## Current baseline

Python is measured with Coverage.py 7.15.2 across `main.py`, `api/`, `db/`,
`process/`, and `service/`. The PTG2 scanner is measured with
cargo-llvm-cov 0.8.7 across all targets with its Python bridge enabled.

| Scope | Metric | Covered / total | Coverage |
| --- | --- | ---: | ---: |
| Python | Lines | 61,507 / 83,945 | 73.27% |
| Python | Branches | 16,984 / 29,250 | 58.07% |
| PTG2 scanner | Lines | 28,883 / 37,775 | 76.46% |
| PTG2 scanner | Functions | 1,907 / 2,602 | 73.29% |
| PTG2 scanner | Regions | 43,828 / 56,533 | 77.53% |

LLVM reports no instrumented branch metric for the scanner, so Rust uses line,
function, and region coverage. The coverage job excludes
`test_mymedicalshopper_ddp_call_uses_overall_deadline_for_heartbeats`: its
one-millisecond timing assertion is valid in the normal test job but unstable
under instrumentation.

## Local check

```bash
python -m coverage run --rcfile=test-coverage.ini \
  -m pytest -q \
  --deselect tests/test_mrf_source_discovery.py::test_mymedicalshopper_ddp_call_uses_overall_deadline_for_heartbeats
python -m coverage json --rcfile=test-coverage.ini \
  -o test-coverage-python.json

cargo llvm-cov --manifest-path support/ptg2_scanner/Cargo.toml \
  --all-targets --features python --json --summary-only \
  --output-path test-coverage-rust.json
python scripts/coverage_ratchet.py
```

Install cargo-llvm-cov 0.8.7 and the Rust `llvm-tools-preview` component before
the local Rust command. The versioned source of truth is
`test-coverage-baseline.json`; CI compares it with the pull request base commit
to prevent lowering or removing a baseline.
After an in-scope source change, regenerate the affected report and run
`python scripts/coverage_ratchet.py --report python --write-baseline` or
`python scripts/coverage_ratchet.py --report rust --write-baseline`. The pinned
Ubuntu CI measurement is canonical; if local counts differ, use the CI counts
rather than committing platform-specific totals.

## CI forecast and provenance

Before either ratchet is enforced, CI runs the same ratchet against a
report-derived temporary baseline and uploads a small forecast diagnostic. This
does not change the ratchet policy or its one-time floor-reset marker: it only
shows the exact uncovered-debt cap, margin, changed-line count, and Python
missing branch arcs that CI is about to enforce.

The forecast is fail closed. Every coverage producer checks out the same
`github.sha` with full history and binds its artifact to the immutable target
base SHA, checked-out head SHA, Coverage.py version, baseline digest, filename,
and file digest. The aggregation job accepts exactly these Python inputs:

- five main-suite shards: `.coverage.main.0` through `.coverage.main.4`;
- one capacity shard: `.coverage.capacity`;
- three PostgreSQL shards: `.coverage.postgres.core`,
  `.coverage.postgres.provider-directory`, and
  `.coverage.postgres.provider-profile`.

Each file has a matching hidden provenance sidecar. Rust remains separate: its
`llvm-cov` JSON report and provenance sidecar are the only accepted inputs to
the Rust forecast. A missing, stale, renamed, extra, mixed-base, or
mixed-head artifact is an error rather than a best-effort forecast.

Pull requests use the pull request base SHA; pushes use the event's previous
SHA. A manual workflow run requires an explicit full `base_sha` input. CI never
falls back to `origin/main` or `HEAD^`. The forecast stages both the measured
report metrics and source-file set into the candidate copy before it invokes
the production ratchet, so a stale committed baseline cannot create a false
coverage failure or hide a real one.
