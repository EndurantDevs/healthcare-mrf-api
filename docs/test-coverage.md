# Test Coverage Ratchet

CI keeps Python and Rust coverage separate. A change fails when any coverage
ratio falls or when any uncovered-code count rises, so growth in one language
cannot hide a regression in the other.

## Current baseline

Python is measured with Coverage.py 7.15.2 across `main.py`, `api/`, `db/`,
`process/`, and `service/`. The PTG2 scanner is measured with
cargo-llvm-cov 0.8.7 across all targets with its Python bridge enabled.

| Scope | Metric | Covered / total | Coverage |
| --- | --- | ---: | ---: |
| Python | Lines | 61,663 / 83,945 | 73.46% |
| Python | Branches | 17,050 / 29,250 | 58.29% |
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
