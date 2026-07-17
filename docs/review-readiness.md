# Repository Review Readiness

This note records reproducible checks used before external review. It avoids
production identifiers and treats live measurements as release evidence, not
as permanent benchmark claims.

## PTG Architecture

The only supported PTG snapshot architecture is `postgres_binary_v3` with
`storage_generation=shared_blocks_v3`. Older snapshots must be reimported.
Runtime readers do not infer an architecture from table presence and do not
materialize PostgreSQL artifacts into a filesystem or process cache.

Old generation names may appear only in cleanup code and rejection tests. They
exist so obsolete physical state can be deleted safely; they are not serving or
import compatibility paths.

## Required Gates

```bash
./venv314/bin/python -m pytest -q
cargo fmt --check --manifest-path support/ptg2_scanner/Cargo.toml
cargo check --all-targets --manifest-path support/ptg2_scanner/Cargo.toml
cargo clippy --all-targets --manifest-path support/ptg2_scanner/Cargo.toml -- -D warnings
cargo test --manifest-path support/ptg2_scanner/Cargo.toml
git diff --check
```

The PostgreSQL integration job must also exercise migration upgrade, strict
shared publication, cross-plan physical reuse, cold reads, snapshot removal,
and shared-block garbage collection.

## Dev Deployment Gate

Every application deployment to dev must start from GitHub Actions for the
exact commit on `main`, after that commit's `CI` workflow succeeds. The
post-CI workflow may queue the node-side image build and GitOps update; a
manually dispatched reader promotion must verify the same successful CI run.
Do not deploy by invoking node build scripts, changing live Kubernetes objects,
editing images or environment variables in a pod, pushing desired-state
manifests by hand, or forcing Flux reconciliation as a substitute for CI.

Direct cluster commands are permitted for read-only diagnosis and smoke
verification after the CI deployment. Any durable correction must be made in
the owning repository and pass through the same CI path.

## Release Evidence

A representative dev import is required before broad deployment. Record:

- start-to-published wall time and phase timings;
- `pg_total_relation_size` split by strict V3 relation;
- cold and warm latency distributions for forward, reverse-NPI, all-prices for
  one NPI, and geo-filtered requests;
- no pod-local serving cache files before or after requests;
- exact source/API audit results from the original JSON or gzip inputs.

Automatic activation uses the bounded PostgreSQL source-witness audit. It
reparses independently selected cohorts of 10,000 emitted price/provider
occurrences and 1,000 provider records for a large population, runs one
served-sample preflight plus one standard API challenge per occurrence,
requires `aiohttp` on `uvloop`, and fails closed at 55 seconds. The report must
be freshly attested against the sealed witness,
sample, and source-set digests, and activation must consume that receipt
through an exact-predecessor transaction.

No complete-file audit is required for activation. The bounded PostgreSQL
witness verifier is the authoritative release gate and compares authenticated
raw source fragments with standard API responses without a second source scan.

Do not claim the 10-15 minute import target or a cold p95 below 40 ms until the
representative dev run satisfies those gates.

## Security Hygiene

Do not put customer, employer-plan, or production source names in fixtures,
reports, commit messages, or public documentation. Validation reports must use
bounded redacted examples and must not contain source URLs or credentials.

The public repository must not name or depend on deployment-specific
dashboards, gateways, automation products, subscription services, proprietary
orchestrators, or their database schemas. Those systems may consume the
generic public and authenticated operator APIs only. Run a repository-wide
dependency/name scan as part of the public-release gate.

Run the repository secret-pattern and public-name hygiene checks before review.
Generated reports, downloaded MRF files, and local database artifacts remain
untracked unless separately approved.
