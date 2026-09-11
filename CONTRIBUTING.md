# Contributing

Work from the repo root in an activated virtualenv. Keep changes focused and
preserve unrelated work.

## Contributor License Agreement

External pull requests must pass CLA Assistant before they can be merged. By
submitting a contribution, you confirm that you have read and can accept the
[EndurantDevs Individual Contributor License Agreement](CLA.md).

CLA Assistant uses the canonical public CLA Gist:
https://gist.github.com/dnikolayev/ed619a73b0095cbb30de041cd6ca4421

If your employer, client, university, or another organization may own rights in
your contribution, make sure you are authorized to contribute before opening a
pull request.

## Branches

Create feature and fix branches from `dev` and open normal pull requests into
`dev`. GitHub CI and testing in the DEV environment must pass before release.
Promotion to stable `main` is a separate, explicitly requested maintainer action:
open a release pull request and use **Rebase and merge**. Merging into `dev` does
not authorize a stable release. The public default branch remains `main`.

Use `type/short-slug` names: `feature/<slug>`, `fix/<slug>`,
`docs/<slug>`, `test/<slug>`, or `chore/<slug>`.

## Public content

Review every outgoing file, branch name, commit message, pull request title and
description, comment, review, log, and release note before publishing. Describe
the public problem, behavior, and validation without naming private projects,
repositories, configuration paths or values, internal URLs, hosts, deployment
details, credentials, or personal data. Use synthetic examples and public
references; do not copy private runbooks or encode private names in examples or
tests.

Prepare publication text locally and check the exact text before submitting it:

```bash
uv run --locked python scripts/ci/public_hygiene.py --text-file /path/to/prepared-text.md
uv run --locked python scripts/check_commit_messages.py --range origin/dev..HEAD
```

Repeat `--text-file` for additional titles, descriptions, comments, or branch
names. The check supplements review; CI runs after publication and cannot undo
disclosure. Keep any supporting private validation details in private records.
Public package names and documented public interfaces remain appropriate.

## Commit Messages

Use `type(scope): imperative summary` subjects so history is readable during
rollbacks, reviews, and deploy audits. See `docs/commit-messages.md` for the
allowed types and examples, and run this before pushing hand-written commits:

```bash
uv run --locked python scripts/check_commit_messages.py --range origin/dev..HEAD
```

## Tests and Smoke Runs

Run focused checks for the behavior you change, for example:

```bash
pytest tests/test_healthcheck.py -q
```

Run the fast Python correctness and inference checks with the pinned developer
tools:

```bash
uv pip install --only-binary=:all: ruff==0.16.6 pylint==4.0.8
ruff check main.py api db process public_evidence service alembic scripts support tests
pylint \
  api/billing_search_selector_contract.py \
  api/billing_search_transport_contract.py \
  api/mrf_discovery_catalog_manifest.py \
  api/plan_pricing_state_scan_contract.py \
  process/fhir_request_failure_policy.py \
  process/formulary_fhir/uhc_drug_parser_contract.py \
  process/formulary_fhir/uhc_drug_transport_contract.py \
  process/provider_directory_rooted_graph_source_contract.py \
  process/provider_directory_rooted_graph_twin_admission_contract.py \
  process/provider_directory_rooted_graph_twin_contract.py \
  process/provider_directory_validated_publication_contract.py \
  public_evidence/evidence_record_token_policy.py
```

Install these tools in the activated, runtime-equipped development virtualenv.
An isolated Pylint tool environment without the application's dependencies
cannot provide the same inference; CI explicitly exposes its verified runtime
dependencies to the separate lint environment.

Ruff owns syntax and undefined-name checks across existing Python code. New
Python files must also pass `ruff check --select I` and `ruff format --check`;
this staged policy avoids a repository-wide formatting rewrite. Pylint covers
the listed security, source, and publication contracts with inference checks
that produce reliable results against the installed application dependencies.
The readability budget remains authoritative for naming, function size,
complexity, and suppression policy.

GitHub CI is the required full-validation gate on the current pull request head.
Do not run local pre-push suites or hooks. When a push is authorized, use
`git push --no-verify` to avoid invoking a local pre-push hook.

After merge, verify the change in DEV before requesting a stable release.
Coordinate tests that change shared runtime state with its owner.

Before merging importer changes that support bounded test mode, run a smoke
import with `--test`, for example:

```bash
python main.py start claims-pricing --test
python main.py start drug-claims --test
```

The NPI importer deliberately has no live `--test` mode because its evidence
and publication rows are immutable. Use the disposable PostgreSQL suites
described in [the NPI import guide](docs/imports/npi.md) instead.

## Import Safety Rules

Imports that publish canonical tables use a staging-to-live swap:
`live -> _old`, `staging -> live`, with matching index renames. `_old` tables
are rollback assets, not cleanup debris.

Do not run `ClaimsPricing_finish` and `DrugClaims_finish` at the same time.
Both finalize paths touch shared `code_catalog` and `code_crosswalk` tables.

## Adding Importers

Add new importer implementation modules under `process/<name>.py`. Register
their CLI entrypoints and ARQ worker classes in `process/__init__.py`, then add
or update the matching runbook under `docs/imports/`.
