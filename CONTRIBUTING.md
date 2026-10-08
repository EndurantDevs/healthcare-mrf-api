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

Run the fast Python correctness checks with pinned Ruff:

```bash
uv pip install --require-hashes --only-binary=:all: -r requirements-quality.lock
ruff check main.py api db process public_evidence service alembic scripts support tests
ruff check --select I tests/test_public_ci.py
ruff format --check tests/test_public_ci.py
```

Run the import-order and formatting checks on the Python files you changed.
CI requires sorted imports and formatting for added or modified Python files.
Unrelated historical files keep their existing readability baseline. Runtime
import smoke tests and the existing contract tests run against locked application
dependencies; they validate actual dependency APIs and source-module imports.

CI also checks eleven selected security, source, and publication contract modules
with pinned ty 0.0.85 and its default diagnostics. uv manages the Python 3.14.7
tool environment separately from the verified application dependencies, which ty
uses to resolve real dependency APIs. A dependency-member canary checks that
inference remains active before the pass. Ruff requires return annotations in
these modules with ANN201, ANN202, ANN204, ANN205, and ANN206, including functions
returning `None`; suppression comments cannot bypass this return guard.

ty permits assigning a `None` result to an inferred variable, including results
from functions annotated with `-> None`. This differs from Pylint's
`assignment-from-no-return` warning. Return annotations and ty reject invalid
consumption of those results, such as subscripting them or returning them from a
function annotated to return `int`. Runtime import checks and contract tests remain
part of CI alongside the static checks.

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
