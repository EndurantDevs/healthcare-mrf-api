# Contributing

Work from the repo root in an activated virtualenv. Keep changes focused.

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

Use `type/short-slug` names: `feature/<slug>`, `fix/<slug>`,
`docs/<slug>`, `test/<slug>`, or `chore/<slug>`.

## Commit Messages

Use `type(scope): imperative summary` subjects so history is readable during
rollbacks, reviews, and deploy audits. See `docs/commit-messages.md` for the
allowed types and examples, and run this before pushing hand-written commits:

```bash
python3 scripts/check_commit_messages.py --last 1
```

## Tests and Smoke Runs

Run the regular suite with:

```bash
pytest tests -x
```

Before merging importer changes, run a smoke import with `--test`, for example:

```bash
python main.py start claims-pricing --test
python main.py start drug-claims --test
python main.py start npi --test
```

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
