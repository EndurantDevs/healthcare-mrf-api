# Contributing

Work from the repo root in an activated virtualenv. Keep changes focused.

## Branches

Use `type/short-slug` names: `feature/<slug>`, `fix/<slug>`,
`docs/<slug>`, `test/<slug>`, or `chore/<slug>`.

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
