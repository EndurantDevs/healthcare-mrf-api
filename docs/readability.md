# Readability Budget

This repo uses `scripts/readability_budget.py` to keep readability debt visible
and prevent new debt from entering unnoticed.

## Rules

- Do not add inline suppression comments such as `# noqa`, `# type: ignore`,
  `# pylint: disable`, or Rust `#[allow(...)]`.
- Fix warnings with clearer code, narrower types, smaller functions, or better
  tests.
- Exclude generated, cache, build, local data, or runtime artifact paths only in
  `readability-budget.json`.
- Keep importers decomposed by source discovery, download, parse, stage, publish,
  and materialize phases.

## Thresholds

- Source files over 500 lines are reported.
- Python functions over 60 lines are reported.
- Python nesting deeper than 4 control-flow levels is reported.
- Inline suppressions are reported and blocked when new.

Existing debt is stored in `readability-baseline.json`. The CI check fails only
when new debt appears relative to that baseline. When debt is removed, regenerate
the baseline in the same change:

```bash
python scripts/readability_budget.py --write-baseline
```

Normal local check:

```bash
python scripts/readability_budget.py
```
