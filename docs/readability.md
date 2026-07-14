# Readability Budget

This repo uses `scripts/readability_budget.py` to keep readability debt visible
and prevent new debt from entering unnoticed.

## Rules

- Do not add inline suppression comments such as `# noqa`, `# type: ignore`,
  `# pylint: disable`, or Rust `#[allow(...)]`.
- Use names that tell the human what role a value plays. Avoid generic
  function names, vague long-scope locals, one-letter names outside tiny scopes,
  and names that shadow Python builtins.
- Boolean names should read as predicates: `is_*`, `has_*`, `should_*`,
  `can_*`, `needs_*`, `supports_*`, or equivalent.
- Collection names should reveal their shape: plural names for lists/sets and
  `_by_*`, `_map`, `_lookup`, `_index`, or similar names for dictionaries.
- Long or public functions should have a contract docstring, not comments that
  merely repeat the next line.
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
- Naming and decomposition debt is reported for generic function/class names,
  vague local variable names in long scopes, boolean-name mismatches, builtin
  shadowing, collection-name mismatches, one-letter names, too many parameters,
  too many locals, global/nonlocal state, missing contract docstrings, placeholder
  bodies, and noisy comments.

Existing debt IDs are stored in `readability-baseline.json`. The normal check
rejects findings that are not present in that synchronized baseline. When debt
is removed or the rules intentionally change, regenerate the baseline in the
same change:

```bash
python scripts/readability_budget.py --write-baseline
```

Normal local check:

```bash
python scripts/readability_budget.py
```

Pull-request CI also compares the synchronized snapshot with the base branch.
New syntax errors, suppressions, placeholder bodies, global state, or builtin
shadowing always fail. Other replacement findings are allowed only when the
complete branch does not increase total readability debt. Dedicated cleanup
changes can still pass a positive `--required-reduction-percent` target.
