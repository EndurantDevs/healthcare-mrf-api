# Readability Budget

This repo uses `scripts/readability_budget.py` to report readability debt and
block new function-level and architecture debt.

## Rules

- Do not add inline suppression comments such as `# noqa`, `# type: ignore`,
  `# pylint: disable`, or Rust `#[allow(...)]`.
- Use names that tell the human what role a value plays. Avoid generic
  function names, vague long-scope locals, one-letter names outside tiny scopes,
  and names that shadow Python builtins.
- Do not distinguish one-item and multi-item functions only with a trailing
  plural. Keep the singular entity name for one item and add explicit
  cardinality or a collection verb for many items, such as `refresh_node_health`
  and `refresh_all_node_health` rather than `refresh_nodes_health`.
- Reserve `readability.confusable_function_name_exceptions` for externally
  defined protocols that require both spellings. Entries are exact stable
  finding IDs, not broad name-pattern exemptions.
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
- Do not add facade/module attribute injection, namespace-copy loops,
  `sys.modules[...].__dict__.update(...)`, or non-allowlisted `__module__`
  rewrites. Intentional public contract-name rewrites use
  `readability.module_attribute_injection_allowlist` entries in
  `<relative path>:<name>` form.
- Do not add numbered `*_part_NN.py`, `*_part_NN.ts`, or `*_part_NN.tsx`
  modules. Rust code also rejects paired `*_a.rs`/`*_b.rs` modules and split
  test `include!(...)` files.

## Thresholds

- Product Python files over 1,500 lines are reported as soft overruns. Rust
  product files over 800 non-test lines are likewise reported; lines inside
  `#[cfg(test)] mod ...` blocks do not count toward that Rust budget. Test,
  migration, and script file lengths do not block a change, but their Python
  functions still receive the rules below.
- A product file that was already over 5,000 raw lines at the base revision may
  not grow. The checker discovers these files from the configured product roots
  and compares each file with the exact base revision, including across renames.
  At this policy transition the set is:

  - `process/provider_directory_fhir.py`
  - `support/ptg2_scanner/src/main.rs`
  - `api/ptg2_serving.py`
  - `process/mrf_source_discovery.py`
  - `api/endpoint/npi.py`
  - `process/entity_address_unified.py`
  - `api/endpoint/pricing.py`
  - `support/ptg2_scanner/src/provider_graph_v4.rs`
  - `process/ptg.py`
  - `db/models/_legacy.py`
  - `process/ptg_parts/ptg2_shared_snapshot_publish.py`
  - `api/ptg2_db_sidecars.py`
  - `process/florida_mqa_profile.py`
- Python functions over 60 lines are reported.
- Python nesting deeper than 4 control-flow levels is reported.
- Inline suppressions are reported and blocked when new.
- Naming and decomposition debt is reported for generic function/class names,
  vague local variable names in long scopes, boolean-name mismatches, builtin
  shadowing, confusable singular/plural function names, collection-name
  mismatches, one-letter names, too many parameters,
  too many locals, global/nonlocal state, missing contract docstrings, placeholder
  bodies, and noisy comments.

Existing debt IDs, including soft file-length overruns, are stored in
`readability-baseline.json`. Soft file-length overruns remain visible but do not
fail a change. New function, naming, suppression, global-state, placeholder,
contract-docstring, module-injection, and split-module findings do fail, as does
growth of a file already over 5,000 lines. Debt reduction is scheduled refactor
work, not a per-PR tax.

When the rules intentionally change, regenerate the baseline in the same
change:

```bash
python scripts/readability_budget.py --write-baseline
```

Standalone report and baseline check (the CI quality gate supplies `--base`
for the huge-file growth check):

```bash
python scripts/readability_budget.py
```
