# Human-Readable Import Architecture

This note records the refactor target for the largest PTG, provider-quality, and
Rust scanner modules. The import behavior is intentionally unchanged; the goal is
to make the implementation understandable and maintainable by engineers who did
not write the original importer.

## Quality Bar

- Keep public import contracts stable while moving implementation into smaller
  modules.
- Prefer explicit domain names over generic helper buckets.
- Keep each new Python module below `max-module-lines = 2000`.
- Keep each new Rust module below 800 lines unless a short module comment
  explains why the boundary is intentionally larger.
- Keep CLI/ARQ entrypoints thin; parsing and dispatch belong at the edge, not in
  the core modules.
- Preserve PTG2 scanner stdout/stderr event formats byte-for-byte unless a
  separate protocol migration is planned.

## Target Boundaries

`process.ptg` remains the compatibility facade for `process.__init__`, tests, and
operator scripts. New implementation code should move into `process.ptg_parts`
by domain:

- `config`: environment variables, defaults, and pure config accessors.
- `screen`: terminal progress output.
- `domain`: dataclasses, enums, normalization, semantic hashes, and builders.
- `artifacts`: retained raw artifacts, range downloads, and logical JSON streams.
- `copying`: PostgreSQL COPY helpers and stage table writers.
- `publish`: snapshot table publish, index creation, and pointer swaps.
- `serving_index`: JSON and DB serving index builders.
- `orchestration`: high-level import flow that wires the parts together.

`process.provider_quality` remains the compatibility facade for CLI and ARQ task
names. New implementation code should move into `process.provider_quality_parts`
by domain:

- `config`: source URLs, queue names, env defaults, dataset metadata.
- `state`: Redis run/materialization progress keys and counters.
- `sources`: source resolution, downloads, and chunk splitting.
- `loaders`: QPP/SVI row normalization and staging loaders.
- `sql`: cohort SQL generation with small named builders.
- `materialize`: sharded and non-sharded materialization orchestration.
- `publish`: table rename/publish and run metadata.

`support/ptg2_scanner` should expose reusable scanner internals from `src/lib.rs`
and keep binaries as thin shells. New Rust modules should use descriptive names:

- `config`: defaults and environment parsing.
- `input`: compressed/plain artifact readers.
- `progress`: scanner progress events.
- `hashing`: canonicalization, xxh3 helpers, and hash-key builders.
- `dedupe`: shared dedupe sets and counters.
- `copy_sink`: COPY file rotation and event emission.
- `compact`: compact TiC stream parsing and worker orchestration.
- `scan`: generic top-level object scanning.

## Migration Rules

- Move one subsystem per commit.
- Keep facade imports/re-exports until all callers are migrated.
- Run targeted tests after every slice.
- Do not combine Python and Rust movement in one commit.
- Treat unrelated dirty files as out of scope.
- Do not change schema, API response shape, ARQ task names, CLI flags, or Rust
  scanner event formats as part of readability-only slices.
