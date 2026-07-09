# 03 Scanner and Scratch

## Rust Scanner

- Keep release builds for deployed scanner binaries.
- Keep Rust panic unwind behavior; do not set `panic = "abort"`.
- Keep `DISTINCT ON` publish dedupe as a correctness backstop during this rollout.
- Use parse-in-workers where the lane profile enables it. Current dev enables it
  for all PTG lanes and uses lane-specific worker/queue overrides from
  import-control.
- Continue splitting large negotiated-rate arrays with
  `HLTHPRT_PTG2_RUST_SPLIT_NEGOTIATED_RATES`.

## Scratch Policy

Hot temporary files must be node-local:

- scanner COPY shards
- `.ready` queue files
- manifest sidecar spill files
- manifest pre-COPY merge files

Retained raw/logical artifacts may use shared `/work`:

- raw downloaded files
- logical JSON artifacts
- retained manifest metadata

Final serving artifacts depend on the snapshot architecture:

- `postgres_binary_v1`: final forward/reverse serving artifacts are copied into
  PostgreSQL artifact tables. API pods read from PostgreSQL and must not
  materialize a disk cache.
- legacy or research sidecar layouts: final PTG2 sidecars may remain under the
  configured artifact root and are cleaned by manifest-driven cleanup tools.

Default env names:

- `HLTHPRT_PTG2_HOT_ARTIFACT_DIR`
- `HLTHPRT_PTG2_MANIFEST_SPILL_DIR`
- `HLTHPRT_PTG2_MANIFEST_MERGE_DIR`

If the hot directory is missing or not writable, fail fast for PTG lanes that
require it rather than silently spilling to RWX/NFS.
