# 03 Scanner and Scratch

## Rust Scanner

- Keep release builds for deployed scanner binaries.
- Keep Rust panic unwind behavior; do not set `panic = "abort"`.
- Keep `DISTINCT ON` publish dedupe as a correctness backstop during this rollout.
- Use parse-in-workers where the lane profile enables it. Current dev enables it
  for all PTG lanes and uses lane-specific worker/queue overrides supplied by
  the external orchestrator.
- Continue splitting large negotiated-rate arrays with
  `HLTHPRT_PTG2_RUST_SPLIT_NEGOTIATED_RATES`.

## Scratch Policy

Hot temporary files must be node-local:

- scanner COPY shards
- `.ready` queue files
- manifest sidecar spill files
- manifest pre-COPY merge files

Strict V3 gives every file scan its own serving-run and sidecar directory. The
Rust scanner places manifest spill files in that owned serving-run directory
and recreates final sidecar parents immediately before finalization. Cleanup
must age-gate both files and empty directories; a fresh empty destination can
belong to an active scan and is never evidence that the build is abandoned.

Retained raw/logical artifacts may use shared `/work`:

- raw downloaded files
- logical JSON artifacts
- retained manifest metadata

For `postgres_binary_v3`, final forward/reverse serving artifacts are copied
into PostgreSQL. API pods read from PostgreSQL and must not materialize a disk
cache. Files under the serving scratch root are publication intermediates only
and are removed after upload or by age-gated recovery cleanup.

Default env names:

- `HLTHPRT_PTG2_HOT_ARTIFACT_DIR`
- `HLTHPRT_PTG2_MANIFEST_SPILL_DIR`
- `HLTHPRT_PTG2_MANIFEST_MERGE_DIR`

If the hot directory is missing or not writable, fail fast for PTG lanes that
require it rather than silently spilling to RWX/NFS.
