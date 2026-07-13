# PTG/MRF Resource Lanes

## Purpose

PTG and large MRF imports have highly variable memory profiles. A small PTG
file can finish quickly with a few GiB, while a UHC-style file can exceed 50
GiB during compact serving. The external orchestration layer must schedule these jobs
by expected memory and worker profile, not by a single global PTG slot.

## Decisions

- Classify PTG/MRF work into `small`, `normal`, `large`, and `huge`.
- Route each class to a dedicated ARQ queue and worker class.
- Admit new work by per-node memory budget plus queue depth.
- Enable Rust parse-in-workers for all PTG lanes after dev telemetry showed it
  is needed to keep large gzip/JSON scans CPU-bound instead of producer-bound.
- Keep PostgreSQL publish dedupe backstops while scanner changes are being
  rolled out.
- Use node-local scratch for scanner hot files. Retained raw/logical artifacts
  may still use `/work`; final `postgres_binary_v1` serving artifacts are stored
  in PostgreSQL and must not be served from pod-local files.
- Use `postgres_binary_v1` as the default PTG2 snapshot architecture for new
  storage-saving imports. It removes the exploded `provider_set_component` and
  `provider_group_rate_scope` tables while preserving forward and reverse API
  lookups through compressed PostgreSQL artifact payloads.

## Non-Goals

- No loss of PTG provider, plan, code, price atom, geo, or reverse-NPI API
  coverage while changing the physical serving layout.
- No removal of `DISTINCT ON` publish backstops in this phase.
- No use of NFS/RWX as the scanner spill or merge hot path.
- No automatic production concurrency increase without dev telemetry.

## Rollback

- Disable lane routing and enqueue PTG through `arq:PTG`.
- Disable parse-in-workers for all lanes.
- Set active PTG lane caps to `huge=1`, all others `0`.
- Switch `HLTHPRT_PTG2_SNAPSHOT_ARCH` back only for a controlled comparison
  import; do not mix snapshot rollback with route-pointer rollback.
- Revert dev Flux image tags to the previous release.
