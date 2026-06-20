# PTG/MRF Resource Lanes

## Purpose

PTG and large MRF imports have highly variable memory profiles. A small PTG
file can finish quickly with a few GiB, while a UHC-style file can exceed 50
GiB during compact serving. The import-control plane must schedule these jobs
by expected memory and worker profile, not by a single global PTG slot.

## Decisions

- Classify PTG/MRF work into `small`, `normal`, `large`, and `huge`.
- Route each class to a dedicated ARQ queue and worker class.
- Admit new work by per-node memory budget plus queue depth.
- Enable Rust parse-in-workers only for `small` and `normal` lanes until dev
  telemetry proves it is safe for larger files.
- Keep PostgreSQL publish dedupe backstops while scanner changes are being
  rolled out.
- Use node-local scratch for scanner hot files and shared `/work` only for
  retained artifacts.

## Non-Goals

- No rewrite of PTG2 serving schema.
- No removal of `DISTINCT ON` publish backstops in this phase.
- No use of NFS/RWX as the scanner spill or merge hot path.
- No automatic production concurrency increase without dev telemetry.

## Rollback

- Disable lane routing and enqueue PTG through `arq:PTG`.
- Disable parse-in-workers for all lanes.
- Set active PTG lane caps to `huge=1`, all others `0`.
- Revert dev Flux image tags to the previous release.
