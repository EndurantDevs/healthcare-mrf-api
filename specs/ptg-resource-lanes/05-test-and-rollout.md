# 05 Test and Rollout

## Local Tests

- Resource classifier unit tests for size/history/unknown cases.
- Queue routing tests for manual PTG, scheduled PTG, and source-file dispatch.
- Worker launcher tests for per-class Kubernetes resources.
- Worker guard tests for mismatched queue/class payloads.
- Existing PTG manifest publish tests must continue to pass.

## Dev-Server Tests

1. Deploy import-control, healthcare-mrf-api, and api-app changes.
2. Reconcile Flux and wait for app health.
3. Run one small PTG source-file import through admin/import-control.
4. Run one normal or large PTG source-file import if capacity is available.
5. Run one MRF chunked smoke/full-safe import.
6. Restart import-control and api-app; verify latest completed runs remain visible.
7. Verify `mrf-3` is the active coderoam lane for this workstream.

## Acceptance Criteria

- Admin import status shows current run, last completed run, and resource lane.
- Small PTG work is not blocked solely because one large/huge PTG is active.
- Worker jobs request/limit memory according to their class.
- No PTG scanner hot files are written to RWX/NFS by default.
- New storage-saving PTG snapshots publish with
  `arch_version=postgres_binary_v1`, no durable `provider_set_component`, no
  durable `provider_group_rate_scope`, PostgreSQL-owned binary serving
  artifacts, and no pod-local serving artifact cache.
- API smoke covers forward plan/code pricing, reverse NPI pricing, and
  geo-filtered provider pricing against a `postgres_binary_v1` snapshot.
- Rollback env flags are documented and tested on dev.
