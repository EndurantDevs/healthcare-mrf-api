# 05 Test and Rollout

## Local Tests

- Resource classifier unit tests for size/history/unknown cases.
- Queue routing tests for manual PTG, scheduled PTG, and source-file dispatch.
- Worker launcher tests for per-class Kubernetes resources.
- Worker guard tests for mismatched queue/class payloads.
- Existing PTG manifest publish tests must continue to pass.

## Dev-Server Tests

1. Deploy the public service and deployment-repository changes through CI.
2. Reconcile the deployment controller and wait for service health.
3. Run one small PTG source-file import through the authenticated operator API.
4. Run one normal or large PTG source-file import if capacity is available.
5. Run one MRF chunked smoke/full-safe import.
6. Restart the service and external orchestrator; verify latest completed runs remain visible.

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
