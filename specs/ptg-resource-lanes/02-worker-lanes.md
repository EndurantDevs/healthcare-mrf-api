# 02 Worker Lanes

## PTG Lanes

| Queue | Worker class | Cap goal | Scanner default |
| --- | --- | ---: | --- |
| `arq:PTGSmall` | `process.PTGSmall` | 16 per node | 4 workers, parse-in-workers on |
| `arq:PTGNormal` | `process.PTGNormal` | 8 per node | 2 workers, parse-in-workers on |
| `arq:PTGLarge` | `process.PTGLarge` | 3 per node | 1 worker, parse-in-workers off |
| `arq:PTGHuge` | `process.PTGHuge` | 1 per node | 1 worker, parse-in-workers off |

The legacy `arq:PTG` and `process.PTG` remain as a compatibility/default lane
while rollout is in progress.

## MRF Lanes

MRF chunked work remains on `arq:MRF` in this phase. Resource metrics are still
recorded so future deployment can split MRF into chunk classes without changing
the import-control API again.

## Runtime Guards

- A PTG run payload carries `resource_class`, expected queue, and expected worker class.
- A worker rejects a PTG payload whose expected queue/class does not match the running class.
- The worker launcher renders Kubernetes resource requests/limits from the
  selected worker class, not one global `HLTHPRT_WORKER_JOB_MEMORY_LIMIT`.

## Deployment Defaults

Dev starts with higher small/normal concurrency and conservative large/huge
isolation. Production should copy the same lane model, but caps must be set from
observed node memory and PostgreSQL headroom.
