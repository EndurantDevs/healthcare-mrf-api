# 02 Worker Lanes

## PTG Lanes

| Queue | Worker class | Code max-jobs default | Dev cap | Scanner default |
| --- | --- | ---: | ---: | --- |
| `arq:PTGSmall` | `process.PTGSmall` | 16 per worker | 1 active job | 4 workers, parse-in-workers on |
| `arq:PTGNormal` | `process.PTGNormal` | 8 per worker | 1 active job | 4 workers, parse-in-workers on; dev override 8 workers |
| `arq:PTGLarge` | `process.PTGLarge` | 3 per worker | 1 active job | 4 workers, parse-in-workers on; dev override 16 workers |
| `arq:PTGHuge` | `process.PTGHuge` | 1 per worker | 1 active job | 4 workers, parse-in-workers on; dev override 8 workers |

The legacy `arq:PTG` and `process.PTG` remain as a compatibility/default lane
while rollout is in progress.

## MRF Lanes

MRF chunked work remains on `arq:MRF` in this phase. Resource metrics are still
recorded so future deployment can split MRF into chunk classes without changing
the authenticated operator API again.

## Runtime Guards

- A PTG run payload carries `resource_class`, expected queue, and expected worker class.
- A worker rejects a PTG payload whose expected queue/class does not match the running class.
- The worker launcher renders Kubernetes resource requests/limits from the
  selected worker class, not one global `HLTHPRT_WORKER_JOB_MEMORY_LIMIT`.

## Deployment Defaults

Dev currently isolates all PTG lanes to one active job per launched worker class
while it validates the PostgreSQL-binary storage path, large-file retry
escalation, and API latency. Production should copy the same lane model, but
caps must be set from observed node memory, PostgreSQL headroom, and import
duration targets.
