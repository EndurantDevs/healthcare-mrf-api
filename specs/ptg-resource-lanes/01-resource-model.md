# 01 Resource Model

## Resource Classes

| Class | Scheduler memory budget | Dev worker limit | Intended work |
| --- | ---: | ---: | --- |
| `small` | 8 GiB | 12 GiB | Small PTG files, small MRF chunks, smoke imports |
| `normal` | 18 GiB | 32 GiB | Typical source-index PTG files and MRF file chunks |
| `large` | 40 GiB | 64 GiB | Large payer files with many rates or provider groups |
| `huge` | 64 GiB | 96 GiB | Known high-memory PTG compact-serving jobs |

The scheduler budget is the capacity reservation used by import-control when
choosing whether a node has headroom. The Kubernetes worker limit is the pod
cgroup ceiling. Keep the worker limit higher than the scheduler budget so the
scanner can survive transient publish/merge peaks without letting the scheduler
overfill the node.

## Classification Inputs

Use these inputs in order:

1. Last successful run metrics for the same `source_file_id` or source identity.
2. Catalog `content_length`, `content_file_count`, and file domain.
3. Import params such as `max_files`, `max_items`, `test_mode`, and `file_url_contains`.
4. Conservative default: `normal` for unknown PTG, `small` for test/smoke.

## Stored Metrics

Each run should store `metrics.ptg_resource` or `metrics.mrf_resource`:

```json
{
  "version": 1,
  "resource_class": "normal",
  "queue": "arq:PTGNormal",
  "worker_class": "process.PTGNormal",
  "memory_budget_mib": 18432,
  "estimated_peak_rss_mib": 12288,
  "estimate_basis": {
    "content_length": 0,
    "history_peak_rss_mib": null
  },
  "scanner": {
    "rust_workers": 4,
    "parse_in_workers": true,
    "work_queue": 4,
    "event_queue": 16,
    "provider_refs_in_workers": true,
    "provider_ref_workers": 8,
    "provider_ref_queue": 8,
    "file_process_concurrency": 1
  },
  "actual": {
    "peak_rss_mib": null,
    "cgroup_peak_mib": null,
    "data_seconds": null,
    "publish_seconds": null
  }
}
```

The import-control admin UI should display class, queue, worker class, memory
budget, and latest actual memory when present.
