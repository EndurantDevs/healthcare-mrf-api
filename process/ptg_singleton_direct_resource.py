# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Immutable PTGSmall resource contract for singleton-direct waves."""

PTG_SMALL_RESOURCE_CONTRACT = {
    "version": 1,
    "resource_class": "small",
    "queue": "arq:PTGSmall",
    "worker_class": "process.PTGSmall",
    "memory_budget_mib": 8192,
    "estimated_peak_rss_mib": 6144,
    "estimate_basis": {},
    "scanner": {
        "rapidgzip_threads": 4,
        "rust_workers": 4,
        "work_queue": 4,
        "event_queue": 8,
        "parse_in_workers": True,
        "top_level_byte_scan": True,
        "provider_refs_in_workers": True,
        "provider_ref_workers": 4,
        "provider_ref_queue": 4,
        "manifest_merge_chunk_bytes": 256 * 1024 * 1024,
        "manifest_merge_sort_workers": 4,
        "file_process_concurrency": 1,
    },
}


__all__ = ["PTG_SMALL_RESOURCE_CONTRACT"]
