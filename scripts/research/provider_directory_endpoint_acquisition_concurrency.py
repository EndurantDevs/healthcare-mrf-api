"""Concurrency guards for the Provider Directory endpoint campaign harness."""

from __future__ import annotations

import fcntl
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

try:
    from scripts.research.provider_directory_endpoint_acquisition_restart import HarnessConflict
except ModuleNotFoundError:
    from provider_directory_endpoint_acquisition_restart import HarnessConflict


ACQUISITION_CLASSIFICATIONS = {"acquisition", "bulk_acquisition"}
EXCLUSIVE_FLAGS = (
    "stale_cleanup",
    "publish_artifacts",
    "publish_after_acquisition",
    "publish_corroboration",
)


@contextmanager
def campaign_state_lock(state_path: Path) -> Iterator[None]:
    """Prevent concurrent harnesses from overwriting the same campaign state."""
    state_path.parent.mkdir(parents=True, exist_ok=True)
    lock_path = state_path.with_name(f"{state_path.name}.lock")
    lock_handle = lock_path.open("a+", encoding="utf-8")
    try:
        try:
            fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise HarnessConflict(f"campaign state is already locked: {state_path}") from exc
        yield
    finally:
        fcntl.flock(lock_handle.fileno(), fcntl.LOCK_UN)
        lock_handle.close()


def _parallel_acquisition_scope(
    params_by_name: dict[str, Any],
    metrics_by_name: dict[str, Any] | None = None,
) -> tuple[set[str], set[str]] | None:
    if params_by_name.get("import_resources") is not True:
        return None
    if any(params_by_name.get(flag_name) is not False for flag_name in EXCLUSIVE_FLAGS):
        return None
    try:
        source_concurrency = int(params_by_name.get("source_concurrency") or 1)
    except (TypeError, ValueError):
        return None
    if source_concurrency != 1:
        return None
    source_values = params_by_name.get("source_ids")
    if not isinstance(source_values, list):
        return None
    source_ids = {
        str(source_id).strip()
        for source_id in source_values
        if str(source_id).strip()
    }
    if not source_ids or len(source_ids) != len(source_values):
        return None
    endpoint_scope = str(
        params_by_name.get("provider_directory_endpoint_scope") or ""
    ).strip().rstrip("/")
    endpoint_scopes = {endpoint_scope} if endpoint_scope else set()
    active_groups = (metrics_by_name or {}).get("active_source_groups")
    if not endpoint_scopes and isinstance(active_groups, list):
        endpoint_scopes.update(
            str(group.get("api_base") or "").strip().rstrip("/")
            for group in active_groups
            if isinstance(group, dict) and str(group.get("api_base") or "").strip()
        )
    return (source_ids, endpoint_scopes) if len(endpoint_scopes) == 1 else None


def is_active_run_conflicting_with_entry(
    entry: dict[str, Any],
    expected_params_by_name: dict[str, Any],
    run_record: dict[str, Any],
) -> bool:
    """Return whether an active run overlaps or lacks safe acquisition scope."""
    if entry["classification"] not in ACQUISITION_CLASSIFICATIONS:
        return True
    scoped_expected_params_by_name = {
        **expected_params_by_name,
        "provider_directory_endpoint_scope": entry["canonical_base"],
    }
    expected_scope = _parallel_acquisition_scope(scoped_expected_params_by_name)
    run_params = run_record.get("params") if isinstance(run_record.get("params"), dict) else {}
    run_metrics = run_record.get("metrics") if isinstance(run_record.get("metrics"), dict) else {}
    active_scope = _parallel_acquisition_scope(run_params, run_metrics)
    if expected_scope is None or active_scope is None:
        return True
    expected_source_ids, _expected_endpoints = expected_scope
    active_source_ids, active_endpoints = active_scope
    expected_endpoint = str(entry["canonical_base"]).rstrip("/")
    return bool(
        expected_source_ids.intersection(active_source_ids)
        or expected_endpoint in active_endpoints
    )
