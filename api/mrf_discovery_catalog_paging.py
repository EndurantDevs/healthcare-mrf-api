"""Memory-bounded paging helpers for MRF discovery file responses."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any


MAX_FILE_PAGE_PLAN_REFERENCES = 10_000

_FILE_PLAN_CURSOR_SEPARATOR = "~plan~"


@dataclass(frozen=True)
class FilePlanWindow:
    """Describe the plan slice emitted for one stored file row."""

    file_data: Mapping[str, Any]
    plan_offset: int
    plan_limit: int
    plan_total: int


def bounded_file_windows(
    file_query_rows: list[Mapping[str, Any]],
    *,
    limit: int,
    cursor_plan_offset: int,
    plan_reference_limit: int,
) -> tuple[list[FilePlanWindow], str | None]:
    """Bound one response by file rows and expanded plan references."""

    plan_windows: list[FilePlanWindow] = []
    consumed_row_count = 0
    consumed_plan_references = 0
    completed_file_id: str | None = None
    for row_index, file_data in enumerate(file_query_rows[:limit]):
        file_id = str(file_data.get("mrf_file_id") or "").strip()
        plan_offset = cursor_plan_offset if row_index == 0 else 0
        plan_total = plan_reference_count(file_data)
        remaining_plan_count = max(plan_total - plan_offset, 0)
        if remaining_plan_count:
            available_plan_count = max(
                plan_reference_limit - consumed_plan_references,
                0,
            )
            if plan_windows and available_plan_count == 0:
                break
            selected_plan_count = min(
                remaining_plan_count,
                max(available_plan_count, 1),
            )
            plan_windows.append(
                FilePlanWindow(
                    file_data=file_data,
                    plan_offset=plan_offset,
                    plan_limit=selected_plan_count,
                    plan_total=plan_total,
                )
            )
            consumed_plan_references += selected_plan_count
            consumed_row_count += 1
            next_plan_offset = plan_offset + selected_plan_count
            if next_plan_offset < plan_total:
                return plan_windows, file_cursor(file_id, next_plan_offset)
        else:
            plan_windows.append(
                FilePlanWindow(
                    file_data=file_data,
                    plan_offset=plan_offset,
                    plan_limit=0,
                    plan_total=plan_total,
                )
            )
            consumed_row_count += 1
        completed_file_id = file_id
        if consumed_plan_references >= plan_reference_limit:
            break
    has_more_rows = consumed_row_count < len(file_query_rows)
    return plan_windows, completed_file_id if has_more_rows else None


def plan_reference_count(file_data: Mapping[str, Any]) -> int:
    """Count raw plan references without expanding plan dictionaries."""

    metadata_value = file_data.get("metadata_json")
    file_metadata = (
        dict(metadata_value) if isinstance(metadata_value, Mapping) else {}
    )
    metadata_plans = file_metadata.get("plan_info")
    if isinstance(metadata_plans, (list, tuple)):
        return len(metadata_plans)
    return max(
        value_count(file_data.get("plan_ids")),
        value_count(file_data.get("plan_names")),
        value_count(file_data.get("market_types")),
        0,
    )


def value_count(candidate_value: Any) -> int:
    """Count scalar or collection values without copying their contents."""

    if isinstance(candidate_value, str):
        return 1 if candidate_value.strip() else 0
    if isinstance(candidate_value, (list, tuple, set)):
        return len(candidate_value)
    return 0


def slice_values(
    candidate_value: Any,
    *,
    offset: int,
    limit: int | None,
) -> list[Any]:
    """Copy only the requested sequence window for plan normalization."""

    if not isinstance(candidate_value, (list, tuple)):
        return []
    stop_index = None if limit is None else offset + limit
    return list(candidate_value[offset:stop_index])


def ambiguous_plan_identity_keys(
    plan_rows: Any,
) -> set[tuple[str, str, str]]:
    """Find no-hash plan identities that require a name discriminator."""

    names_by_key: dict[tuple[str, str, str], set[str]] = {}
    if not isinstance(plan_rows, (list, tuple)):
        return set()
    for plan_row in plan_rows:
        if not isinstance(plan_row, Mapping):
            continue
        if any(
            str(plan_row.get(hash_field) or "").strip()
            for hash_field in ("engine_plan_hash", "plan_hash", "ptg2_plan_hash")
        ):
            continue
        plan_name = str(plan_row.get("plan_name") or "").strip()
        plan_key = plan_identity_key(plan_row)
        if plan_key is None or not plan_name:
            continue
        names_by_key.setdefault(plan_key, set()).add(plan_name)
    return {
        plan_key
        for plan_key, plan_names in names_by_key.items()
        if len(plan_names) > 1
    }


def plan_identity_key(
    plan_row: Mapping[str, Any],
) -> tuple[str, str, str] | None:
    """Return the importer-compatible base identity for one plan row."""

    plan_id = str(plan_row.get("plan_id") or "").strip()
    market_type = str(plan_row.get("plan_market_type") or "").strip().upper()
    if not plan_id or not market_type:
        return None
    plan_id_type = str(plan_row.get("plan_id_type") or "").strip().upper()
    return plan_id, plan_id_type, market_type


def parse_file_cursor(cursor: str | None) -> tuple[str | None, int]:
    """Decode a legacy file cursor or a composite plan-offset cursor."""

    normalized_cursor = str(cursor or "").strip()
    if not normalized_cursor:
        return None, 0
    if _FILE_PLAN_CURSOR_SEPARATOR not in normalized_cursor:
        return normalized_cursor, 0
    file_id, raw_offset = normalized_cursor.rsplit(
        _FILE_PLAN_CURSOR_SEPARATOR,
        1,
    )
    try:
        plan_offset = int(raw_offset)
    except ValueError as exc:
        raise ValueError("file cursor plan offset must be an integer") from exc
    if not file_id or plan_offset < 1:
        raise ValueError("file cursor plan offset must be greater than zero")
    return file_id, plan_offset


def file_cursor(file_id: str, plan_offset: int) -> str:
    """Encode a cursor that resumes within one plan-dense file."""

    return f"{file_id}{_FILE_PLAN_CURSOR_SEPARATOR}{plan_offset}"
