# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Budgeted map conversions shared by candidate-audit readers."""

from __future__ import annotations

from typing import Any, Mapping

from api.ptg2_candidate_audit_capacity import CandidateAuditDecodedRetentionBudget


INTEGER_MAP_BYTES = 224
INTEGER_MAP_ENTRY_BYTES = 64
INTEGER_MAP_BUCKET_BYTES = 280
INTEGER_TUPLE_PEAK_BYTES = 112
INTEGER_TUPLE_PEAK_MEMBERSHIP_BYTES = 16
INTEGER_TUPLE_BYTES = 56
INTEGER_TUPLE_MEMBERSHIP_BYTES = 8
INTEGER_SET_MEMBERSHIP_BYTES = 112


def freeze_integer_set_map(
    source_map: Mapping[Any, set[int]],
    retention_budget: CandidateAuditDecodedRetentionBudget | None,
    *,
    category: str,
) -> dict[Any, tuple[int, ...]]:
    """Freeze nonempty integer sets while claiming the conversion peak."""

    if retention_budget is None:
        return {
            key: tuple(sorted(integer_keys))
            for key, integer_keys in source_map.items()
            if integer_keys
        }
    retained_result_bytes = INTEGER_MAP_BYTES
    retention_budget.claim(
        retained_result_bytes,
        category=f"the frozen {category} map",
    )
    frozen_map: dict[Any, tuple[int, ...]] = {}
    try:
        for key, integer_keys in source_map.items():
            if not integer_keys:
                continue
            key_count = len(integer_keys)
            ordering_bytes = (
                INTEGER_TUPLE_PEAK_BYTES
                + key_count * INTEGER_TUPLE_PEAK_MEMBERSHIP_BYTES
            )
            retention_budget.claim(
                INTEGER_MAP_ENTRY_BYTES + ordering_bytes,
                category=f"a frozen {category} tuple",
            )
            retained_result_bytes += INTEGER_MAP_ENTRY_BYTES + ordering_bytes
            frozen_keys = tuple(sorted(integer_keys))
            tuple_bytes = (
                INTEGER_TUPLE_BYTES + key_count * INTEGER_TUPLE_MEMBERSHIP_BYTES
            )
            retention_budget.release(ordering_bytes - tuple_bytes)
            retained_result_bytes -= ordering_bytes - tuple_bytes
            frozen_map[key] = frozen_keys
    except BaseException:
        retention_budget.release(retained_result_bytes)
        raise
    source_bytes = INTEGER_MAP_BYTES + sum(
        INTEGER_MAP_BUCKET_BYTES + len(integer_keys) * INTEGER_SET_MEMBERSHIP_BYTES
        for integer_keys in source_map.values()
    )
    retention_budget.release(source_bytes)
    return frozen_map


__all__ = [
    "INTEGER_MAP_BUCKET_BYTES",
    "INTEGER_MAP_BYTES",
    "freeze_integer_set_map",
]
