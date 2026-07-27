"""Reference-group loading and shaping for exact PTG snapshot controls."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any


ReferenceRowLoader = Callable[..., Awaitable[list[Any]]]


async def load_snapshot_reference_rows(
    schema: str,
    snapshot_id: str,
    loader: ReferenceRowLoader,
) -> dict[str, list[Any]]:
    """Load every pointer and release-binding reference group."""

    reference_specs = (
        ("global", "ptg2_current_snapshot", "slot", "snapshot_id"),
        ("source", "ptg2_current_source_snapshot", "source_key", "snapshot_id"),
        ("plan", "ptg2_current_plan_source", "plan_source_key", "snapshot_id"),
        ("previous_global", "ptg2_current_snapshot", "slot", "previous_snapshot_id"),
        (
            "previous_source",
            "ptg2_current_source_snapshot",
            "source_key",
            "previous_snapshot_id",
        ),
        (
            "previous_plan",
            "ptg2_current_plan_source",
            "plan_source_key",
            "previous_snapshot_id",
        ),
        ("pin", "ptg2_snapshot_pin", "owner_type, owner_id", "snapshot_id"),
        (
            "release_binding",
            "plan_release_snapshot_binding",
            "serving_revision_id, role, binding_ordinal",
            "snapshot_id",
        ),
    )
    reference_rows_by_kind: dict[str, list[Any]] = {}
    for kind, table, selected_fields, reference_field in reference_specs:
        reference_rows_by_kind[kind] = await loader(
            schema,
            snapshot_id,
            table=table,
            selected_fields=selected_fields,
            reference_field=reference_field,
        )
    return reference_rows_by_kind


def reference_string_values(
    reference_rows: list[Any],
    field: str,
    row_mapping: Callable[[Any], dict[str, Any]],
) -> list[str]:
    """Return one stable string value from every loaded reference row."""

    return [
        str(row_mapping(reference_row).get(field))
        for reference_row in reference_rows
    ]
