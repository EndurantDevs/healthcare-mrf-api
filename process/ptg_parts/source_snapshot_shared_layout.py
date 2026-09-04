# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exact shared-layout validation for targeted source snapshot removal."""

from __future__ import annotations

from typing import Any

from sqlalchemy import text

from process.ptg_parts.db_tables import _quote_ident
from process.ptg_parts.ptg2_shared_blocks import (
    PTG2_V3_SHARED_GENERATION,
    PTG2_V4_SHARED_GENERATION,
)
from process.ptg_parts.source_snapshot_control_policy import (
    manifest_dict,
    strict_shared_snapshot_key,
)


def _row_mapping(row: Any) -> dict[str, Any]:
    if isinstance(row, dict):
        return row
    return dict(getattr(row, "_mapping", row))


async def _lock_snapshot_layout(
    session: Any,
    *,
    schema: str,
    snapshot_id: str,
) -> dict[str, Any]:
    """Lock and return the single shared layout bound to a snapshot."""

    binding_query_result = await session.execute(
        text(
            f"""
            SELECT binding.snapshot_key, layout.generation, layout.state
              FROM {_quote_ident(schema)}.ptg2_v3_snapshot_binding AS binding
              JOIN {_quote_ident(schema)}.ptg2_v3_snapshot_layout AS layout
                ON layout.snapshot_key = binding.snapshot_key
             WHERE binding.snapshot_id = :snapshot_id
             FOR UPDATE OF binding, layout
            """
        ),
        {"snapshot_id": snapshot_id},
    )
    binding_rows = binding_query_result.all()
    if not binding_rows:
        return {}
    if len(binding_rows) != 1:
        raise RuntimeError("snapshot has multiple shared layout bindings")
    return _row_mapping(binding_rows[0])


async def _lock_expected_layout(
    session: Any,
    *,
    schema: str,
    snapshot_key: int,
) -> dict[str, Any]:
    """Lock the manifest-selected layout when logical binding never completed."""

    layout_query_result = await session.execute(
        text(
            f"""
            SELECT layout.snapshot_key, layout.generation, layout.state
              FROM {_quote_ident(schema)}.ptg2_v3_snapshot_layout AS layout
             WHERE layout.snapshot_key = :snapshot_key
             FOR UPDATE OF layout
            """
        ),
        {"snapshot_key": snapshot_key},
    )
    layout_rows = layout_query_result.all()
    if not layout_rows:
        return {}
    if len(layout_rows) != 1:
        raise RuntimeError("snapshot manifest resolved multiple shared layouts")
    return _row_mapping(layout_rows[0])


async def _require_complete_v4_root(
    session: Any,
    *,
    schema: str,
    snapshot_key: int,
) -> None:
    """Require the immutable complete packed-map root for a V4 layout."""

    root_query_result = await session.execute(
        text(
            f"""
            SELECT state
              FROM {_quote_ident(schema)}.ptg2_v4_snapshot_map_root
             WHERE snapshot_key = :snapshot_key
             FOR KEY SHARE
            """
        ),
        {"snapshot_key": snapshot_key},
    )
    root_rows = root_query_result.all()
    if (
        len(root_rows) != 1
        or str(_row_mapping(root_rows[0]).get("state") or "").strip().lower()
        != "complete"
    ):
        raise ValueError(
            "PTG V4 snapshot binding is missing its complete packed map root"
        )


async def bound_shared_layout_keys(
    session: Any,
    *,
    schema: str,
    snapshot_id: str,
    expected_generation: str,
    expected_snapshot_key: Any,
    allow_missing_binding: bool = False,
) -> tuple[int, ...]:
    """Validate and return the exact physical layout owned by one snapshot."""

    supported_generations = {
        PTG2_V3_SHARED_GENERATION,
        PTG2_V4_SHARED_GENERATION,
    }
    if expected_generation not in supported_generations:
        raise ValueError("snapshot removal plan has an unsupported storage generation")
    expected_layout_key = strict_shared_snapshot_key(expected_snapshot_key)
    layout_by_field = await _lock_snapshot_layout(
        session,
        schema=schema,
        snapshot_id=snapshot_id,
    )
    if not layout_by_field:
        if not allow_missing_binding:
            raise ValueError("snapshot is missing its shared layout binding")
        layout_by_field = await _lock_expected_layout(
            session,
            schema=schema,
            snapshot_key=expected_layout_key,
        )
        if not layout_by_field:
            return ()
    snapshot_key = int(layout_by_field.get("snapshot_key"))
    layout_generation = str(
        layout_by_field.get("generation") or ""
    ).strip().lower()
    if layout_generation != expected_generation:
        raise ValueError("snapshot binding storage generation does not match manifest")
    if str(layout_by_field.get("state") or "").strip().lower() != "sealed":
        raise ValueError("snapshot binding does not reference a sealed shared layout")
    if expected_layout_key != snapshot_key:
        raise ValueError(
            "snapshot manifest does not match its shared layout binding"
        )
    if expected_generation == PTG2_V4_SHARED_GENERATION:
        await _require_complete_v4_root(
            session,
            schema=schema,
            snapshot_key=snapshot_key,
        )
    return (snapshot_key,)


async def validate_retirement_shared_layout(
    session: Any,
    *,
    schema: str,
    snapshot_id: str,
    snapshot: dict[str, Any],
) -> None:
    """Require exact manifest-to-layout identity before pointer retirement."""

    if not snapshot:
        return
    manifest = manifest_dict(snapshot.get("manifest"))
    serving_index_value = manifest.get("serving_index")
    serving_index = (
        serving_index_value if isinstance(serving_index_value, dict) else {}
    )
    await bound_shared_layout_keys(
        session,
        schema=schema,
        snapshot_id=snapshot_id,
        expected_generation=str(
            serving_index.get("storage_generation") or ""
        ).strip().lower(),
        expected_snapshot_key=serving_index.get("shared_snapshot_key"),
    )
