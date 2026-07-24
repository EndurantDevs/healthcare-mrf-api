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


def _v4_manifest_snapshot_key(expected_snapshot_key: Any) -> int:
    """Return the required positive V4 layout coordinate from the manifest."""

    try:
        snapshot_key = int(expected_snapshot_key)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            "PTG V4 snapshot manifest is missing its shared snapshot key"
        ) from exc
    if snapshot_key <= 0:
        raise ValueError(
            "PTG V4 snapshot manifest is missing its shared snapshot key"
        )
    return snapshot_key


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
) -> tuple[int, ...]:
    """Validate and return the exact physical layout owned by one snapshot."""

    supported_generations = {
        PTG2_V3_SHARED_GENERATION,
        PTG2_V4_SHARED_GENERATION,
    }
    if expected_generation not in supported_generations:
        raise ValueError("snapshot removal plan has an unsupported storage generation")
    layout_by_field = await _lock_snapshot_layout(
        session,
        schema=schema,
        snapshot_id=snapshot_id,
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
    if expected_generation == PTG2_V4_SHARED_GENERATION:
        if _v4_manifest_snapshot_key(expected_snapshot_key) != snapshot_key:
            raise ValueError(
                "PTG V4 snapshot manifest does not match its shared layout binding"
            )
        await _require_complete_v4_root(
            session,
            schema=schema,
            snapshot_key=snapshot_key,
        )
    return (snapshot_key,)
