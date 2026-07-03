# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Cleanup helpers for source-scoped PTG2 snapshot tables."""

from __future__ import annotations

import json
import os
import re
from typing import Any

from db.connection import db
from process.ptg_parts.db_tables import _quote_ident


def _dedupe_preserve_table_names(seq: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for item in seq:
        if item not in seen:
            seen.add(item)
            result.append(item)
    return result


def _snapshot_manifest_table_names(serving_index: dict[str, Any] | None) -> list[str]:
    if not serving_index or serving_index.get("storage") not in {"db_compact_snapshot", "manifest_snapshot"}:
        return []
    table_values = [
        serving_index.get("table"),
        serving_index.get("price_code_set_table"),
        serving_index.get("price_atom_table"),
        serving_index.get("price_table"),
        serving_index.get("price_set_entry_table"),
        serving_index.get("procedure_table"),
        serving_index.get("provider_set_table"),
        serving_index.get("provider_set_component_table"),
        serving_index.get("provider_set_entry_table"),
        serving_index.get("provider_entry_component_table"),
        serving_index.get("provider_group_member_table"),
        serving_index.get("provider_group_location_table"),
        serving_index.get("provider_group_rate_scope_table"),
        serving_index.get("code_count_table"),
    ]
    allowed_prefixes = (
        "ptg2_serving_",
        "ptg2_serving_rate_compact_",
        "ptg2_code_count_",
        "ptg2_price_atom_",
        "ptg2_price_set_",
        "ptg2_price_set_entry_",
        "ptg2_procedure_",
        "ptg2_provider_set_",
        "ptg2_provider_set_entry_",
        "ptg2_provider_entry_component_",
        "ptg2_provider_group_member_",
        "ptg2_provider_group_location_",
        "ptg2_provider_group_rate_scope_",
    )
    result: list[str] = []
    for value in table_values:
        if not value:
            continue
        table_name = str(value).split(".", 1)[1] if "." in str(value) else str(value)
        if table_name.startswith(allowed_prefixes) and re.fullmatch(r"[a-z0-9_]{1,63}", table_name):
            result.append(table_name)
    return _dedupe_preserve_table_names(result)


async def _drop_ptg2_snapshot_table_names(table_names: list[str]) -> None:
    if not table_names:
        return
    schema_name = os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    for table_name in table_names:
        await db.status(f"DROP TABLE IF EXISTS {_quote_ident(schema_name)}.{_quote_ident(table_name)};")


async def _drop_ptg2_snapshot_tables_for_manifest(serving_index: dict[str, Any] | None) -> None:
    await _drop_ptg2_snapshot_table_names(_snapshot_manifest_table_names(serving_index))


async def _cleanup_old_ptg2_source_tables(source_key: str, keep_snapshot_ids: set[str]) -> None:
    schema_name = os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    rows = await db.all(
        f"""
        SELECT snapshot_id, manifest
          FROM {_quote_ident(schema_name)}.ptg2_snapshot
         WHERE manifest->'serving_index'->>'source_key' = :source_key
           AND manifest->'serving_index'->>'storage' = 'db_compact_snapshot'
        """,
        source_key=source_key,
    )
    table_names: list[str] = []
    for row in rows:
        data = row if isinstance(row, dict) else row._mapping
        if str(data.get("snapshot_id") or "") in keep_snapshot_ids:
            continue
        manifest = data.get("manifest") or {}
        if isinstance(manifest, str):
            try:
                manifest = json.loads(manifest)
            except json.JSONDecodeError:
                manifest = {}
        table_names.extend(_snapshot_manifest_table_names((manifest or {}).get("serving_index")))
    await _drop_ptg2_snapshot_table_names(_dedupe_preserve_table_names(table_names))
