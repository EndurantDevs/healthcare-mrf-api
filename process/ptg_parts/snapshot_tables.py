# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Source-scoped PTG2 snapshot table naming helpers."""

from __future__ import annotations

import re

from process.ptg_parts.canonical import hash_prefix, semantic_hash


def _normalize_source_key(source_key: str | None) -> str | None:
    if not source_key:
        return None
    normalized = "".join(ch if ch.isalnum() else "_" for ch in str(source_key).strip().lower()).strip("_")
    if not normalized:
        return None
    if len(normalized) > 48:
        suffix = hash_prefix(semantic_hash(normalized, domain="ptg2_source_key"), 10)
        normalized = f"{normalized[:37]}_{suffix}"
    return normalized


def _ptg2_snapshot_table_token(source_key: str, snapshot_id: str) -> str:
    return hash_prefix(
        semantic_hash({"source_key": source_key, "snapshot_id": snapshot_id}, domain="ptg2_snapshot_tables"),
        16,
    )


def _ptg2_snapshot_table_name(kind: str, source_key: str, snapshot_id: str) -> str:
    safe_kind = re.sub(r"[^a-z0-9_]+", "_", kind.lower()).strip("_")
    return f"ptg2_{safe_kind}_{_ptg2_snapshot_table_token(source_key, snapshot_id)}"[:63]


def _ptg2_snapshot_index_name(table_name: str, role: str) -> str:
    suffix = hash_prefix(semantic_hash({"table": table_name, "role": role}, domain="ptg2_snapshot_index"), 10)
    base = re.sub(r"[^a-z0-9_]+", "_", f"{table_name}_{role}").strip("_")
    max_base = max(1, 62 - len(suffix))
    return f"{base[:max_base]}_{suffix}"[:63]
