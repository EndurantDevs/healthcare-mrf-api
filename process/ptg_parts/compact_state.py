# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""PTG2 compact import state and payload helpers."""

from __future__ import annotations

import datetime
from typing import Any

from process.ptg_parts.canonical import money_number
from process.ptg_parts.config import (
    PTG2_COMPACT_BATCH_ROWS_ENV,
    PTG2_STREAMING_DEDUPE_ENV,
    _env_bool,
    _env_int,
)


def _compact_state(batch_rows: int | None = None) -> dict[str, Any]:
    return {
        "batch_rows": batch_rows or max(_env_int(PTG2_COMPACT_BATCH_ROWS_ENV, 5000), 1),
        "seen": {
            "provider_group": set(),
            "provider_group_member": set(),
            "provider_set": set(),
            "provider_set_component": set(),
            "provider_location": set(),
            "procedure": set(),
            "price_atom": set(),
            "price_set": set(),
            "source_trace": set(),
            "source_trace_set": set(),
            "rate_pack": set(),
            "serving_rate_compact": set(),
            "serving_rate": set(),
        },
        "rows": {
            "provider_group": [],
            "provider_group_member": [],
            "provider_set": [],
            "provider_set_component": [],
            "provider_location": [],
            "procedure": [],
            "price_atom": [],
            "price_set": [],
            "source_trace": [],
            "source_trace_set": [],
            "rate_pack": [],
            "serving_rate_compact": [],
            "serving_rate": [],
        },
        "rate_pack_groups": {},
        "chunk_rate_packs": {},
        "procedure_payloads": {},
        "price_payloads": {},
        "provider_set_counts": {},
        "provider_set_network_names": {},
        "existing_price_set_hashes": None,
        "counts": {
            "provider_groups": 0,
            "provider_group_members": 0,
            "provider_sets": 0,
            "provider_set_components": 0,
            "provider_locations": 0,
            "procedures": 0,
            "price_atoms": 0,
            "price_sets": 0,
            "rate_packs": 0,
            "serving_rates": 0,
        },
        "pending_writes": [],
    }


def _compact_streaming_dedupe_tables() -> set[str]:
    if not _env_bool(PTG2_STREAMING_DEDUPE_ENV, False):
        return set()
    return {"price_set", "serving_rate"}


def _compact_add_unique(state: dict[str, Any], table_key: str, hash_key: str | tuple[str, ...], row: dict[str, Any]) -> bool:
    if table_key in _compact_streaming_dedupe_tables():
        state["rows"][table_key].append(row)
        return True
    if isinstance(hash_key, tuple):
        value = tuple(row.get(key) for key in hash_key)
    else:
        value = row.get(hash_key)
    if not value or value in state["seen"][table_key]:
        return False
    state["seen"][table_key].add(value)
    state["rows"][table_key].append(row)
    return True


def _ptg2_price_atom_payload(price_atom_row: dict[str, Any]) -> dict[str, Any]:
    expiration_date = price_atom_row.get("expiration_date")
    return {
        "negotiated_type": price_atom_row.get("negotiated_type"),
        "negotiated_rate": money_number(price_atom_row.get("negotiated_rate")),
        "expiration_date": expiration_date.isoformat() if isinstance(expiration_date, datetime.date) else expiration_date,
        "service_code": price_atom_row.get("service_code") or [],
        "billing_class": price_atom_row.get("billing_class"),
        "setting": price_atom_row.get("setting"),
        "billing_code_modifier": price_atom_row.get("billing_code_modifier") or [],
        "additional_information": price_atom_row.get("additional_information"),
    }


def _ptg2_source_trace_payload(source_trace_row: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        {
            "source_file_version_id": source_trace_row.get("source_file_version_id"),
            "url": source_trace_row.get("original_url"),
            "canonical_url": source_trace_row.get("canonical_url"),
            "statement": "Published negotiated rate from Transparency in Coverage source file.",
        }
    ]
