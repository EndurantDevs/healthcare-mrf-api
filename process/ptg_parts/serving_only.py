# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Pure helpers for the PTG2 Python serving-only fallback path."""

from __future__ import annotations

import hashlib
import pickle
import tempfile
from pathlib import Path
from typing import Any, Callable

from process.ptg_parts.canonical import (
    _canonicalize_for_json,
    canonical_json_dumps,
    hash_prefix,
    money_number,
    normalize_money,
    semantic_hash,
)
from process.ptg_parts.domain import (
    PTG2_CONFIDENCE_NPPES_PRACTICE_LOCATION,
    PTG2_CONFIDENCE_TIC_RATE_NPI_TIN,
    ptg2_confidence_statement,
)
from process.ptg_parts.import_rows import (
    _build_provider_set_entry,
    _combine_provider_set_entries,
    _fast_provider_entry_from_provider_refs,
)
from process.ptg_parts.progress import _utcnow
from process.ptg_parts.provider_cache import _normalize_provider_ref, _provider_cache_get
from process.ptg_parts.row_helpers import _as_int_list, _as_list, _coerce_date
from process.ptg_parts.serving_rows import (
    _provider_group_member_rows,
    _provider_set_component_rows,
    _ptg2_compact_serving_rate_row,
    _ptg2_serving_rate_row,
)

_SERVING_ONLY_WORKER_KEY_FIELDS = {
    "serving_rows": "serving_rate_id",
    "serving_rate_compact_rows": "serving_rate_id",
    "provider_set_rows": "provider_set_hash",
    "price_set_rows": "price_set_hash",
    "provider_set_component_rows": ("provider_set_hash", "provider_group_hash"),
    "provider_group_member_rows": ("provider_group_hash", "npi"),
    "procedure_rows": "procedure_hash",
}


def _serving_only_price_payload(negotiated_prices: list[dict[str, Any]]) -> list[dict[str, Any]]:
    payload, _key = _serving_only_price_payload_and_key(negotiated_prices)
    return payload


def _normalize_serving_price_payload(prices: list[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized_prices: list[dict[str, Any]] = []
    for negotiated_price in prices:
        raw_rate = negotiated_price.get("negotiated_rate")
        rate_value = str(raw_rate) if isinstance(raw_rate, float) else raw_rate
        normalized_rate = normalize_money(rate_value)
        normalized_prices.append(
            {
                **negotiated_price,
                "negotiated_rate": money_number(normalized_rate),
            }
        )
    return normalized_prices


def _serving_only_key_list(value: Any) -> tuple[str, ...]:
    return tuple(sorted(str(item) for item in _as_list(value) if item is not None))


def _serving_only_key_value(value: Any) -> Any:
    if isinstance(value, (str, int, bool)) or value is None:
        return value
    if isinstance(value, float):
        return str(value)
    return canonical_json_dumps(value)


def _serving_only_hash_text(domain: str, *parts: Any) -> str:
    hasher = hashlib.sha256()
    hasher.update(domain.encode("utf-8"))
    for part in parts:
        text = "" if part is None else str(part)
        encoded = text.encode("utf-8")
        hasher.update(b"\x1f")
        hasher.update(str(len(encoded)).encode("ascii"))
        hasher.update(b":")
        hasher.update(encoded)
    return hasher.hexdigest()


def _serving_only_hash_int_sets(domain: str, *sets: set[int] | list[int] | tuple[int, ...]) -> str:
    hasher = hashlib.sha256()
    hasher.update(domain.encode("utf-8"))
    for values in sets:
        sorted_values = sorted(values)
        hasher.update(b"\x1e")
        hasher.update(str(len(sorted_values)).encode("ascii"))
        for value in sorted_values:
            hasher.update(b",")
            hasher.update(str(int(value)).encode("ascii"))
    return hasher.hexdigest()


def _serving_only_hash_price_key(price_key: tuple[tuple[Any, ...], ...]) -> str:
    hasher = hashlib.sha256()
    hasher.update(b"serving_price_set")
    for price_part in price_key:
        hasher.update(b"\x1e")
        hasher.update(str(len(price_part)).encode("ascii"))
        for value in price_part:
            if isinstance(value, tuple):
                encoded = "\x1d".join(str(item) for item in value).encode("utf-8")
            else:
                encoded = ("" if value is None else str(value)).encode("utf-8")
            hasher.update(b"\x1f")
            hasher.update(str(len(encoded)).encode("ascii"))
            hasher.update(b":")
            hasher.update(encoded)
    return hasher.hexdigest()


def _serving_only_rows_for_payload(
    payload: dict[str, Any],
    *,
    provider_map,
    plan_fields: dict[str, Any],
    snapshot_id: str,
    plan_month_id: str | None = None,
    source_trace_payload: list[dict[str, Any]],
    slim_serving_rows: bool = False,
    compact_serving: bool = False,
    source_trace_set_hash: str | None = None,
    include_price_set_rows: bool = False,
) -> list[dict[str, Any]] | dict[str, list[dict[str, Any]]]:
    """Build serving-only rows for one parsed PTG negotiated-rate payload."""
    procedure_payload = {
        "billing_code_type": payload.get("billing_code_type"),
        "billing_code": payload.get("billing_code"),
        "name": payload.get("name"),
        "description": payload.get("description"),
    }
    procedure_hash = semantic_hash(
        {
            "billing_code_type": payload.get("billing_code_type"),
            "billing_code_type_version": payload.get("billing_code_type_version"),
            "billing_code": payload.get("billing_code"),
            "name": payload.get("name"),
            "description": payload.get("description"),
        },
        domain="procedure",
    )
    procedure_row = {
        "procedure_hash": procedure_hash,
        "hash_prefix": hash_prefix(procedure_hash),
        "billing_code_type": payload.get("billing_code_type"),
        "billing_code_type_version": payload.get("billing_code_type_version"),
        "billing_code": payload.get("billing_code"),
        "name": payload.get("name"),
        "description": payload.get("description"),
        "canonical_payload": _canonicalize_for_json(
            {
                "billing_code_type": payload.get("billing_code_type"),
                "billing_code_type_version": payload.get("billing_code_type_version"),
                "billing_code": payload.get("billing_code"),
                "name": payload.get("name"),
                "description": payload.get("description"),
            }
        ),
        "created_at": _utcnow(),
    }
    item_price_groups: dict[str, dict[str, Any]] = {}
    for negotiated_rate in payload.get("negotiated_rates", []):
        provider_groups_inline = negotiated_rate.get("provider_groups") or []
        provider_refs = negotiated_rate.get("provider_references") or []
        combined_entry = None
        if provider_refs and not provider_groups_inline:
            combined_entry, _missing_refs = _fast_provider_entry_from_provider_refs(provider_map, provider_refs)
        else:
            groups_to_use: list[dict[str, Any]] = []
            for provider_ref in provider_refs:
                groups_to_use.extend(
                    _provider_cache_get(provider_map, _normalize_provider_ref(provider_ref))
                    or _provider_cache_get(provider_map, provider_ref)
                )
            if provider_groups_inline:
                inline_entry, _inline_row = _build_provider_set_entry(
                    file_id=0,
                    provider_group_ref=None,
                    provider_groups=provider_groups_inline,
                    network_names=negotiated_rate.get("network_name") or [],
                )
                if inline_entry is not None:
                    groups_to_use.append(inline_entry)
            combined_entry, _combined_row = _combine_provider_set_entries(
                file_id=0,
                entries=groups_to_use,
                network_names=negotiated_rate.get("network_name") or [],
            )
        if combined_entry is None:
            continue
        price_payload, price_key = _serving_only_price_payload_and_key(negotiated_rate.get("negotiated_prices", []))
        if not price_payload:
            continue
        item_group = item_price_groups.setdefault(
            price_key,
            {
                "prices": price_payload,
                "provider_entry_hashes": set(),
                "provider_group_hashes": set(),
                "provider_group_member_rows": [],
                "provider_count": 0,
                "network_names": set(),
            },
        )
        entry_hash = int(combined_entry["__hash__"])
        if entry_hash in item_group["provider_entry_hashes"]:
            continue
        item_group["provider_entry_hashes"].add(entry_hash)
        item_group["provider_group_hashes"].update(
            int(value) for value in combined_entry.get("provider_group_hashes") or [entry_hash]
        )
        item_group["provider_group_member_rows"].extend(_provider_group_member_rows(combined_entry))
        item_group["provider_count"] += int(
            combined_entry.get("provider_count") or len(_as_int_list(combined_entry.get("npi")))
        )
        item_group["network_names"].update(
            str(value) for value in _as_list(combined_entry.get("network_name")) if str(value or "").strip()
        )

    rows: list[dict[str, Any]] = []
    compact_rows: list[dict[str, Any]] = []
    provider_set_rows: list[dict[str, Any]] = []
    provider_set_component_rows: list[dict[str, Any]] = []
    provider_group_member_rows: list[dict[str, Any]] = []
    price_set_rows: list[dict[str, Any]] = []
    confidence_payload = {
        "network": PTG2_CONFIDENCE_TIC_RATE_NPI_TIN,
        "location": PTG2_CONFIDENCE_NPPES_PRACTICE_LOCATION,
        "acceptance_statement": ptg2_confidence_statement(PTG2_CONFIDENCE_TIC_RATE_NPI_TIN),
    }
    for price_key, item_group in item_price_groups.items():
        provider_group_hashes = sorted({int(value) for value in item_group["provider_group_hashes"]})
        provider_set_hash = _serving_only_hash_int_sets(
            "serving_provider_set",
            provider_group_hashes,
        )
        provider_count = int(item_group["provider_count"] or 0)
        provider_set_rows.append(
            {
                "provider_set_hash": provider_set_hash,
                "provider_count": provider_count,
                "created_at": _utcnow(),
            }
        )
        price_set_hash = _serving_only_hash_price_key(price_key)
        rate_pack_hash = _serving_only_hash_text(
            "serving_rate_pack",
            snapshot_id,
            procedure_hash,
            provider_set_hash,
            price_set_hash,
        )
        rows.append(
            _ptg2_serving_rate_row(
                snapshot_id=snapshot_id,
                plan_fields=plan_fields,
                procedure_payload=procedure_payload,
                rate_pack_row={
                    "rate_pack_hash": rate_pack_hash,
                    "provider_set_hash": provider_set_hash,
                    "price_set_hash": price_set_hash,
                },
                provider_set_hashes=[provider_set_hash],
                provider_count=provider_count,
                provider_set_count=1,
                prices=None if slim_serving_rows else item_group["prices"],
                source_trace=None if slim_serving_rows else source_trace_payload,
                source_trace_set_hash=source_trace_set_hash if slim_serving_rows else None,
                network_names=item_group["network_names"],
                confidence={} if slim_serving_rows else confidence_payload,
                confidence_code=PTG2_CONFIDENCE_TIC_RATE_NPI_TIN,
            )
        )
        if not compact_serving:
            provider_set_component_rows.extend(
                _provider_set_component_rows(provider_set_hash, provider_group_hashes)
            )
        provider_group_member_rows.extend(item_group["provider_group_member_rows"])
        if compact_serving and plan_month_id:
            compact_rows.append(
                _ptg2_compact_serving_rate_row(rows[-1], plan_month_id=plan_month_id, procedure_hash=procedure_hash)
            )
    if include_price_set_rows:
        return {
            "serving_rows": rows,
            "serving_rate_compact_rows": compact_rows,
            "provider_set_rows": provider_set_rows,
            "price_set_rows": price_set_rows,
            "provider_set_component_rows": provider_set_component_rows,
            "provider_group_member_rows": provider_group_member_rows,
            "procedure_rows": [procedure_row],
        }
    return rows



def _serving_only_price_payload_and_key(
    negotiated_prices: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], tuple[tuple[Any, ...], ...]]:
    payload: list[dict[str, Any]] = []
    key_parts: list[tuple[Any, ...]] = []
    for negotiated_price in negotiated_prices:
        expiration_date = _coerce_date(negotiated_price.get("expiration_date"))
        expiration_text = expiration_date.isoformat() if expiration_date else None
        raw_rate = negotiated_price.get("negotiated_rate")
        rate_value = str(raw_rate) if isinstance(raw_rate, float) else raw_rate
        normalized_rate = normalize_money(rate_value)
        service_codes = _as_list(negotiated_price.get("service_code"))
        billing_code_modifiers = _as_list(negotiated_price.get("billing_code_modifier"))
        payload.append(
            {
                "negotiated_type": negotiated_price.get("negotiated_type"),
                "negotiated_rate": money_number(normalized_rate),
                "expiration_date": expiration_text,
                "service_code": service_codes,
                "billing_class": negotiated_price.get("billing_class"),
                "setting": negotiated_price.get("setting"),
                "billing_code_modifier": billing_code_modifiers,
                "additional_information": negotiated_price.get("additional_information"),
            }
        )
        key_parts.append(
            (
                _serving_only_key_value(negotiated_price.get("negotiated_type")),
                normalized_rate,
                expiration_text,
                _serving_only_key_list(service_codes),
                _serving_only_key_value(negotiated_price.get("billing_class")),
                _serving_only_key_value(negotiated_price.get("setting")),
                _serving_only_key_list(billing_code_modifiers),
                _serving_only_key_value(negotiated_price.get("additional_information")),
            )
        )
    return payload, tuple(sorted(key_parts))


def _serving_only_merge_worker_result(
    dest: dict[str, list[dict[str, Any]]],
    src: dict[str, list[dict[str, Any]]] | None,
) -> None:
    if not src:
        return
    seen: dict[str, set[Any]] = dest.setdefault("__seen__", {})
    for key, id_field in _SERVING_ONLY_WORKER_KEY_FIELDS.items():
        rows = src.get(key) or []
        if not rows:
            continue
        key_seen = seen.setdefault(key, set())
        out = dest.setdefault(key, [])
        for row in rows:
            if isinstance(id_field, tuple):
                dedupe_id = tuple(row.get(part) for part in id_field)
            else:
                dedupe_id = row.get(id_field)
            if dedupe_id in key_seen:
                continue
            key_seen.add(dedupe_id)
            out.append(row)


def _serving_only_worker_process_chunk_to_files(
    payloads_or_raw: list[dict[str, Any] | bytes | bytearray],
    worker_process: Callable[[dict[str, Any] | bytes | bytearray], dict[str, list[dict[str, Any]]]],
) -> dict[str, Any]:
    temp_dir = Path(tempfile.mkdtemp(prefix="ptg2_worker_result_"))
    handles: dict[str, Any] = {}
    paths: dict[str, str] = {}
    counts: dict[str, int] = {}
    seen: dict[str, set[Any]] = {key: set() for key in _SERVING_ONLY_WORKER_KEY_FIELDS}
    try:
        for payload_or_raw in payloads_or_raw:
            result = worker_process(payload_or_raw)
            for key, id_field in _SERVING_ONLY_WORKER_KEY_FIELDS.items():
                rows = result.get(key) or []
                if not rows:
                    continue
                handle = handles.get(key)
                if handle is None:
                    path = temp_dir / f"{key}.pickle"
                    handle = path.open("wb")
                    handles[key] = handle
                    paths[key] = str(path)
                key_seen = seen[key]
                for row in rows:
                    if isinstance(id_field, tuple):
                        dedupe_id = tuple(row.get(part) for part in id_field)
                    else:
                        dedupe_id = row.get(id_field)
                    if dedupe_id in key_seen:
                        continue
                    key_seen.add(dedupe_id)
                    pickle.dump(row, handle, protocol=pickle.HIGHEST_PROTOCOL)
                    counts[key] = counts.get(key, 0) + 1
    finally:
        for handle in handles.values():
            handle.close()
    return {
        "__worker_result_files__": True,
        "temp_dir": str(temp_dir),
        "paths": paths,
        "counts": counts,
    }


def _worker_payload_size(payload_or_raw: dict[str, Any] | bytes | bytearray) -> int:
    if isinstance(payload_or_raw, (bytes, bytearray)):
        return len(payload_or_raw)
    try:
        return len(canonical_json_dumps(payload_or_raw).encode("utf-8"))
    except Exception:
        return 1024 * 1024


def _iter_worker_result_rows(path: str | Path):
    with Path(path).open("rb") as fp:
        while True:
            try:
                yield pickle.load(fp)
            except EOFError:
                break


def _ptg2_worker_capacity_wait_needed(
    *,
    pending_count: int,
    pending_input_bytes: int,
    next_batch_bytes: int,
    max_pending_batches: int,
    max_pending_bytes: int,
) -> bool:
    if pending_count >= max_pending_batches:
        return True
    if pending_count > 0 and pending_input_bytes + next_batch_bytes > max_pending_bytes:
        return True
    return False
