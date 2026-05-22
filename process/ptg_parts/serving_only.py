# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Pure helpers for the PTG2 Python serving-only fallback path."""

from __future__ import annotations

import hashlib
import pickle
from pathlib import Path
from typing import Any

from process.ptg_parts.canonical import canonical_json_dumps, money_number, normalize_money
from process.ptg_parts.row_helpers import _as_list, _coerce_date

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
    seen: dict[str, set[Any]] = dest.setdefault("__seen__", {})  # type: ignore[assignment]
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
