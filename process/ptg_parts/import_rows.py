# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""PTG2 import row builders and provider-set composition helpers."""

from __future__ import annotations

import datetime
from typing import Any

from process.ptg_parts.canonical import (
    _canonical_sort_key,
    _canonicalize_for_json,
    canonicalize_url,
    hash_prefix,
    normalize_money,
    semantic_hash,
)
from process.ptg_parts.config import (
    PTG2_FAST_PROVIDER_UNION_ENV,
    PTG2_PROVIDER_SET_INLINE_NPI_LIMIT_ENV,
    _env_bool,
    _env_int,
)
from process.ptg_parts.domain import PTG2_DOMAIN_IN_NETWORK, PTG2PriceAtomEvent, PTG2SourceVersion
from process.ptg_parts.provider_cache import _normalize_provider_ref, _provider_cache_get
from process.ptg_parts.progress import _utcnow
from process.ptg_parts.row_helpers import (
    _as_int_list,
    _as_list,
    _coerce_date,
    _make_checksum,
    _normalize_code_component,
    _normalize_tin_type,
    _normalize_tin_value,
    _normalized_npi_list,
    _provider_group_hash_prefix,
    _provider_group_identity_hash,
)
from process.ptg_parts.values import build_price_atom, build_source_trace_set


def _normalize_import_id(import_id: str | None) -> str:
    if not import_id:
        return datetime.date.today().strftime("%Y%m%d")
    normalized = "".join(ch if ch.isalnum() else "_" for ch in str(import_id))
    if not normalized:
        return datetime.date.today().strftime("%Y%m%d")
    if len(normalized) > 34:
        suffix = hash_prefix(semantic_hash(normalized, domain="import_id"), 8)
        normalized = f"{normalized[:25]}_{suffix}"
    return normalized


def _ptg2_provider_group_rows(
    *,
    provider_groups: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for group in provider_groups:
        tin_info = group.get("tin") or {}
        normalized_npi = _normalized_npi_list(group.get("npi"))
        provider_group_hash = _provider_group_identity_hash(tin_info, normalized_npi)
        tin_type = _normalize_tin_type(tin_info.get("type"))
        tin_value = _normalize_tin_value(tin_info.get("value"))
        payload = {
            "tin_type": tin_type,
            "tin_value": tin_value,
            "npi": normalized_npi,
        }
        rows.append(
            {
                "provider_group_hash": provider_group_hash,
                "hash_prefix": _provider_group_hash_prefix(provider_group_hash),
                "provider_count": len(normalized_npi),
                "npi": normalized_npi,
                "tin_type": tin_type or None,
                "tin_value": tin_value or None,
                "tin_business_name": tin_info.get("business_name"),
                "canonical_payload": _canonicalize_for_json(payload),
                "created_at": _utcnow(),
            }
        )
    return rows


def _build_provider_set_entry(
    *,
    file_id: int,
    provider_group_ref: Any,
    provider_groups: list[dict[str, Any]],
    network_names: list[str] | None = None,
) -> tuple[dict[str, Any], dict[str, Any]] | tuple[None, None]:
    """Build one normalized provider-set entry and metadata row."""
    group_payloads: list[dict[str, Any]] = []
    union_npis: set[int] = set()
    tin_values: set[tuple[str, str]] = set()
    business_names: list[str] = []
    for group in provider_groups:
        tin_info = group.get("tin") or {}
        normalized_npi = _normalized_npi_list(group.get("npi"))
        provider_group_hash = _provider_group_identity_hash(tin_info, normalized_npi)
        tin_type = _normalize_tin_type(tin_info.get("type"))
        tin_value = _normalize_tin_value(tin_info.get("value"))
        if tin_type or tin_value:
            tin_values.add((tin_type, tin_value))
        if tin_info.get("business_name"):
            business_names.append(str(tin_info.get("business_name")))
        union_npis.update(normalized_npi)
        group_payloads.append(
            {
                "provider_group_hash": provider_group_hash,
                "tin_type": tin_type,
                "tin_value": tin_value,
                "npi": normalized_npi,
            }
        )
    if not group_payloads:
        return None, None
    group_payloads = sorted(group_payloads, key=_canonical_sort_key)
    provider_hash = (
        group_payloads[0]["provider_group_hash"]
        if len(group_payloads) == 1
        else _make_checksum("provider_set", group_payloads)
    )
    sorted_tins = sorted(tin_values)
    single_tin = sorted_tins[0] if len(sorted_tins) == 1 else None
    tin_type = single_tin[0] if single_tin else ("set" if sorted_tins else None)
    tin_value = single_tin[1] if single_tin else None
    tin_business_name = business_names[0] if len(set(business_names)) == 1 else None
    npi_values = sorted(union_npis)
    provider_entry = {
        "provider_group_id": provider_group_ref,
        "network_name": network_names or [],
        "__hash__": provider_hash,
        "npi": npi_values,
        "provider_count": len(npi_values),
        "tin": {"type": tin_type, "value": tin_value, "business_name": tin_business_name},
        "provider_group_hashes": [payload["provider_group_hash"] for payload in group_payloads],
        "provider_group_count": len(group_payloads),
    }
    row = {
        "provider_group_hash": provider_hash,
        "provider_group_ref": provider_group_ref,
        "file_id": file_id,
        "network_names": network_names or [],
        "tin_type": tin_type,
        "tin_value": tin_value,
        "tin_business_name": tin_business_name,
        "npi": npi_values,
    }
    return provider_entry, row


def _combine_provider_set_entries(
    *,
    file_id: int,
    entries: list[dict[str, Any]],
    network_names: list[str] | None = None,
) -> tuple[dict[str, Any], dict[str, Any]] | tuple[None, None]:
    """Combine compatible provider-set entries into one row."""
    clean_entries = [entry for entry in entries if entry and entry.get("__hash__")]
    if not clean_entries:
        return None, None
    if len(clean_entries) == 1:
        entry = dict(clean_entries[0])
        entry["provider_count"] = int(entry.get("provider_count") or len(_as_int_list(entry.get("npi"))))
        row = {
            "provider_group_hash": entry["__hash__"],
            "provider_group_ref": entry.get("provider_group_id"),
            "file_id": file_id,
            "network_names": entry.get("network_name") or network_names or [],
            "tin_type": (entry.get("tin") or {}).get("type"),
            "tin_value": (entry.get("tin") or {}).get("value"),
            "tin_business_name": (entry.get("tin") or {}).get("business_name"),
            "npi": _normalized_npi_list(entry.get("npi")),
        }
        return entry, row
    entry_hashes = sorted({int(entry["__hash__"]) for entry in clean_entries})
    provider_hash = _make_checksum("provider_rate_provider_set", entry_hashes)
    fast_provider_union = _env_bool(PTG2_FAST_PROVIDER_UNION_ENV, False)
    npi_values: set[int] = set()
    provider_count = 0
    provider_group_hashes: set[int] = set()
    merged_network_names: set[str] = set(network_names or [])
    for entry in clean_entries:
        if fast_provider_union:
            provider_count += int(entry.get("provider_count") or len(_as_int_list(entry.get("npi"))))
        else:
            npi_values.update(_as_int_list(entry.get("npi")))
        provider_group_hashes.update(int(value) for value in entry.get("provider_group_hashes") or [entry["__hash__"]])
        merged_network_names.update(str(value) for value in _as_list(entry.get("network_name")) if value)
    sorted_npis = [] if fast_provider_union else sorted(npi_values)
    if not fast_provider_union:
        provider_count = len(sorted_npis)
    provider_entry = {
        "provider_group_id": None,
        "network_name": sorted(merged_network_names),
        "__hash__": provider_hash,
        "npi": sorted_npis,
        "provider_count": provider_count,
        "provider_count_mode": "summed_provider_groups" if fast_provider_union else "exact_npi_union",
        "tin": {"type": "set", "value": None, "business_name": None},
        "provider_group_hashes": sorted(provider_group_hashes),
        "provider_group_count": len(provider_group_hashes),
    }
    row = {
        "provider_group_hash": provider_hash,
        "provider_group_ref": None,
        "file_id": file_id,
        "network_names": sorted(merged_network_names),
        "tin_type": "set",
        "tin_value": None,
        "tin_business_name": None,
        "npi": sorted_npis,
    }
    return provider_entry, row


def _fast_provider_entry_from_parts(
    *,
    entry_hashes: set[int],
    provider_group_hashes: set[int],
    provider_count: int,
    network_names: set[str] | None = None,
) -> dict[str, Any] | None:
    if not entry_hashes:
        return None
    sorted_entry_hashes = sorted(entry_hashes)
    provider_hash = (
        sorted_entry_hashes[0]
        if len(sorted_entry_hashes) == 1
        else _make_checksum("provider_rate_provider_set", sorted_entry_hashes)
    )
    return {
        "provider_group_id": None,
        "network_name": sorted(network_names or []),
        "__hash__": provider_hash,
        "npi": [],
        "provider_count": int(provider_count or 0),
        "provider_count_mode": "summed_provider_groups",
        "tin": {"type": "set", "value": None, "business_name": None},
        "provider_group_hashes": sorted(provider_group_hashes),
        "provider_group_count": len(provider_group_hashes),
    }


def _fast_provider_entry_from_provider_refs(
    provider_map,
    provider_refs: list[Any],
) -> tuple[dict[str, Any] | None, list[Any]]:
    entry_hashes: set[int] = set()
    provider_group_hashes: set[int] = set()
    network_names: set[str] = set()
    provider_count = 0
    missing_refs: list[Any] = []
    for provider_ref in provider_refs:
        provider_key = _normalize_provider_ref(provider_ref)
        groups = _provider_cache_get(provider_map, provider_key) or _provider_cache_get(provider_map, provider_ref)
        if not groups:
            missing_refs.append(provider_ref)
            continue
        for entry in groups:
            if not entry or "__hash__" not in entry:
                continue
            entry_hash = int(entry["__hash__"])
            if entry_hash in entry_hashes:
                continue
            entry_hashes.add(entry_hash)
            provider_group_hashes.update(int(value) for value in entry.get("provider_group_hashes") or [entry_hash])
            provider_count += int(entry.get("provider_count") or len(_as_int_list(entry.get("npi"))))
            network_names.update(str(value) for value in _as_list(entry.get("network_name")) if value)
    return (
        _fast_provider_entry_from_parts(
            entry_hashes=entry_hashes,
            provider_group_hashes=provider_group_hashes,
            provider_count=provider_count,
            network_names=network_names,
        ),
        missing_refs,
    )


def _ptg2_provider_set_row(provider_entry: dict[str, Any]) -> dict[str, Any]:
    tin = provider_entry.get("tin") or {}
    npi_values = _normalized_npi_list(provider_entry.get("npi"))
    provider_count = int(provider_entry.get("provider_count") or len(npi_values))
    provider_group_hashes = sorted({int(value) for value in provider_entry.get("provider_group_hashes") or []})
    provider_group_count = provider_entry.get("provider_group_count") or len(provider_group_hashes)
    identity_payload = {
        "tin_type": _normalize_tin_type(tin.get("type")),
        "tin_value": _normalize_tin_value(tin.get("value")),
        "provider_group_hashes": provider_group_hashes,
        "provider_group_count": provider_group_count,
    }
    if not provider_group_hashes:
        identity_payload["npi"] = npi_values
    provider_set_hash = semantic_hash(identity_payload, domain="provider_set")
    inline_limit = max(_env_int(PTG2_PROVIDER_SET_INLINE_NPI_LIMIT_ENV, 0), 0)
    inline_npi = not provider_group_hashes or (bool(npi_values) and len(npi_values) <= inline_limit)
    canonical_payload = {
        **identity_payload,
        "provider_count": provider_count,
        "provider_count_mode": provider_entry.get("provider_count_mode") or "exact_npi_union",
        "npi_inline": inline_npi,
    }
    if inline_npi:
        canonical_payload["npi"] = npi_values
    return {
        "provider_set_hash": provider_set_hash,
        "hash_prefix": hash_prefix(provider_set_hash),
        "provider_count": provider_count,
        "npi": npi_values if inline_npi else None,
        "tin_type": identity_payload["tin_type"] or None,
        "tin_value": identity_payload["tin_value"] or None,
        "canonical_payload": _canonicalize_for_json(canonical_payload),
        "created_at": _utcnow(),
    }


def _ptg2_procedure_row(in_item: dict[str, Any]) -> dict[str, Any]:
    identity_payload = {
        "billing_code_type": _normalize_code_component(in_item.get("billing_code_type")),
        "billing_code_type_version": _normalize_code_component(in_item.get("billing_code_type_version")),
        "billing_code": _normalize_code_component(in_item.get("billing_code")),
        "negotiation_arrangement": _normalize_code_component(in_item.get("negotiation_arrangement")),
    }
    procedure_hash = semantic_hash(identity_payload, domain="procedure")
    return {
        "procedure_hash": procedure_hash,
        "billing_code_type": in_item.get("billing_code_type"),
        "billing_code_type_version": in_item.get("billing_code_type_version"),
        "billing_code": in_item.get("billing_code"),
        "name": in_item.get("name"),
        "description": in_item.get("description"),
        "created_at": _utcnow(),
    }


def _ptg2_price_atom_row(negotiated_price: dict[str, Any]) -> dict[str, Any]:
    raw_rate = negotiated_price.get("negotiated_rate")
    rate_value = str(raw_rate) if isinstance(raw_rate, float) else raw_rate
    payload = PTG2PriceAtomEvent(
        negotiated_type=negotiated_price.get("negotiated_type"),
        negotiated_rate=rate_value,
        expiration_date=_coerce_date(negotiated_price.get("expiration_date")),
        service_code=tuple(_as_list(negotiated_price.get("service_code"))),
        billing_class=negotiated_price.get("billing_class"),
        setting=negotiated_price.get("setting"),
        billing_code_modifier=tuple(_as_list(negotiated_price.get("billing_code_modifier"))),
        additional_information=negotiated_price.get("additional_information"),
    )
    built = build_price_atom(payload)
    return {
        "price_atom_hash": built["price_atom_hash"],
        "negotiated_type": negotiated_price.get("negotiated_type"),
        "negotiated_rate": normalize_money(rate_value) if rate_value is not None else None,
        "expiration_date": _coerce_date(negotiated_price.get("expiration_date")),
        "service_code": _as_list(negotiated_price.get("service_code")),
        "billing_class": negotiated_price.get("billing_class"),
        "setting": negotiated_price.get("setting"),
        "billing_code_modifier": _as_list(negotiated_price.get("billing_code_modifier")),
        "additional_information": negotiated_price.get("additional_information"),
        "created_at": _utcnow(),
    }


def _ptg2_source_trace_rows(source_version: PTG2SourceVersion | None, source_url: str) -> tuple[dict[str, Any], dict[str, Any]]:
    payload = {
        "source_file_version_id": source_version.source_file_version_id if source_version else None,
        "original_url": source_version.original_url if source_version else source_url,
        "canonical_url": source_version.canonical_url if source_version else canonicalize_url(source_url),
        "json_pointer": None,
        "statement": "Published negotiated rate from Transparency in Coverage source file.",
    }
    source_trace_hash = semantic_hash(payload, domain="source_trace")
    source_trace_row = {
        "source_trace_hash": source_trace_hash,
        "source_file_version_id": payload["source_file_version_id"],
        "original_url": payload["original_url"],
        "canonical_url": payload["canonical_url"],
        "json_pointer": payload["json_pointer"],
        "line_number": None,
        "created_at": _utcnow(),
    }
    source_trace_set = build_source_trace_set([source_trace_hash])
    source_trace_set_row = {
        **source_trace_set,
        "created_at": _utcnow(),
    }
    return source_trace_row, source_trace_set_row


def _ptg2_context_row(
    plan_fields: dict[str, Any],
    import_month: datetime.date,
    source_version: PTG2SourceVersion | None,
) -> dict[str, Any]:
    payload = {
        "domain": PTG2_DOMAIN_IN_NETWORK,
        "plan": plan_fields,
        "import_month": import_month.isoformat(),
        "source_file_version_id": source_version.source_file_version_id if source_version else None,
    }
    context_hash = semantic_hash(payload, domain="rate_set_context")
    return {
        "context_hash": context_hash,
        "hash_prefix": hash_prefix(context_hash),
        "domain": PTG2_DOMAIN_IN_NETWORK,
        "canonical_payload": _canonicalize_for_json(payload),
        "created_at": _utcnow(),
    }


def _ptg2_plan_rows(
    plan_fields: dict[str, Any],
    snapshot_id: str,
    import_month: datetime.date,
) -> tuple[dict[str, Any], list[dict[str, Any]], dict[str, Any]]:
    payload = {
        "plan_id": plan_fields.get("plan_id"),
        "plan_id_type": plan_fields.get("plan_id_type"),
        "plan_name": plan_fields.get("plan_name"),
        "plan_market_type": plan_fields.get("plan_market_type"),
        "issuer_name": plan_fields.get("issuer_name"),
        "plan_sponsor_name": plan_fields.get("plan_sponsor_name"),
    }
    plan_hash = semantic_hash(payload, domain="plan")
    plan_row = {
        "plan_hash": plan_hash,
        "hash_prefix": hash_prefix(plan_hash),
        **payload,
        "canonical_payload": _canonicalize_for_json(payload),
        "created_at": _utcnow(),
    }
    alias_rows: list[dict[str, Any]] = []
    for alias_type, alias_value in (("plan_id", payload.get("plan_id")), ("plan_name", payload.get("plan_name"))):
        if not alias_value:
            continue
        alias_payload = {"plan_hash": plan_hash, "alias_type": alias_type, "alias_value": str(alias_value)}
        alias_hash = semantic_hash(alias_payload, domain="plan_alias")
        alias_rows.append(
            {
                "alias_hash": alias_hash,
                "plan_hash": plan_hash,
                "alias_type": alias_type,
                "alias_value": str(alias_value),
                "created_at": _utcnow(),
            }
        )
    plan_month_payload = {"snapshot_id": snapshot_id, "plan_hash": plan_hash, "import_month": import_month.isoformat()}
    plan_month_id = semantic_hash(plan_month_payload, domain="plan_month")[:32]
    plan_month_row = {
        "plan_month_id": plan_month_id,
        "snapshot_id": snapshot_id,
        "plan_hash": plan_hash,
        "import_month": import_month,
        "created_at": _utcnow(),
    }
    return plan_row, alias_rows, plan_month_row
