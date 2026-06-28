# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Audit direct provider-location evidence embedded in TiC artifacts."""

from __future__ import annotations

from collections import Counter
import json
from pathlib import Path
from typing import Any

try:
    import ijson
except ImportError:  # pragma: no cover - depends on runtime image contents.
    ijson = None

from process.ptg_parts.artifact_streams import open_json_artifact_stream

DEFAULT_JSON_FALLBACK_MAX_BYTES = 64 * 1024 * 1024
ADDRESS_FIELD_NAMES = {
    "address",
    "addresses",
    "address1",
    "address2",
    "addressline",
    "addressline1",
    "addressline2",
    "addressline3",
    "addr",
    "city",
    "cityname",
    "location",
    "locations",
    "postalcode",
    "practiceaddress",
    "state",
    "statename",
    "street",
    "streetaddress",
    "zipcode",
    "zip",
    "zip5",
}
PHONE_FIELD_NAMES = {
    "fax",
    "faxnumber",
    "phone",
    "phonenumber",
    "telephone",
    "telephonenumber",
}
NETWORK_NAME_FIELD_NAMES = {"networkname", "networknames"}
STREET_FIELD_NAMES = {
    "address1",
    "addressline",
    "addressline1",
    "practiceaddress",
    "street",
    "streetaddress",
}
CITY_FIELD_NAMES = {"city", "cityname"}
REGION_FIELD_NAMES = {"postalcode", "state", "statename", "zipcode", "zip", "zip5"}


def _normalized_field_name(value: Any) -> str:
    return "".join(ch for ch in str(value or "").lower() if ch.isalnum())


def _compact_scalar(value: Any) -> Any:
    if isinstance(value, str):
        text = value.strip()
        return text[:200]
    if isinstance(value, (int, float, bool)) or value is None:
        return value
    return str(value)[:200]


def _sample_value(value: Any, *, max_items: int = 6) -> Any:
    if isinstance(value, dict):
        sampled: dict[str, Any] = {}
        for index, (key, child) in enumerate(value.items()):
            if index >= max_items:
                sampled["..."] = f"{len(value) - max_items} more field(s)"
                break
            sampled[str(key)] = _sample_value(child, max_items=max_items)
        return sampled
    if isinstance(value, list):
        sampled_list = [_sample_value(item, max_items=max_items) for item in value[:max_items]]
        if len(value) > max_items:
            sampled_list.append(f"... {len(value) - max_items} more item(s)")
        return sampled_list
    return _compact_scalar(value)


def _nonempty_scalar(value: Any) -> bool:
    if isinstance(value, str):
        return bool(value.strip())
    return value not in (None, "", [], {})


def _has_displayable_address_value(value: Any) -> bool:
    """Return true when a raw object includes enough address data to display.

    This intentionally stays shape-based. It proves that the payer emitted a
    street-like field, or city plus state/ZIP, but not that serving has
    materialized or trusted that field yet.
    """

    if isinstance(value, dict):
        has_city = False
        has_region = False
        for key, child in value.items():
            normalized_key = _normalized_field_name(key)
            if normalized_key in STREET_FIELD_NAMES and _nonempty_scalar(child):
                return True
            if normalized_key in CITY_FIELD_NAMES and _nonempty_scalar(child):
                has_city = True
            if normalized_key in REGION_FIELD_NAMES and _nonempty_scalar(child):
                has_region = True
            if _has_displayable_address_value(child):
                return True
        return has_city and has_region
    if isinstance(value, list):
        return any(_has_displayable_address_value(child) for child in value)
    return False


def _collect_matching_fields(
    value: Any,
    *,
    prefix: str = "",
    address_fields: dict[str, Any] | None = None,
    phone_fields: dict[str, Any] | None = None,
    network_name_fields: dict[str, Any] | None = None,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    address_fields = address_fields if address_fields is not None else {}
    phone_fields = phone_fields if phone_fields is not None else {}
    network_name_fields = network_name_fields if network_name_fields is not None else {}
    if isinstance(value, dict):
        for key, child in value.items():
            normalized_key = _normalized_field_name(key)
            path = f"{prefix}.{key}" if prefix else str(key)
            if normalized_key in ADDRESS_FIELD_NAMES:
                address_fields[path] = _sample_value(child)
            if normalized_key in PHONE_FIELD_NAMES:
                phone_fields[path] = _sample_value(child)
            if normalized_key in NETWORK_NAME_FIELD_NAMES:
                network_name_fields[path] = _sample_value(child)
            _collect_matching_fields(
                child,
                prefix=path,
                address_fields=address_fields,
                phone_fields=phone_fields,
                network_name_fields=network_name_fields,
            )
    elif isinstance(value, list):
        for index, child in enumerate(value):
            _collect_matching_fields(
                child,
                prefix=f"{prefix}[{index}]",
                address_fields=address_fields,
                phone_fields=phone_fields,
                network_name_fields=network_name_fields,
            )
    return address_fields, phone_fields, network_name_fields


def _add_field_counts(counter: Counter[str], fields: dict[str, Any]) -> None:
    for path in fields:
        counter[path] += 1


def _append_sample(samples: list[dict[str, Any]], sample: dict[str, Any], max_samples: int) -> None:
    if len(samples) < max_samples:
        samples.append(sample)


def _load_small_artifact_without_ijson(path: str | Path, max_json_fallback_bytes: int) -> dict[str, Any]:
    byte_count = Path(path).stat().st_size
    if byte_count > max_json_fallback_bytes:
        raise RuntimeError(
            "ijson is required to audit large TiC artifacts without loading them into memory "
            f"(artifact_bytes={byte_count}, fallback_limit={max_json_fallback_bytes})"
        )
    with open_json_artifact_stream(path) as fp:
        payload = json.load(fp)
    return payload if isinstance(payload, dict) else {}


def _iter_provider_references(path: str | Path, max_json_fallback_bytes: int):
    if ijson is not None:
        with open_json_artifact_stream(path) as fp:
            for ref in ijson.items(fp, "provider_references.item"):
                if isinstance(ref, dict):
                    yield ref
        return
    data = _load_small_artifact_without_ijson(path, max_json_fallback_bytes)
    for ref in data.get("provider_references") or []:
        if isinstance(ref, dict):
            yield ref


def _iter_inline_provider_groups(path: str | Path, max_json_fallback_bytes: int):
    if ijson is not None:
        with open_json_artifact_stream(path) as fp:
            for group in ijson.items(fp, "in_network.item.negotiated_rates.item.provider_groups.item"):
                if isinstance(group, dict):
                    yield group
        return
    data = _load_small_artifact_without_ijson(path, max_json_fallback_bytes)
    for in_network_item in data.get("in_network") or []:
        for rate in (in_network_item or {}).get("negotiated_rates") or []:
            for group in (rate or {}).get("provider_groups") or []:
                if isinstance(group, dict):
                    yield group


def _audit_provider_reference(
    ref: dict[str, Any],
    *,
    summary: dict[str, Any],
    address_paths: Counter[str],
    phone_paths: Counter[str],
    network_paths: Counter[str],
    samples: list[dict[str, Any]],
    max_samples: int,
) -> None:
    summary["provider_references"] += 1
    ref_id = ref.get("provider_group_id") or ref.get("provider_group_ref")
    ref_fields = dict(ref)
    ref_fields.pop("provider_groups", None)
    ref_addresses, ref_phones, ref_network_names = _collect_matching_fields(ref_fields)
    if ref_addresses or ref_phones:
        summary["provider_references_with_direct_location_fields"] += 1
        displayable_address = _has_displayable_address_value(ref_fields)
        if displayable_address:
            summary["provider_references_with_displayable_location_fields"] += 1
        _add_field_counts(address_paths, ref_addresses)
        _add_field_counts(phone_paths, ref_phones)
        _append_sample(
            samples,
            {
                "scope": "provider_reference",
                "provider_group_id": ref_id,
                "displayable_address_present": displayable_address,
                "address_fields": ref_addresses,
                "phone_fields": ref_phones,
            },
            max_samples,
        )
    if ref_network_names:
        summary["provider_references_with_network_names"] += 1
        _add_field_counts(network_paths, ref_network_names)

    for group in ref.get("provider_groups") or []:
        if not isinstance(group, dict):
            continue
        summary["provider_groups"] += 1
        group_addresses, group_phones, group_network_names = _collect_matching_fields(group)
        if group_addresses or group_phones:
            summary["provider_groups_with_direct_location_fields"] += 1
            displayable_address = _has_displayable_address_value(group)
            if displayable_address:
                summary["provider_groups_with_displayable_location_fields"] += 1
            _add_field_counts(address_paths, group_addresses)
            _add_field_counts(phone_paths, group_phones)
            _append_sample(
                samples,
                {
                    "scope": "provider_reference.provider_groups",
                    "provider_group_id": ref_id,
                    "tin": _sample_value(group.get("tin")),
                    "npi_count": len(group.get("npi") or []) if isinstance(group.get("npi"), list) else None,
                    "displayable_address_present": displayable_address,
                    "address_fields": group_addresses,
                    "phone_fields": group_phones,
                },
                max_samples,
            )
        if group_network_names:
            _add_field_counts(network_paths, group_network_names)


def _audit_inline_provider_group(
    group: dict[str, Any],
    *,
    summary: dict[str, Any],
    address_paths: Counter[str],
    phone_paths: Counter[str],
    network_paths: Counter[str],
    samples: list[dict[str, Any]],
    max_samples: int,
) -> None:
    summary["inline_provider_groups"] += 1
    address_fields, phone_fields, network_name_fields = _collect_matching_fields(group)
    if address_fields or phone_fields:
        summary["inline_provider_groups_with_direct_location_fields"] += 1
        displayable_address = _has_displayable_address_value(group)
        if displayable_address:
            summary["inline_provider_groups_with_displayable_location_fields"] += 1
        _add_field_counts(address_paths, address_fields)
        _add_field_counts(phone_paths, phone_fields)
        _append_sample(
            samples,
            {
                "scope": "in_network.negotiated_rates.provider_groups",
                "tin": _sample_value(group.get("tin")),
                "npi_count": len(group.get("npi") or []) if isinstance(group.get("npi"), list) else None,
                "displayable_address_present": displayable_address,
                "address_fields": address_fields,
                "phone_fields": phone_fields,
            },
            max_samples,
        )
    if network_name_fields:
        _add_field_counts(network_paths, network_name_fields)


def audit_tic_provider_location_evidence(
    path: str | Path,
    *,
    max_samples: int = 5,
    scan_inline_provider_groups: bool = True,
    max_json_fallback_bytes: int = DEFAULT_JSON_FALLBACK_MAX_BYTES,
) -> dict[str, Any]:
    """Summarize direct address/phone-like fields in a TiC JSON artifact.

    This audits raw source shape only. A direct field match is evidence that a
    payer emitted location-like data, not proof that the address is a service
    location for a negotiated rate.
    """

    artifact_path = str(Path(path))
    summary: dict[str, Any] = {
        "path": artifact_path,
        "provider_references": 0,
        "provider_references_with_direct_location_fields": 0,
        "provider_references_with_displayable_location_fields": 0,
        "provider_references_with_network_names": 0,
        "provider_groups": 0,
        "provider_groups_with_direct_location_fields": 0,
        "provider_groups_with_displayable_location_fields": 0,
        "inline_provider_groups": 0,
        "inline_provider_groups_with_direct_location_fields": 0,
        "inline_provider_groups_with_displayable_location_fields": 0,
    }
    address_paths: Counter[str] = Counter()
    phone_paths: Counter[str] = Counter()
    network_paths: Counter[str] = Counter()
    samples: list[dict[str, Any]] = []

    for ref in _iter_provider_references(path, max_json_fallback_bytes):
        _audit_provider_reference(
            ref,
            summary=summary,
            address_paths=address_paths,
            phone_paths=phone_paths,
            network_paths=network_paths,
            samples=samples,
            max_samples=max_samples,
        )

    if scan_inline_provider_groups:
        for group in _iter_inline_provider_groups(path, max_json_fallback_bytes):
            _audit_inline_provider_group(
                group,
                summary=summary,
                address_paths=address_paths,
                phone_paths=phone_paths,
                network_paths=network_paths,
                samples=samples,
                max_samples=max_samples,
            )

    summary["direct_location_fields_present"] = bool(
        summary["provider_references_with_direct_location_fields"]
        or summary["provider_groups_with_direct_location_fields"]
        or summary["inline_provider_groups_with_direct_location_fields"]
    )
    summary["direct_displayable_location_fields_present"] = bool(
        summary["provider_references_with_displayable_location_fields"]
        or summary["provider_groups_with_displayable_location_fields"]
        or summary["inline_provider_groups_with_displayable_location_fields"]
    )
    summary["direct_phone_fields_present"] = bool(phone_paths)
    summary["address_field_paths"] = dict(sorted(address_paths.items()))
    summary["phone_field_paths"] = dict(sorted(phone_paths.items()))
    summary["network_name_field_paths"] = dict(sorted(network_paths.items()))
    summary["samples"] = samples
    return summary
