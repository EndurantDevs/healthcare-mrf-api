# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""PTG2 manifest-backed snapshot reader and bounded serving search."""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping
from urllib.parse import unquote, urlsplit

from process.ptg_parts.ptg2_manifest_artifacts import (
    PTG2_MANIFEST_MEMBERSHIP_FORMAT,
    PTG2ManifestArtifactError,
    read_global_sidecar_entries,
    read_manifest,
)

from api.code_systems import canonical_catalog_code
from api.ptg2_code_filters import _normalize_code, _normalize_code_system
from api.ptg2_response import _coerce_json_payload, _price_response_fields, _request_bool
from api.ptg2_serving_utils import _normalize_zip5
from api.ptg2_types import PTG2ServingTables

PTG2_MANIFEST_SNAPSHOT_ARTIFACT_TYPES = {
    "ptg2_manifest_snapshot",
    "ptg2_manifest_snapshot",
    "snapshot_index",
}
PTG2_MANIFEST_SERVING_ROW_KINDS = {"skinny_serving_rows", "serving_rows"}
PTG2_MANIFEST_PRICE_SET_KINDS = {"price_sets", "prices"}
PTG2_MANIFEST_PROVIDER_KINDS = {"providers", "provider_entries"}
PTG2_MANIFEST_PRICE_ATOM_KINDS = {"price_atoms", "price_entries"}
PTG2_MANIFEST_PROVIDER_SET_MEMBER_KINDS = {"provider_set_members", "provider_set_membership", "provider_members"}
PTG2_MANIFEST_PRICE_SET_MEMBER_KINDS = {"price_set_members", "price_set_membership", "price_members"}
PTG2_MANIFEST_SOURCE_TRACE_KINDS = {"source_trace_sets", "source_traces"}
PTG2_MANIFEST_ROW_LIMIT_ENV = "HLTHPRT_PTG2_MANIFEST_ROW_LIMIT"
PTG2_MANIFEST_BYTE_LIMIT_ENV = "HLTHPRT_PTG2_MANIFEST_BYTE_LIMIT"
PTG_NO_DISPLAY_ADDRESS_FIELDS = {
    "address",
    "formatted_address",
    "address_key",
    "city",
    "state",
    "zip5",
    "zip_code",
    "postal_code",
    "lat",
    "long",
    "latitude",
    "longitude",
    "distance",
    "distance_miles",
    "zip_match_type",
    "coordinates",
    "google_maps_url",
    "google_map_url",
    "maps_url",
    "phone",
    "telephone",
    "telephone_number",
    "phone_number",
    "fax",
    "fax_number",
    "location_hash",
    "location_source",
    "location_confidence_code",
    "address_sources",
    "address_precision",
    "source_count",
    "multi_source_confirmed",
    "source_mask",
    "address_source_mask",
}
PTG_NO_DISPLAY_VERIFICATION_FIELDS = {
    "location_source",
    "location_confidence_code",
    "address_precision",
    "address_sources",
    "source_count",
    "multi_source_confirmed",
    "source_mask",
    "address_source_mask",
    "provider_directory_source_id",
    "provider_directory_location_resource_id",
    "provider_directory_location_name",
    "provider_directory_plan_context_matched",
    "provider_directory_network_name_matched",
    "provider_directory_network_context_present",
    "provider_directory_network_refs",
    "provider_directory_network_names",
    "provider_directory_network_matches",
    "provider_directory_insurance_plan_refs",
    "provider_directory_insurance_plan_matches",
    "provider_directory_match_type",
    "address_verification_evidence",
}
PTG_DIRECT_PAYER_LOCATION_RECORD_KEYS = {
    "source_record_id",
    "source_record_key",
    "source_provider_reference_id",
    "provider_reference_id",
    "provider_reference",
    "provider_group_id",
    "provider_group_global_id_128",
    "provider_group_hash",
    "provider_group_location_hash",
    "raw_provider_location_key",
    "json_pointer",
}


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return int(str(raw).strip())
    except ValueError:
        return default


def _coerce_str_list(value: Any) -> list[str]:
    if value in (None, ""):
        return []
    payload = _coerce_json_payload(value, None) if isinstance(value, str) else _coerce_json_payload(value, [])
    if payload in (None, ""):
        payload = value
    if isinstance(payload, str):
        payload = [payload]
    if not isinstance(payload, list):
        return []
    values: list[str] = []
    for item in payload:
        text = str(item or "").strip()
        if text and text not in values:
            values.append(text)
    return values


def _optional_bool(value: Any) -> bool | None:
    if value in (None, ""):
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return bool(value)
    normalized = str(value or "").strip().lower()
    if normalized in {"1", "true", "t", "yes", "y"}:
        return True
    if normalized in {"0", "false", "f", "no", "n"}:
        return False
    return None


def _nonempty_value(source: Mapping[str, Any], *keys: str) -> bool:
    return any(source.get(key) not in (None, "", [], {}) for key in keys)


def _has_displayable_address(source: Mapping[str, Any]) -> bool:
    if _nonempty_value(source, "first_line", "address_line_1", "street", "street_address"):
        return True
    has_city = _nonempty_value(source, "city", "city_name")
    has_region = _nonempty_value(source, "state", "state_name", "postal_code", "zip5")
    if has_city and has_region:
        return True
    nested_address = source.get("address")
    return isinstance(nested_address, Mapping) and _has_displayable_address(nested_address)


def _normalized_markers(*values: Any) -> set[str]:
    return {
        str(value or "").strip().lower().replace("-", "_")
        for value in values
        if str(value or "").strip()
    }


def _manifest_plan_context_matched(address_payload: Mapping[str, Any]) -> bool:
    if address_payload.get("provider_directory_plan_context_matched") is True:
        return True
    evidence = _coerce_json_payload(address_payload.get("address_verification_evidence"), {})
    if isinstance(evidence, dict):
        return str(evidence.get("matched_on") or "").strip().lower().endswith("_plan")
    return False


def _canonical_network_name(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value or "").lower())


def _manifest_network_name_matches(
    item: Mapping[str, Any],
    address_payload: Mapping[str, Any],
) -> list[dict[str, Any]]:
    ptg_names = _coerce_str_list(item.get("network_names") or address_payload.get("network_names"))
    ptg_by_key = {
        _canonical_network_name(value): value
        for value in ptg_names
        if _canonical_network_name(value)
    }
    if not ptg_by_key:
        return []
    raw_directory_networks = _coerce_json_payload(address_payload.get("provider_directory_network_matches"), [])
    directory_networks = list(raw_directory_networks) if isinstance(raw_directory_networks, list) else []
    for name in _coerce_str_list(address_payload.get("provider_directory_network_names")):
        directory_networks.append({"name": name})

    matches: list[dict[str, Any]] = []
    seen_candidate_keys: set[str] = set()
    for network in directory_networks:
        if not isinstance(network, dict):
            continue
        candidate_values = [network.get("name"), network.get("provider_directory_network_name")]
        aliases = _coerce_json_payload(network.get("aliases"), [])
        if isinstance(aliases, list):
            candidate_values.extend(aliases)
        for candidate in candidate_values:
            candidate_key = _canonical_network_name(candidate)
            if not candidate_key or candidate_key not in ptg_by_key or candidate_key in seen_candidate_keys:
                continue
            seen_candidate_keys.add(candidate_key)
            matches.append(
                {
                    "ptg_network_name": ptg_by_key[candidate_key],
                    "provider_directory_network_name": str(candidate or ""),
                    "provider_directory_network_resource_id": (
                        network.get("resource_id") or network.get("provider_directory_network_resource_id")
                    ),
                    "provider_directory_network_ref": network.get("ref") or network.get("provider_directory_network_ref"),
                }
            )
    return matches


def _manifest_address_verification_evidence(
    address_payload: Mapping[str, Any],
    network_name_matches: list[dict[str, Any]],
) -> dict[str, Any] | None:
    evidence = _coerce_json_payload(address_payload.get("address_verification_evidence"), {})
    if not isinstance(evidence, dict):
        evidence = {}
    if not evidence and not network_name_matches:
        return None
    updated = dict(evidence)
    if network_name_matches and not _manifest_plan_context_matched(address_payload):
        matched_on = str(updated.get("matched_on") or "").strip()
        if matched_on and not matched_on.endswith("_network_name"):
            updated["matched_on"] = f"{matched_on}_network_name"
        elif not matched_on:
            updated["matched_on"] = "npi_address_key_role_location_network_name"
        updated["network_name_context_matched"] = True
        updated["network_name_matches"] = network_name_matches
    elif not network_name_matches and not _manifest_plan_context_matched(address_payload):
        matched_on = str(updated.get("matched_on") or "").strip()
        if matched_on.endswith("_network_name"):
            updated["matched_on"] = matched_on.removesuffix("_network_name") or "npi_address_key_role_location"
        updated.pop("network_name_context_matched", None)
        updated.pop("network_name_matches", None)
    return updated


def _manifest_has_direct_payer_location_record_evidence(address_payload: Mapping[str, Any]) -> bool:
    evidence = _coerce_json_payload(address_payload.get("address_verification_evidence"), {})
    if not isinstance(evidence, dict):
        return False
    return any(evidence.get(key) not in (None, "", [], {}) for key in PTG_DIRECT_PAYER_LOCATION_RECORD_KEYS)


def _manifest_has_source_file_version_trace(item: Mapping[str, Any]) -> bool:
    source_trace = item.get("source_trace")
    if not isinstance(source_trace, list):
        return False
    return any(
        isinstance(entry, Mapping) and str(entry.get("source_file_version_id") or "").strip()
        for entry in source_trace
    )


def _manifest_address_payload(item: Mapping[str, Any], provider: Mapping[str, Any] | None = None) -> dict[str, Any]:
    provider = provider or {}
    payload = _coerce_json_payload(item.get("address_payload") or provider.get("address_payload"), {})
    address = _coerce_json_payload(item.get("address") or provider.get("address"), {})
    merged: dict[str, Any] = {}
    if isinstance(address, dict):
        merged.update(address)
    if isinstance(payload, dict):
        merged.update(payload)
    return merged


def _manifest_address_verification(
    item: Mapping[str, Any],
    *,
    provider: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    provider = provider or {}
    address_payload = _manifest_address_payload(item, provider)
    address_sources = _coerce_str_list(item.get("address_sources") or address_payload.get("address_sources"))
    location_source = (
        item.get("location_source")
        or provider.get("location_source")
        or address_payload.get("location_source")
    )
    location_confidence_code = (
        item.get("location_confidence_code")
        or provider.get("location_confidence_code")
        or address_payload.get("location_confidence_code")
    )
    displayed = _has_displayable_address(address_payload) or _has_displayable_address(item)
    if displayed is False:
        return {
            "rate_network_binding": "tic_provider_group_npi_tin",
            "address_network_binding": "inferred_from_provider_identity",
            "address_evidence_level": "unknown",
            "requires_location_confirmation": True,
            "reason": "PTG proves the provider identity is in network, but no displayable address is available.",
            "displayed_address_present": False,
            "network_bound_address": False,
        }

    markers = {
        *(_normalized_markers(*address_sources)),
        *_normalized_markers(location_source, location_confidence_code),
    }
    direct_payer = (
        bool(
            markers
            & {
                "payer_confirmed_location",
                "payer_provider_group_location",
                "ptg_provider_group_location",
                "tic_provider_group_location",
            }
        )
        and _manifest_has_direct_payer_location_record_evidence(address_payload)
        and _manifest_has_source_file_version_trace(item)
    )
    provider_directory = bool(
        markers & {"provider_directory", "provider_directory_fhir", "payer_provider_directory"}
    )
    provider_directory_network_name_matches = _manifest_network_name_matches(item, address_payload)
    provider_directory_network_context = bool(
        provider_directory
        and (
            _manifest_plan_context_matched(address_payload)
            or provider_directory_network_name_matches
        )
    )
    provider_directory_evidence = _manifest_address_verification_evidence(
        address_payload,
        provider_directory_network_name_matches,
    )
    nppes = bool(markers & {"nppes", "npi_address"})
    if direct_payer:
        address_binding = "payer_confirmed_location"
        evidence_level = "payer_confirmed_location"
        requires_confirmation = False
        reason = "The payer/PTG source supplied the provider location used for this result."
    elif provider_directory_network_context:
        address_binding = "payer_directory_corroborated_location"
        evidence_level = "payer_directory_network_location"
        requires_confirmation = False
        reason = "A payer Provider Directory record links this provider, network or plan context, and displayed address."
    elif provider_directory:
        address_binding = "inferred_from_provider_identity"
        evidence_level = "provider_directory_address"
        requires_confirmation = True
        reason = "A payer Provider Directory record corroborates the displayed provider address, but the PTG rate file did not supply it."
    elif nppes:
        address_binding = "inferred_from_provider_identity"
        evidence_level = "nppes_provider_address"
        requires_confirmation = True
        reason = "PTG proves the NPI/TIN is in network; the displayed address comes from NPPES/provider enrichment."
    elif displayed:
        address_binding = "inferred_from_provider_identity"
        evidence_level = "unified_provider_address"
        requires_confirmation = True
        reason = "PTG proves the provider identity is in network; the displayed address comes from provider-address enrichment."
    else:
        address_binding = "inferred_from_provider_identity"
        evidence_level = "unknown"
        requires_confirmation = True
        reason = "PTG proves the provider identity is in network, but address provenance is weak or unavailable."
    response_address_sources = list(address_sources)
    if address_binding != "payer_confirmed_location":
        response_address_sources = [
            value
            for value in response_address_sources
            if value.lower().replace("-", "_") not in {"ptg", "tic", "tic_provider_group"}
        ]
    provider_directory_plan_context = (
        True
        if _manifest_plan_context_matched(address_payload)
        else _optional_bool(address_payload.get("provider_directory_plan_context_matched"))
    )
    payload = {
        "rate_network_binding": "tic_provider_group_npi_tin",
        "address_network_binding": address_binding,
        "address_evidence_level": evidence_level,
        "requires_location_confirmation": requires_confirmation,
        "reason": reason,
        "displayed_address_present": displayed,
        "network_bound_address": address_binding in {
            "payer_confirmed_location",
            "payer_directory_corroborated_location",
        },
    }
    optional_fields = {
        "location_source": location_source,
        "location_confidence_code": location_confidence_code,
        "address_sources": response_address_sources,
        "provider_directory_plan_context_matched": provider_directory_plan_context,
        "provider_directory_network_name_matched": bool(provider_directory_network_name_matches) or None,
        "provider_directory_network_names": _coerce_str_list(address_payload.get("provider_directory_network_names")),
        "provider_directory_network_matches": provider_directory_network_name_matches,
        "address_verification_evidence": provider_directory_evidence,
    }
    for key, value in optional_fields.items():
        if value not in (None, "", []):
            payload[key] = value
    return payload


def _strip_no_display_address_fields(item: dict[str, Any]) -> None:
    verification = item.get("address_verification")
    if not isinstance(verification, dict) or verification.get("displayed_address_present") is not False:
        return
    for key in PTG_NO_DISPLAY_ADDRESS_FIELDS:
        item.pop(key, None)
    for key in PTG_NO_DISPLAY_VERIFICATION_FIELDS:
        verification.pop(key, None)


def _include_unverified_ptg_addresses(args: Mapping[str, Any]) -> bool:
    return _request_bool(args.get("include_unverified_addresses"), default=True)


def _is_plan_scoped_ptg_request(args: Mapping[str, Any]) -> bool:
    return bool(
        str(
            args.get("plan_id")
            or args.get("plan_external_id")
            or args.get("plan_market_type")
            or args.get("market_type")
            or ""
        ).strip()
    )


def _apply_ptg_address_display_policy(item: dict[str, Any], args: Mapping[str, Any]) -> None:
    verification = item.get("address_verification")
    if not isinstance(verification, dict):
        _strip_no_display_address_fields(item)
        return
    if (
        verification.get("displayed_address_present") is True
        and verification.get("network_bound_address") is not True
        and _is_plan_scoped_ptg_request(args)
        and not _include_unverified_ptg_addresses(args)
    ):
        verification["displayed_address_present"] = False
        verification["network_bound_address"] = False
        verification["address_network_binding"] = "inferred_from_provider_identity"
        verification["requires_location_confirmation"] = True
        verification["reason"] = (
            "PTG proves the provider identity is in network, but the displayed address is not tied "
            "to the priced plan or network; address and phone fields are suppressed by request."
        )
    _strip_no_display_address_fields(item)


@dataclass(frozen=True)
class PTG2ManifestSnapshot:
    snapshot_id: str
    source_uri: str
    manifest: dict[str, Any]
    plans: dict[str, Any]
    procedures: dict[str, Any]
    rows: tuple[dict[str, Any], ...]
    providers: dict[str, Any]
    price_sets: dict[str, Any]
    price_atoms: dict[str, Any]
    provider_set_members: dict[str, tuple[str, ...]]
    price_set_members: dict[str, tuple[str, ...]]
    source_trace_sets: dict[str, Any]


def _path_from_local_uri(uri: str) -> Path:
    if uri.startswith("file://"):
        return Path(unquote(urlsplit(uri).path))
    if "://" in uri:
        raise PTG2ManifestArtifactError("PTG2 serving artifacts currently require local file paths")
    return Path(uri)


def _sidecar_path(manifest_path: Path, sidecar: Mapping[str, Any]) -> Path:
    raw_path = sidecar.get("path")
    if not isinstance(raw_path, str) or not raw_path:
        raise PTG2ManifestArtifactError("PTG2 sidecar is missing a relative path")
    path = Path(raw_path)
    if path.is_absolute() or ".." in path.parts:
        raise PTG2ManifestArtifactError("PTG2 sidecar paths must stay under the manifest directory")
    return manifest_path.parent / path


def _sidecars(manifest: Mapping[str, Any], kinds: set[str]) -> list[Mapping[str, Any]]:
    return [
        sidecar
        for sidecar in manifest.get("sidecars") or []
        if isinstance(sidecar, Mapping) and str(sidecar.get("kind") or "") in kinds
    ]


def _membership_sidecars(manifest: Mapping[str, Any], kinds: set[str]) -> list[Mapping[str, Any]]:
    return [
        sidecar
        for sidecar in manifest.get("sidecars") or []
        if isinstance(sidecar, Mapping)
        and str(sidecar.get("kind") or "") in kinds
        and sidecar.get("record_format") == PTG2_MANIFEST_MEMBERSHIP_FORMAT
    ]


def _read_json(path: Path) -> Any:
    with open(path, "r", encoding="utf-8") as fp:
        return json.load(fp)


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with open(path, "r", encoding="utf-8") as fp:
        for line in fp:
            text_value = line.strip()
            if not text_value:
                continue
            payload = json.loads(text_value)
            if not isinstance(payload, dict):
                raise PTG2ManifestArtifactError("PTG2 serving row sidecar entries must be JSON objects")
            rows.append(payload)
    return rows


def _load_rows(manifest_path: Path, manifest: Mapping[str, Any]) -> tuple[dict[str, Any], ...]:
    rows: list[dict[str, Any]] = []
    row_limit = max(_env_int(PTG2_MANIFEST_ROW_LIMIT_ENV, 250_000), 1)
    byte_limit = max(_env_int(PTG2_MANIFEST_BYTE_LIMIT_ENV, 256 * 1024 * 1024), 1)

    def guard_rows() -> None:
        if len(rows) > row_limit:
            raise PTG2ManifestArtifactError(
                "PTG2 manifest row payload is too large for in-process serving; use the DB-backed PTG2 path"
            )

    inline_rows = manifest.get("rows") or manifest.get("serving_rows")
    if isinstance(inline_rows, list):
        rows.extend(dict(row) for row in inline_rows if isinstance(row, Mapping))
        guard_rows()
    for sidecar in _sidecars(manifest, PTG2_MANIFEST_SERVING_ROW_KINDS):
        sidecar_path = _sidecar_path(manifest_path, sidecar)
        try:
            sidecar_bytes = int(sidecar.get("byte_count") or sidecar_path.stat().st_size)
        except (OSError, TypeError, ValueError):
            sidecar_bytes = 0
        if sidecar_bytes > byte_limit:
            raise PTG2ManifestArtifactError(
                "PTG2 manifest row sidecar is too large for in-process serving; use the DB-backed PTG2 path"
            )
        sidecar_format = str(sidecar.get("format") or "").strip().lower()
        if sidecar_format == "jsonl" or sidecar_path.suffix == ".jsonl":
            rows.extend(_read_jsonl(sidecar_path))
            guard_rows()
            continue
        payload = _read_json(sidecar_path)
        if isinstance(payload, list):
            rows.extend(dict(row) for row in payload if isinstance(row, Mapping))
        elif isinstance(payload, dict) and isinstance(payload.get("rows"), list):
            rows.extend(dict(row) for row in payload["rows"] if isinstance(row, Mapping))
        else:
            raise PTG2ManifestArtifactError("PTG2 serving row sidecar must contain rows")
        guard_rows()
    return tuple(rows)


def _load_mapping_sidecars(
    manifest_path: Path,
    manifest: Mapping[str, Any],
    kinds: set[str],
    inline_keys: Iterable[str],
) -> dict[str, Any]:
    mapping: dict[str, Any] = {}
    for key in inline_keys:
        inline = manifest.get(key)
        if isinstance(inline, Mapping):
            mapping.update({str(mapping_key): value for mapping_key, value in inline.items()})
    for sidecar in _sidecars(manifest, kinds):
        payload = _read_json(_sidecar_path(manifest_path, sidecar))
        if not isinstance(payload, Mapping):
            raise PTG2ManifestArtifactError("PTG2 mapping sidecars must be JSON objects")
        mapping.update({str(mapping_key): value for mapping_key, value in payload.items()})
    return mapping


def _load_membership_sidecars(
    manifest_path: Path,
    manifest: Mapping[str, Any],
    kinds: set[str],
    inline_keys: Iterable[str],
) -> dict[str, tuple[str, ...]]:
    mapping: dict[str, tuple[str, ...]] = {}
    for key in inline_keys:
        inline = manifest.get(key)
        if isinstance(inline, Mapping):
            for owner, members in inline.items():
                if isinstance(members, Iterable) and not isinstance(members, (str, bytes, bytearray)):
                    mapping[str(owner)] = tuple(str(member) for member in members)
    for sidecar in _membership_sidecars(manifest, kinds):
        entries = read_global_sidecar_entries(_sidecar_path(manifest_path, sidecar), metadata=sidecar)
        mapping.update({entry.owner.hex(): tuple(member.hex() for member in entry.members) for entry in entries})
    return mapping


def load_ptg2_manifest_snapshot(path_or_uri: str | Path) -> PTG2ManifestSnapshot:
    manifest_uri = str(path_or_uri)
    manifest_path = _path_from_local_uri(manifest_uri)
    manifest = read_manifest(manifest_path, validate_sidecars=True)
    artifact_type = str(manifest.get("artifact_type") or manifest.get("type") or "").strip()
    if artifact_type and artifact_type not in PTG2_MANIFEST_SNAPSHOT_ARTIFACT_TYPES:
        raise PTG2ManifestArtifactError(f"unsupported PTG2 snapshot artifact type: {artifact_type!r}")
    snapshot_id = str(manifest.get("snapshot_id") or "").strip()
    if not snapshot_id:
        raise PTG2ManifestArtifactError("PTG2 snapshot manifest is missing snapshot_id")
    plans = {str(key): value for key, value in dict(manifest.get("plans") or {}).items()}
    procedures = {str(key): value for key, value in dict(manifest.get("procedures") or {}).items()}
    return PTG2ManifestSnapshot(
        snapshot_id=snapshot_id,
        source_uri=manifest_path.resolve().as_uri(),
        manifest=manifest,
        plans=plans,
        procedures=procedures,
        rows=_load_rows(manifest_path, manifest),
        providers=_load_mapping_sidecars(manifest_path, manifest, PTG2_MANIFEST_PROVIDER_KINDS, ("providers", "provider_entries")),
        price_sets=_load_mapping_sidecars(manifest_path, manifest, PTG2_MANIFEST_PRICE_SET_KINDS, ("price_sets", "prices")),
        price_atoms=_load_mapping_sidecars(manifest_path, manifest, PTG2_MANIFEST_PRICE_ATOM_KINDS, ("price_atoms", "price_entries")),
        provider_set_members=_load_membership_sidecars(
            manifest_path,
            manifest,
            PTG2_MANIFEST_PROVIDER_SET_MEMBER_KINDS,
            ("provider_set_members", "provider_set_membership", "provider_members"),
        ),
        price_set_members=_load_membership_sidecars(
            manifest_path,
            manifest,
            PTG2_MANIFEST_PRICE_SET_MEMBER_KINDS,
            ("price_set_members", "price_set_membership", "price_members"),
        ),
        source_trace_sets=_load_mapping_sidecars(
            manifest_path,
            manifest,
            PTG2_MANIFEST_SOURCE_TRACE_KINDS,
            ("source_trace_sets", "source_traces"),
        ),
    )


def _has_unsupported_provider_or_geo_request(args: Mapping[str, Any]) -> bool:
    if _normalize_zip5(args.get("zip5")):
        return True
    for key in (
        "state",
        "city",
        "lat",
        "long",
        "radius_miles",
        "npi",
        "specialty",
        "taxonomy_code",
        "taxonomy_classification",
        "taxonomy_specialization",
        "taxonomy_section",
    ):
        if str(args.get(key) or "").strip():
            return True
    return False


def _procedure_lookup(snapshot: PTG2ManifestSnapshot, row: Mapping[str, Any]) -> Mapping[str, Any]:
    reported_code = str(row.get("reported_code") or row.get("billing_code") or "").strip()
    reported_system = str(row.get("reported_code_system") or row.get("billing_code_type") or "").strip()
    for key in (
        str(row.get("procedure_key") or "").strip(),
        f"{reported_system}:{reported_code}" if reported_system and reported_code else "",
        reported_code,
    ):
        if key and isinstance(snapshot.procedures.get(key), Mapping):
            return snapshot.procedures[key]
    return {}


def _mapping_or_empty(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _prices(snapshot: PTG2ManifestSnapshot, row: Mapping[str, Any]) -> Any:
    inline_prices = row.get("prices")
    if inline_prices is not None:
        return inline_prices
    for key in ("price_set_key", "price_set_id", "price_set_hash"):
        value = str(row.get(key) or "").strip()
        if value and value in snapshot.price_sets:
            return snapshot.price_sets[value]
    price_set_global_id = str(row.get("price_set_global_id_128") or "").strip()
    if price_set_global_id and price_set_global_id in snapshot.price_set_members:
        prices = []
        for price_atom_id in snapshot.price_set_members[price_set_global_id]:
            price_atom = snapshot.price_atoms.get(price_atom_id)
            if price_atom is None:
                return None
            if isinstance(price_atom, list):
                prices.extend(price_atom)
            else:
                prices.append(price_atom)
        return prices
    return []


def _provider_set_id(row: Mapping[str, Any]) -> str:
    for key in ("provider_set_global_id_128", "provider_set_hash", "provider_set_id", "provider_set_key"):
        value = str(row.get(key) or "").strip()
        if value:
            return value
    return ""


def _price_set_id(row: Mapping[str, Any]) -> str:
    for key in ("price_set_global_id_128", "price_set_hash", "price_set_id", "price_set_key"):
        value = str(row.get(key) or "").strip()
        if value:
            return value
    return ""


def _provider_payload(snapshot: PTG2ManifestSnapshot, provider_id: str) -> Mapping[str, Any] | None:
    provider = snapshot.providers.get(provider_id)
    if isinstance(provider, Mapping):
        return provider
    return None


def _provider_items(snapshot: PTG2ManifestSnapshot, row: Mapping[str, Any]) -> tuple[Mapping[str, Any], ...] | None:
    provider_set_id = _provider_set_id(row)
    if not provider_set_id:
        return None
    provider_ids = snapshot.provider_set_members.get(provider_set_id)
    if provider_ids is None:
        return None
    providers: list[Mapping[str, Any]] = []
    for provider_id in provider_ids:
        payload = _provider_payload(snapshot, provider_id)
        if payload is None:
            return None
        providers.append(payload)
    return tuple(providers)


def _source_trace(snapshot: PTG2ManifestSnapshot, row: Mapping[str, Any]) -> Any:
    inline_trace = row.get("source_trace")
    if inline_trace is not None:
        return inline_trace
    for key in ("source_trace_set_key", "source_trace_set_id", "source_trace_set_hash"):
        value = str(row.get(key) or "").strip()
        if value and value in snapshot.source_trace_sets:
            return snapshot.source_trace_sets[value]
    return []


def _matches_exact_request(row: Mapping[str, Any], requested_plan: str, requested_code: str, code_system: str) -> bool:
    plan_id = str(row.get("plan_id") or "").strip()
    if plan_id != requested_plan:
        return False
    row_code = _normalize_code(row.get("reported_code") or row.get("billing_code"))
    row_system = _normalize_code_system(row.get("reported_code_system") or row.get("billing_code_type"))
    if row_system:
        row_code = canonical_catalog_code(row_system, row_code)
    if row_code != requested_code:
        return False
    if not code_system:
        return True
    return row_system == code_system


def search_ptg2_manifest_snapshot(
    snapshot: PTG2ManifestSnapshot,
    args: Mapping[str, Any],
    pagination: Any,
    *,
    mode_value: str,
) -> dict[str, Any] | None:
    if _has_unsupported_provider_or_geo_request(args):
        return None
    requested_plan = str(args.get("plan_id") or args.get("plan_external_id") or "").strip()
    requested_system = _normalize_code_system(args.get("code_system"))
    requested_code = canonical_catalog_code(requested_system, args.get("code")) if requested_system else _normalize_code(args.get("code"))
    if not requested_plan or not requested_code:
        return None
    if str(args.get("q") or "").strip():
        return None
    expand_providers = _request_bool(args.get("include_providers"))

    matched_rows = [
        row
        for row in snapshot.rows
        if _matches_exact_request(row, requested_plan, requested_code, requested_system)
    ]
    if not matched_rows:
        return None

    offset = int(getattr(pagination, "offset", 0) or 0)
    limit = int(getattr(pagination, "limit", 25) or 25)
    page_rows = matched_rows if expand_providers else matched_rows[offset : offset + limit]
    items: list[dict[str, Any]] = []
    for row in page_rows:
        procedure = _procedure_lookup(snapshot, row)
        raw_prices = _prices(snapshot, row)
        if raw_prices is None:
            return None
        prices = _coerce_json_payload(raw_prices, [])
        source_trace = _coerce_json_payload(_source_trace(snapshot, row), [])
        reported_code = row.get("reported_code") or row.get("billing_code") or requested_code
        reported_system = row.get("reported_code_system") or row.get("billing_code_type") or requested_system or None
        hp_code = row.get("procedure_code") if row.get("procedure_code") is not None else procedure.get("procedure_code")
        provider_set_hash = _provider_set_id(row) or None
        price_set_hash = _price_set_id(row) or None
        base_item = {
                "serving_rate_id": row.get("serving_rate_id"),
                "provider_ordinal": provider_set_hash,
                "provider_set_hash": provider_set_hash,
                "provider_set_hashes": row.get("provider_set_hashes") or ([provider_set_hash] if provider_set_hash else []),
                "provider_name": row.get("provider_name") or "TiC provider set",
                "provider_count": row.get("provider_count") or 0,
                "provider_set_count": row.get("provider_set_count") or 0,
                "plan_id": requested_plan,
                "plan_name": row.get("plan_name") or _mapping_or_empty(snapshot.plans.get(requested_plan)).get("plan_name"),
                "procedure_code": hp_code if hp_code is not None else reported_code,
                "hp_procedure_code": hp_code,
                "procedure_name": row.get("procedure_name")
                or row.get("procedure_display_name")
                or procedure.get("name")
                or procedure.get("display_name"),
                "procedure_description": row.get("procedure_description") or procedure.get("description"),
                "service_code": row.get("billing_code") or reported_code,
                "service_code_system": row.get("billing_code_type") or reported_system,
                "reported_code": reported_code,
                "reported_code_system": reported_system,
                "billing_code": row.get("billing_code") or reported_code,
                "billing_code_type": row.get("billing_code_type") or reported_system,
                **_price_response_fields(prices),
                "price_set_hash": price_set_hash,
                "rate_pack_hash": row.get("rate_pack_hash"),
                "source_trace": source_trace,
                "network_names": _coerce_str_list(row.get("network_names")),
                "confidence": row.get("confidence")
                or {
                    "network": "tic_rate_npi_tin",
                    "location": "nppes_practice_location",
                },
            }
        base_item["address_verification"] = _manifest_address_verification(base_item)
        _apply_ptg_address_display_policy(base_item, args)
        if not expand_providers:
            items.append(base_item)
            continue
        providers = _provider_items(snapshot, row)
        if providers is None:
            return None
        for provider in providers:
            item = dict(base_item)
            npi = provider.get("npi") or provider.get("provider_npi")
            address_payload = _manifest_address_payload(provider)
            item.update(
                {
                    "npi": npi,
                    "provider_ordinal": npi or provider_set_hash,
                    "provider_name": provider.get("provider_name") or provider.get("name") or item["provider_name"],
                    "state": provider.get("state"),
                    "city": provider.get("city"),
                    "zip5": provider.get("zip5"),
                    "location_hash": provider.get("location_hash"),
                    "location_source": provider.get("location_source"),
                    "location_confidence_code": provider.get("location_confidence_code"),
                    "address": address_payload,
                    "telephone_number": provider.get("telephone_number") or address_payload.get("telephone_number"),
                    "fax_number": provider.get("fax_number") or address_payload.get("fax_number"),
                    "taxonomy_codes": provider.get("taxonomy_codes") or [],
                    "specialties": provider.get("specialties") or [],
                }
            )
            item["address_verification"] = _manifest_address_verification(item, provider=provider)
            _apply_ptg_address_display_policy(item, args)
            items.append(item)
    total_items = len(items) if expand_providers else len(matched_rows)
    if expand_providers:
        items = items[offset : offset + limit]

    return {
        "items": items,
        "pagination": {
            "total": total_items,
            "limit": limit,
            "offset": offset,
            "page": (offset // limit) + 1 if limit else 1,
        },
        "query": {
            "plan_id": args.get("plan_id"),
            "plan_external_id": args.get("plan_external_id"),
            "plan_market_type": args.get("plan_market_type") or None,
            "source_key": args.get("source_key") or None,
            "snapshot_id": snapshot.snapshot_id,
            "mode": mode_value,
            "code": args.get("code") or None,
            "code_system": args.get("code_system") or None,
            "q": None,
            "state": None,
            "city": None,
            "zip5": None,
            "source": "ptg2_artifact",
            "serving_table": None,
            "include_providers": expand_providers,
            "result_granularity": "provider" if expand_providers else "provider_set",
            "procedure_consolidation": "HP_PROCEDURE_CODE",
        },
    }


async def search_ptg2_manifest_serving_snapshot(
    snapshot_id: str,
    args: Mapping[str, Any],
    pagination: Any,
    *,
    serving_tables: PTG2ServingTables,
    mode_value: str,
) -> dict[str, Any] | None:
    if not serving_tables.artifact_uri:
        return None
    snapshot = load_ptg2_manifest_snapshot(serving_tables.artifact_uri)
    if snapshot.snapshot_id != snapshot_id:
        return None
    return search_ptg2_manifest_snapshot(snapshot, args, pagination, mode_value=mode_value)
