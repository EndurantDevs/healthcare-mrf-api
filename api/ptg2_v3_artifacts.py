# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""PTG2 manifest-backed snapshot reader and bounded serving search."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping
from urllib.parse import unquote, urlsplit

from process.ptg_parts.ptg2_v3_artifacts import (
    PTG2_V3_MEMBERSHIP_FORMAT,
    PTG2V3ArtifactError,
    read_global_sidecar_entries,
    read_manifest,
)

from api.ptg2_code_filters import _normalize_code, _normalize_code_system
from api.ptg2_response import _coerce_json_payload, _price_response_fields, _request_bool
from api.ptg2_serving_utils import _normalize_zip5
from api.ptg2_types import PTG2ServingTables

PTG2_V3_SNAPSHOT_ARTIFACT_TYPES = {
    "ptg2_v3_snapshot",
    "ptg2_v3_manifest_snapshot",
    "snapshot_index",
}
PTG2_V3_SERVING_ROW_KINDS = {"skinny_serving_rows", "serving_rows"}
PTG2_V3_PRICE_SET_KINDS = {"price_sets", "prices"}
PTG2_V3_PROVIDER_KINDS = {"providers", "provider_entries"}
PTG2_V3_PRICE_ATOM_KINDS = {"price_atoms", "price_entries"}
PTG2_V3_PROVIDER_SET_MEMBER_KINDS = {"provider_set_members", "provider_set_membership", "provider_members"}
PTG2_V3_PRICE_SET_MEMBER_KINDS = {"price_set_members", "price_set_membership", "price_members"}
PTG2_V3_SOURCE_TRACE_KINDS = {"source_trace_sets", "source_traces"}
PTG2_V3_MANIFEST_ROW_LIMIT_ENV = "HLTHPRT_PTG2_V3_MANIFEST_ROW_LIMIT"
PTG2_V3_MANIFEST_BYTE_LIMIT_ENV = "HLTHPRT_PTG2_V3_MANIFEST_BYTE_LIMIT"


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return int(str(raw).strip())
    except ValueError:
        return default


@dataclass(frozen=True)
class PTG2V3Snapshot:
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
        raise PTG2V3ArtifactError("PTG2 serving artifacts currently require local file paths")
    return Path(uri)


def _sidecar_path(manifest_path: Path, sidecar: Mapping[str, Any]) -> Path:
    raw_path = sidecar.get("path")
    if not isinstance(raw_path, str) or not raw_path:
        raise PTG2V3ArtifactError("PTG2 sidecar is missing a relative path")
    path = Path(raw_path)
    if path.is_absolute() or ".." in path.parts:
        raise PTG2V3ArtifactError("PTG2 sidecar paths must stay under the manifest directory")
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
        and sidecar.get("record_format") == PTG2_V3_MEMBERSHIP_FORMAT
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
                raise PTG2V3ArtifactError("PTG2 serving row sidecar entries must be JSON objects")
            rows.append(payload)
    return rows


def _load_rows(manifest_path: Path, manifest: Mapping[str, Any]) -> tuple[dict[str, Any], ...]:
    rows: list[dict[str, Any]] = []
    row_limit = max(_env_int(PTG2_V3_MANIFEST_ROW_LIMIT_ENV, 250_000), 1)
    byte_limit = max(_env_int(PTG2_V3_MANIFEST_BYTE_LIMIT_ENV, 256 * 1024 * 1024), 1)

    def guard_rows() -> None:
        if len(rows) > row_limit:
            raise PTG2V3ArtifactError(
                "PTG2 manifest row payload is too large for in-process serving; use the DB-backed PTG2 path"
            )

    inline_rows = manifest.get("rows") or manifest.get("serving_rows")
    if isinstance(inline_rows, list):
        rows.extend(dict(row) for row in inline_rows if isinstance(row, Mapping))
        guard_rows()
    for sidecar in _sidecars(manifest, PTG2_V3_SERVING_ROW_KINDS):
        sidecar_path = _sidecar_path(manifest_path, sidecar)
        try:
            sidecar_bytes = int(sidecar.get("byte_count") or sidecar_path.stat().st_size)
        except (OSError, TypeError, ValueError):
            sidecar_bytes = 0
        if sidecar_bytes > byte_limit:
            raise PTG2V3ArtifactError(
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
            raise PTG2V3ArtifactError("PTG2 serving row sidecar must contain rows")
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
            raise PTG2V3ArtifactError("PTG2 mapping sidecars must be JSON objects")
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


def load_ptg2_v3_snapshot(path_or_uri: str | Path) -> PTG2V3Snapshot:
    manifest_uri = str(path_or_uri)
    manifest_path = _path_from_local_uri(manifest_uri)
    manifest = read_manifest(manifest_path, validate_sidecars=True)
    artifact_type = str(manifest.get("artifact_type") or manifest.get("type") or "").strip()
    if artifact_type and artifact_type not in PTG2_V3_SNAPSHOT_ARTIFACT_TYPES:
        raise PTG2V3ArtifactError(f"unsupported PTG2 snapshot artifact type: {artifact_type!r}")
    snapshot_id = str(manifest.get("snapshot_id") or "").strip()
    if not snapshot_id:
        raise PTG2V3ArtifactError("PTG2 snapshot manifest is missing snapshot_id")
    plans = {str(key): value for key, value in dict(manifest.get("plans") or {}).items()}
    procedures = {str(key): value for key, value in dict(manifest.get("procedures") or {}).items()}
    return PTG2V3Snapshot(
        snapshot_id=snapshot_id,
        source_uri=manifest_path.resolve().as_uri(),
        manifest=manifest,
        plans=plans,
        procedures=procedures,
        rows=_load_rows(manifest_path, manifest),
        providers=_load_mapping_sidecars(manifest_path, manifest, PTG2_V3_PROVIDER_KINDS, ("providers", "provider_entries")),
        price_sets=_load_mapping_sidecars(manifest_path, manifest, PTG2_V3_PRICE_SET_KINDS, ("price_sets", "prices")),
        price_atoms=_load_mapping_sidecars(manifest_path, manifest, PTG2_V3_PRICE_ATOM_KINDS, ("price_atoms", "price_entries")),
        provider_set_members=_load_membership_sidecars(
            manifest_path,
            manifest,
            PTG2_V3_PROVIDER_SET_MEMBER_KINDS,
            ("provider_set_members", "provider_set_membership", "provider_members"),
        ),
        price_set_members=_load_membership_sidecars(
            manifest_path,
            manifest,
            PTG2_V3_PRICE_SET_MEMBER_KINDS,
            ("price_set_members", "price_set_membership", "price_members"),
        ),
        source_trace_sets=_load_mapping_sidecars(
            manifest_path,
            manifest,
            PTG2_V3_SOURCE_TRACE_KINDS,
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


def _procedure_lookup(snapshot: PTG2V3Snapshot, row: Mapping[str, Any]) -> Mapping[str, Any]:
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


def _prices(snapshot: PTG2V3Snapshot, row: Mapping[str, Any]) -> Any:
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


def _provider_payload(snapshot: PTG2V3Snapshot, provider_id: str) -> Mapping[str, Any] | None:
    provider = snapshot.providers.get(provider_id)
    if isinstance(provider, Mapping):
        return provider
    return None


def _provider_items(snapshot: PTG2V3Snapshot, row: Mapping[str, Any]) -> tuple[Mapping[str, Any], ...] | None:
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


def _source_trace(snapshot: PTG2V3Snapshot, row: Mapping[str, Any]) -> Any:
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
    if row_code != requested_code:
        return False
    if not code_system:
        return True
    row_system = _normalize_code_system(row.get("reported_code_system") or row.get("billing_code_type"))
    return row_system == code_system


def search_ptg2_v3_snapshot(
    snapshot: PTG2V3Snapshot,
    args: Mapping[str, Any],
    pagination: Any,
    *,
    mode_value: str,
) -> dict[str, Any] | None:
    if _has_unsupported_provider_or_geo_request(args):
        return None
    requested_plan = str(args.get("plan_id") or args.get("plan_external_id") or "").strip()
    requested_code = _normalize_code(args.get("code"))
    requested_system = _normalize_code_system(args.get("code_system"))
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
                "confidence": row.get("confidence")
                or {
                    "network": "tic_rate_npi_tin",
                    "location": "nppes_practice_location",
                },
            }
        if not expand_providers:
            items.append(base_item)
            continue
        providers = _provider_items(snapshot, row)
        if providers is None:
            return None
        for provider in providers:
            item = dict(base_item)
            npi = provider.get("npi") or provider.get("provider_npi")
            item.update(
                {
                    "npi": npi,
                    "provider_ordinal": npi or provider_set_hash,
                    "provider_name": provider.get("provider_name") or provider.get("name") or item["provider_name"],
                    "state": provider.get("state"),
                    "city": provider.get("city"),
                    "zip5": provider.get("zip5"),
                    "location_hash": provider.get("location_hash"),
                    "address_payload": provider.get("address_payload"),
                    "taxonomy_codes": provider.get("taxonomy_codes") or [],
                    "specialties": provider.get("specialties") or [],
                }
            )
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


async def search_ptg2_v3_serving_snapshot(
    snapshot_id: str,
    args: Mapping[str, Any],
    pagination: Any,
    *,
    serving_tables: PTG2ServingTables,
    mode_value: str,
) -> dict[str, Any] | None:
    if not serving_tables.artifact_uri:
        return None
    snapshot = load_ptg2_v3_snapshot(serving_tables.artifact_uri)
    if snapshot.snapshot_id != snapshot_id:
        return None
    return search_ptg2_v3_snapshot(snapshot, args, pagination, mode_value=mode_value)
