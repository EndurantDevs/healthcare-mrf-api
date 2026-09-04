# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exact bounded hydration for statewide pricing scan pages."""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Mapping

import orjson

from api.plan_pricing_projection_contract import PlanPricingProjectionUnavailable
from api.plan_pricing_projection_v3_provider_cells import (
    MAX_PROVIDER_STATE_FRAGMENT_BYTES,
    PROVIDER_STATE_FRAGMENT_VERSION,
    _has_valid_state_address_shape,
)
from api.plan_pricing_state_scan_contract import (
    STATE_SCAN_PRICE_ATOM_LIMIT,
    PlanPricingStateScanBudgetExceeded,
)
from api.plan_release_serving import PlanReleaseServingSelection, binding_query_args
from api.ptg2_response import _is_request_flag_enabled
from process.ptg_parts.ptg2_manifest_artifacts import (
    ManifestReadLimitError,
    PTG2ManifestArtifactError,
)


@dataclass(frozen=True)
class _BindingScope:
    binding: Any
    serving_tables: Any
    occurrences: list[dict[str, Any]]
    query_args: Mapping[str, Any]


_PROVIDER_REQUIRED_FIELDS = frozenset(
    {
        "npi", "provider_name", "entity_type_code", "credential",
        "provider_sex_code", "taxonomy_codes", "specialties",
        "primary_specialty", "classifications", "specializations",
        "primary_specialization", "state", "city", "zip5",
        "location_hash", "location_source", "location_confidence_code",
        "address_payload",
    }
)
_EVIDENCE_SOURCE_ID = {
    "nppes_registry_address": 1,
    "multi_issuer_marketplace_address": 2,
    "cms_doctors_source_with_nppes_identity_anchor": 3,
}


def _fragment_bytes(raw_fragment: Any) -> bytes:
    if isinstance(raw_fragment, memoryview):
        raw_fragment = raw_fragment.tobytes()
    if not isinstance(raw_fragment, bytes) or not 2 <= len(raw_fragment) <= MAX_PROVIDER_STATE_FRAGMENT_BYTES:
        raise PTG2ManifestArtifactError("pricing state scan provider witness is invalid")
    return raw_fragment


def _positive_source_id(value: Any) -> int | None:
    if type(value) is int and value > 0:
        return value
    return None


def _state_code(value: Any) -> str | None:
    state = str(value or "")
    if (
        len(state) == 2
        and state.isascii()
        and state.isalpha()
        and state == state.upper()
    ):
        return state
    return None


def _is_string_list(value: Any) -> bool:
    return isinstance(value, list) and all(
        isinstance(item, str) and bool(item.strip()) for item in value
    )


def _address_provenance(
    address: Mapping[str, Any],
    serving: Any,
) -> tuple[list[Mapping[str, Any]], int | None]:
    source_id = _EVIDENCE_SOURCE_ID.get(
        str(address.get("geo_evidence_level") or "")
    )
    provenance = address.get("address_provenance")
    if (
        source_id is None
        or not isinstance(provenance, list)
        or not provenance
        or any(
            not isinstance(entry, Mapping)
            or _positive_source_id(entry.get("source_id")) is None
            or not serving._is_complete_address_provenance_entry(entry)
            for entry in provenance
        )
    ):
        return [], None
    return provenance, source_id


def _validated_coordinates(address: Mapping[str, Any]) -> None:
    latitude = address.get("lat")
    longitude = address.get("long")
    if (latitude is None) != (longitude is None):
        raise PTG2ManifestArtifactError("pricing state scan provider address is inconsistent")
    if latitude is None:
        return
    if isinstance(latitude, bool) or isinstance(longitude, bool):
        raise PTG2ManifestArtifactError("pricing state scan provider address is invalid")
    try:
        lat_value = float(latitude)
        long_value = float(longitude)
    except (TypeError, ValueError, OverflowError) as exc:
        raise PTG2ManifestArtifactError("pricing state scan provider address is invalid") from exc
    if (
        not math.isfinite(lat_value)
        or not math.isfinite(long_value)
        or not -90 <= lat_value <= 90
        or not -180 <= long_value <= 180
    ):
        raise PTG2ManifestArtifactError("pricing state scan provider address is invalid")


def _validated_address(
    provider: Mapping[str, Any],
    expected_npi: int,
    expected_state: str,
    serving: Any,
) -> dict[str, Any]:
    address = provider.get("address_payload")
    if not isinstance(address, dict) or not _has_valid_state_address_shape(address):
        raise PTG2ManifestArtifactError("pricing state scan provider address is incomplete")
    postal_code = "".join(
        character
        for character in str(address.get("postal_code") or "")
        if character.isdigit()
    )[:5]
    location_key = str(address.get("location_key") or "")
    provenance, source_id = _address_provenance(address, serving)
    has_admitted_provenance = any(
        _positive_source_id(entry.get("source_id")) == source_id
        for entry in provenance
    )
    if (
        type(address.get("npi")) is not int
        or address.get("npi") != expected_npi
        or address.get("state") != expected_state
        or postal_code != provider.get("zip5")
        or address.get("city") != provider.get("city")
        or not location_key
        or provider.get("location_hash") != f"entity_address_unified:{location_key}"
        or source_id is None
        or not has_admitted_provenance
    ):
        raise PTG2ManifestArtifactError("pricing state scan provider address is inconsistent")
    _validated_coordinates(address)
    return dict(address)


def _validated_provider_context(
    raw_fragment: Any,
    expected_npi: int,
    expected_state: str,
    include_response_evidence: bool,
    serving: Any,
) -> dict[str, Any]:
    try:
        fragment = orjson.loads(_fragment_bytes(raw_fragment))
    except orjson.JSONDecodeError as exc:
        raise PTG2ManifestArtifactError("pricing state scan provider witness is invalid") from exc
    provider = fragment.get("provider") if isinstance(fragment, dict) else None
    if (
        not isinstance(fragment, dict)
        or fragment.get("version") != PROVIDER_STATE_FRAGMENT_VERSION
        or not isinstance(provider, dict)
        or not _PROVIDER_REQUIRED_FIELDS.issubset(provider)
        or type(provider.get("npi")) is not int
        or provider.get("npi") != expected_npi
        or _state_code(provider.get("state")) != expected_state
        or _state_code(expected_state) is None
        or not isinstance(provider.get("provider_name"), str)
        or not provider.get("provider_name", "").strip()
        or not str(provider.get("zip5") or "").isdigit()
        or len(str(provider.get("zip5") or "")) != 5
        or any(
            not _is_string_list(provider.get(field_name))
            for field_name in (
                "taxonomy_codes",
                "specialties",
                "classifications",
                "specializations",
            )
        )
        or provider.get("location_source") != "entity_address_unified"
        or provider.get("location_confidence_code") != "entity_address_unified"
    ):
        raise PTG2ManifestArtifactError("pricing state scan provider witness is inconsistent")
    provider_by_field = dict(provider)
    address_by_field = _validated_address(
        provider_by_field, expected_npi, expected_state, serving
    )
    for field_name in (
        "npi",
        "type",
        "checksum",
        "county_fips",
        "premise_key",
    ):
        address_by_field.pop(field_name, None)
    if not include_response_evidence:
        address_by_field.pop("address_provenance", None)
        address_by_field.pop("geo_evidence_level", None)
    provider_by_field["address_payload"] = address_by_field
    return provider_by_field


def _validated_providers_by_npi(
    provider_fragments: Mapping[int, Any],
    args: Mapping[str, Any],
    serving: Any,
) -> dict[int, dict[str, Any]]:
    expected_state = str(args.get("state") or "").strip().upper()
    include_response_evidence = any(
        _is_request_flag_enabled(args.get(flag_name), default=False)
        for flag_name in ("include_evidence", "include_debug", "include_details")
    )
    providers_by_npi: dict[int, dict[str, Any]] = {}
    for raw_npi, raw_fragment in sorted(provider_fragments.items()):
        if type(raw_npi) is not int or raw_npi <= 0:
            raise PTG2ManifestArtifactError("pricing state scan provider witness NPI is invalid")
        providers_by_npi[raw_npi] = _validated_provider_context(
            raw_fragment,
            raw_npi,
            expected_state,
            include_response_evidence,
            serving,
        )
    return providers_by_npi


def eligible_provider_npis(
    provider_fragments: Mapping[int, Any],
    args: Mapping[str, Any],
) -> tuple[int, ...]:
    """Apply the normal inferred-taxonomy rule to frozen provider witnesses."""

    from api import ptg2_serving as serving

    inferred_rule = serving._inferred_provider_taxonomy_rule(dict(args))
    if inferred_rule is None:
        return tuple(provider_fragments)
    allowed_codes = set(inferred_rule.taxonomy_codes)
    providers_by_npi = _validated_providers_by_npi(provider_fragments, args, serving)
    return tuple(
        npi
        for npi, provider_by_field in providers_by_npi.items()
        if type(provider_by_field.get("entity_type_code")) is int
        and provider_by_field["entity_type_code"] == 1
        and any(
            str(taxonomy_code).strip().upper() in allowed_codes
            for taxonomy_code in provider_by_field["taxonomy_codes"]
        )
    )


def _provider_payload_for_npi(
    providers_by_npi: Mapping[int, dict[str, Any]],
    npi: int,
    serving: Any,
) -> dict[str, Any]:
    provider_by_field = providers_by_npi.get(npi)
    if provider_by_field is None:
        raise PTG2ManifestArtifactError("pricing state scan cannot hydrate the projected provider")
    return serving._request_local_provider_payload(provider_by_field)


def _serving_row(occurrence_by_field: Mapping[str, Any], binding: Any) -> dict[str, Any]:
    group_fragment_by_field = occurrence_by_field.get("group_fragment")
    if not isinstance(group_fragment_by_field, dict):
        raise PTG2ManifestArtifactError("pricing state scan rate occurrence is invalid")
    return {
        **group_fragment_by_field,
        "serving_content_hash_128": str(occurrence_by_field["rate_pack_ref"]),
        "provider_set_global_id_128": str(occurrence_by_field["provider_set_ref"]),
        "_ptg_provider_set_key": int(occurrence_by_field["provider_set_key"]),
        "provider_count": int(occurrence_by_field["provider_count"]),
        "price_set_global_id_128": str(occurrence_by_field["price_set_ref"]),
        "price_key": int(occurrence_by_field["price_key"]),
        "source_key": int(occurrence_by_field["source_artifact_key"]),
        "source_artifact_key": int(occurrence_by_field["source_artifact_key"]),
        "logical_source_key": binding.source_key,
    }


def _binding_scope(
    selection: PlanReleaseServingSelection,
    args: Mapping[str, Any],
    occurrence_rows: list[dict[str, Any]],
    binding_ordinal: int,
) -> _BindingScope:
    binding_by_ordinal = {binding.binding_ordinal: binding for binding in selection.in_network_bindings}
    binding = binding_by_ordinal.get(binding_ordinal)
    if binding is None:
        raise PTG2ManifestArtifactError("pricing state scan references an unknown release binding")
    serving_tables = selection.serving_tables_for_snapshot(binding.snapshot_id)
    if serving_tables is None:
        raise PlanPricingProjectionUnavailable("the selected release is missing its validated serving tables")
    binding_occurrences = [
        occurrence_by_field
        for occurrence_by_field in occurrence_rows
        if int(occurrence_by_field["binding_ordinal"]) == binding_ordinal
    ]
    return _BindingScope(
        binding,
        serving_tables,
        binding_occurrences,
        binding_query_args(args, binding),
    )


async def _prices_for_scope(
    session: Any,
    scope: _BindingScope,
    remaining_price_atoms: int,
    serving: Any,
) -> tuple[dict[str, list[dict[str, Any]]], int]:
    price_key_by_set_id = {
        str(occurrence_by_field["price_set_ref"]): int(occurrence_by_field["price_key"])
        for occurrence_by_field in scope.occurrences
    }
    try:
        prices_by_key = await serving._version_three_bounded_prices_by_key(
            session,
            scope.serving_tables,
            tuple(price_key_by_set_id.values()),
            maximum_atom_count=remaining_price_atoms,
        )
    except ManifestReadLimitError as exc:
        raise PlanPricingStateScanBudgetExceeded("state scan page exceeds its complete price-atom budget") from exc
    prices_by_set = {
        price_set_id: prices_by_key.get(price_key, []) for price_set_id, price_key in price_key_by_set_id.items()
    }
    if any(not price_entries for price_entries in prices_by_set.values()):
        raise PTG2ManifestArtifactError("pricing state scan cannot hydrate a projected price set")
    logical_atom_count = sum(
        len(prices_by_set[str(occurrence_by_field["price_set_ref"])])
        * int(occurrence_by_field["occurrence_multiplicity"])
        for occurrence_by_field in scope.occurrences
    )
    if logical_atom_count > remaining_price_atoms:
        raise PlanPricingStateScanBudgetExceeded("state scan page exceeds its complete price-atom budget")
    return prices_by_set, logical_atom_count


async def _source_context(
    session: Any,
    scope: _BindingScope,
    serving_rows: list[dict[str, Any]],
    serving: Any,
) -> tuple[Mapping[Any, Any], Mapping[Any, Any]]:
    procedure_details = await serving._procedure_details_for_rows(session, serving_rows)
    source_provenance = (
        await serving._ptg2_source_provenance_for_rows(session, scope.serving_tables, serving_rows)
        if serving._include_ptg2_sources(dict(scope.query_args))
        else {}
    )
    return procedure_details, source_provenance


def _response_items(
    scope: _BindingScope,
    serving_rows: list[dict[str, Any]],
    providers_by_npi: Mapping[int, dict[str, Any]],
    prices_by_set: Mapping[str, list[dict[str, Any]]],
    procedure_details: Mapping[Any, Any],
    source_provenance: Mapping[Any, Any],
    serving: Any,
) -> list[dict[str, Any]]:
    response_items: list[dict[str, Any]] = []
    for occurrence_by_field, serving_data_by_field in zip(scope.occurrences, serving_rows, strict=True):
        npi = int(occurrence_by_field["npi"])
        provider_context = _provider_payload_for_npi(
            providers_by_npi, npi, serving
        )
        provenance_by_field = source_provenance.get(int(occurrence_by_field["source_artifact_key"]))
        if provenance_by_field is not None:
            serving_data_by_field.update(serving._item_source_provenance(provenance_by_field))
        catalog_key = serving._catalog_key(
            serving_data_by_field.get("reported_code_system"),
            serving_data_by_field.get("reported_code"),
        ) or ("", "")
        response_item_by_field = serving._ptg2_manifest_provider_procedure_item(
            npi=npi,
            serving_data=serving_data_by_field,
            prices=prices_by_set.get(str(occurrence_by_field["price_set_ref"]), []),
            procedure_detail=procedure_details.get(catalog_key, {}),
            provider_context=provider_context,
            args=scope.query_args,
        )
        for field_name in ("entity_type_code", "credential", "provider_sex_code"):
            if provider_context.get(field_name) is not None:
                response_item_by_field[field_name] = provider_context[field_name]
        response_items.extend(
            dict(response_item_by_field) for _ in range(int(occurrence_by_field["occurrence_multiplicity"]))
        )
    return response_items


async def _hydrate_binding(
    session: Any,
    scope: _BindingScope,
    providers_by_npi: Mapping[int, dict[str, Any]],
    remaining_price_atoms: int,
    serving: Any,
) -> tuple[list[dict[str, Any]], int]:
    prices_by_set, logical_atom_count = await _prices_for_scope(session, scope, remaining_price_atoms, serving)
    serving_rows = [_serving_row(occurrence_by_field, scope.binding) for occurrence_by_field in scope.occurrences]
    procedure_details, source_provenance = await _source_context(session, scope, serving_rows, serving)
    return (
        _response_items(
            scope,
            serving_rows,
            providers_by_npi,
            prices_by_set,
            procedure_details,
            source_provenance,
            serving,
        ),
        logical_atom_count,
    )


async def hydrate_selected_groups(
    session: Any,
    selection: PlanReleaseServingSelection,
    args: Mapping[str, Any],
    occurrence_rows: list[dict[str, Any]],
    provider_fragments: Mapping[int, Any],
) -> list[dict[str, Any]]:
    """Hydrate only projected complete groups for the selected NPI page."""

    from api import ptg2_serving as serving

    hydrated_items: list[dict[str, Any]] = []
    retained_price_atom_count = 0
    providers_by_npi = _validated_providers_by_npi(
        provider_fragments, args, serving
    )
    occurrence_npis = {int(occurrence_by_field["npi"]) for occurrence_by_field in occurrence_rows}
    if not occurrence_npis.issubset(providers_by_npi):
        raise PTG2ManifestArtifactError("pricing state scan provider witness is incomplete")
    binding_ordinals = sorted({int(occurrence_by_field["binding_ordinal"]) for occurrence_by_field in occurrence_rows})
    is_multi_binding = len(selection.in_network_bindings) > 1
    for binding_ordinal in binding_ordinals:
        scope = _binding_scope(selection, args, occurrence_rows, binding_ordinal)
        binding_items, logical_atom_count = await _hydrate_binding(
            session,
            scope,
            providers_by_npi,
            STATE_SCAN_PRICE_ATOM_LIMIT - retained_price_atom_count,
            serving,
        )
        binding_items = serving._merge_provider_rates_for_request(
            binding_items, {}
        )
        if is_multi_binding and scope.binding.source_key:
            for binding_item in binding_items:
                binding_item.setdefault("network", scope.binding.source_key)
        hydrated_items.extend(binding_items)
        retained_price_atom_count += logical_atom_count
    hydrated_items.sort(
        key=lambda merged_item: (
            int(merged_item.get("npi") or 0),
            str(merged_item.get("reported_code_system") or ""),
            str(merged_item.get("reported_code") or ""),
            str(merged_item.get("source_artifact_key") or ""),
        )
    )
    serving._hide_source_artifact_key_unless_requested(hydrated_items, args)
    return hydrated_items


__all__ = ["eligible_provider_npis", "hydrate_selected_groups"]
