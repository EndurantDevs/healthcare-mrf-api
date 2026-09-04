# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Frozen provider-cell materialization for pricing projection v3."""

from __future__ import annotations

import math
from typing import Any, Iterable, Mapping

import orjson
from sqlalchemy import text

from api.plan_pricing_projection_materialize import digest_row
from api.plan_pricing_projection_source import projection_provider_rows_for_npis
from api.plan_pricing_projection_v3_types import _BuildState, _insert_batches


PROVIDER_NPI_BATCH_SIZE = 5_000
MAX_PROVIDER_CELLS_PER_BATCH = 100_000
MAX_PROJECTION_PROVIDER_CELLS = 8_000_000
MAX_PROJECTION_PROVIDER_FRAGMENT_BYTES = 16 * 1024 * 1024 * 1024
MAX_PROVIDER_STATE_FRAGMENT_BYTES = 16 * 1024
PROVIDER_STATE_FRAGMENT_VERSION = "plan_pricing_provider_state_v1"
_EVIDENCE_SOURCE_ID = {
    "nppes_registry_address": 1,
    "multi_issuer_marketplace_address": 2,
    "cms_doctors_source_with_nppes_identity_anchor": 3,
}
_STATE_ADDRESS_FIELDS = frozenset(
    {
        "npi", "type", "first_line", "second_line", "city", "state",
        "postal_code", "country_code", "address_key", "location_key",
        "address_precision", "address_sources", "source_record_ids",
        "source_count", "multi_source_confirmed", "source_mask",
        "address_source_mask", "location_confidence_id",
        "geo_evidence_level", "address_provenance", "lat", "long",
    }
)
_PROVENANCE_FIELDS = (
    "dataset_id",
    "source_record_id",
    "record_version_id",
    "retrieved_at",
)
_CANONICAL_PROVENANCE_FIELDS = ("source_id", *_PROVENANCE_FIELDS)


async def _next_provider_npis(session: Any, after_npi: int) -> list[int]:
    result = await session.execute(
        text(
            f"""
            SELECT npi
              FROM plan_pricing_provider_npi_pending_stage
             WHERE npi > :after_npi
             ORDER BY npi
             LIMIT {PROVIDER_NPI_BATCH_SIZE}
            """
        ),
        {"after_npi": after_npi},
    )
    return [int(npi) for npi in result.scalars().all()]


def _normalized_taxonomy_codes(
    provider_by_field: Mapping[str, Any],
) -> tuple[str, ...]:
    return tuple(
        dict.fromkeys(
            normalized_code
            for taxonomy_code in provider_by_field.get("taxonomy_codes") or ()
            if (normalized_code := str(taxonomy_code).strip().upper())
        )
    )


def _provider_fragment(
    provider_by_field: Mapping[str, Any],
    taxonomy_codes: tuple[str, ...] | None = None,
) -> bytes:
    if taxonomy_codes is None:
        taxonomy_codes = _normalized_taxonomy_codes(provider_by_field)
    classifications = list(provider_by_field.get("classifications") or ())
    return orjson.dumps(
        {
            "npi": int(provider_by_field["npi"]),
            "provider_name": provider_by_field.get("provider_name")
            or "TiC provider",
            "entity_type_code": provider_by_field.get("entity_type_code"),
            "credential": provider_by_field.get("credential"),
            "taxonomy_code": taxonomy_codes[0] if taxonomy_codes else None,
            "primary_specialty": provider_by_field.get("primary_specialty"),
            "classification": classifications[0] if classifications else None,
            "city": provider_by_field.get("city"),
            "state": provider_by_field.get("state"),
            "zip5": provider_by_field["zip5"],
        }
    )


def _normalized_strings(values: Any) -> list[str]:
    return list(
        dict.fromkeys(
            normalized
            for value in values or ()
            if (normalized := str(value).strip())
        )
    )


def _is_string_list(value: Any) -> bool:
    return isinstance(value, list) and bool(value) and all(
        isinstance(item, str) and bool(item.strip()) for item in value
    )


def _has_valid_state_address_shape(address: Mapping[str, Any]) -> bool:
    address_key = address.get("address_key")
    return bool(
        _STATE_ADDRESS_FIELDS.issubset(address)
        and str(address.get("type") or "")
        in {"practice", "primary", "secondary", "site"}
        and (
            address_key is None
            or isinstance(address_key, str)
            and bool(address_key.strip())
        )
        and str(address.get("location_key") or "")
        and type(address.get("source_count")) is int
        and address.get("source_count") > 0
        and type(address.get("multi_source_confirmed")) is bool
        and type(address.get("source_mask")) is int
        and address.get("source_mask") >= 0
        and type(address.get("address_source_mask")) is int
        and address.get("address_source_mask") >= 0
        and type(address.get("location_confidence_id")) is int
        and address.get("location_confidence_id") >= 0
        and _is_string_list(address.get("address_sources"))
        and _is_string_list(address.get("source_record_ids"))
    )


def _state_address_payload(provider_by_field: Mapping[str, Any]) -> dict[str, Any]:
    raw_payload = provider_by_field.get("address_payload")
    try:
        address_payload = orjson.loads(raw_payload)
    except (orjson.JSONDecodeError, TypeError) as exc:
        raise ValueError("pricing projection provider-state address is invalid") from exc
    if not isinstance(address_payload, dict):
        raise ValueError("pricing projection provider-state address is invalid")
    return address_payload


def _state_code(value: Any) -> str | None:
    state = str(value or "").strip().upper()
    if not state:
        return None
    if len(state) != 2 or not state.isascii() or not state.isalpha():
        raise ValueError("pricing projection provider-state code is invalid")
    return state


def _source_id(value: Any) -> int | None:
    if type(value) is int and value > 0:
        return value
    return None


def _is_complete_provenance(entry: Any) -> bool:
    return (
        isinstance(entry, Mapping)
        and _source_id(entry.get("source_id")) is not None
        and all(entry.get(field_name) not in (None, "", []) for field_name in _PROVENANCE_FIELDS)
    )


def _has_valid_coordinates(address: Mapping[str, Any]) -> bool:
    latitude = address.get("lat")
    longitude = address.get("long")
    if latitude is None or longitude is None:
        return latitude is None and longitude is None
    if isinstance(latitude, bool) or isinstance(longitude, bool):
        return False
    try:
        lat_value = float(latitude)
        long_value = float(longitude)
    except (TypeError, ValueError, OverflowError):
        return False
    return (
        math.isfinite(lat_value)
        and math.isfinite(long_value)
        and -90 <= lat_value <= 90
        and -180 <= long_value <= 180
    )


def _validated_state_address(
    provider_by_field: Mapping[str, Any],
    address: Mapping[str, Any],
    npi: int,
    state: str,
) -> None:
    zip5 = str(provider_by_field.get("zip5") or "")
    postal_code = "".join(
        character
        for character in str(address.get("postal_code") or "")
        if character.isdigit()
    )[:5]
    location_key = str(address.get("location_key") or "")
    evidence_level = str(address.get("geo_evidence_level") or "")
    admitted_source_id = _EVIDENCE_SOURCE_ID.get(evidence_level)
    provenance = address.get("address_provenance")
    complete_provenance = (
        provenance
        if isinstance(provenance, list)
        and provenance
        and all(_is_complete_provenance(entry) for entry in provenance)
        else ()
    )
    if (
        not _has_valid_state_address_shape(address)
        or type(address.get("npi")) is not int
        or address.get("npi") != npi
        or str(address.get("state") or "") != state
        or postal_code != zip5
        or address.get("city") != provider_by_field.get("city")
        or not location_key
        or provider_by_field.get("location_hash")
        != f"entity_address_unified:{location_key}"
        or provider_by_field.get("location_source") != "entity_address_unified"
        or provider_by_field.get("location_confidence_code")
        != "entity_address_unified"
        or admitted_source_id is None
        or not any(
            _source_id(entry.get("source_id")) == admitted_source_id
            for entry in complete_provenance
        )
        or not _has_valid_coordinates(address)
    ):
        raise ValueError("pricing projection provider-state address is inconsistent")


def _canonical_state_address(address: Mapping[str, Any]) -> dict[str, Any]:
    provenance_entries = [
        {field_name: entry[field_name] for field_name in _CANONICAL_PROVENANCE_FIELDS}
        for entry in address["address_provenance"]
    ]
    admitted_source_id = _EVIDENCE_SOURCE_ID[str(address["geo_evidence_level"])]
    canonical_address_by_field = dict(address)
    canonical_address_by_field["address_provenance"] = provenance_entries
    canonical_address_by_field["source_record_ids"] = sorted(
        {
            str(entry["source_record_id"]).strip()
            for entry in provenance_entries
            if _source_id(entry["source_id"]) == admitted_source_id
        }
    )
    return canonical_address_by_field


def _provider_state_fragment(
    provider_by_field: Mapping[str, Any],
    taxonomy_codes: tuple[str, ...],
) -> bytes:
    address_payload = _state_address_payload(provider_by_field)
    npi = int(provider_by_field["npi"])
    state = _state_code(provider_by_field.get("state"))
    if state is None:
        raise ValueError("pricing projection provider-state code is missing")
    _validated_state_address(provider_by_field, address_payload, npi, state)
    address_payload = _canonical_state_address(address_payload)
    fragment = orjson.dumps(
        {
            "version": PROVIDER_STATE_FRAGMENT_VERSION,
            "provider": {
                "npi": npi,
                "provider_name": provider_by_field.get("provider_name") or "TiC provider",
                "entity_type_code": provider_by_field.get("entity_type_code"),
                "credential": provider_by_field.get("credential"),
                "provider_sex_code": provider_by_field.get("provider_sex_code"),
                "taxonomy_codes": list(taxonomy_codes),
                "specialties": _normalized_strings(provider_by_field.get("specialties")),
                "primary_specialty": provider_by_field.get("primary_specialty"),
                "classifications": _normalized_strings(provider_by_field.get("classifications")),
                "specializations": _normalized_strings(provider_by_field.get("specializations")),
                "primary_specialization": provider_by_field.get("primary_specialization"),
                "state": state,
                "city": provider_by_field.get("city"),
                "zip5": provider_by_field.get("zip5"),
                "location_hash": provider_by_field.get("location_hash"),
                "location_source": provider_by_field.get("location_source"),
                "location_confidence_code": provider_by_field.get("location_confidence_code"),
                "address_payload": address_payload,
            },
        },
    )
    if not 2 <= len(fragment) <= MAX_PROVIDER_STATE_FRAGMENT_BYTES:
        raise ValueError("pricing projection provider-state fragment bound exceeded")
    return fragment


def _state_fragment(
    provider_by_field: Mapping[str, Any],
    taxonomy_codes: tuple[str, ...],
) -> bytes | None:
    raw_rank = provider_by_field.get("state_address_rank")
    if raw_rank is None:
        return None
    if type(raw_rank) is not int or raw_rank <= 0:
        raise ValueError("pricing projection provider-state rank is invalid")
    return (
        _provider_state_fragment(provider_by_field, taxonomy_codes)
        if raw_rank == 1
        else None
    )


def _materialized_provider_cell(
    projection_id: str,
    state: _BuildState,
    npi: int,
    provider_by_field: Mapping[str, Any],
    state_witnesses: set[tuple[int, str]],
) -> tuple[dict[str, Any], str | None]:
    state_code = _state_code(provider_by_field.get("state"))
    taxonomy_codes = _normalized_taxonomy_codes(provider_by_field)
    fragment = _provider_fragment(provider_by_field, taxonomy_codes)
    state_fragment = _state_fragment(provider_by_field, taxonomy_codes)
    geo_cell = str(provider_by_field["zip5"])
    semantic_fragment = orjson.dumps(
        (
            fragment.decode("utf-8"),
            provider_by_field.get("entity_type_code"),
            taxonomy_codes,
        )
    )
    fragment_bytes = len(fragment) + len(state_fragment or b"")
    if (
        state.provider_cell_count >= MAX_PROJECTION_PROVIDER_CELLS
        or state.provider_fragment_byte_count + fragment_bytes
        > MAX_PROJECTION_PROVIDER_FRAGMENT_BYTES
    ):
        raise ValueError("pricing projection provider-cell bound exceeded")
    digest_row(
        state.content_digest,
        "provider-cell",
        (npi, geo_cell),
        semantic_fragment,
    )
    if state_fragment is not None:
        state_key = (npi, state_code or "")
        if state_key in state_witnesses:
            raise ValueError("pricing projection provider-state witness is duplicated")
        digest_row(
            state.content_digest,
            "provider-state",
            (state_key[1], npi),
            state_fragment,
        )
        state_witnesses.add(state_key)
        state.provider_state_count += 1
    state.provider_cell_count += 1
    state.provider_fragment_byte_count += fragment_bytes
    return (
        {
            "projection_id": projection_id,
            "geo_cell": geo_cell,
            "npi": npi,
            "entity_type_code": provider_by_field.get("entity_type_code"),
            "taxonomy_codes": list(taxonomy_codes),
            "fragment": fragment,
            "state_fragment": state_fragment,
        },
        state_code,
    )


def _provider_cell_rows(
    projection_id: str,
    state: _BuildState,
    npi_batch: list[int],
    providers_by_npi: Mapping[int, Iterable[Mapping[str, Any]]],
) -> list[dict[str, Any]]:
    if (
        set(providers_by_npi) - set(npi_batch)
        or sum(map(len, providers_by_npi.values()))
        > MAX_PROVIDER_CELLS_PER_BATCH
    ):
        raise ValueError("pricing projection provider-cell bound exceeded")
    provider_cell_rows: list[dict[str, Any]] = []
    state_witnesses: set[tuple[int, str]] = set()
    expected_state_witnesses: set[tuple[int, str]] = set()
    for npi in npi_batch:
        for provider_by_field in providers_by_npi.get(npi, ()):
            provider_cell_row, state_code = _materialized_provider_cell(
                projection_id,
                state,
                npi,
                provider_by_field,
                state_witnesses,
            )
            if state_code is not None:
                expected_state_witnesses.add((npi, state_code))
            provider_cell_rows.append(provider_cell_row)
    if state_witnesses != expected_state_witnesses:
        raise ValueError("pricing projection provider-state witness is incomplete")
    return provider_cell_rows


async def _materialize_provider_cells(
    session: Any,
    projection_id: str,
    state: _BuildState,
    *,
    next_provider_npis: Any = _next_provider_npis,
    provider_rows_for_npis: Any = projection_provider_rows_for_npis,
    provider_cell_rows: Any = _provider_cell_rows,
    insert_batches: Any = _insert_batches,
) -> None:
    after_npi = 0
    while True:
        npi_batch = await next_provider_npis(session, after_npi)
        if not npi_batch:
            break
        providers_by_npi = await provider_rows_for_npis(session, npi_batch)
        cell_rows = provider_cell_rows(
            projection_id, state, npi_batch, providers_by_npi
        )
        await insert_batches(
            session,
            """
            INSERT INTO plan_pricing_provider_cell_stage (
                projection_id, geo_cell, npi, entity_type_code,
                taxonomy_codes, fragment, state_fragment
            ) VALUES (
                :projection_id, :geo_cell, :npi, :entity_type_code,
                :taxonomy_codes, :fragment, :state_fragment
            )
            """,
            cell_rows,
        )
        await session.execute(
            text(
                """
                INSERT INTO plan_pricing_provider_npi_materialized_stage (npi)
                SELECT UNNEST(CAST(:npis AS bigint[]))
                ON CONFLICT DO NOTHING
                """
            ),
            {"npis": npi_batch},
        )
        await session.execute(
            text(
                """
                DELETE FROM plan_pricing_provider_npi_pending_stage
                 WHERE npi = ANY(CAST(:npis AS bigint[]))
                """
            ),
            {"npis": npi_batch},
        )
        after_npi = npi_batch[-1]
