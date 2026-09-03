# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exact rate occurrences retained by plan-pricing projection v4."""

from __future__ import annotations

from collections import Counter
from typing import Any, Iterable, Mapping

import orjson
from sqlalchemy import text

from api.plan_pricing_projection_contract import row_mapping, table
from api.plan_pricing_projection_materialize import digest_row
from api.plan_pricing_projection_source import BindingProjection
from api.plan_pricing_projection_v3_provider import _binding_ordinal
from api.plan_pricing_projection_v3_types import _BuildState, _insert_batches


_RATE_OCCURRENCE_GROUP_FIELDS = (
    "plan_id",
    "plan_market_type",
    "reported_code_system",
    "reported_code",
    "negotiation_arrangement",
    "billing_code_type_version",
    "source_procedure_name",
    "source_procedure_description",
    "network_names",
)
_STORE_OCCURRENCES_SQL = f"""
    INSERT INTO {table('plan_pricing_rate_occurrence')} (
        projection_id, code_system, code, binding_ordinal,
        occurrence_ordinal, provider_set_key, provider_set_ref,
        price_key, price_set_ref, rate_pack_ref,
        source_artifact_key, provider_count, group_fragment,
        occurrence_multiplicity
    )
    SELECT :projection_id, :code_system, :code, binding_ordinal,
           ROW_NUMBER() OVER (
               PARTITION BY binding_ordinal
               ORDER BY provider_set_key, provider_set_ref,
                        price_key, price_set_ref, rate_pack_ref,
                        source_artifact_key, group_fragment::text
           ) - 1,
           provider_set_key, provider_set_ref, price_key,
           price_set_ref, rate_pack_ref, source_artifact_key,
           provider_count, group_fragment, occurrence_multiplicity
      FROM plan_pricing_rate_occurrence_stage
     ORDER BY binding_ordinal, provider_set_key, provider_set_ref,
              price_key, price_set_ref, rate_pack_ref,
              source_artifact_key, group_fragment::text
"""
_READ_OCCURRENCES_SQL = f"""
    SELECT binding_ordinal, occurrence_ordinal, provider_set_key,
           provider_set_ref, price_key, price_set_ref, rate_pack_ref,
           source_artifact_key, provider_count, group_fragment,
           occurrence_multiplicity
      FROM {table('plan_pricing_rate_occurrence')}
     WHERE projection_id = :projection_id
       AND code_system = :code_system AND code = :code
     ORDER BY binding_ordinal, occurrence_ordinal
"""


def _rate_occurrence_fragment(serving_row: Mapping[str, Any]) -> bytes:
    """Retain every source field used by the final provider-rate group key."""

    fragment_by_field = {field: serving_row.get(field) for field in _RATE_OCCURRENCE_GROUP_FIELDS}
    network_names = fragment_by_field["network_names"]
    if network_names is None:
        fragment_by_field["network_names"] = []
    elif isinstance(network_names, (list, tuple)):
        normalized_network_names = {
            str(network_name or "").strip() for network_name in network_names if str(network_name or "").strip()
        }
        fragment_by_field["network_names"] = sorted(normalized_network_names)
    else:
        raise ValueError("pricing projection rate occurrence is invalid")
    return orjson.dumps(fragment_by_field, option=orjson.OPT_SORT_KEYS)


def _validated_occurrence_key(
    serving: Any,
    serving_row: Mapping[str, Any],
) -> tuple[Any, ...]:
    provider_set_ref = serving._ptg2_manifest_id(serving_row.get("provider_set_global_id_128"))
    price_set_ref = serving._ptg2_manifest_id(serving_row.get("price_set_global_id_128"))
    rate_pack_ref = serving._ptg2_manifest_id(serving_row.get("serving_content_hash_128"))
    raw_numbers_by_name = {
        "provider_set_key": serving_row.get("_ptg_provider_set_key"),
        "price_key": serving_row.get("price_key"),
        "source_artifact_key": serving_row.get("source_key"),
        "provider_count": serving_row.get("provider_count"),
    }
    if (
        not provider_set_ref
        or not price_set_ref
        or not rate_pack_ref
        or any(isinstance(raw_number, bool) or raw_number is None for raw_number in raw_numbers_by_name.values())
    ):
        raise ValueError("pricing projection rate occurrence is incomplete")
    try:
        numbers_by_name = {name: int(raw_number) for name, raw_number in raw_numbers_by_name.items()}
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError("pricing projection rate occurrence is incomplete") from exc
    if any(number < 0 for number in numbers_by_name.values()):
        raise ValueError("pricing projection rate occurrence is invalid")
    return (
        numbers_by_name["provider_set_key"],
        provider_set_ref,
        numbers_by_name["price_key"],
        price_set_ref,
        rate_pack_ref,
        numbers_by_name["source_artifact_key"],
        numbers_by_name["provider_count"],
        _rate_occurrence_fragment(serving_row),
    )


def _occurrence_record(
    binding_ordinal: int,
    occurrence_key: tuple[Any, ...],
    multiplicity: int,
) -> dict[str, Any]:
    return {
        "binding_ordinal": binding_ordinal,
        "provider_set_key": occurrence_key[0],
        "provider_set_ref": occurrence_key[1],
        "price_key": occurrence_key[2],
        "price_set_ref": occurrence_key[3],
        "rate_pack_ref": occurrence_key[4],
        "source_artifact_key": occurrence_key[5],
        "provider_count": occurrence_key[6],
        "group_fragment": occurrence_key[7].decode("utf-8"),
        "occurrence_multiplicity": multiplicity,
    }


def rate_occurrence_rows(
    serving: Any,
    binding_ordinal: int,
    serving_rows: Iterable[Mapping[str, Any]],
    retained_price_ids: set[str],
) -> Iterable[dict[str, Any]]:
    """Collapse only byte-identical occurrences while preserving multiplicity."""

    occurrence_counts: Counter[tuple[Any, ...]] = Counter()
    for serving_row in serving_rows:
        occurrence_key = _validated_occurrence_key(serving, serving_row)
        if occurrence_key[3] in retained_price_ids:
            occurrence_counts[occurrence_key] += 1
    for occurrence_key, multiplicity in sorted(occurrence_counts.items()):
        yield _occurrence_record(binding_ordinal, occurrence_key, multiplicity)


async def insert_rate_occurrences(
    session: Any,
    binding: BindingProjection,
    serving_rows: Iterable[Mapping[str, Any]],
    retained_price_ids: set[str],
    *,
    insert_batches: Any = _insert_batches,
) -> None:
    """Persist bounded v4 rate occurrences for one release binding."""

    from api import ptg2_serving as serving

    await insert_batches(
        session,
        """
        INSERT INTO plan_pricing_rate_occurrence_stage (
            binding_ordinal, provider_set_key, provider_set_ref,
            price_key, price_set_ref, rate_pack_ref, source_artifact_key,
            provider_count, group_fragment, occurrence_multiplicity
        ) VALUES (
            :binding_ordinal, :provider_set_key, :provider_set_ref,
            :price_key, :price_set_ref, :rate_pack_ref, :source_artifact_key,
            :provider_count, CAST(:group_fragment AS jsonb),
            :occurrence_multiplicity
        )
        """,
        rate_occurrence_rows(
            serving,
            _binding_ordinal(binding),
            serving_rows,
            retained_price_ids,
        ),
    )


def _digest_occurrence(
    raw_occurrence: Mapping[str, Any],
    code_identity: tuple[str, str],
    state: _BuildState,
) -> None:
    occurrence_by_field = row_mapping(raw_occurrence)
    identity = tuple(
        int(occurrence_by_field[field])
        for field in (
            "binding_ordinal",
            "occurrence_ordinal",
            "provider_set_key",
            "price_key",
            "source_artifact_key",
            "provider_count",
            "occurrence_multiplicity",
        )
    ) + tuple(str(occurrence_by_field[field]) for field in ("provider_set_ref", "price_set_ref", "rate_pack_ref"))
    if identity[6] <= 0:
        raise ValueError("pricing projection rate occurrence is invalid")
    digest_row(
        state.content_digest,
        "rate-occurrence",
        (code_identity[0], code_identity[1], *identity),
        orjson.dumps(occurrence_by_field["group_fragment"], option=orjson.OPT_SORT_KEYS),
    )
    state.rate_occurrence_count += 1


async def store_rate_occurrences(
    session: Any,
    projection_id: str,
    code_identity: tuple[str, str],
    state: _BuildState,
) -> None:
    """Persist and authenticate exact group inputs for one admitted code."""

    parameters_by_name = {
        "projection_id": projection_id,
        "code_system": code_identity[0],
        "code": code_identity[1],
    }
    await session.execute(text(_STORE_OCCURRENCES_SQL), parameters_by_name)
    occurrence_stream = await session.stream(
        text(_READ_OCCURRENCES_SQL).execution_options(yield_per=1),
        parameters_by_name,
    )
    async for raw_occurrence in occurrence_stream.mappings():
        _digest_occurrence(raw_occurrence, code_identity, state)


__all__ = [
    "insert_rate_occurrences",
    "rate_occurrence_rows",
    "store_rate_occurrences",
]
