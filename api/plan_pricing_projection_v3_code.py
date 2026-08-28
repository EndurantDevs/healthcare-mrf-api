# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bounded code and price staging for pricing projection v3."""

from __future__ import annotations

from collections import Counter
from decimal import Decimal
from typing import Any, Iterable, Mapping

import orjson
from sqlalchemy import text

from api.plan_pricing_projection_contract import row_mapping, table
from api.plan_pricing_projection_materialize import digest_row, rate_fragment
from api.plan_pricing_projection_source import BindingProjection, numeric_rates
from api.plan_pricing_projection_v3_provider import (
    _binding_ordinal,
    _stage_code_provider_sets,
)
from api.plan_pricing_projection_v3_types import _BuildState, _insert_batches


MAX_CODE_OCCURRENCES = 65_536
MAX_CODE_PRICE_ATOMS = MAX_CODE_OCCURRENCES
MAX_CODE_RATE_PROFILE_WORK_ROWS = 8_000_000
MAX_PROJECTION_RATE_PROFILE_WORK_ROWS = 2_000_000_000


_STORE_RATE_PROFILES_SQL = f"""
    INSERT INTO {table('plan_pricing_rate_profile')} (
        projection_id, code_system, code, binding_ordinal,
        provider_set_key, membership_count, minimum_negotiated_rate,
        maximum_negotiated_rate, rate_count, negotiated_rates,
        rate_multiplicities
    )
    WITH rate_frequency AS MATERIALIZED (
        SELECT occurrence.binding_ordinal, occurrence.provider_set_key,
               price.negotiated_rate,
               SUM(occurrence.occurrence_count
                   * price.rate_multiplicity)::bigint AS multiplicity
          FROM plan_pricing_code_occurrence_stage occurrence
          JOIN plan_pricing_price_rate_stage price
            ON price.binding_ordinal = occurrence.binding_ordinal
           AND price.price_set_id = occurrence.price_set_id
          JOIN plan_pricing_provider_set_stage membership
            ON membership.binding_ordinal = occurrence.binding_ordinal
           AND membership.provider_set_key = occurrence.provider_set_key
         WHERE membership.membership_count > 0
         GROUP BY occurrence.binding_ordinal, occurrence.provider_set_key,
                  price.negotiated_rate
    )
    SELECT :projection_id, :code_system, :code,
           rate.binding_ordinal, rate.provider_set_key,
           membership.membership_count,
           MIN(rate.negotiated_rate), MAX(rate.negotiated_rate),
           SUM(rate.multiplicity)::bigint,
           ARRAY_AGG(rate.negotiated_rate ORDER BY rate.negotiated_rate),
           ARRAY_AGG(rate.multiplicity ORDER BY rate.negotiated_rate)
      FROM rate_frequency rate
      JOIN plan_pricing_provider_set_stage membership
        USING (binding_ordinal, provider_set_key)
     GROUP BY rate.binding_ordinal, rate.provider_set_key,
              membership.membership_count
     ORDER BY rate.binding_ordinal, rate.provider_set_key
"""


def _manifest_id(serving: Any, value: Any) -> str | None:
    return serving._ptg2_manifest_id(value)


async def _binding_code_rows(
    session: Any,
    binding: BindingProjection,
    code_rows: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], Mapping[str, Iterable[Mapping[str, Any]]]]:
    from api import ptg2_serving as serving

    declared_occurrences = serving._declared_geo_rate_count(code_rows)
    if declared_occurrences > MAX_CODE_OCCURRENCES:
        raise ValueError("pricing projection code occurrence bound exceeded")
    serving_rows = await serving._merge_manifest_code_variant_rows(
        session,
        binding.serving_tables,
        code_rows=code_rows,
        provider_set_keys=None,
        source_trace_set_hash=None,
        network_names=binding.serving_tables.network_names or [],
        limit=MAX_CODE_OCCURRENCES + 1,
        offset=0,
    )
    if serving_rows is None or len(serving_rows) != declared_occurrences:
        raise ValueError("pricing projection could not read a bounded rate layout")
    price_key_by_set_id: dict[str, int] = {}
    for serving_row in serving_rows:
        price_set_id = _manifest_id(
            serving, serving_row.get("price_set_global_id_128")
        )
        raw_price_key = serving_row.get("price_key")
        raw_provider_set_key = serving_row.get("_ptg_provider_set_key")
        if (
            not price_set_id
            or isinstance(raw_price_key, bool)
            or isinstance(raw_provider_set_key, bool)
        ):
            raise ValueError("pricing projection rate identity is incomplete")
        try:
            price_key = int(raw_price_key)
            int(raw_provider_set_key)
        except (TypeError, ValueError, OverflowError) as exc:
            raise ValueError(
                "pricing projection rate identity is incomplete"
            ) from exc
        existing_price_key = price_key_by_set_id.setdefault(
            price_set_id, price_key
        )
        if existing_price_key != price_key:
            raise ValueError("pricing projection price identity is inconsistent")
    prices_by_key = await serving._version_three_bounded_prices_by_key(
        session,
        binding.serving_tables,
        price_key_by_set_id.values(),
        maximum_atom_count=MAX_CODE_PRICE_ATOMS,
    )
    return serving_rows, {
        price_set_id: prices_by_key[price_key]
        for price_set_id, price_key in price_key_by_set_id.items()
    }


def _exact_numeric_rates(
    prices: Iterable[Mapping[str, Any]],
) -> tuple[Decimal, ...]:
    price_rows = tuple(prices)
    rates = numeric_rates(price_rows)
    if len(rates) != len(price_rows):
        raise ValueError("pricing projection contains a non-numeric rate")
    return rates


def _code_occurrences(
    serving: Any,
    serving_rows: Iterable[Mapping[str, Any]],
) -> Counter[tuple[int, str]]:
    occurrences: Counter[tuple[int, str]] = Counter()
    for serving_row in serving_rows:
        provider_set_key = int(serving_row["_ptg_provider_set_key"])
        price_set_id = _manifest_id(
            serving, serving_row["price_set_global_id_128"]
        )
        if price_set_id is None:
            raise ValueError("pricing projection rate identity is incomplete")
        occurrences[(provider_set_key, price_set_id)] += 1
    return occurrences


async def _insert_code_occurrences(
    session: Any,
    binding_ordinal: int,
    occurrences: Counter[tuple[int, str]],
    *,
    insert_batches: Any = _insert_batches,
) -> None:
    await insert_batches(
        session,
        """
        INSERT INTO plan_pricing_code_occurrence_stage (
            binding_ordinal, provider_set_key, price_set_id, occurrence_count
        ) VALUES (
            :binding_ordinal, :provider_set_key, :price_set_id,
            :occurrence_count
        )
        """,
        (
            {
                "binding_ordinal": binding_ordinal,
                "provider_set_key": provider_set_key,
                "price_set_id": price_set_id,
                "occurrence_count": occurrence_count,
            }
            for (provider_set_key, price_set_id), occurrence_count in sorted(
                occurrences.items()
            )
        ),
    )


async def _insert_price_rates(
    session: Any,
    binding_ordinal: int,
    rates_by_price_id: Mapping[str, tuple[Decimal, ...]],
    *,
    insert_batches: Any = _insert_batches,
) -> None:
    await insert_batches(
        session,
        """
        INSERT INTO plan_pricing_price_rate_stage (
            binding_ordinal, price_set_id, negotiated_rate, rate_multiplicity
        ) VALUES (
            :binding_ordinal, :price_set_id, :negotiated_rate,
            :rate_multiplicity
        )
        """,
        (
            {
                "binding_ordinal": binding_ordinal,
                "price_set_id": price_set_id,
                "negotiated_rate": rate,
                "rate_multiplicity": multiplicity,
            }
            for price_set_id in sorted(rates_by_price_id)
            for rate, multiplicity in sorted(
                Counter(rates_by_price_id[price_set_id]).items()
            )
        ),
    )


async def _has_staged_code_inputs(
    session: Any,
    state: _BuildState,
    code_identity: tuple[str, str],
    bindings: list[BindingProjection],
    *,
    binding_code_rows: Any = _binding_code_rows,
    stage_code_provider_sets: Any = _stage_code_provider_sets,
) -> bool:
    from api import ptg2_serving as serving

    normalized_occurrence_count = sum(
        serving._declared_geo_rate_count(
            binding.code_rows_by_identity.get(code_identity) or ()
        )
        for binding in bindings
    )
    if normalized_occurrence_count > MAX_CODE_OCCURRENCES:
        raise ValueError(
            "pricing projection normalized occurrence bound exceeded"
        )
    await session.execute(text("TRUNCATE plan_pricing_code_occurrence_stage"))
    await session.execute(text("TRUNCATE plan_pricing_price_rate_stage"))
    has_staged_rates = False
    bounded_inputs = []
    normalized_atom_count = 0
    for binding in sorted(bindings, key=_binding_ordinal):
        bounded_input = await _bounded_binding_code_input(
            session,
            binding,
            code_identity,
            binding_code_rows,
        )
        if bounded_input is None:
            continue
        rates_by_price_id = bounded_input[3]
        normalized_atom_count += sum(map(len, rates_by_price_id.values()))
        if normalized_atom_count > MAX_CODE_PRICE_ATOMS:
            raise ValueError(
                "pricing projection normalized price-atom bound exceeded"
            )
        bounded_inputs.append(bounded_input)
    for binding, serving_rows, occurrences, rates_by_price_id in bounded_inputs:
        await stage_code_provider_sets(
            session,
            binding,
            serving_rows,
            {provider_set_key for provider_set_key, _ in occurrences},
            state,
        )
        has_staged_rates = True
        binding_ordinal = _binding_ordinal(binding)
        await _insert_code_occurrences(session, binding_ordinal, occurrences)
        await _insert_price_rates(session, binding_ordinal, rates_by_price_id)
    return has_staged_rates


async def _bounded_binding_code_input(
    session: Any,
    binding: BindingProjection,
    code_identity: tuple[str, str],
    binding_code_rows: Any,
) -> tuple[
    BindingProjection,
    list[dict[str, Any]],
    Counter[tuple[int, str]],
    dict[str, tuple[Decimal, ...]],
] | None:
    code_rows = binding.code_rows_by_identity.get(code_identity)
    if not code_rows:
        return None
    serving_rows, prices_by_set = await binding_code_rows(
        session, binding, code_rows
    )
    from api import ptg2_serving as serving

    occurrences = _code_occurrences(serving, serving_rows)
    rates_by_price_id = {
        price_set_id: _exact_numeric_rates(prices_by_set.get(price_set_id, ()))
        for _provider_set_key, price_set_id in occurrences
    }
    occurrences = Counter(
        {
            key: count
            for key, count in occurrences.items()
            if rates_by_price_id[key[1]]
        }
    )
    if not occurrences:
        return None
    return binding, serving_rows, occurrences, rates_by_price_id


def _rate_profile_fragment(
    rates: tuple[Decimal, ...],
    multiplicities: tuple[int, ...],
) -> bytes:
    return orjson.dumps(
        [
            (rate_fragment(rate), multiplicity)
            for rate, multiplicity in zip(rates, multiplicities, strict=True)
        ]
    )


def _validated_rate_profile(raw_profile: Mapping[str, Any]) -> tuple[Any, ...]:
    profile_by_field = row_mapping(raw_profile)
    rates = tuple(Decimal(rate) for rate in profile_by_field["negotiated_rates"])
    multiplicities = tuple(
        int(multiplicity)
        for multiplicity in profile_by_field["rate_multiplicities"]
    )
    membership_count = int(profile_by_field["membership_count"])
    rate_count = int(profile_by_field["rate_count"])
    if (
        not rates
        or len(rates) != len(multiplicities)
        or len(rates) > MAX_CODE_PRICE_ATOMS
        or rates != tuple(sorted(set(rates)))
        or any(multiplicity <= 0 for multiplicity in multiplicities)
        or sum(multiplicities) != rate_count
        or rates[0] != Decimal(profile_by_field["minimum_negotiated_rate"])
        or rates[-1] != Decimal(profile_by_field["maximum_negotiated_rate"])
        or not 1 <= membership_count <= 16_384
    ):
        raise ValueError("pricing projection rate profile is invalid")
    return (
        int(profile_by_field["binding_ordinal"]),
        int(profile_by_field["provider_set_key"]),
        membership_count,
        rates,
        multiplicities,
        rate_count,
    )


async def _store_rate_profiles(
    session: Any,
    projection_id: str,
    code_identity: tuple[str, str],
    state: _BuildState,
    *,
    store_sql: str = _STORE_RATE_PROFILES_SQL,
) -> None:
    """Persist one code's exact rate profiles and bind them into the receipt."""

    query_parameters_by_name = {
        "projection_id": projection_id,
        "code_system": code_identity[0],
        "code": code_identity[1],
    }
    await session.execute(text(store_sql), query_parameters_by_name)
    profile_stream = await session.stream(
        text(
            f"""
            SELECT binding_ordinal, provider_set_key, membership_count,
                   minimum_negotiated_rate, maximum_negotiated_rate,
                   rate_count, negotiated_rates, rate_multiplicities
              FROM {table('plan_pricing_rate_profile')}
             WHERE projection_id = :projection_id
               AND code_system = :code_system
               AND code = :code
             ORDER BY binding_ordinal, provider_set_key
            """
        ).execution_options(yield_per=32),
        query_parameters_by_name,
    )
    async for raw_profile in profile_stream.mappings():
        _digest_rate_profile(raw_profile, code_identity, state)


def _digest_rate_profile(
    raw_profile: Mapping[str, Any],
    code_identity: tuple[str, str],
    state: _BuildState,
) -> None:
    """Validate and authenticate one stored rate profile."""

    (
        binding_ordinal,
        provider_set_key,
        membership_count,
        rates,
        multiplicities,
        rate_count,
    ) = _validated_rate_profile(raw_profile)
    digest_row(
        state.content_digest,
        "rate-profile",
        (
            code_identity[0],
            code_identity[1],
            binding_ordinal,
            provider_set_key,
            membership_count,
            rate_count,
        ),
        _rate_profile_fragment(rates, multiplicities),
    )
    state.rate_profile_count += 1
