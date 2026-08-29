# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bounded code and price staging for pricing projection v3."""

from __future__ import annotations

from collections import Counter
from decimal import Decimal
from typing import Any, Awaitable, Callable, Iterable, Mapping

import orjson
from sqlalchemy import text

from api.plan_pricing_projection_contract import row_mapping, table
from api.plan_pricing_projection_materialize import digest_row, rate_fragment
from api.plan_pricing_projection_source import BindingProjection
from api.plan_pricing_projection_v3_provider import (
    _binding_ordinal,
    _stage_code_provider_sets,
)
from api.plan_pricing_projection_v3_price import (
    MAX_CODE_STAGED_PRICE_ATOMS,
    MAX_PRICE_HYDRATION_ATOMS,
    _exact_numeric_rates,
    _insert_price_rates,
    _stage_binding_price_rates,
)
from api.plan_pricing_projection_v3_types import _BuildState, _insert_batches
from api.ptg2_db_sidecars import _preflight_price_membership_aliases_from_db
from process.ptg_parts.ptg2_manifest_artifacts import ManifestReadLimitError


MAX_CODE_OCCURRENCES = 65_536
MAX_RATE_PROFILE_RATES = 65_536
MAX_CODE_RATE_PROFILE_WORK_ROWS = 8_000_000
MAX_PROJECTION_RATE_PROFILE_WORK_ROWS = 2_000_000_000
_BoundedBindingInput = tuple[
    BindingProjection,
    list[dict[str, Any]],
    Counter[tuple[int, str]],
    dict[str, int],
]


class _PriceMembershipMetadataReadLimitError(ManifestReadLimitError):
    """Identify bounded price-membership metadata admission failures."""


class _PriceHydrationReadLimitError(ManifestReadLimitError):
    """Identify bounded price-hydration admission failures."""


async def _diagnostic_checkpoint(
    diagnostic_stage: Callable[[str], Awaitable[str | None]] | None,
    stage: str,
) -> None:
    """Record one optional census-only database stage."""

    if diagnostic_stage is not None:
        await diagnostic_stage(stage)


_PROFILE_RATE_LIMIT_SQL = """
    SELECT EXISTS (
        SELECT 1
          FROM plan_pricing_rate_frequency_stage
         GROUP BY binding_ordinal, provider_set_key
        HAVING COUNT(*)
               > :maximum_rate_profile_rates
         LIMIT 1
    )
"""


_STORE_RATE_PROFILES_SQL = f"""
    INSERT INTO {table('plan_pricing_rate_profile')} (
        projection_id, code_system, code, binding_ordinal,
        provider_set_key, membership_count, minimum_negotiated_rate,
        maximum_negotiated_rate, rate_count, negotiated_rates,
        rate_multiplicities
    )
    SELECT :projection_id, :code_system, :code,
           rate.binding_ordinal, rate.provider_set_key,
           membership.membership_count,
           MIN(rate.negotiated_rate), MAX(rate.negotiated_rate),
           SUM(rate.multiplicity)::bigint,
           ARRAY_AGG(rate.negotiated_rate ORDER BY rate.negotiated_rate),
           ARRAY_AGG(rate.multiplicity ORDER BY rate.negotiated_rate)
      FROM plan_pricing_rate_frequency_stage rate
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
) -> tuple[list[dict[str, Any]], dict[str, int]]:
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
    return serving_rows, price_key_by_set_id


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


async def _has_staged_code_inputs(
    session: Any,
    state: _BuildState,
    code_identity: tuple[str, str],
    bindings: list[BindingProjection],
    *,
    binding_code_rows: Any = _binding_code_rows,
    stage_code_provider_sets: Any = _stage_code_provider_sets,
    preflight_price_membership_aliases: Any = (
        _preflight_price_membership_aliases_from_db
    ),
    diagnostic_stage: Callable[[str], Awaitable[str | None]] | None = None,
) -> bool:
    """Stage every bounded binding that contributes the requested code."""

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
    await _diagnostic_checkpoint(diagnostic_stage, "reset_code_inputs")
    await session.execute(text("TRUNCATE plan_pricing_code_occurrence_stage"))
    await session.execute(text("TRUNCATE plan_pricing_price_rate_stage"))
    has_staged_rates = False
    remaining_atom_count = MAX_CODE_STAGED_PRICE_ATOMS
    for binding in sorted(bindings, key=_binding_ordinal):
        await _diagnostic_checkpoint(diagnostic_stage, "code_layout")
        bounded_input = await _bounded_binding_code_input(
            session,
            binding,
            code_identity,
            binding_code_rows,
        )
        if bounded_input is None:
            continue
        staged_rates, consumed_atom_count = await _stage_bounded_binding_input(
            session,
            state,
            bounded_input,
            remaining_atom_count,
            stage_code_provider_sets,
            preflight_price_membership_aliases,
            diagnostic_stage,
        )
        remaining_atom_count -= consumed_atom_count
        has_staged_rates = has_staged_rates or staged_rates
    return has_staged_rates


async def _stage_bounded_binding_input(
    session: Any,
    state: _BuildState,
    bounded_input: _BoundedBindingInput,
    remaining_atom_count: int,
    stage_code_provider_sets: Any,
    preflight_price_membership_aliases: Any,
    diagnostic_stage: Callable[[str], Awaitable[str | None]] | None = None,
) -> tuple[bool, int]:
    """Stage one binding after metadata and price-rate admission."""

    binding, serving_rows, occurrences, price_key_by_set_id = bounded_input
    try:
        await _diagnostic_checkpoint(diagnostic_stage, "price_membership_metadata")
        block_span = await _preflight_binding_price_memberships(
            session,
            state,
            binding,
            price_key_by_set_id,
            preflight_price_membership_aliases,
        )
    except ManifestReadLimitError as exc:
        raise _PriceMembershipMetadataReadLimitError(str(exc)) from exc
    try:
        await _diagnostic_checkpoint(diagnostic_stage, "price_hydration")
        retained_price_ids, consumed_atom_count = (
            await _stage_binding_price_rates(
                session,
                binding,
                price_key_by_set_id,
                maximum_atom_count=remaining_atom_count,
                block_span=block_span,
            )
        )
    except ManifestReadLimitError as exc:
        raise _PriceHydrationReadLimitError(str(exc)) from exc
    occurrences = Counter(
        {
            key: count
            for key, count in occurrences.items()
            if key[1] in retained_price_ids
        }
    )
    if not occurrences:
        return False, consumed_atom_count
    await _diagnostic_checkpoint(diagnostic_stage, "provider_set_staging")
    await stage_code_provider_sets(
        session,
        binding,
        serving_rows,
        {provider_set_key for provider_set_key, _ in occurrences},
        state,
    )
    await _diagnostic_checkpoint(diagnostic_stage, "code_occurrence_staging")
    await _insert_code_occurrences(
        session, _binding_ordinal(binding), occurrences
    )
    return True, consumed_atom_count


async def _preflight_binding_price_memberships(
    session: Any,
    state: _BuildState,
    binding: BindingProjection,
    price_key_by_set_id: Mapping[str, int],
    preflight_price_membership_aliases: Any,
) -> int:
    """Validate and retain one binding's bounded price metadata identity."""

    from api import ptg2_serving as serving

    block_span = serving._required_price_cache_span(
        binding.serving_tables.price_key_block_span,
        "price_key_block_span",
    )
    await preflight_price_membership_aliases(
        session,
        serving._required_shared_snapshot_key(binding.serving_tables),
        price_key_by_set_id.values(),
        block_span=block_span,
        schema_name=serving.PTG2_SCHEMA,
        cache=state.price_membership_alias_cache,
    )
    return block_span


async def _bounded_binding_code_input(
    session: Any,
    binding: BindingProjection,
    code_identity: tuple[str, str],
    binding_code_rows: Any,
) -> _BoundedBindingInput | None:
    code_rows = binding.code_rows_by_identity.get(code_identity)
    if not code_rows:
        return None
    serving_rows, price_key_by_set_id = await binding_code_rows(
        session, binding, code_rows
    )
    from api import ptg2_serving as serving

    occurrences = _code_occurrences(serving, serving_rows)
    selected_price_ids = {price_set_id for _, price_set_id in occurrences}
    if any(
        price_set_id not in price_key_by_set_id
        for price_set_id in selected_price_ids
    ):
        raise ValueError("pricing projection price hydration is incomplete")
    return binding, serving_rows, occurrences, {
        price_set_id: price_key_by_set_id[price_set_id]
        for price_set_id in selected_price_ids
    }


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
        or len(rates) > MAX_RATE_PROFILE_RATES
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
    if await session.scalar(
        text(_PROFILE_RATE_LIMIT_SQL),
        {"maximum_rate_profile_rates": MAX_RATE_PROFILE_RATES},
    ):
        raise ValueError("pricing projection rate profile is too large")
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
        ).execution_options(yield_per=1),
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
