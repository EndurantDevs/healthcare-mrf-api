# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bounded price hydration and TEMP staging for projection v3."""

from __future__ import annotations

from collections import Counter
from decimal import Decimal
from itertools import groupby
from typing import Any, Mapping

from api.plan_pricing_projection_source import BindingProjection, numeric_rates
from api.plan_pricing_projection_v3_provider import _binding_ordinal
from api.plan_pricing_projection_v3_types import _insert_batches
from process.ptg_parts.ptg2_manifest_artifacts import ManifestReadLimitError


MAX_PRICE_HYDRATION_ATOMS = 65_536
MAX_CODE_STAGED_PRICE_ATOMS = 8_000_000


def _exact_numeric_rates(prices: Any) -> tuple[Decimal, ...]:
    price_rows = tuple(prices)
    rates = numeric_rates(price_rows)
    if len(rates) != len(price_rows):
        raise ValueError("pricing projection contains a non-numeric rate")
    return rates


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


def _price_key_batches(
    price_key_by_set_id: Mapping[str, int],
    block_span: int,
) -> list[tuple[tuple[str, int], ...]]:
    price_items = sorted(
        price_key_by_set_id.items(), key=lambda item: (item[1], item[0])
    )
    batches = [
        tuple(block_items)
        for _block_key, block_items in groupby(
            price_items,
            key=lambda item: item[1] // block_span,
        )
    ]
    batches.reverse()
    return batches


async def _price_batch_rates(
    session: Any,
    binding: BindingProjection,
    batch: tuple[tuple[str, int], ...],
) -> dict[str, tuple[Decimal, ...]]:
    from api import ptg2_serving as serving

    requested_price_keys = tuple(sorted({price_key for _, price_key in batch}))
    prices_by_key = await serving._version_three_bounded_prices_by_key(
        session,
        binding.serving_tables,
        requested_price_keys,
        maximum_atom_count=MAX_PRICE_HYDRATION_ATOMS,
    )
    if set(prices_by_key) != set(requested_price_keys):
        raise ValueError("pricing projection price hydration is incomplete")
    rates_by_key = {
        price_key: _exact_numeric_rates(prices_by_key[price_key])
        for price_key in requested_price_keys
    }
    return {
        price_set_id: rates_by_key[price_key]
        for price_set_id, price_key in batch
        if rates_by_key[price_key]
    }


async def _stage_binding_price_rates(
    session: Any,
    binding: BindingProjection,
    price_key_by_set_id: Mapping[str, int],
    *,
    maximum_atom_count: int = MAX_CODE_STAGED_PRICE_ATOMS,
    block_span: int | None = None,
    insert_price_rates: Any = _insert_price_rates,
) -> tuple[set[str], int]:
    """Hydrate bounded price-key batches and spill their exact rates."""

    from api import ptg2_serving as serving

    if type(maximum_atom_count) is not int or maximum_atom_count < 0:
        raise ValueError("pricing projection staged price-atom bound is invalid")
    effective_span = serving._required_price_cache_span(
        binding.serving_tables.price_key_block_span
        if block_span is None
        else block_span,
        "price_key_block_span",
    )
    pending_batches = _price_key_batches(price_key_by_set_id, effective_span)
    retained_price_ids: set[str] = set()
    consumed_atom_count = 0
    while pending_batches:
        batch = pending_batches.pop()
        try:
            rates_by_price_id = await _price_batch_rates(session, binding, batch)
        except ManifestReadLimitError:
            if len({price_key for _, price_key in batch}) == 1:
                raise
            midpoint = len(batch) // 2
            pending_batches.extend((batch[midpoint:], batch[:midpoint]))
            continue
        batch_atom_count = sum(map(len, rates_by_price_id.values()))
        if consumed_atom_count + batch_atom_count > maximum_atom_count:
            raise ValueError("pricing projection staged price-atom bound exceeded")
        if rates_by_price_id:
            await insert_price_rates(
                session, _binding_ordinal(binding), rates_by_price_id
            )
            retained_price_ids.update(rates_by_price_id)
            consumed_atom_count += batch_atom_count
        del rates_by_price_id
    return retained_price_ids, consumed_atom_count
