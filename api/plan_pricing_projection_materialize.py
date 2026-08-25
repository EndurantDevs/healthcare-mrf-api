# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Materialize immutable pricing-card and ZIP-aggregate fragments."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Iterable, Mapping

import orjson
from sqlalchemy import text

from api.plan_pricing_projection_contract import (
    INSERT_BATCH_SIZE,
    canonical_json,
    table,
)
from api.plan_pricing_projection_source import (
    BindingProjection,
    eligible_projection_providers,
    numeric_rates,
    projection_provider_rows_for_npis,
)


@dataclass
class CardStats:
    provider: dict[str, Any]
    minimum: Decimal
    maximum: Decimal
    rate_count: int

    def add(self, rates: tuple[Decimal, ...]) -> None:
        """Merge another sealed rate set for the same provider and ZIP."""

        self.minimum = min(self.minimum, min(rates))
        self.maximum = max(self.maximum, max(rates))
        self.rate_count += len(rates)


@dataclass
class _ProjectedRateState:
    cards_by_identity: dict[tuple[str, int], CardStats] = field(
        default_factory=dict
    )
    aggregate_rates_by_cell: dict[str, list[Decimal]] = field(
        default_factory=lambda: defaultdict(list)
    )
    aggregate_npis_by_cell: dict[str, set[int]] = field(
        default_factory=lambda: defaultdict(set)
    )


@dataclass(frozen=True)
class _BindingRateInputs:
    serving_rows: tuple[dict[str, Any], ...]
    prices_by_set: Mapping[str, Iterable[Mapping[str, Any]]]
    npis_by_set: Mapping[str, Iterable[int]]
    providers_by_npi: Mapping[int, Iterable[dict[str, Any]]]


def rate_fragment(rate: Decimal) -> orjson.Fragment:
    """Encode a Decimal as an unquoted JSON number fragment."""

    expanded = format(rate, "f")
    if "." in expanded:
        expanded = expanded.rstrip("0").rstrip(".")
    return orjson.Fragment((expanded or "0").encode("ascii"))


def card_fragment(stats: CardStats) -> bytes:
    """Encode one stable provider-card response fragment."""

    provider_by_field = stats.provider
    taxonomy_codes = list(provider_by_field.get("taxonomy_codes") or [])
    classifications = list(provider_by_field.get("classifications") or [])
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
            "minimum_negotiated_rate": rate_fragment(stats.minimum),
            "maximum_negotiated_rate": rate_fragment(stats.maximum),
            "rate_count": stats.rate_count,
        }
    )


def aggregate_fragment(
    geo_cell: str,
    provider_count: int,
    rates: list[Decimal],
) -> tuple[bytes, Decimal, Decimal, Decimal]:
    """Encode one aggregate fragment and return its ordered statistics."""

    ordered_rates = sorted(rates)
    minimum = ordered_rates[0]
    midpoint = len(ordered_rates) // 2
    median = ordered_rates[midpoint]
    if len(ordered_rates) % 2 == 0:
        median = (ordered_rates[midpoint - 1] + median) / 2
    maximum = ordered_rates[-1]
    return (
        orjson.dumps(
            {
                "geo_cell": geo_cell,
                "provider_count": provider_count,
                "rate_count": len(ordered_rates),
                "minimum_negotiated_rate": rate_fragment(minimum),
                "median_negotiated_rate": rate_fragment(median),
                "maximum_negotiated_rate": rate_fragment(maximum),
            }
        ),
        minimum,
        median,
        maximum,
    )


def digest_row(
    digest: Any,
    kind: str,
    key: tuple[Any, ...],
    fragment: bytes,
) -> None:
    """Extend the content digest with one length-delimited row."""

    key_bytes = canonical_json([kind, *key]).encode("utf-8")
    digest.update(len(key_bytes).to_bytes(4, "big"))
    digest.update(key_bytes)
    digest.update(len(fragment).to_bytes(8, "big"))
    digest.update(fragment)


async def insert_batches(
    session: Any,
    statement: str,
    rows_by_order: list[dict[str, Any]],
) -> None:
    """Insert bounded batches through one prepared SQL statement."""

    for start in range(0, len(rows_by_order), INSERT_BATCH_SIZE):
        await session.execute(
            text(statement),
            rows_by_order[start : start + INSERT_BATCH_SIZE],
        )


async def _binding_rate_inputs(
    session: Any,
    binding_projection: BindingProjection,
    code_rows: list[dict[str, Any]],
) -> _BindingRateInputs:
    from api import ptg2_serving as serving
    serving_tables = binding_projection.serving_tables
    serving_rows = await serving._merge_manifest_code_variant_rows(
        session,
        serving_tables,
        code_rows=code_rows,
        provider_set_keys=None,
        source_trace_set_hash=None,
        network_names=serving_tables.network_names or [],
        limit=None,
        offset=0,
    )
    if serving_rows is None:
        raise ValueError("pricing projection could not read a sealed rate layout")
    price_key_by_set_id = {
        serving._ptg2_manifest_id(rate_row.get("price_set_global_id_128")): int(
            rate_row["price_key"]
        )
        for rate_row in serving_rows
        if rate_row.get("price_key") is not None
        and serving._ptg2_manifest_id(rate_row.get("price_set_global_id_128"))
    }
    prices_by_set = await serving._prices_for_price_sets(
        session,
        serving_tables,
        list(price_key_by_set_id),
        price_key_by_set_id=price_key_by_set_id,
    )
    provider_set_ids = sorted(
        {
            serving._ptg2_manifest_id(
                rate_row.get("provider_set_global_id_128")
            )
            for rate_row in serving_rows
            if serving._ptg2_manifest_id(
                rate_row.get("provider_set_global_id_128")
            )
        }
    )
    npis_by_set = await serving._provider_npis_for_sets(
        session,
        serving_tables,
        provider_set_ids,
        limit_per_set=None,
    )
    providers_by_npi = await projection_provider_rows_for_npis(
        session,
        (npi for npi_group in npis_by_set.values() for npi in npi_group),
    )
    return _BindingRateInputs(
        tuple(serving_rows),
        prices_by_set,
        npis_by_set,
        providers_by_npi,
    )


def _add_provider_rates(
    state: _ProjectedRateState,
    providers: Iterable[dict[str, Any]],
    rates: tuple[Decimal, ...],
) -> None:
    providers_by_cell: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for provider_by_field in providers:
        geo_cell = provider_by_field["zip5"]
        providers_by_cell[geo_cell].append(provider_by_field)
        card_identity = (geo_cell, int(provider_by_field["npi"]))
        existing_stats = state.cards_by_identity.get(card_identity)
        if existing_stats is None:
            state.cards_by_identity[card_identity] = CardStats(
                provider_by_field,
                min(rates),
                max(rates),
                len(rates),
            )
        else:
            existing_stats.add(rates)
    for geo_cell, cell_providers in providers_by_cell.items():
        state.aggregate_rates_by_cell[geo_cell].extend(rates)
        state.aggregate_npis_by_cell[geo_cell].update(
            int(provider_by_field["npi"])
            for provider_by_field in cell_providers
        )


def _add_binding_rate_inputs(
    state: _ProjectedRateState,
    inputs: _BindingRateInputs,
    code_identity: tuple[str, str],
) -> None:
    from api import ptg2_serving as serving

    for serving_row in inputs.serving_rows:
        provider_set_id = serving._ptg2_manifest_id(
            serving_row.get("provider_set_global_id_128")
        )
        price_set_id = serving._ptg2_manifest_id(
            serving_row.get("price_set_global_id_128")
        )
        rates = numeric_rates(inputs.prices_by_set.get(price_set_id, ()))
        if not provider_set_id or not rates:
            continue
        eligible_providers = eligible_projection_providers(
            (
                provider_by_field
                for npi in inputs.npis_by_set.get(provider_set_id, ())
                for provider_by_field in inputs.providers_by_npi.get(npi, ())
            ),
            code_identity,
        )
        _add_provider_rates(state, eligible_providers, rates)


def _card_rows(
    state: _ProjectedRateState,
    projection_id: str,
    code_identity: tuple[str, str],
    content_digest: Any,
) -> tuple[list[dict[str, Any]], int]:
    code_system, code = code_identity
    rows_list: list[dict[str, Any]] = []
    fragment_byte_count = 0
    for (geo_cell, npi), stats in sorted(state.cards_by_identity.items()):
        fragment = card_fragment(stats)
        fragment_byte_count += len(fragment)
        digest_row(
            content_digest,
            "card",
            (code_system, code, geo_cell, npi),
            fragment,
        )
        rows_list.append(
            {
                "projection_id": projection_id,
                "code_system": code_system,
                "code": code,
                "geo_cell": geo_cell,
                "npi": npi,
                "minimum_rate": stats.minimum,
                "maximum_rate": stats.maximum,
                "rate_count": stats.rate_count,
                "fragment": fragment,
            }
        )
    return rows_list, fragment_byte_count


def _aggregate_rows(
    state: _ProjectedRateState,
    projection_id: str,
    code_identity: tuple[str, str],
    content_digest: Any,
) -> tuple[list[dict[str, Any]], int]:
    code_system, code = code_identity
    rows_list: list[dict[str, Any]] = []
    fragment_byte_count = 0
    for geo_cell, rates in sorted(state.aggregate_rates_by_cell.items()):
        fragment, minimum, median, maximum = aggregate_fragment(
            geo_cell,
            len(state.aggregate_npis_by_cell[geo_cell]),
            rates,
        )
        fragment_byte_count += len(fragment)
        digest_row(
            content_digest,
            "aggregate",
            (code_system, code, geo_cell),
            fragment,
        )
        rows_list.append(
            {
                "projection_id": projection_id,
                "code_system": code_system,
                "code": code,
                "geo_cell": geo_cell,
                "provider_count": len(state.aggregate_npis_by_cell[geo_cell]),
                "rate_count": len(rates),
                "minimum_rate": minimum,
                "median_rate": median,
                "maximum_rate": maximum,
                "fragment": fragment,
            }
        )
    return rows_list, fragment_byte_count


async def project_code(
    session: Any,
    projection_id: str,
    code_identity: tuple[str, str],
    bindings: list[BindingProjection],
    content_digest: Any,
) -> tuple[int, int, int]:
    """Materialize one normalized code identity into both projection tables."""

    state = _ProjectedRateState()
    for binding in bindings:
        code_rows = binding.code_rows_by_identity.get(code_identity)
        if code_rows:
            inputs = await _binding_rate_inputs(session, binding, code_rows)
            _add_binding_rate_inputs(state, inputs, code_identity)

    card_rows_by_order, card_bytes = _card_rows(
        state, projection_id, code_identity, content_digest
    )
    aggregate_rows_by_order, aggregate_bytes = _aggregate_rows(
        state, projection_id, code_identity, content_digest
    )
    await insert_batches(
        session,
        f"""
        INSERT INTO {table('plan_pricing_card')} (
            projection_id, code_system, code, geo_cell, npi,
            minimum_negotiated_rate, maximum_negotiated_rate,
            rate_count, fragment
        ) VALUES (
            :projection_id, :code_system, :code, :geo_cell, :npi,
            :minimum_rate, :maximum_rate, :rate_count, :fragment
        )
        """,
        card_rows_by_order,
    )
    await insert_batches(
        session,
        f"""
        INSERT INTO {table('plan_pricing_cell_aggregate')} (
            projection_id, code_system, code, geo_cell, provider_count,
            rate_count, minimum_negotiated_rate, median_negotiated_rate,
            maximum_negotiated_rate, fragment
        ) VALUES (
            :projection_id, :code_system, :code, :geo_cell, :provider_count,
            :rate_count, :minimum_rate, :median_rate, :maximum_rate, :fragment
        )
        """,
        aggregate_rows_by_order,
    )
    return (
        len(card_rows_by_order),
        len(aggregate_rows_by_order),
        card_bytes + aggregate_bytes,
    )
