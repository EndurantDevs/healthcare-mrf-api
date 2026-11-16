# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Recompute legacy ZIP aggregates from qualified offices and retained rates."""

from collections import defaultdict

import orjson
from sqlalchemy import text

from api.address_source_filter import parse_min_address_sources
from api.network_address_scope import scoped_address_parameters
from api.plan_pricing_legacy_source_memberships import bounded_source_provider_sets
from api.plan_pricing_projection_aggregate_read import (
    _MAX_PARTIAL_MEMBERSHIPS,
    _MAX_PARTIAL_PROFILES,
    _MAX_PARTIAL_RATE_VALUES,
    MAX_AGGREGATE_REQUEST_STORED_BYTES,
)
from api.plan_pricing_projection_contract import (
    PlanPricingProjectionUnavailable,
    PlanPricingProjectionUnsupported,
    table,
)
from api.plan_pricing_projection_materialize import _add_provider_rates, _ProjectedRateState, aggregate_fragment
from api.plan_pricing_projection_source import numeric_rates
from api.plan_release_serving import binding_query_args, prepare_plan_release_bindings
from api.pricing_address_source_filter import qualifying_cell_sql
from process.ptg_parts.ptg2_manifest_artifacts import ManifestReadLimitError


async def _qualified_cells(session, request, geo_cells, minimum_sources):
    statement = text(f"""SELECT item.npi, item.geo_cell
        FROM {table("plan_pricing_card")} item
        WHERE item.projection_id = :projection_id AND item.code_system = :code_system AND item.code = :code
          AND item.geo_cell = ANY(CAST(:geo_cells AS varchar[]))
          AND {qualifying_cell_sql("item", minimum_sources)}
        ORDER BY item.npi, item.geo_cell LIMIT :office_probe_limit""")
    result = await session.execute(
        statement,
        scoped_address_parameters(
            {
                "projection_id": request.projection_id,
                "code_system": request.code_system,
                "code": request.code,
                "geo_cells": geo_cells,
                "min_address_sources": minimum_sources,
                "office_probe_limit": _MAX_PARTIAL_MEMBERSHIPS + 1,
            }
        ),
    )
    cells_by_npi = defaultdict(list)
    office_count = 0
    for row in result.mappings():
        office_count += 1
        if office_count > _MAX_PARTIAL_MEMBERSHIPS:
            raise PlanPricingProjectionUnsupported("aggregate projection exceeds its native membership bound")
        cells_by_npi[int(row["npi"])].append({"npi": int(row["npi"]), "zip5": str(row["geo_cell"])})
    return cells_by_npi


async def _binding_rate_rows(session, selection, binding, args, cells_by_npi, work_counts_by_kind):
    from api import ptg2_serving as serving
    from api.ptg2_code_scope import load_sealed_code_rows

    serving_tables = selection.serving_tables_for_snapshot(binding.snapshot_id)
    if serving_tables is None:
        raise PlanPricingProjectionUnavailable("legacy aggregate retained rates are unavailable")
    sets_by_npi = await bounded_source_provider_sets(
        session,
        serving_tables,
        tuple(cells_by_npi),
        _MAX_PARTIAL_MEMBERSHIPS - work_counts_by_kind["memberships"],
    )
    work_counts_by_kind["memberships"] += sum(len(sets) for sets in sets_by_npi.values())
    if work_counts_by_kind["memberships"] > _MAX_PARTIAL_MEMBERSHIPS:
        raise PlanPricingProjectionUnsupported("aggregate projection exceeds its native membership bound")
    set_ids = sorted({set_id for sets in sets_by_npi.values() for set_id in sets})
    if len(set_ids) > _MAX_PARTIAL_PROFILES:
        raise PlanPricingProjectionUnsupported("aggregate projection exceeds its native provider-set bound")
    if not set_ids:
        return serving_tables, (), {}, sets_by_npi
    set_keys_by_id = await serving._provider_set_keys_for_ids(session, serving_tables, set_ids)
    if set(set_keys_by_id) != set(set_ids):
        raise PlanPricingProjectionUnavailable("legacy aggregate provider-set membership is incomplete")
    code_rows = await load_sealed_code_rows(session, serving_tables, binding_query_args(args, binding))
    remaining = _MAX_PARTIAL_PROFILES - work_counts_by_kind["profiles"]
    rate_rows = await serving._merge_manifest_code_variant_rows(
        session,
        serving_tables,
        code_rows=code_rows,
        provider_set_keys=tuple(sorted(set_keys_by_id.values())),
        source_trace_set_hash=None,
        network_names=serving_tables.network_names or [],
        limit=remaining + 1,
        offset=0,
    )
    if rate_rows is None:
        raise PlanPricingProjectionUnavailable("legacy aggregate retained rate rows are unavailable")
    if len(rate_rows) > remaining:
        raise PlanPricingProjectionUnsupported("aggregate projection exceeds its native rate-row bound")
    work_counts_by_kind["profiles"] += len(rate_rows)
    price_keys_by_id = {
        serving._ptg2_manifest_id(rate_row.get("price_set_global_id_128")): int(rate_row["price_key"])
        for rate_row in rate_rows
    }
    try:
        prices_by_key = await serving._version_three_bounded_prices_by_key(
            session,
            serving_tables,
            price_keys_by_id.values(),
            maximum_atom_count=_MAX_PARTIAL_RATE_VALUES - work_counts_by_kind["rates"],
        )
    except ManifestReadLimitError as error:
        raise PlanPricingProjectionUnsupported("aggregate projection exceeds its native rate-value bound") from error
    prices_by_set = {set_id: prices_by_key.get(key, ()) for set_id, key in price_keys_by_id.items()}
    return serving_tables, rate_rows, prices_by_set, sets_by_npi


def _append_binding_rates(state, rate_rows, prices_by_set, sets_by_npi, cells_by_npi, work_counts_by_kind):
    from api import ptg2_serving as serving

    providers_by_set = defaultdict(list)
    for npi, set_ids in sets_by_npi.items():
        for set_id in set_ids:
            providers_by_set[set_id].extend(cells_by_npi[npi])
    for row in rate_rows:
        set_id = serving._ptg2_manifest_id(row.get("provider_set_global_id_128"))
        price_id = serving._ptg2_manifest_id(row.get("price_set_global_id_128"))
        providers = providers_by_set.get(set_id, ())
        rates = numeric_rates(prices_by_set.get(price_id, ()))
        if not providers or not rates:
            continue
        work_counts_by_kind["rates"] += len(rates) * len({provider["zip5"] for provider in providers})
        if work_counts_by_kind["rates"] > _MAX_PARTIAL_RATE_VALUES:
            raise PlanPricingProjectionUnsupported("aggregate projection exceeds its native rate-value bound")
        _add_provider_rates(state, providers, rates)


def _aggregate_page(state, args, pagination):
    ordered_fragments = []
    retained_bytes = 0
    for cell, rates in sorted(state.aggregate_rates_by_cell.items()):
        fragment, minimum, _, _ = aggregate_fragment(cell, len(state.aggregate_npis_by_cell[cell]), rates)
        retained_bytes += len(fragment)
        if retained_bytes > MAX_AGGREGATE_REQUEST_STORED_BYTES:
            raise PlanPricingProjectionUnsupported("aggregate projection exceeds its request byte bound")
        ordered_fragments.append((minimum, cell, orjson.Fragment(fragment)))
    ordered_fragments.sort(key=lambda entry: entry[1])
    ordered_fragments.sort(key=lambda entry: entry[0], reverse=str(args.get("order") or "asc").lower() == "desc")
    start = int(pagination.offset)
    return [entry[2] for entry in ordered_fragments[start : start + int(pagination.limit)]], len(ordered_fragments)


async def read_legacy_source_aggregate_page(session, request, geo_cells, args, pagination, selection, bindings):
    """Filter complete office cells before expanding their exact source memberships."""
    from api import ptg2_serving as serving

    cells_by_npi = await _qualified_cells(session, request, geo_cells, parse_min_address_sources(args))
    if not cells_by_npi or bindings == ():
        return [], 0
    bindings = selection.in_network_bindings if bindings is None else bindings
    selection = await prepare_plan_release_bindings(session, selection, bindings)
    state = _ProjectedRateState()
    seen_sources = set()
    work_counts_by_kind = {"memberships": 0, "profiles": 0, "rates": 0}
    for binding in bindings:
        serving_tables = selection.serving_tables_for_snapshot(binding.snapshot_id)
        if serving_tables is None:
            raise PlanPricingProjectionUnavailable("legacy aggregate retained source is unavailable")
        source_identity = (
            serving._payload_schema(serving_tables),
            serving_tables.storage_generation,
            serving._required_shared_snapshot_key(serving_tables),
            serving_tables.coverage_scope_id,
            binding.plan_id,
            binding.plan_market_type,
        )
        if source_identity in seen_sources:
            continue
        seen_sources.add(source_identity)
        _, rate_rows, prices_by_set, sets_by_npi = await _binding_rate_rows(
            session,
            selection,
            binding,
            args,
            cells_by_npi,
            work_counts_by_kind,
        )
        _append_binding_rates(state, rate_rows, prices_by_set, sets_by_npi, cells_by_npi, work_counts_by_kind)
    return _aggregate_page(state, args, pagination)
