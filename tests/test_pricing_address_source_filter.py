# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Address eligibility must precede optimized pricing selection and pagination."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import orjson
import pytest

from api import custom_import_plan_pricing as imported
from api import plan_pricing_em_distance as em
from api import plan_pricing_projection_aggregate_read as aggregates
from api import plan_pricing_projection_read as cards
from api import plan_pricing_projection_source as provider_source
from api import plan_pricing_state_scan as state_scan
from api import plan_pricing_state_scan_sources as state_sources
from api import ptg2_serving as serving
from api.endpoint import pricing


@pytest.mark.parametrize("minimum", [0, 3, 2147483647])
def test_manual_pricing_predicates_expose_the_validated_threshold(minimum):
    from api import pricing_address_source_filter as predicates

    statements = (
        provider_source._assured_addresses_sql(serving, min_address_sources=minimum),
        state_scan._provider_page_sql(minimum_sources=minimum),
        predicates.qualifying_cell_sql("provider", minimum),
        predicates.qualifying_location_sql("location", minimum),
    )
    for statement in statements:
        assert ":min_address_sources" not in statement
        assert (f"source_count >= {minimum}" in statement) is bool(minimum)


@pytest.mark.parametrize("unified", [True, False])
def test_membership_predicate_filters_both_exact_and_raw_knn_candidates(unified):
    parameters_by_name = {}
    parts, prefilter, _ = serving._membership_location_filter_plan(
        {"min_address_sources": "3"},
        parameters_by_name,
        "distance",
        candidate_npis=None,
        coarse_taxonomy_knn=False,
        uses_unified_addresses=unified,
    )
    expected = "addr.source_count >= 3" if unified else "FALSE"
    assert expected in parts[0]
    assert expected in prefilter
    assert parameters_by_name["min_address_sources"] == 3


@pytest.mark.asyncio
async def test_count_only_filter_uses_stored_counts_without_spatial_reference_queries(monkeypatch):
    address_lookup = AsyncMock(return_value="mrf.entity_address_unified")
    geo_lookup = AsyncMock(side_effect=AssertionError("count-only filter must not read spatial references"))
    monkeypatch.setattr(serving, "_ptg2_address_serving_table", address_lookup)
    monkeypatch.setattr(serving, "is_provider_address_geo_capability_available", geo_lookup)
    assert (
        await serving._membership_address_table_for_request(object(), {"min_address_sources": "3"})
        == "mrf.entity_address_unified"
    )
    assert "source_count" in address_lookup.await_args.args[1]
    geo_lookup.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("minimum", [0, 3])
async def test_imported_pricing_count_only_filter_selects_address_eligibility(monkeypatch, minimum):
    from tests.test_custom_import_plan_pricing import _selection

    selection = _selection()
    monkeypatch.setattr(
        imported, "admitted_plan_release_bindings", AsyncMock(return_value=selection.in_network_bindings)
    )
    monkeypatch.setattr(imported, "prepare_plan_release_bindings", AsyncMock(return_value=selection))
    location = object()
    address_lookup = AsyncMock(return_value=location)
    monkeypatch.setattr(serving, "_membership_location_query", address_lookup)
    scopes = await imported._native_scopes(object(), selection, {"min_address_sources": str(minimum)}, serving)
    assert scopes[0].location_context is (location if minimum else None)
    assert address_lookup.await_count == bool(minimum)


def test_allowed_amount_filter_precedes_best_office_choice():
    parameters_by_name = {}
    join, _, required = pricing._allowed_amount_location_sql(
        {"min_address_sources": "3"},
        address_table="mrf.entity_address_unified",
        parameter_map=parameters_by_name,
    )
    assert join.index("addr.source_count >= 3") < join.index("LIMIT 1")
    assert required == "AND allowed_location.npi IS NOT NULL"
    assert parameters_by_name["min_address_sources"] == 3
    assert (
        pricing._allowed_amount_location_sql(
            {"min_address_sources": "3"},
            address_table=None,
            parameter_map={},
        )[2]
        == "AND FALSE"
    )


@pytest.mark.asyncio
async def test_exact_provider_prices_do_not_restore_an_unknown_legacy_office(monkeypatch):
    legacy_lookup = AsyncMock(side_effect=AssertionError("legacy fallback must stay disabled"))
    monkeypatch.setattr(serving, "_ptg2_table_columns", legacy_lookup)
    statement = await serving._provider_enrichment_statement(
        object(),
        "mrf.npi",
        "mrf.entity_address_unified",
        min_address_sources=3,
    )
    sql = str(statement)
    assert sql.index("addr.source_count >= 3") < sql.index("LIMIT 1")
    assert "fallback_addresses" not in sql
    assert "'entity_address_unified'::varchar AS location_source" in sql
    legacy_lookup.assert_not_awaited()


def test_cards_and_aggregates_filter_the_complete_office_population_before_limits(monkeypatch):
    monkeypatch.setattr(serving, "_inferred_provider_taxonomy_rule", lambda _args: None)
    predicate, parameters_by_name = serving._factorized_card_frozen_taxonomy({"min_address_sources": "3"})
    assert "source_address.npi = provider.npi" in predicate
    assert "= provider.geo_cell" in predicate
    assert "source_address.source_count >= 3" in predicate
    assert parameters_by_name == {"min_address_sources": 3}
    assert serving._factorized_card_frozen_taxonomy({"min_address_sources": "0"}) == ("", {})
    card_sql = cards._page_sql("provider_cards", "ASC", 3)
    assert card_sql.index("source_address.source_count") < card_sql.index("projected AS MATERIALIZED")
    aggregate_sql = str(aggregates._partial_aggregate_query((0,), 3))
    assert aggregate_sql.index("source_address.source_count") < aggregate_sql.index("LIMIT :membership_probe_limit")


@pytest.mark.asyncio
async def test_distance_projection_checks_the_exact_office_before_knn_limit(monkeypatch):
    statements = []

    async def execute(statement, parameters_by_name):
        statements.append((str(statement), parameters_by_name))
        return SimpleNamespace(
            mappings=lambda: SimpleNamespace(
                all=lambda: [{"projection_ready": True, "candidate_count": 0, "unique_count": 0, "npi": None}]
            )
        )

    await em._progressive_page_rows(
        SimpleNamespace(execute=execute),
        SimpleNamespace(serving_revision_id="revision", binding_set_digest="digest"),
        {"min_address_sources": "3"},
        SimpleNamespace(limit=1, offset=0),
        ("10001", None, None),
        0,
    )
    sql, parameters_by_name = statements[0]
    assert "source_address.npi = location.npi" in sql
    assert "source_address.location_key = location.location_key" in sql
    assert sql.index("source_address.source_count") < sql.index("LIMIT :candidate_limit")
    assert parameters_by_name["min_address_sources"] == 3


def test_state_scan_filters_before_identity_page_and_binds_cursor_threshold():
    sql = state_scan._provider_page_sql(minimum_sources=3)
    assert sql.index("source_address.source_count") < sql.index("LIMIT :npi_sentinel_limit")
    fingerprint = lambda args: state_scan._request_fingerprint(args, "CPT", "93320", "MI", 10)
    assert fingerprint({"min_address_sources": "3"}) != fingerprint({"min_address_sources": "2"})
    assert fingerprint({"min_address_sources": "0"}) == fingerprint({})


@pytest.mark.asyncio
async def test_state_scan_replaces_only_the_office_and_keeps_frozen_provider_facts(monkeypatch):
    frozen_by_field = {
        "version": "frozen_by_field",
        "provider": {"npi": 101, "provider_name": "Frozen name", "city": "Old"},
    }
    qualifying_by_field = {
        "state": "MI",
        "state_address_rank": 1,
        "city": "New",
        "zip5": "48001",
        "location_hash": "entity_address_unified:qualified",
        "location_source": "entity_address_unified",
        "location_confidence_code": "entity_address_unified",
        "address_payload": {"source_count": 3},
    }
    lookup = AsyncMock(return_value={101: (qualifying_by_field,)})
    monkeypatch.setattr(state_sources, "projection_provider_rows_for_npis", lookup)
    session = object()
    fragments_by_npi = await state_sources.qualifying_state_fragments(
        session,
        [{"npi": 101, "provider_fragment": orjson.dumps(frozen_by_field)}],
        "MI",
        3,
    )
    lookup.assert_awaited_once_with(session, (101,), min_address_sources=3)
    provider = orjson.loads(fragments_by_npi[101])["provider"]
    assert provider["provider_name"] == "Frozen name"
    assert provider["city"] == "New"
    assert provider["address_payload"]["source_count"] == 3


@pytest.mark.asyncio
async def test_unknown_claims_locations_are_excluded_before_count_and_page(monkeypatch):
    statements = []

    async def execute(statement):
        statements.append(str(statement.compile(compile_kwargs={"literal_binds": True})))
        return SimpleNamespace(scalar=lambda: 0) if len(statements) == 1 else []

    result = await pricing._query_provider_procedure_location_page(
        SimpleNamespace(execute=execute),
        {"min_address_sources": "1"},
        SimpleNamespace(limit=1, offset=0),
        [],
        False,
    )
    assert result["total"] == 0 and result["items"] == []
    assert all("WHERE false" in statement for statement in statements)


def _mismatched_offices(first_line):
    location_by_field = {
        "npi": 101,
        "location_hash": "entity_address_unified:qualified",
        "city": "Example City",
        "state": "IL",
        "zip5": "60001",
        "address_payload": orjson.dumps({"first_line": first_line, "city": "Example City", "source_count": 3}).decode(),
    }
    enriched_by_field = {
        "npi": 101,
        "provider_name": "Example Provider",
        "city": "Other City",
        "state": "IL",
        "zip5": "60002",
        "telephone_number": "3125550100",
        "address_payload": orjson.dumps({"first_line": "900 Unqualified Lane", "source_count": 1}).decode(),
    }
    return location_by_field, enriched_by_field


@pytest.mark.parametrize("minimum", [0, 3])
@pytest.mark.parametrize("first_line", ["", "100 Qualifying Road"])
def test_positive_threshold_keeps_the_selected_office_atomic(minimum, first_line):
    location_by_field, enriched_by_field = _mismatched_offices(first_line)
    provider_by_field = serving._graph_provider_data(
        location_by_field, enriched_by_field, "entity_address_unified", min_address_sources=minimum
    )
    assert provider_by_field["provider_name"] == "Example Provider"
    assert provider_by_field["location_hash"] == location_by_field["location_hash"]
    if minimum:
        assert provider_by_field["address_payload"] == location_by_field["address_payload"]
        assert provider_by_field["city"] == "Example City" and provider_by_field["zip5"] == "60001"
        assert provider_by_field["telephone_number"] is None
    else:
        assert provider_by_field["telephone_number"] == "3125550100"
        if not first_line:
            assert provider_by_field["address_payload"] == enriched_by_field["address_payload"]


@pytest.mark.asyncio
@pytest.mark.parametrize("path", ["selected", "graph", "imported", "sets"])
async def test_every_provider_completion_forwards_threshold_and_keeps_the_qualified_office(monkeypatch, path):
    from tests.ptg2_serving_coverage_paydown_support import strict_v3_tables

    location_by_field, enriched_by_field = _mismatched_offices("")
    enrichment = AsyncMock(return_value=[enriched_by_field])
    monkeypatch.setattr(serving, "_enriched_provider_rows_for_npis", enrichment)
    monkeypatch.setattr(serving, "_provider_set_ids_for_keys", AsyncMock(return_value={7: "set"}))
    monkeypatch.setattr(serving, "_ptg2_address_serving_table", AsyncMock(return_value="mrf.entity_address_unified"))
    monkeypatch.setattr(serving, "_membership_location_rows", AsyncMock(return_value=[location_by_field]))
    session, tables, args_by_name = object(), strict_v3_tables(), {"min_address_sources": "3"}
    if path == "selected":
        providers = await serving._selected_provider_rows_by_set(
            session,
            tables,
            npis=(101,),
            provider_set_ids_by_npi={101: ("set",)},
            args=args_by_name,
            snapshot_id="snapshot",
            matched_location_rows_by_npi={101: location_by_field},
        )
        provider_by_field = providers["set"][0]
    elif path == "graph":
        _, providers = await serving._project_graph_candidates(
            session,
            tables,
            serving._GraphLocationCandidates([location_by_field], {101: {7}}),
            plan_id="plan",
            snapshot_id="snapshot",
            source_key=None,
            min_address_sources=3,
        )
        provider_by_field = providers["set"][0]
    elif path == "imported":
        scope = SimpleNamespace(
            binding=SimpleNamespace(plan_id="plan", snapshot_id="snapshot", source_key="source"),
            serving_tables=tables,
            args=args_by_name,
            location_context=object(),
        )
        provider_by_field = (await imported._providers_for_scope(session, scope, (101,), serving))[101]
    else:
        monkeypatch.setattr(serving, "_provider_npis_by_set_for_request", AsyncMock(return_value={"01" * 16: (101,)}))
        await serving._provider_rows_for_sets(session, tables, ("01" * 16,), limit_per_set=1, args=args_by_name)
        assert enrichment.await_args.kwargs["min_address_sources"] == 3
        return
    assert enrichment.await_args.kwargs["min_address_sources"] == 3
    assert provider_by_field["address_payload"] == location_by_field["address_payload"]
    assert provider_by_field["zip5"] == "60001" and provider_by_field["telephone_number"] is None


@pytest.mark.asyncio
@pytest.mark.parametrize("minimum", [0, 3])
@pytest.mark.parametrize("path", ["list", "detail"])
async def test_exact_provider_prescriptions_keep_utilization_and_suppress_unknown_geography(monkeypatch, minimum, path):
    from tests.test_pricing_api import FakeResult

    record_by_field = {
        "npi": 101,
        "rx_code_system": "HP_RX_CODE",
        "rx_code": "DEMO",
        "total_claims": 7,
        "total_drug_cost": 70,
        "city": "Example City",
        "state": "IL",
        "zip5": "60001",
        "country": "US",
    }
    monkeypatch.setattr(pricing, "_is_table_available", AsyncMock(return_value=True))
    monkeypatch.setattr(pricing, "_resolve_year", AsyncMock(return_value=(2023, "request")))
    monkeypatch.setattr(pricing, "_resolve_external_rx_codes_for_internal", AsyncMock(return_value={}))
    monkeypatch.setattr(
        pricing,
        "_resolve_internal_rx_codes_for_request",
        AsyncMock(return_value=(["DEMO"], {"input_code": "DEMO", "resolved_codes": [], "matched_via": "internal"})),
    )
    query_results = (
        [FakeResult(scalar=1), FakeResult(rows=[record_by_field])]
        if path == "list"
        else [FakeResult(rows=[record_by_field])]
    )
    request = SimpleNamespace(
        args={"year": "2023", "min_address_sources": str(minimum)},
        ctx=SimpleNamespace(sa_session=SimpleNamespace(execute=AsyncMock(side_effect=query_results))),
    )
    if path == "list":
        http_response = await pricing.list_provider_prescriptions(request, "101")
        item_by_field = orjson.loads(http_response.body)["items"][0]
    else:
        http_response = await pricing.get_provider_prescription(request, "101", "HP_RX_CODE", "DEMO")
        item_by_field = orjson.loads(http_response.body)
    assert item_by_field["npi"] == 101 and item_by_field["total_prescriptions"] == 7
    assert item_by_field["total_allowed_amount"] == 70
    assert all((field not in item_by_field) == bool(minimum) for field in ("city", "state", "zip5", "country"))
