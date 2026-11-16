# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Address-source thresholds retain validation, identity and detail fallbacks."""

import json
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from sanic.exceptions import InvalidUsage, NotFound
from sanic.request.parameters import RequestParameters
from sqlalchemy import BigInteger, Integer, column, table
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.sql import Select

from api import provider_batch, provider_profile
from api.endpoint import npi, reports
from api.provider_detail_network_contract import parse_managed_detail_query
from tests import test_npi_location_paging as paging
from tests.provider_profile_snapshot_test_support import (
    stub_provider_profile_snapshot as stub_provider_profile_snapshot,
)
from tests.test_custom_import_provider_list import (
    _configure_imported_page_dependencies,
    _import_context,
    _Session,
)
from tests.test_npi_all_names_like import FakeAcquire, RecordingConnection

NPI = 1234567890
PROFILE_NPI = 1000000004


class _TaxonomyCountConnection:
    def __init__(self, count_format):
        self.calls = []
        self.count_key = 123 if count_format == "full_taxonomy" else "Family Medicine"

    async def all(self, statement, **parameters):
        self.calls.append((str(statement), parameters, statement))
        return [] if "1=0" in str(statement) else [(self.count_key, 1)]


async def _scoped_taxonomy_count(monkeypatch, scope, count_format, **extra_parameters):
    connection = _TaxonomyCountConnection(count_format)
    _configure_imported_page_dependencies(monkeypatch)
    monkeypatch.setattr(npi, "_address_serving_table_sql", AsyncMock(return_value="mrf.entity_address_unified"))
    monkeypatch.setattr(npi, "_plan_release_npi_scope", AsyncMock(return_value=(None, {})))
    session = _Session(connection)
    query_parameter_map = {
        "count_only": ["1"],
        "format": [count_format],
        "min_address_sources": ["3"],
        **{name: [entry] for name, entry in extra_parameters.items()},
    }
    options_by_name = {"session": session}
    if scope == "native":
        options_by_name["native_npis"] = (1000000002,)
    request = SimpleNamespace(args={}, ctx=SimpleNamespace(sa_session=session))
    if scope == "ordinary":
        request.args = RequestParameters(query_parameter_map)
        request.ctx.sa_session = None
        options_by_name = {}
        monkeypatch.setattr(npi.db, "acquire", lambda: FakeAcquire(connection))
    else:
        options_by_name["native_args"] = RequestParameters(query_parameter_map)
    reply = await npi.list_providers(request, **options_by_name)
    return json.loads(reply.body), connection, session


@pytest.mark.asyncio
@pytest.mark.parametrize("count_format", ["all", "classification", "full_taxonomy"])
async def test_scoped_taxonomy_counts_retain_membership(monkeypatch, count_format):
    document, connection, session = await _scoped_taxonomy_count(monkeypatch, "native", count_format)

    assert document == {"rows": {str(connection.count_key): 1}}
    assert session.proxy_inputs == [session]
    statement_sql, parameters, statement = connection.calls[-1]
    assert parameters["min_address_sources"] == 3
    assert statement_sql.index("c.source_count >= 3") < statement_sql.rindex("GROUP BY")
    assert "(COALESCE(c.npi, c.inferred_npi)) = ANY(:__native_batch_npis)" in statement_sql
    assert parameters["__native_batch_npis"] == [1000000002]
    native_type = statement._bindparams["__native_batch_npis"].type
    assert isinstance(native_type, ARRAY) and isinstance(native_type.item_type, BigInteger)


@pytest.mark.asyncio
@pytest.mark.parametrize("count_format", ["all", "classification", "full_taxonomy"])
async def test_imported_taxonomy_counts_remain_unsupported(monkeypatch, count_format):
    address_reader = AsyncMock(side_effect=AssertionError("unsupported imported count reached storage"))
    monkeypatch.setattr(npi, "_address_serving_table_sql", address_reader)
    context = _import_context(direction=None, require_match=True)
    native = RequestParameters({"count_only": ["1"], "format": [count_format], "min_address_sources": ["3"]})

    with pytest.raises(InvalidUsage, match="custom-import provider pages require provider results"):
        await npi.list_providers(SimpleNamespace(args={}), native_args=native, import_context=context)

    address_reader.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("count_format", ["all", "classification", "full_taxonomy"])
async def test_scoped_taxonomy_counts_reject_unresolved_procedures(monkeypatch, count_format):
    resolver = AsyncMock(return_value=([], "unresolved"))
    monkeypatch.setattr(npi, "_resolve_internal_filter_codes", resolver)
    monkeypatch.setattr(npi, "_resolve_filter_year", AsyncMock(return_value=(None, "none")))
    monkeypatch.setattr(
        npi,
        "_resolve_npi_filter_capabilities",
        AsyncMock(
            return_value={
                "npi_procedures_array_available": True,
                "npi_medications_array_available": True,
                "pricing_provider_procedure_available": False,
                "pricing_provider_prescription_available": False,
            }
        ),
    )

    document, connection, _ = await _scoped_taxonomy_count(monkeypatch, "native", count_format, procedure_codes="1001")

    assert document == {"rows": {}}
    resolver.assert_awaited_once()
    assert "1=0" in connection.calls[-1][0]


@pytest.mark.asyncio
@pytest.mark.parametrize("scope", ["ordinary", "native"])
@pytest.mark.parametrize("count_format", ["all", "classification", "full_taxonomy"])
async def test_taxonomy_phone_counts_preserve_scope_bounds(monkeypatch, scope, count_format):
    _, connection, _ = await _scoped_taxonomy_count(monkeypatch, scope, count_format, phone="2125550100")

    statement_sql, parameters, _ = connection.calls[-1]
    assert "phone_candidate_best_addresses AS MATERIALIZED" in statement_sql
    assert ("LIMIT :candidate_limit" in statement_sql) is (scope == "ordinary")
    assert ("candidate_limit" in parameters) is (scope == "ordinary")
    assert "qualifying_address.source_count >= 3" in statement_sql
    assert parameters["min_address_sources"] == 3


@pytest.mark.parametrize("minimum", [0, 3, 2147483647])
@pytest.mark.parametrize("body_has_minimum", [False, True])
def test_batch_native_threshold_options_agree_without_changing_the_body(minimum, body_has_minimum):
    body_by_field = {"npis": [NPI]}
    if body_has_minimum:
        body_by_field["min_address_sources"] = minimum
    params = npi._normalize_npi_batch_request(body_by_field)
    original = deepcopy(params)
    native = provider_batch.parse_native_batch_query({"min_address_sources": str(minimum)})

    combined = provider_batch._batch_shape_params(params, native)

    assert combined["min_address_sources"] == minimum
    assert params == original
    assert combined["npis"] == [NPI]


@pytest.mark.asyncio
@pytest.mark.parametrize("body_minimum,native_minimum", [(0, 3), (3, 0), (3, 4)])
async def test_conflicting_batch_thresholds_refuse_before_provider_reads(monkeypatch, body_minimum, native_minimum):
    prepare = AsyncMock(side_effect=AssertionError("conflicting thresholds must not reach storage"))
    monkeypatch.setattr(provider_batch, "_prepare_native_batch", prepare)
    params = npi._normalize_npi_batch_request({"npis": [NPI], "min_address_sources": body_minimum})
    native = provider_batch.parse_native_batch_query({"min_address_sources": str(native_minimum)})

    with pytest.raises(InvalidUsage, match="address source options must agree"):
        await provider_batch.read_native_batch(SimpleNamespace(), params, native_args=native, session=object())

    prepare.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("minimum", [0, 3])
async def test_body_only_batch_threshold_reaches_eligibility_before_hydration(monkeypatch, minimum):
    params = npi._normalize_npi_batch_request({"npis": [NPI], "min_address_sources": minimum})
    state = provider_batch._NativeBatchState({NPI: {"npi": NPI}}, {NPI: []}, frozenset({NPI}))
    prepare = AsyncMock(return_value=state)
    eligible = AsyncMock(return_value=[NPI])
    hydrate = AsyncMock(return_value=[{"npi": NPI, "status": 200, "provider": {"npi": NPI, "address_list": []}}])
    monkeypatch.setattr(provider_batch, "_prepare_native_batch", prepare)
    monkeypatch.setattr(provider_batch, "_batch_eligible_npis", eligible)
    monkeypatch.setattr(provider_batch, "_hydrate_native_batch", hydrate)

    result = await provider_batch.read_native_batch(
        SimpleNamespace(), params, native_args=provider_batch.parse_native_batch_query({}), session=object()
    )

    assert prepare.await_args.args[1].get("min_address_sources") == str(minimum)
    assert eligible.await_args.args[1].get("min_address_sources") == str(minimum)
    assert hydrate.await_args.args[3].get("min_address_sources") == str(minimum)
    assert result["found"] == 1 and result["not_found"] == 0
    assert result["items"][0]["provider"]["address_list"] == []


@pytest.mark.asyncio
async def test_native_batch_retains_an_authorized_address_only_identity_after_filtering(monkeypatch):
    address = paging._address(paging.ADDRESS_A, "100 Example Avenue", source_id="nppes")
    monkeypatch.setattr(npi, "_build_npi_identity_details_map", AsyncMock(return_value={}))
    monkeypatch.setattr(npi, "_fetch_npi_location_candidates_map", AsyncMock(return_value={NPI: [address]}))
    monkeypatch.setattr(npi, "_apply_location_statuses", AsyncMock())
    monkeypatch.setattr(npi, "_fetch_other_names_map", AsyncMock(return_value={}))
    monkeypatch.setattr(npi, "_fetch_provider_enrichment_summary_map", AsyncMock(return_value={}))
    monkeypatch.setattr(npi, "_project_provider_network_metadata", AsyncMock(side_effect=lambda rows, **_: rows))
    overlays = AsyncMock(side_effect=AssertionError("live overlays cannot supply stored count evidence"))
    monkeypatch.setattr(npi, "_fetch_provider_directory_address_overlay_map", overlays)
    params = npi._normalize_npi_batch_request({"npis": [NPI], "min_address_sources": 3})

    result = await provider_batch.build_native_batch_payload(params)

    assert result["found"] == 1 and result["not_found"] == 0
    assert result["items"][0]["status"] == 200
    provider = result["items"][0]["provider"]
    assert provider["npi"] == NPI and provider["address_list"] == []
    assert provider["address_pagination"]["total"] == 0
    overlays.assert_not_awaited()


@pytest.mark.parametrize("minimum", ["0", "3", "2147483647"])
def test_managed_detail_accepts_the_same_bounded_threshold_as_public_detail(minimum):
    query = parse_managed_detail_query({"npi": str(NPI), "min_address_sources": minimum})
    assert dict(query.native_args)["min_address_sources"] == minimum
    assert query.npi == str(NPI)


@pytest.mark.parametrize("minimum", ["-1", "03", "+3", "2147483648", 3, True, None])
def test_managed_detail_rejects_noncanonical_threshold_wire_values(minimum):
    with pytest.raises(ValueError, match="managed_detail_query_invalid"):
        parse_managed_detail_query({"npi": str(NPI), "min_address_sources": minimum})


@pytest.mark.parametrize("missing_alias", [False, True])
@pytest.mark.parametrize("address_limit", [None, 1])
def test_detail_query_alias_fallback_keeps_filters_order_and_limit(monkeypatch, missing_alias, address_limit):
    offices = table(
        "boundary_offices", column("type"), column("first_line"), column("city_name"), column("source_count", Integer)
    )
    type_clause = MagicMock(side_effect=lambda _model, rows: rows.c.type == "practice")
    monkeypatch.setattr(npi, "_provider_detail_address_type_clause", type_clause)
    if missing_alias:

        def absent_alias(_statement, *_args, **_kwargs):
            raise NameError("optional alias hook unavailable")

        monkeypatch.setattr(Select, "alias", absent_alias)

    result = npi._npi_detail_address_subquery(
        npi.EntityAddressUnified, offices, list(offices.c), [offices.c.source_count >= 3], address_limit
    )

    sql = str(result.compile(compile_kwargs={"literal_binds": True}))
    assert "boundary_offices.source_count >= 3" in sql
    assert "npi_address_rows.type = 'practice'" in sql
    assert "ORDER BY npi_address_rows.type, npi_address_rows.first_line, npi_address_rows.city_name" in sql
    assert ("LIMIT 1" in sql) == (address_limit == 1)
    assert isinstance(result, Select) is missing_alias
    type_clause.assert_called_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("model", [npi.EntityAddressUnified, npi.NPIAddress])
@pytest.mark.parametrize("minimum", [0, 3])
async def test_candidate_reader_requires_stored_evidence_without_losing_zero_behavior(monkeypatch, model, minimum):
    monkeypatch.setattr(npi, "_address_serving_model", AsyncMock(return_value=model))
    monkeypatch.setattr(npi, "_table_columns", AsyncMock(return_value=set(model.__table__.c.keys())))
    execute = AsyncMock(return_value=SimpleNamespace(all=lambda: []))
    monkeypatch.setattr(npi, "_execute_stmt", execute)

    result = await npi._fetch_npi_location_candidates_map([NPI], min_address_sources=minimum)

    assert result == {}
    statement = execute.await_args.args[0]
    sql = str(statement)
    if minimum and model is npi.NPIAddress:
        assert "false" in sql.lower()
    else:
        assert ("source_count >=" in sql) == bool(minimum)
        assert NPI in next(value for value in statement.compile().params.values() if isinstance(value, list))
    if minimum and model is npi.EntityAddressUnified:
        assert f"source_count >= {minimum}" in sql


@pytest.mark.asyncio
@pytest.mark.parametrize("minimum", [0, 3])
async def test_classification_counts_apply_threshold_to_eligible_offices(monkeypatch, minimum):
    connection = RecordingConnection()
    monkeypatch.setattr(npi.db, "acquire", lambda: FakeAcquire(connection))
    monkeypatch.setattr(npi, "_address_serving_model", AsyncMock(return_value=npi.EntityAddressUnified))

    result = await npi.get_all(
        SimpleNamespace(
            args={
                "count_only": "1",
                "format": "all",
                "classification": "Family Medicine",
                "min_address_sources": str(minimum),
            }
        )
    )

    assert result.status == 200
    sql, parameters = connection.sql_calls[-1]
    assert (f"c.source_count >= {minimum}" in sql) == bool(minimum)
    if minimum:
        assert parameters["min_address_sources"] == minimum
        assert sql.index("c.source_count >=") < sql.rindex("GROUP BY")


def _profile_route_dependencies(monkeypatch):
    profile_by_field = {"generation_id": "a" * 64, "categories": {"identity": {"items": []}}}
    reported_profile_by_field = {
        "categories": {},
        "practice_location": "100 Example Avenue",
        "endpoint": {"address": "https://example.invalid/fhir"},
    }
    evidence_by_field = {
        "records": [{"id": "reported-contact", "address_line_1": "100 Example Avenue", "city": "Example City"}]
    }
    monkeypatch.setattr(npi, "fetch_provider_profile_projection", AsyncMock(return_value=None))
    monkeypatch.setattr(
        npi,
        "_fetch_provider_directory_profile_map",
        AsyncMock(return_value={PROFILE_NPI: {"profile": reported_profile_by_field}}),
    )
    monkeypatch.setattr(npi, "compose_provider_profile", MagicMock(return_value=deepcopy(profile_by_field)))
    filtered_composer = MagicMock(return_value=deepcopy(profile_by_field))
    monkeypatch.setattr(provider_profile, "compose_provider_profile", filtered_composer)
    monkeypatch.setattr(npi, "compose_provider_profile_evidence", MagicMock(return_value=deepcopy(evidence_by_field)))
    return reported_profile_by_field, filtered_composer


@pytest.mark.asyncio
@pytest.mark.parametrize("minimum", [0, 3])
async def test_profile_route_suppresses_unknown_postal_evidence_and_keeps_other_facts(monkeypatch, minimum):
    reported_profile_by_field, filtered_composer = _profile_route_dependencies(monkeypatch)

    result = await npi.get_provider_profile(
        SimpleNamespace(args={"min_address_sources": str(minimum), "include_evidence": "true"}), str(PROFILE_NPI)
    )

    assert result.status == 200
    body_by_field = json.loads(result.body)
    contact = body_by_field["provider_profile_evidence"]["records"][0]
    assert contact["id"] == "reported-contact"
    assert ("address_line_1" in contact) is (minimum == 0)
    if minimum:
        forwarded = filtered_composer.call_args.kwargs["fhir_profile"]
        assert "practice_location" not in forwarded
        assert forwarded["endpoint"]["address"] == "https://example.invalid/fhir"
    assert reported_profile_by_field["practice_location"] == "100 Example Avenue"


@pytest.mark.asyncio
async def test_changing_profile_threshold_invalidates_existing_category_generation(monkeypatch):
    _profile_route_dependencies(monkeypatch)
    request = SimpleNamespace(args={"min_address_sources": "3", "category": "identity", "generation_id": "a" * 64})

    result = await npi.get_provider_profile(request, str(PROFILE_NPI))

    assert result.status == 409
    assert json.loads(result.body)["error"] == "provider_profile_generation_changed"
    npi.compose_provider_profile_evidence.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("is_member", [False, True])
async def test_filtered_detail_requires_original_network_membership_even_with_identity(monkeypatch, is_member):
    paging._install_route_mocks(monkeypatch)
    monkeypatch.setattr(npi, "current_network_address_scope", lambda: SimpleNamespace(network_ids=(42,)))
    member_office = paging._detail_payload()["address_list"][0]
    candidates = AsyncMock(side_effect=[[], [member_office] if is_member else []])
    monkeypatch.setattr(npi, "_fetch_npi_location_candidates", candidates)
    request = paging._detail_request(limit="5")
    request.args["min_address_sources"] = "3"

    if is_member:
        result = await npi.read_npi_detail(request, str(NPI))
        assert result.status == 200
        body_by_field = json.loads(result.body)
        assert body_by_field["npi"] == NPI and body_by_field["address_list"] == []
    else:
        with pytest.raises(NotFound):
            await npi.read_npi_detail(request, str(NPI))
    assert candidates.await_count == 2
    assert candidates.await_args_list[0].kwargs["min_address_sources"] == 3
    assert "min_address_sources" not in candidates.await_args_list[1].kwargs


@pytest.mark.parametrize("address_relation", ["mrf.entity_address_unified", "mrf.npi_address"])
def test_pharmacy_market_totals_and_pages_share_filtered_address_inputs(address_relation):
    options_by_name = dict(
        scope="state",
        sort="pharmacy_count",
        order="desc",
        include_staffing=True,
        has_partd=False,
        has_license=False,
        has_other_id=False,
        market_id_filter=None,
        state="IL",
        city=None,
        county=None,
        zip_code=None,
        chain=None,
        address_table_sql=address_relation,
    )
    ordinary = reports._build_market_sql(**options_by_name)
    assert reports._build_market_sql(**options_by_name, min_address_sources=0) == ordinary

    count_sql, page_sql, parameters = reports._build_market_sql(**options_by_name, min_address_sources=3)

    predicate = "source_address.source_count >= 3" if "unified" in address_relation else "FALSE"
    filtered = f"(SELECT * FROM {address_relation} source_address WHERE {predicate})"
    assert parameters["min_address_sources"] == 3
    for statement in (count_sql, page_sql):
        assert f"FROM {filtered} a" in statement
        assert f"FROM {filtered} p" in statement
        assert statement.index(filtered) < statement.index("GROUP BY swm.market_id")
    assert "LIMIT :limit OFFSET :offset" in page_sql
