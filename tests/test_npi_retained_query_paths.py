# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Retained NPI cache, address selection, and enrichment query contracts."""

import json
from collections import OrderedDict
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from sanic.exceptions import InvalidUsage
from sqlalchemy import column, select, table

from api.endpoint import npi as npi_module
from tests.npi_location_hydration_support import unified_location_mapping
from tests.test_custom_import_provider_geo_sql import _context as imported_geo_context
from tests.test_npi_api_extended import FakeAcquire
from tests.test_npi_batch import _ResultRows
from tests.test_npi_canonical_publication_api_cache import _ScalarResult
from tests.test_npi_facility_providers_api import FakeResult, FakeSession


def test_classification_cache_keeps_new_value_with_backdated_clock(monkeypatch):
    """A clock adjustment must evict an older key rather than the new result."""
    monkeypatch.setattr(npi_module, "_CLASSIFICATION_CACHE_MAX_KEYS", 1)
    cache_by_classification = {"previous": (20, [1000000001])}

    npi_module._set_limited_classification_cache(cache_by_classification, "current", [1000000002], 10)

    assert cache_by_classification == {"current": (10, [1000000002])}


@pytest.mark.parametrize(
    "fetch_name",
    [
        "_fetch_npi_address_rows_map",
        "_build_npi_identity_details_map",
        "_fetch_other_names_map",
        "_fetch_provider_directory_address_overlay_map",
    ],
)
async def test_empty_provider_batches_skip_storage(monkeypatch, fetch_name):
    """Empty provider batches return no records without opening storage."""
    execute_query = AsyncMock()
    monkeypatch.setattr(npi_module, "_execute_stmt", execute_query)
    monkeypatch.setattr(npi_module, "db", SimpleNamespace())

    assert await getattr(npi_module, fetch_name)([]) == {}
    execute_query.assert_not_awaited()


async def test_hydrated_locations_are_independent_copies(monkeypatch):
    """Already hydrated locations need no query and cannot mutate input rows."""
    fetch_rows = AsyncMock()
    monkeypatch.setattr(npi_module, "_fetch_npi_address_rows", fetch_rows)
    selected_locations = [{"npi": 1000000001, "city_name": "Example City"}]

    hydrated_locations = await npi_module._hydrate_selected_provider_locations(
        1000000001,
        selected_locations,
        already_hydrated=True,
        include_sources=False,
        include_evidence=False,
        address_key=None,
        session=None,
    )

    assert hydrated_locations == selected_locations
    hydrated_locations[0]["city_name"] = "Another City"
    assert selected_locations[0]["city_name"] == "Example City"
    fetch_rows.assert_not_awaited()


def test_address_candidates_without_provider_identity_are_ignored():
    """An unattributed location must not be attached to another provider."""
    selected_columns = [column("npi"), column("inferred_npi"), column("checksum")]
    query_rows = _ResultRows(
        [
            {"npi": None, "inferred_npi": None, "checksum": 11},
            {"npi": None, "inferred_npi": 1000000001, "checksum": 12},
        ]
    )

    grouped_locations = npi_module._group_npi_location_candidates(query_rows, selected_columns)

    assert list(grouped_locations) == [1000000001]
    assert grouped_locations[1000000001][0]["checksum"] == 12
    assert grouped_locations[1000000001][0]["npi"] == 1000000001


@pytest.mark.parametrize("scalar_available", [False, True])
async def test_unsealed_primary_counts_remain_uncached(monkeypatch, scalar_available):
    """Legacy counts remain available without reusing an unsealed generation."""
    connection = _install_query_connection(monkeypatch, [(7,)])
    scalar_query = AsyncMock(return_value=7)
    if scalar_available:
        monkeypatch.setattr(npi_module.db, "scalar", scalar_query, raising=False)
    monkeypatch.setattr(npi_module, "_address_serving_model", AsyncMock(return_value=npi_module.NPIAddress))
    monkeypatch.setattr(npi_module, "_npi_count_cache_identity", AsyncMock(return_value=None))
    store_count = AsyncMock()
    monkeypatch.setattr(npi_module, "_primary_total_cache_set", store_count)

    assert await npi_module._fast_primary_npi_count() == 7
    assert await npi_module._fast_primary_npi_count() == 7
    store_count.assert_not_called()
    if scalar_available:
        assert scalar_query.await_count == 2
        connection.all.assert_not_awaited()
    else:
        assert connection.all.await_count == 2


async def test_unified_insurance_count_uses_location_filters_without_cache(monkeypatch):
    """Unsealed unified counts deduplicate NPIs and honor city and state."""
    session = SimpleNamespace(execute=AsyncMock(return_value=_ScalarResult(4)))
    monkeypatch.setattr(npi_module, "db", SimpleNamespace(session=lambda: FakeAcquire(session)))
    monkeypatch.setattr(npi_module, "_address_serving_model", AsyncMock(return_value=npi_module.EntityAddressUnified))
    monkeypatch.setattr(npi_module, "_npi_count_cache_identity", AsyncMock(return_value=None))
    store_count = AsyncMock()
    monkeypatch.setattr(npi_module, "_has_insurance_total_cache_set", store_count)

    assert await npi_module._fast_has_insurance_count("EXAMPLE CITY", "AA") == 4

    statement = session.execute.await_args.args[0].compile()
    assert "count(distinct(coalesce(" in str(statement).lower()
    assert "EXAMPLE CITY" in statement.params.values()
    assert "AA" in statement.params.values()
    store_count.assert_not_called()


@pytest.mark.parametrize("enrollment_id", [None, "example-enrollment"])
async def test_enrichment_missing_subfiles_preserves_enrollment(monkeypatch, enrollment_id):
    """Missing optional tables must preserve the visible enrollment payload."""
    enrollment_payload_map = {"enrollment_id": enrollment_id, "multiple_npi_flag": "N"}
    record = SimpleNamespace(to_json_dict=lambda: dict(enrollment_payload_map))
    query_result = SimpleNamespace(scalars=lambda: [record])
    execute_query = AsyncMock(return_value=query_result)
    available_table = AsyncMock(
        side_effect=lambda table_name, **_kwargs: table_name == npi_module.ProviderEnrollmentFFS.__tablename__
    )
    monkeypatch.setattr(npi_module, "_fetch_provider_enrichment_summary_map", AsyncMock(return_value={}))
    monkeypatch.setattr(npi_module, "_is_table_available", available_table)
    monkeypatch.setattr(npi_module, "_execute_stmt", execute_query)

    detail = await npi_module._fetch_provider_enrichment_detail(1000000001)

    assert detail["summary"] is None
    assert detail["enrollments"]["ffs_public"] == [enrollment_payload_map]
    assert all(not rows for rows in detail["ffs_subfiles"].values())
    assert detail["ffs_visibility"]["chain_hidden"] is False
    execute_query.assert_awaited_once()
    assert 1000000001 in execute_query.await_args.args[0].compile().params.values()


def test_mrf_source_details_ignore_unattributed_and_blank_issuers():
    """Only selected addresses with a named public issuer receive provenance."""
    address_key = "00000000-0000-0000-0000-000000000001"
    selected_address_by_field = {"npi": 1000000001, "address_key": address_key, "address_sources": ["mrf"]}
    source_rows = [
        {"npi": 1000000001, "address_key": address_key, "issuer_name": " "},
        {"npi": 1000000001, "address_key": address_key, "issuer_name": "Example Issuer", "source_urls": [None]},
    ]
    addresses = [None, selected_address_by_field]

    assert npi_module._mrf_source_address_pairs(addresses) == [(1000000001, address_key)]
    source_details = npi_module._mrf_source_details_by_pair(source_rows, {(1000000001, address_key)})
    npi_module._apply_mrf_source_details(addresses, source_details)

    assert selected_address_by_field[npi_module.MRF_SOURCE_COUNT_KEY] == 1
    assert selected_address_by_field[npi_module.MRF_SOURCE_DETAIL_KEY] == [
        {
            "source": "mrf",
            "issuer_name": "Example Issuer",
            "source_name": "Example Issuer",
            "issuer_ids": [],
            "source_urls": [],
        }
    ]


def test_match_candidate_sources_keep_phone_and_directory_evidence():
    """Phone witnesses and opt-in directory evidence remain in candidate output."""
    provider_by_field = {"phone_source_record_ids": ["example-role"], "source_record_ids": []}
    evidence = npi_module._match_candidate_evidence_map(provider_by_field, None, ["provider_directory_fhir"])
    candidate_map = {"sources": {}}
    directory_sources = [{"source_id": "example-directory"}]

    npi_module._include_match_candidate_source_details(candidate_map, {}, directory_sources)

    assert evidence["phone_source_record_ids"] == ["example-role"]
    assert evidence["address_sources"] == ["provider_directory_fhir"]
    assert candidate_map[npi_module.PROVIDER_DIRECTORY_SOURCE_DETAIL_KEY] == directory_sources
    assert candidate_map["sources"] == {}
    assert npi_module._match_candidate_source_count({"sources": {"fhir": None}}) == 0
    assert npi_module._is_match_candidate_provider_type_matched({"match_signals": {"taxonomy": None}}) is False


def test_geo_candidate_duplicate_addresses_preserve_first_seen_order():
    """Duplicate selected addresses must not consume multiple candidate slots."""
    selected_addresses = [
        {"npi": 1000000002, "address_key": "second"},
        {"npi": 1000000001, "address_key": "first"},
        {"npi": 1000000002, "address_key": "second"},
    ]

    assert npi_module._geo_candidate_address_pairs(selected_addresses) == [
        (1000000002, "second"),
        (1000000001, "first"),
    ]


async def test_match_query_without_session_avoids_session_settings(monkeypatch):
    """A standalone query preserves parameters without issuing session settings."""
    query_result = _ResultRows([{"npi": 1000000001}])
    execute_query = AsyncMock(return_value=query_result)
    monkeypatch.setattr(npi_module, "_execute_stmt", execute_query)
    query = select(column("npi"))
    query_parameters_by_name = {"npi": 1000000001}

    assert await npi_module._execute_match_candidate_query(query, query_parameters_by_name, None) is query_result
    execute_query.assert_awaited_once_with(query, session=None, params=query_parameters_by_name)


async def test_match_query_cleanup_accepts_synchronous_session_methods():
    """Synchronous cleanup hooks must both run after a query failure."""
    cleanup_events = []
    session = SimpleNamespace(
        rollback=lambda: cleanup_events.append("rollback"), close=lambda: cleanup_events.append("close")
    )

    await npi_module._rollback_match_candidate_session(session)

    assert cleanup_events == ["rollback", "close"]


@pytest.mark.parametrize("payload", ['{"active": true}', "invalid-json"])
def test_directory_evidence_decodes_persisted_json(payload):
    """Persisted JSON evidence is decoded; malformed evidence stays empty."""
    expected_evidence = {"active": True} if payload.startswith("{") else {}
    assert npi_module._provider_directory_evidence_payload({"payload": payload}, "payload") == expected_evidence


def test_directory_role_acceptance_alias_is_bidirectional():
    """Both public acceptance names are available for a legacy role payload."""
    detail = npi_module._provider_directory_role_detail(
        {
            "source_id": "example-directory",
            "resource_id": "example-role",
            "role_new_patient_acceptance": False,
        }
    )

    assert detail["accepting_patients"] is False
    assert detail["new_patient_acceptance"] is False


async def test_location_status_failure_is_visible_when_fail_closed(monkeypatch):
    """Required location-status queries must propagate storage failures."""
    session = SimpleNamespace(execute=AsyncMock(side_effect=RuntimeError("query unavailable")))
    monkeypatch.setattr(npi_module, "db", SimpleNamespace(session=lambda: FakeAcquire(session)))

    with pytest.raises(RuntimeError, match="query unavailable"):
        await npi_module._fetch_location_status_by_record_id(
            ["provider_directory_fhir:practitioner_role:example:role"], fail_closed=True
        )
    session.execute.assert_awaited_once()


async def test_missing_overlay_identity_cannot_be_cached(monkeypatch):
    """A missing identity row must reject generation-bound cache construction."""
    execute_query = AsyncMock(return_value=SimpleNamespace(first=lambda: None))
    monkeypatch.setattr(npi_module, "_execute_stmt", execute_query)

    with pytest.raises(RuntimeError, match="address_serving_identity_missing"):
        await npi_module._provider_directory_address_overlay_serving_identity()
    execute_query.assert_awaited_once()


@pytest.mark.parametrize("route_name", ["list_providers", "get_near_npi"])
async def test_provider_routes_reject_untyped_import_context(monkeypatch, route_name):
    """An untyped import context cannot start any provider storage work."""
    execute_query = AsyncMock()
    monkeypatch.setattr(npi_module, "_execute_stmt", execute_query)
    request = SimpleNamespace(args={}, app=SimpleNamespace())

    with pytest.raises(InvalidUsage, match="context is invalid"):
        await getattr(npi_module, route_name)(request, native_args={}, import_context=object())
    execute_query.assert_not_awaited()


@pytest.mark.parametrize("field_name", ["procedure_code_system", "medication_code_system"])
async def test_nearby_rejects_invalid_code_system_without_codes(monkeypatch, field_name):
    """Code-system validation applies even when no code tokens are supplied."""
    execute_query = AsyncMock()
    monkeypatch.setattr(npi_module, "_execute_stmt", execute_query)
    request = SimpleNamespace(args={field_name: "invalid-system"}, app=SimpleNamespace())

    with pytest.raises(InvalidUsage, match=field_name):
        await npi_module.get_near_npi(request)
    execute_query.assert_not_awaited()


@pytest.mark.parametrize("field_name", ["procedure_code_system", "medication_code_system"])
async def test_provider_search_validates_code_system_without_codes(monkeypatch, field_name):
    """Search rejects unsupported code systems before consulting storage."""
    execute_query = AsyncMock()
    monkeypatch.setattr(npi_module, "_execute_stmt", execute_query)
    request = SimpleNamespace(args={field_name: "invalid-system"}, app=SimpleNamespace())

    with pytest.raises(InvalidUsage, match=field_name):
        await npi_module.list_providers(request)
    execute_query.assert_not_awaited()


@pytest.mark.parametrize(
    ("query_args", "expected_response"),
    [
        ({"count_only": "1", "format": "all"}, {"rows": {}}),
        ({"count_only": "1"}, {"rows": 0}),
        ({}, {"total": 0, "page": 1, "limit": 50, "offset": 0, "total_source": "computed", "rows": []}),
    ],
)
async def test_unresolved_provider_codes_preserve_response_shape(monkeypatch, query_args, expected_response):
    """Unresolved procedure tokens produce the normal empty search/count shape."""
    connection = _install_query_connection(monkeypatch)
    resolve_codes = AsyncMock(return_value=([], "unresolved"))
    monkeypatch.setattr(npi_module, "_resolve_internal_filter_codes", resolve_codes)
    monkeypatch.setattr(npi_module, "_address_serving_table_sql", AsyncMock(return_value="mrf.npi_address"))
    monkeypatch.setattr(npi_module, "_plan_release_npi_scope", AsyncMock(return_value=(None, {})))
    request = SimpleNamespace(args={**query_args, "procedure_codes": "1001"}, app=SimpleNamespace())

    endpoint_response = await npi_module.list_providers(request)

    assert json.loads(endpoint_response.body) == expected_response
    resolve_codes.assert_awaited_once()
    connection.all.assert_not_awaited()


async def test_filtered_insurance_count_returns_scalar_shape(monkeypatch):
    """Insurance-only searches use a filtered exact count without a result page."""
    connection = _install_query_connection(monkeypatch)
    count_insured = AsyncMock(return_value=9)
    monkeypatch.setattr(npi_module, "_fast_has_insurance_count", count_insured)
    monkeypatch.setattr(npi_module, "_address_serving_table_sql", AsyncMock(return_value="mrf.npi_address"))
    monkeypatch.setattr(npi_module, "_plan_release_npi_scope", AsyncMock(return_value=(None, {})))
    request = SimpleNamespace(
        args={"count_only": "1", "has_insurance": "1", "city": "Example City", "state": "AA"}, app=SimpleNamespace()
    )

    endpoint_response = await npi_module.list_providers(request)

    assert json.loads(endpoint_response.body) == {"rows": 9}
    count_insured.assert_awaited_once_with("EXAMPLE CITY", "AA")
    connection.all.assert_not_awaited()


async def test_facility_query_filters_use_bound_request_session(monkeypatch):
    """All facility locators bind to the supplied session, with stats omitted."""
    session = FakeSession([FakeResult(first_row={"total_providers": 0}), FakeResult(), FakeResult()])
    monkeypatch.setattr(npi_module, "_is_table_available", AsyncMock(return_value=True))
    monkeypatch.setattr(npi_module, "db", SimpleNamespace())
    request = SimpleNamespace(
        args={
            "organization_name": "Example Facility",
            "city": "Example City",
            "state": "AA",
            "reporting_year": "2024",
            "include_specialty_stats": "0",
        },
        ctx=SimpleNamespace(sa_session=session),
    )

    endpoint_response = await npi_module.get_facility_connected_providers(request)

    response_map = json.loads(endpoint_response.body)
    assert response_map["providers"] == []
    assert response_map["total_providers"] == 0
    assert "specialty_stats" not in response_map
    assert len(session.calls) == 3
    for statement_sql, query_parameters in session.calls:
        assert "LIKE :organization_name" in statement_sql
        assert "= :city" in statement_sql
        assert "= :state" in statement_sql
        assert "= :reporting_year" in statement_sql
        assert query_parameters["organization_name"] == "%example facility%"
        assert query_parameters["city"] == "EXAMPLE CITY"
        assert query_parameters["state"] == "AA"
        assert query_parameters["reporting_year"] == 2024
        assert "ccn" not in query_parameters


@pytest.mark.parametrize(("classification_npis", "offset"), [([], "0"), ([1000000001], "1")])
async def test_sitemap_empty_page_skips_provider_query(monkeypatch, classification_npis, offset):
    """Empty taxonomy membership and an exhausted page both return no rows."""
    connection = _install_query_connection(monkeypatch)
    get_members = AsyncMock(return_value=classification_npis)
    monkeypatch.setattr(npi_module, "_get_classification_npi_list", get_members)
    monkeypatch.setattr(npi_module, "_address_serving_table_sql", AsyncMock(return_value="mrf.npi_address"))
    monkeypatch.setattr(npi_module, "_plan_release_npi_scope", AsyncMock(return_value=(None, {})))
    request = SimpleNamespace(
        args={"view": "sitemap", "classification": "Pharmacy", "offset": offset, "include_total": "0"},
        app=SimpleNamespace(),
    )

    endpoint_response = await npi_module.list_providers(request)

    assert json.loads(endpoint_response.body)["rows"] == []
    get_members.assert_awaited_once_with("Pharmacy", primary_only=True, session=None)
    connection.all.assert_not_awaited()


async def test_sitemap_provider_rows_keep_public_projection(monkeypatch):
    """Sitemap pages retain address labels and omit unprojected result rows."""
    connection = _install_query_connection(
        monkeypatch,
        [
            ("unprojected",),
            SimpleNamespace(
                _mapping={
                    "npi": 1000000001,
                    "provider_organization_name": "Example Pharmacy",
                    "formatted_address": "1 Example Road",
                }
            ),
        ],
    )
    monkeypatch.setattr(npi_module, "_get_classification_npi_list", AsyncMock(return_value=[1000000001]))
    monkeypatch.setattr(npi_module, "_address_serving_table_sql", AsyncMock(return_value="mrf.npi_address"))
    monkeypatch.setattr(npi_module, "_plan_release_npi_scope", AsyncMock(return_value=(None, {})))
    request = SimpleNamespace(
        args={"view": "sitemap", "classification": "Pharmacy", "include_total": "0"}, app=SimpleNamespace()
    )

    endpoint_response = await npi_module.list_providers(request)

    provider_rows = json.loads(endpoint_response.body)["rows"]
    assert len(provider_rows) == 1
    assert provider_rows[0]["npi"] == 1000000001
    assert provider_rows[0]["provider_organization_name"] == "Example Pharmacy"
    assert provider_rows[0]["formatted_address"] == "1 Example Road"
    assert provider_rows[0]["do_business_as"] == []
    connection.all.assert_awaited_once()
    assert connection.all.await_args.kwargs["page_npis"] == [1000000001]


@pytest.mark.parametrize(
    ("query_args", "has_session", "has_cursor", "error_type", "message"),
    [
        ({}, False, False, InvalidUsage, "cursor is invalid"),
        ({}, False, True, RuntimeError, "requires a request session"),
        ({"limit": "51"}, True, True, InvalidUsage, "must not exceed 50"),
        ({}, True, True, InvalidUsage, "require exact totals"),
    ],
)
async def test_imported_nearby_pages_require_complete_query_context(
    query_args, has_session, has_cursor, error_type, message
):
    """Imported nearby pages reject missing session, cursor, total, and limit contracts."""
    request = SimpleNamespace(args={}, ctx=SimpleNamespace(sa_session=object() if has_session else None))

    with pytest.raises(error_type, match=message):
        await npi_module.get_near_npi(
            request,
            native_args=query_args,
            import_context=imported_geo_context(),
            prepare_cursor=AsyncMock() if has_cursor else None,
        )


async def test_sealed_primary_fallback_count_is_cached(monkeypatch):
    """Fallback counting binds its cached value to the sealed publication."""
    connection = _install_query_connection(monkeypatch, [(6,)])
    monkeypatch.setattr(npi_module, "_address_serving_model", AsyncMock(return_value=npi_module.NPIAddress))
    monkeypatch.setattr(npi_module, "_npi_count_cache_identity", AsyncMock(return_value="publication-one"))
    monkeypatch.setattr(npi_module, "_primary_total_cache_get", Mock(return_value=None))
    store_count = Mock(return_value=6)
    monkeypatch.setattr(npi_module, "_primary_total_cache_set", store_count)

    assert await npi_module._fast_primary_npi_count() == 6

    store_count.assert_called_once_with("publication-one", 6)
    connection.all.assert_awaited_once()


def test_hydration_ignores_rows_without_provider_identity():
    """Unattributed rows are skipped while inferred provider locations survive."""
    selected_columns = [column("npi"), column("inferred_npi"), column("checksum")]
    query_rows = _ResultRows(
        [
            {"npi": None, "inferred_npi": None, "checksum": 11},
            {"npi": None, "inferred_npi": 1000000001, "checksum": 12},
        ]
    )

    hydrated_by_npi = npi_module._hydrate_address_query_rows_map(query_rows, selected_columns, set())

    assert list(hydrated_by_npi) == [1000000001]
    assert hydrated_by_npi[1000000001][0]["npi"] == 1000000001
    assert hydrated_by_npi[1000000001][0]["checksum"] == 12


def test_detail_projection_preserves_zero_total_and_unified_location_key(monkeypatch):
    """A zero address count and opt-in unified key stay explicit in output."""
    monkeypatch.setattr(npi_module, "_npi_serving_columns", lambda: (column("npi"),))

    detail = npi_module._npi_detail_from_result_row((1000000001, [], []), include_address_rows=False, address_total=0)
    allowed_columns = npi_module._npi_detail_allowed_address_columns(
        npi_module.EntityAddressUnified, include_sources=False, include_evidence=False, include_location_key=True
    )

    assert detail["npi"] == 1000000001
    assert detail["address_total"] == 0
    assert detail["address_list"] == []
    assert "location_key" in allowed_columns


async def test_other_names_without_checksum_are_not_discarded(monkeypatch):
    """Legacy other-name rows without checksums preserve each distinct label."""
    name_rows = [
        SimpleNamespace(npi=1000000001, to_json_dict=lambda: {"npi": 1000000001, "name": "Example Clinic"}),
        SimpleNamespace(npi=1000000001, to_json_dict=lambda: {"npi": 1000000001, "name": "Example Practice"}),
    ]
    execute_query = AsyncMock(return_value=SimpleNamespace(scalars=lambda: name_rows))
    monkeypatch.setattr(npi_module, "_execute_stmt", execute_query)

    names_by_npi = await npi_module._fetch_other_names_map([1000000001])

    assert names_by_npi == {1000000001: [{"name": "Example Clinic"}, {"name": "Example Practice"}]}
    execute_query.assert_awaited_once()


async def test_unavailable_affiliation_evidence_skips_query(monkeypatch):
    """Missing required directory relations cannot yield affiliation evidence."""
    execute_query = AsyncMock()
    table_flags = AsyncMock(return_value=None)
    monkeypatch.setattr(npi_module, "_provider_directory_evidence_tables", table_flags)
    monkeypatch.setattr(npi_module, "_execute_stmt", execute_query)
    session = object()

    assert (
        await npi_module._fetch_provider_directory_affiliation_evidence_map(
            [("example-source", "example-affiliation")], session=session
        )
        == {}
    )
    assert table_flags.await_args.args[0] is session
    execute_query.assert_not_awaited()


def test_visible_enrollments_do_not_override_summary_without_chain():
    """A provider with only ordinary enrollments retains its summary values."""
    summary_by_npi = {1000000001: {"ffs_enrollment_ids": ["example-enrollment"]}}
    enrollments_by_npi = {1000000001: [{"enrollment_id": "example-enrollment", "multiple_npi_flag": "N"}]}

    assert npi_module._visible_ffs_rows_by_npi(summary_by_npi, enrollments_by_npi, include_chain=False) == {}
    assert summary_by_npi[1000000001]["ffs_enrollment_ids"] == ["example-enrollment"]
    assert summary_by_npi[1000000001]["ffs_chain_hidden"] is False
    assert summary_by_npi[1000000001]["ffs_chain_enrollment_count"] == 0


@pytest.mark.parametrize(
    ("address_groups", "is_cacheable"),
    [
        (["ignored", {"members": [{"lat": 1}]}], True),
        ([{"members": [{"lat": 1}, {}]}], False),
        ([{"members": []}], True),
    ],
)
def test_grouped_addresses_control_cacheability(address_groups, is_cacheable):
    assert (
        npi_module._is_npi_detail_response_cacheable(
            {"address_groups": address_groups},
            force_address_update=False,
            sync_geocode=True,
        )
        is is_cacheable
    )


@pytest.mark.parametrize(("ttl", "max_keys"), [(0, 2), (10, 0)])
def test_disabled_cache_preserves_response(monkeypatch, ttl, max_keys):
    response_cache = OrderedDict()
    monkeypatch.setattr(npi_module, "_NPI_DETAIL_RESPONSE_CACHE", response_cache)
    monkeypatch.setattr(npi_module, "_NPI_DETAIL_RESPONSE_CACHE_TTL_SECONDS", ttl)
    monkeypatch.setattr(npi_module, "_NPI_DETAIL_RESPONSE_CACHE_MAX_KEYS", max_keys)

    assert npi_module._npi_detail_response_cache_set("provider", b"detail") == b"detail"
    assert not response_cache


def test_response_cache_evicts_oldest_provider(monkeypatch):
    response_cache = OrderedDict()
    monkeypatch.setattr(npi_module, "_NPI_DETAIL_RESPONSE_CACHE", response_cache)
    monkeypatch.setattr(npi_module, "_NPI_DETAIL_RESPONSE_CACHE_TTL_SECONDS", 10)
    monkeypatch.setattr(npi_module, "_NPI_DETAIL_RESPONSE_CACHE_MAX_KEYS", 2)
    monkeypatch.setattr(npi_module.time, "monotonic", lambda: 1)

    for provider_key in ("first", "second", "first", "third"):
        npi_module._npi_detail_response_cache_set(provider_key, provider_key.encode())

    assert list(response_cache) == ["first", "third"]
    assert npi_module._npi_detail_response_cache_get("second") is None
    assert npi_module._npi_detail_response_cache_get("first") == b"first"


def _address_projection_table():
    return table(
        "retained_addresses",
        column("npi"),
        column("checksum"),
        column("premise_key"),
        column("procedures_array"),
        column("medications_array"),
    )


@pytest.mark.parametrize(
    ("existing_columns", "has_arrays", "expected_columns", "empty_arrays"),
    [
        (
            {"npi", "checksum", "procedures_array", "medications_array"},
            True,
            ["npi", "checksum", "procedures_array", "medications_array"],
            0,
        ),
        ({"npi", "checksum"}, True, ["npi", "checksum", "procedures_array", "medications_array"], 2),
        (
            {"npi", "checksum", "procedures_array", "medications_array"},
            False,
            ["npi", "checksum", "procedures_array", "medications_array"],
            2,
        ),
        ({"npi", "procedures_array", "medications_array"}, True, ["npi", "procedures_array", "medications_array"], 0),
    ],
)
def test_address_projection_preserves_safe_arrays(
    existing_columns,
    has_arrays,
    expected_columns,
    empty_arrays,
):
    address_columns = npi_module._npi_detail_address_columns(
        _address_projection_table(),
        existing_columns,
        {"npi", "checksum", "procedures_array", "medications_array"},
        {"npi_procedures_array_available": has_arrays, "npi_medications_array_available": has_arrays},
    )

    assert [address_column.key for address_column in address_columns] == expected_columns
    statement_sql = str(select(*address_columns))
    assert statement_sql.count("'{}'::INTEGER[]") == empty_arrays
    assert "premise_key" not in statement_sql


@pytest.mark.parametrize(
    ("address_model", "identities", "expected_selection", "uses_inferred_npi"),
    [
        (npi_module.NPIAddress, None, None, False),
        (
            npi_module.NPIAddress,
            ["legacy:address:12", "legacy:address:-7", "legacy:bad", "location:ignored"],
            [-7, 12],
            False,
        ),
        (
            npi_module.EntityAddressUnified,
            ["location:site-b", "legacy:ignored", "location:site-a"],
            ["site-a", "site-b"],
            True,
        ),
    ],
)
def test_address_filters_preserve_selected_identities(
    address_model,
    identities,
    expected_selection,
    uses_inferred_npi,
):
    address_filters = npi_module._npi_detail_address_filters(
        address_model,
        address_model.__table__,
        1000000001,
        "selected-address",
        identities,
    )
    compiled_statement = select(address_model.__table__.c.npi).where(*address_filters).compile()

    assert ("coalesce(" in str(compiled_statement)) is uses_inferred_npi
    assert "selected-address" in compiled_statement.params.values()
    assert 1000000001 in compiled_statement.params.values()
    if expected_selection is not None:
        assert expected_selection in compiled_statement.params.values()
        assert len(address_filters) == 3
    else:
        assert len(address_filters) == 2


def _reviewed_enrichment_override():
    return {
        "ffs_enrollment_ids": ["reviewed"],
        "ffs_pecos_asct_cntl_ids": ["reviewed-pecos"],
        "ffs_secondary_provider_type_codes": ["01"],
        "ffs_secondary_provider_type_texts": ["Synthetic specialty"],
        "ffs_practice_zip_codes": ["12345"],
        "ffs_practice_cities": ["Example City"],
        "ffs_practice_states": ["AA"],
        "ffs_related_npis": [1000000003],
        "ffs_related_npi_count": 1,
        "ffs_reassignment_in_count": 2,
        "ffs_reassignment_out_count": 3,
    }


def test_enrichment_overrides_keep_providers_independent():
    summaries_by_npi = {1000000001: {"status": "first"}, 1000000002: {"status": "second"}}
    visible_by_npi = {
        1000000001: [{"enrollment_id": "unreviewed"}],
        1000000002: [
            {"enrollment_id": " E1 ", "pecos_asct_cntl_id": "P1"},
            {"enrollment_id": "E1", "pecos_asct_cntl_id": " P1 "},
            {"enrollment_id": "", "pecos_asct_cntl_id": None},
        ],
        1000000003: [{"enrollment_id": "not-in-summary"}],
    }
    overrides_by_npi = {1000000001: _reviewed_enrichment_override()}
    original_visible = deepcopy(visible_by_npi)
    original_overrides = deepcopy(overrides_by_npi)

    npi_module._apply_provider_enrichment_overrides(
        summaries_by_npi,
        visible_by_npi,
        overrides_by_npi,
    )

    assert summaries_by_npi[1000000001] == {"status": "first", **original_overrides[1000000001]}
    assert summaries_by_npi[1000000002] == {
        "status": "second",
        "ffs_enrollment_ids": ["E1"],
        "ffs_pecos_asct_cntl_ids": ["P1"],
        "ffs_secondary_provider_type_codes": [],
        "ffs_secondary_provider_type_texts": [],
        "ffs_practice_zip_codes": [],
        "ffs_practice_cities": [],
        "ffs_practice_states": [],
        "ffs_related_npis": [],
        "ffs_related_npi_count": 0,
        "ffs_reassignment_in_count": 0,
        "ffs_reassignment_out_count": 0,
    }
    assert set(summaries_by_npi) == {1000000001, 1000000002}
    assert visible_by_npi == original_visible
    assert overrides_by_npi == original_overrides


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("address_model", "has_premise_column", "should_query"),
    [
        (npi_module.NPIAddress, False, False),
        (npi_module.EntityAddressUnified, False, False),
        (npi_module.EntityAddressUnified, True, True),
    ],
)
async def test_site_candidates_require_compatible_storage(
    monkeypatch,
    address_model,
    has_premise_column,
    should_query,
):
    location_mapping = unified_location_mapping()
    location_mapping["premise_key"] = "selected-site"
    existing_columns = set(location_mapping) & set(address_model.__table__.c.keys())
    if has_premise_column:
        existing_columns.add("premise_key")
    else:
        existing_columns.discard("premise_key")
    monkeypatch.setattr(npi_module, "_address_serving_model", AsyncMock(return_value=address_model))
    monkeypatch.setattr(npi_module, "_table_columns", AsyncMock(return_value=existing_columns))
    execute = AsyncMock(return_value=_ResultRows([location_mapping]))
    monkeypatch.setattr(npi_module, "_execute_stmt", execute)

    candidates_by_npi = await npi_module._fetch_npi_location_candidates_map(
        [1234567890],
        address_key="selected-address",
        address_site_key="selected-site",
    )

    if should_query:
        assert set(candidates_by_npi) == {1234567890}
        compiled_statement = execute.await_args.args[0].compile()
        assert "selected-site" in compiled_statement.params.values()
        assert "selected-address" in compiled_statement.params.values()
        assert "premise_key" in str(compiled_statement)
    else:
        assert candidates_by_npi == {}
        execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_empty_candidate_batch_avoids_storage(monkeypatch):
    serving_model = AsyncMock()
    execute = AsyncMock()
    monkeypatch.setattr(npi_module, "_address_serving_model", serving_model)
    monkeypatch.setattr(npi_module, "_execute_stmt", execute)

    assert await npi_module._fetch_npi_location_candidates_map([]) == {}
    serving_model.assert_not_awaited()
    execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("relation_oid", "has_identity"),
    [(None, False), (False, False), (True, False), (0, False), (-1, False), ("7", False), (7.0, False), (7, True)],
)
async def test_count_cache_requires_real_relation(monkeypatch, relation_oid, has_identity):
    publication_identity = "1:nppub1_" + "a" * 43
    monkeypatch.setattr(
        npi_module,
        "_npi_canonical_publication_identity",
        AsyncMock(return_value=publication_identity),
    )
    execute = AsyncMock(return_value=_ScalarResult(relation_oid))
    monkeypatch.setattr(npi_module, "_execute_stmt", execute)

    identity = await npi_module._npi_count_cache_identity(npi_module.EntityAddressUnified)

    assert identity == (f"{publication_identity}|address:oid:7" if has_identity else None)
    assert "to_regclass" in str(execute.await_args.args[0])


@pytest.mark.asyncio
async def test_count_cache_skips_unsealed_publication(monkeypatch):
    monkeypatch.setattr(npi_module, "_npi_canonical_publication_identity", AsyncMock(return_value=None))
    execute = AsyncMock()
    monkeypatch.setattr(npi_module, "_execute_stmt", execute)

    assert await npi_module._npi_count_cache_identity(npi_module.EntityAddressUnified) is None
    execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_count_cache_survives_catalog_failure(monkeypatch):
    monkeypatch.setattr(
        npi_module,
        "_npi_canonical_publication_identity",
        AsyncMock(return_value="sealed-publication"),
    )
    monkeypatch.setattr(npi_module, "_execute_stmt", AsyncMock(side_effect=RuntimeError("catalog unavailable")))

    assert await npi_module._npi_count_cache_identity(npi_module.EntityAddressUnified) is None


def _install_query_connection(monkeypatch, query_rows=()):
    connection = SimpleNamespace(
        all=AsyncMock(return_value=list(query_rows)),
        first=AsyncMock(return_value=None),
    )
    monkeypatch.setattr(npi_module, "db", SimpleNamespace(acquire=lambda: FakeAcquire(connection)))
    return connection


@pytest.mark.asyncio
@pytest.mark.parametrize(("has_arrays", "has_relations"), [(True, True), (True, False), (False, True), (False, False)])
async def test_nearby_filters_preserve_code_sources(monkeypatch, has_arrays, has_relations):
    connection = _install_query_connection(monkeypatch)
    monkeypatch.setattr(npi_module, "_address_serving_table_sql", AsyncMock(return_value="mrf.npi_address"))
    monkeypatch.setattr(npi_module, "_plan_release_npi_scope", AsyncMock(return_value=(None, {})))
    monkeypatch.setattr(
        npi_module,
        "_resolve_npi_filter_capabilities",
        AsyncMock(
            return_value={
                "npi_procedures_array_available": has_arrays,
                "npi_medications_array_available": has_arrays,
                "pricing_provider_procedure_available": has_relations,
                "pricing_provider_prescription_available": has_relations,
            }
        ),
    )
    request = SimpleNamespace(
        args={
            "long": "-87",
            "lat": "41",
            "procedure_codes": "1001",
            "medication_codes": "2001",
            "year": "2023",
            "limit": "1",
        },
        app=SimpleNamespace(),
    )

    endpoint_response = await npi_module.get_near_npi(request)

    assert json.loads(endpoint_response.body) == []
    statement_sql = str(connection.all.await_args.args[0])
    assert ("a.procedures_array @>" in statement_sql) is has_arrays
    assert ("a.medications_array @>" in statement_sql) is has_arrays
    assert ("FROM mrf.pricing_provider_procedure" in statement_sql) is has_relations
    assert ("FROM mrf.pricing_provider_prescription" in statement_sql) is has_relations
    if has_arrays and has_relations:
        assert statement_sql.count(" OR EXISTS (") == 2
    if not has_arrays and not has_relations:
        assert statement_sql.count("1=0") == 2
    assert connection.all.await_args.kwargs["procedure_code_0"] == 1001
    assert connection.all.await_args.kwargs["medication_code_0"] == 2001
    assert connection.all.await_args.kwargs["filter_year"] == 2023


@pytest.mark.asyncio
@pytest.mark.parametrize("include_stats", [False, True])
async def test_missing_facility_preserves_empty_shape(monkeypatch, include_stats):
    connection = _install_query_connection(monkeypatch)
    monkeypatch.setattr(npi_module, "_is_table_available", AsyncMock(return_value=False))
    request = SimpleNamespace(
        args={
            "organization_name": "Synthetic Facility",
            "include_specialty_stats": str(int(include_stats)),
        },
        app=SimpleNamespace(),
    )

    endpoint_response = await npi_module.get_facility_connected_providers(request)

    response_map = json.loads(endpoint_response.body)
    assert response_map["providers"] == []
    assert response_map["total_providers"] == 0
    assert response_map["matched_facilities"] == []
    assert response_map["query"]["organization_name"] == "Synthetic Facility"
    assert ("specialty_stats" in response_map) is include_stats
    if include_stats:
        assert response_map["specialty_stats"] == []
    connection.all.assert_not_awaited()


def _classification_filter_args(include_site):
    args_by_name = {
        "count_only": "1",
        "format": "all",
        "classification": "Family Medicine",
        "specialization": "General Practice",
        "section": "Physicians",
        "display_name": "Synthetic Specialty",
        "codes": "207Q00000X",
        "plan_network": "11,12",
        "has_insurance": "1",
        "city": "Example City",
        "state": "IL",
        "zip_code": "60601",
        "phone": "5550101234",
        "address_key": "11111111-1111-4111-8111-111111111111",
        "npi": "1000000001",
        "first_name": "Example",
        "last_name": "Provider",
        "entity_type_code": "1",
        "provider_sex_code": "F",
    }
    if include_site:
        args_by_name["address_site_key"] = "22222222-2222-4222-8222-222222222222"
    return args_by_name


@pytest.mark.asyncio
@pytest.mark.parametrize("uses_unified_addresses", [False, True])
async def test_classification_counts_keep_optional_predicates(monkeypatch, uses_unified_addresses):
    connection = _install_query_connection(monkeypatch, [("Synthetic Classification", 4)])
    address_table_sql = "mrf.entity_address_unified" if uses_unified_addresses else "mrf.npi_address"
    monkeypatch.setattr(npi_module, "_address_serving_table_sql", AsyncMock(return_value=address_table_sql))
    monkeypatch.setattr(
        npi_module,
        "_plan_release_npi_scope",
        AsyncMock(
            return_value=(
                "SELECT npi FROM mrf.npi WHERE npi = :plan_scope_npi",
                {"plan_scope_npi": 1000000001},
            )
        ),
    )
    request = SimpleNamespace(args=_classification_filter_args(uses_unified_addresses), app=SimpleNamespace())

    endpoint_response = await npi_module.get_all(request)

    assert json.loads(endpoint_response.body) == {"rows": {"Synthetic Classification": 4}}
    statement_sql = str(connection.all.await_args.args[0])
    query_parameters = connection.all.await_args.kwargs
    assert "COUNT(DISTINCT ft.npi)" in statement_sql
    assert "plans_network_array && :plan_network_array" in statement_sql
    assert "NOT (plans_network_array @@ '0'::query_int)" in statement_sql
    assert "classification = :classification" in statement_sql
    assert "specialization = :specialization" in statement_sql
    assert "section = :section" in statement_sql
    assert "display_name = :display_name" in statement_sql
    assert "code = ANY(:codes)" in statement_sql
    assert "sex_provider.provider_sex_code = :provider_sex_code" in statement_sql
    assert "SELECT npi FROM mrf.npi WHERE npi = :plan_scope_npi" in statement_sql
    assert query_parameters["plan_network_array"] == [11, 12]
    assert query_parameters["city"] == "EXAMPLE CITY"
    assert query_parameters["state"] == "IL"
    assert query_parameters["zip_code"] == "60601"
    assert query_parameters["npi_filter"] == query_parameters["plan_scope_npi"] == 1000000001
    assert query_parameters["phone_digits"] == "5550101234"
    assert ("phone_candidates AS MATERIALIZED" in statement_sql) is uses_unified_addresses
    if uses_unified_addresses:
        assert "c.premise_key = CAST(:address_site_key AS uuid)" in statement_sql
    else:
        assert "regexp_replace(COALESCE(c.telephone_number" in statement_sql
