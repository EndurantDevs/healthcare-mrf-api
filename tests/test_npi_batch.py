import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
import yaml
from sanic.exceptions import InvalidUsage

from api import provider_batch
from api.endpoint import npi as npi_module
from process.custom_import.read_contracts import CustomImportReadUnavailableError
from tests.npi_location_hydration_support import unified_location_mapping


class _ResultRows:
    def __init__(self, rows):
        self._rows = rows

    def all(self):
        return self._rows


@pytest.mark.asyncio
@pytest.mark.parametrize("native_query", [{}, {"limit": "1"}, {"offset": "2"}, {"order_by": "npi", "limit": "1"}])
async def test_extended_batch_pages_successes_without_reclassifying_excluded_rows(monkeypatch, native_query):
    requested_npis = [9000000001, 9000000000, 9000000002, 9000000003]
    params = npi_module._normalize_npi_batch_request({"npis": requested_npis})
    state = provider_batch._NativeBatchState(
        {identity: {"npi": identity} for identity in requested_npis[:2]}, {identity: [] for identity in requested_npis}
    )
    monkeypatch.setattr(provider_batch, "_prepare_native_batch", AsyncMock(return_value=state))

    async def eligible(request, native_args, context, original, found, session, **kwargs):
        assert original == requested_npis and found == requested_npis[:2]
        return sorted(found)

    async def hydrate(selected, same_state, options, native_args, session, **kwargs):
        assert same_state is state
        return [{"npi": identity, "status": 200, "provider": {"npi": identity}} for identity in selected]

    monkeypatch.setattr(provider_batch, "_batch_eligible_npis", eligible)
    monkeypatch.setattr(provider_batch, "_hydrate_native_batch", hydrate)
    batch_payload = await provider_batch.read_native_batch(
        SimpleNamespace(), params, native_args=provider_batch.parse_native_batch_query(native_query), session=object()
    )
    assert (batch_payload["requested"], batch_payload["found"], batch_payload["not_found"]) == (4, 2, 2)
    assert batch_payload["pagination"]["total"] == 2
    failures = [provider_item["npi"] for provider_item in batch_payload["items"] if provider_item["status"] == 404]
    assert failures == requested_npis[2:]
    if not native_query:
        assert [provider_item["npi"] for provider_item in batch_payload["items"]] == requested_npis
        assert batch_payload["pagination"]["limit"] == 4
    elif "offset" in native_query:
        assert len(batch_payload["items"]) == 2 and batch_payload["pagination"]["has_more"] is False
    else:
        expected = 9000000000 if "order_by" in native_query else 9000000001
        assert batch_payload["items"][0]["npi"] == expected and batch_payload["pagination"]["has_more"] is True


@pytest.mark.asyncio
async def test_extended_batch_uses_configured_effective_sort_before_pagination(monkeypatch):
    requested_npis = [9000000001, 9000000000]
    params = npi_module._normalize_npi_batch_request({"npis": requested_npis})
    state = provider_batch._NativeBatchState(
        {identity: {"npi": identity} for identity in requested_npis}, {identity: [] for identity in requested_npis}
    )
    context = SimpleNamespace(prepared=SimpleNamespace(normalized_order_terms=("configured",)))
    monkeypatch.setattr(provider_batch, "_prepare_native_batch", AsyncMock(return_value=state))
    monkeypatch.setattr(provider_batch, "_batch_eligible_npis", AsyncMock(return_value=list(reversed(requested_npis))))
    hydrate = AsyncMock(
        return_value=[{"npi": requested_npis[1], "status": 200, "provider": {"npi": requested_npis[1]}}]
    )
    monkeypatch.setattr(provider_batch, "_hydrate_native_batch", hydrate)
    payload = await provider_batch.read_native_batch(
        SimpleNamespace(),
        params,
        native_args=provider_batch.parse_native_batch_query({"limit": "1"}),
        session=object(),
        import_context=context,
    )
    assert hydrate.await_args.args[0] == [requested_npis[1]]
    assert payload["found"] == 2 and payload["not_found"] == 0 and payload["pagination"]["has_more"]


@pytest.mark.asyncio
async def test_batch_native_filters_forward_one_finite_scope_and_all_predicates(monkeypatch):
    requested_npis = [9000000000, 9000000001]
    args = provider_batch.parse_native_batch_query(
        {"q": "Synthetic", "zip_code": "12345", "page": "8", "limit": "1", "view": "card"}
    )
    reply = SimpleNamespace(
        status=200,
        body=json.dumps({"rows": [{"npi": requested_npis[1]}], "total": 1, "total_source": "computed"}).encode(),
    )
    read = AsyncMock(return_value=reply)
    monkeypatch.setattr(npi_module, "list_providers", read)
    selected = await provider_batch._batch_eligible_npis(
        SimpleNamespace(), args, None, requested_npis, requested_npis, object()
    )
    assert selected == [requested_npis[1]] and read.await_count == 1
    forwarded = read.await_args.kwargs
    assert forwarded["native_npis"] == tuple(requested_npis) and forwarded["import_context"] is None
    assert forwarded["native_args"].get("q") == "Synthetic" and forwarded["native_args"].get("zip_code") == "12345"
    assert forwarded["native_args"].get("page") == "1" and forwarded["native_args"].get("limit") == "2"
    assert forwarded["native_args"].get("view") == "card"


@pytest.mark.asyncio
async def test_batch_exact_npi_keeps_identity_only_provider_without_address_queries(monkeypatch):
    requested_npis = [9000000000, 9000000001]
    read = AsyncMock(side_effect=AssertionError("exact identity scope needs no address eligibility query"))
    monkeypatch.setattr(npi_module, "list_providers", read)
    selected = await provider_batch._batch_eligible_npis(
        SimpleNamespace(),
        provider_batch.parse_native_batch_query({"npi": str(requested_npis[1])}),
        None,
        requested_npis,
        requested_npis,
        object(),
    )
    assert selected == [requested_npis[1]] and read.await_count == 0


@pytest.mark.parametrize(
    "native_query",
    [
        {"view": "sitemap"},
        {"view": "unknown"},
        {"address_grouping": "other"},
        {"address_grouping": "premise", "address_key": "00000000-0000-0000-0000-000000000001"},
        {"extra_info": True},
        {"unsupported": "true"},
    ],
)
def test_batch_list_shape_is_closed_and_preserves_original_grouping_constraints(native_query):
    with pytest.raises((InvalidUsage, ValueError)):
        provider_batch.parse_native_batch_query(native_query)


@pytest.mark.asyncio
async def test_native_batch_endpoint_reuses_extended_shared_path_only_when_requested(monkeypatch):
    params_by_field = {"npis": [9000000000], "native_query": {"q": "Synthetic", "view": "full"}}
    shared = AsyncMock(
        return_value={"items": [], "requested": 1, "found": 0, "not_found": 1, "pagination": {"total": 0}}
    )
    original = AsyncMock(side_effect=AssertionError("extended list query must not be dropped"))
    monkeypatch.setattr(provider_batch, "read_native_batch", shared)
    monkeypatch.setattr(npi_module, "_build_npi_batch_payload", original)
    request = SimpleNamespace(args={}, json=params_by_field, ctx=SimpleNamespace(sa_session=object()))
    reply = await npi_module.get_npi_batch(request)
    assert reply.status == 200 and shared.await_count == 1 and original.await_count == 0
    assert shared.await_args.kwargs["native_args"].get("q") == "Synthetic"
    assert json.loads(reply.body)["meta"]["view"] == "summary"


@pytest.mark.asyncio
async def test_batch_extra_info_preserves_nonstreet_addresses_before_ranking(monkeypatch):
    identity = 9000000000
    street_by_field = {**_ranked_batch_addresses(identity)[0], "type": "primary"}
    mail_by_field = {**street_by_field, "type": "mail", "first_line": "PO Box 1", "address_key": "synthetic-mail"}
    monkeypatch.setattr(
        npi_module, "_build_npi_identity_details_map", AsyncMock(return_value={identity: {"npi": identity}})
    )
    ranked = AsyncMock(return_value={identity: [street_by_field]})
    monkeypatch.setattr(npi_module, "_rank_npi_batch_addresses", ranked)
    candidates = AsyncMock(return_value={identity: [street_by_field, mail_by_field]})
    monkeypatch.setattr(npi_module, "_fetch_npi_location_candidates_map", candidates)
    monkeypatch.setattr(npi_module, "_fetch_provider_directory_address_overlay_map", AsyncMock(return_value={}))
    monkeypatch.setattr(npi_module, "_apply_location_statuses", AsyncMock())
    ordinary = await provider_batch._prepare_native_batch(
        [identity], provider_batch.parse_native_batch_query({}), object()
    )
    expanded = await provider_batch._prepare_native_batch(
        [identity], provider_batch.parse_native_batch_query({"extra_info": "true"}), object()
    )
    assert ordinary.addresses[identity] == [street_by_field]
    assert {address["first_line"] for address in expanded.addresses[identity]} == {"1 Main Street", "PO Box 1"}
    assert ranked.await_count == candidates.await_count == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("extra_info", [False, True])
@pytest.mark.parametrize("imported", [False, True])
async def test_batch_status_lookup_uses_the_pinned_import_session(monkeypatch, extra_info, imported):
    identity = 9000000000
    session = object()
    role_id = "provider_directory_fhir:practitioner_role:synthetic:role"
    address_by_field = {**_ranked_batch_addresses(identity)[0], "source_record_ids": [role_id]}
    monkeypatch.setattr(
        npi_module, "_build_npi_identity_details_map", AsyncMock(return_value={identity: {"npi": identity}})
    )
    monkeypatch.setattr(
        npi_module, "_fetch_npi_location_candidates_map", AsyncMock(return_value={identity: [address_by_field]})
    )
    monkeypatch.setattr(npi_module, "_fetch_provider_directory_address_overlay_map", AsyncMock(return_value={}))
    statuses = AsyncMock(return_value={role_id: "active"})
    monkeypatch.setattr(npi_module, "_fetch_location_status_by_record_id", statuses)
    args = provider_batch.parse_native_batch_query({"extra_info": str(extra_info).lower()})

    state = await provider_batch._prepare_native_batch(
        [identity], args, session, import_context=object() if imported else None
    )

    statuses.assert_awaited_once_with([role_id], session=session, use_request_session=imported, fail_closed=imported)
    assert state.addresses[identity][0]["location_status"] == "active"


@pytest.mark.asyncio
@pytest.mark.parametrize("extra_info", [False, True])
async def test_import_batch_status_failure_propagates_before_provider_selection(monkeypatch, extra_info):
    identity = 9000000000
    address_by_field = {
        **_ranked_batch_addresses(identity)[0],
        "source_record_ids": ["provider_directory_fhir:practitioner_role:synthetic:role"],
    }
    monkeypatch.setattr(npi_module, "_build_npi_identity_details_map", AsyncMock(return_value={}))
    monkeypatch.setattr(
        npi_module, "_fetch_npi_location_candidates_map", AsyncMock(return_value={identity: [address_by_field]})
    )
    monkeypatch.setattr(npi_module, "_fetch_provider_directory_address_overlay_map", AsyncMock(return_value={}))
    statuses = AsyncMock(side_effect=RuntimeError("status unavailable"))
    monkeypatch.setattr(npi_module, "_fetch_location_status_by_record_id", statuses)
    args = provider_batch.parse_native_batch_query({"extra_info": str(extra_info).lower()})
    session = object()

    with pytest.raises(RuntimeError, match="status unavailable"):
        await provider_batch._prepare_native_batch([identity], args, session, import_context=object())

    assert statuses.await_args.kwargs == {"session": session, "use_request_session": True, "fail_closed": True}


@pytest.mark.asyncio
@pytest.mark.parametrize("view", ["full", "card"])
async def test_batch_shaping_reuses_selected_addresses_and_card_projection(monkeypatch, view):
    identity = 9000000000
    addresses = _ranked_batch_addresses(identity)
    state = provider_batch._NativeBatchState(
        {identity: {"npi": identity, "provider_first_name": "Synthetic"}}, {identity: addresses}
    )
    hydrate = AsyncMock(return_value={identity: [addresses[1]]})
    names = AsyncMock(return_value={identity: []})
    summaries = AsyncMock(return_value={})
    monkeypatch.setattr(npi_module, "_hydrate_npi_batch_addresses", hydrate)
    monkeypatch.setattr(npi_module, "_fetch_other_names_map", names)
    monkeypatch.setattr(npi_module, "_fetch_provider_enrichment_summary_map", summaries)
    params = npi_module._normalize_npi_batch_request({"npis": [identity], "address_limit": 1, "address_offset": 1})
    provider_rows = await provider_batch._hydrate_native_batch(
        [identity], state, params, provider_batch.parse_native_batch_query({"view": view}), object()
    )
    assert hydrate.await_args.args[1] == {identity: [addresses[1]]}
    assert hydrate.await_count == names.await_count == summaries.await_count == 1
    if view == "full":
        assert provider_rows[0]["provider"]["address_pagination"] == {
            "limit": 1,
            "offset": 1,
            "returned": 1,
            "total": 3,
            "has_more": True,
        }
        assert provider_rows[0]["provider"]["address_list"][0]["first_line"] == "2 Main Street"
    else:
        assert provider_rows[0]["provider"] == npi_module._provider_card_from_mapping(
            {**state.details[identity], **addresses[1], "npi": identity}
        )


@pytest.mark.asyncio
async def test_batch_premise_groups_page_groups_and_bound_selected_members(monkeypatch):
    identity = 9000000000
    site_key = "10000000-0000-0000-0000-000000000001"
    addresses = [
        {
            **_ranked_batch_addresses(identity)[0],
            "address_key": f"00000000-0000-0000-0000-{index:012d}",
            "premise_key": site_key,
        }
        for index in range(6)
    ]
    state = provider_batch._NativeBatchState({identity: {"npi": identity}}, {identity: addresses})
    hydrate = AsyncMock(return_value={identity: addresses[:5]})
    monkeypatch.setattr(npi_module, "_hydrate_npi_batch_addresses", hydrate)
    monkeypatch.setattr(npi_module, "_fetch_other_names_map", AsyncMock(return_value={identity: []}))
    monkeypatch.setattr(npi_module, "_fetch_provider_enrichment_summary_map", AsyncMock(return_value={}))
    params = npi_module._normalize_npi_batch_request({"npis": [identity], "address_limit": 1})
    rows = await provider_batch._hydrate_native_batch(
        [identity], state, params, provider_batch.parse_native_batch_query({"address_grouping": "premise"}), object()
    )
    provider = rows[0]["provider"]
    assert "address_list" not in provider and "address_pagination" not in provider
    assert provider["address_group_pagination"]["total"] == 1 and len(provider["address_groups"]) == 1
    group = provider["address_groups"][0]
    assert group["group_key"] == site_key and len(group["members"]) == 5
    assert group["member_pagination"]["total"] == 6 and group["member_pagination"]["has_more"]
    assert len(hydrate.await_args.args[1][identity]) == 5


def test_batch_debug_and_address_selectors_keep_native_shaping_contract():
    identity = 9000000000
    address_key = "00000000-0000-0000-0000-000000000002"
    addresses = [
        {**address, "address_key": f"00000000-0000-0000-0000-{index + 1:012d}"}
        for index, address in enumerate(_ranked_batch_addresses(identity))
    ]
    state = provider_batch._NativeBatchState({identity: {"npi": identity}}, {identity: addresses})
    args = provider_batch.parse_native_batch_query({"address_key": address_key, "debug": "true"})
    params = npi_module._normalize_npi_batch_request({"npis": [identity]})
    scoped = provider_batch._filter_batch_address_state(state, args)
    assert scoped.addresses[identity] == [addresses[1]] and state.addresses[identity] == addresses
    assert provider_batch._batch_shape_params(params, args)["include_sources"] is True
    assert provider_batch._batch_shape_params(params, args)["include_evidence"] is True
    with pytest.raises(InvalidUsage):
        provider_batch.parse_native_batch_query({"address_key": "not-an-address-key"})


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "rows,total,total_source",
    [
        ([9000000000, 9000000000], 2, "computed"),
        ([9000000002], 1, "computed"),
        ([9000000000], 2, "computed"),
        ([9000000000], 1, "estimated_page_floor"),
    ],
)
async def test_batch_native_eligibility_rejects_duplicate_foreign_or_unproven_totals(
    monkeypatch, rows, total, total_source
):
    reply = SimpleNamespace(
        status=200,
        body=json.dumps(
            {"rows": [{"npi": identity} for identity in rows], "total": total, "total_source": total_source}
        ).encode(),
    )
    monkeypatch.setattr(npi_module, "list_providers", AsyncMock(return_value=reply))
    from process.custom_import.read_contracts import CustomImportReadUnavailableError

    with pytest.raises(CustomImportReadUnavailableError):
        await provider_batch._batch_eligible_npis(
            SimpleNamespace(),
            provider_batch.parse_native_batch_query({"q": "Synthetic"}),
            None,
            [9000000000, 9000000001],
            [9000000000, 9000000001],
            object(),
        )


@pytest.mark.asyncio
async def test_enrichment_batch_query_uses_runtime_schema(monkeypatch):
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "provider_tenant")
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    execute = AsyncMock(return_value=_ResultRows([]))
    monkeypatch.setattr(npi_module, "_execute_stmt", execute)

    await npi_module._provider_enrichment_rows_for_columns(
        [1234567890],
        {"npi"},
    )

    assert "FROM provider_tenant.provider_enrichment_summary" in str(execute.await_args.args[0])


def test_openapi_documents_npi_batch_contract():
    operation = yaml.safe_load(Path("doc/openapi.yaml").read_text())["paths"]["/npi/id/batch"]["post"]
    request_schema = operation["requestBody"]["content"]["application/json"]["schema"]
    npi_schema = request_schema["properties"]["npis"]
    assert npi_schema["uniqueItems"] is True
    assert "normalization" in npi_schema["description"]
    assert request_schema["properties"]["address_limit"]["oneOf"] == [
        {"type": "integer", "minimum": 0, "maximum": 1000},
        {"type": "string", "enum": ["all"]},
    ]
    response_schema = operation["responses"]["200"]["content"]["application/json"]["schema"]
    meta_schema = response_schema["properties"]["meta"]
    assert set(meta_schema["required"]) == {"elapsed_ms", "max_batch_size", "view"}
    assert meta_schema["properties"]["view"]["enum"] == ["summary"]


def test_batch_request_requires_unique_10_digit_npis_and_caps_at_100(monkeypatch):
    normalized = npi_module._normalize_npi_batch_request({"npis": [str(1_000_000_000 + index) for index in range(100)]})
    assert len(normalized["npis"]) == 100
    assert normalized["address_limit"] == 5

    for invalid_request_body, message in (
        ({"npis": []}, "between 1 and 100"),
        ({"npis": [True]}, "10-digit"),
        ({"npis": [{}]}, "10-digit"),
        ({"npis": ["1234567890", 1234567890]}, "unique"),
        ({"npis": ["123"]}, "10-digit"),
        ({"npis": [str(1_000_000_000 + index) for index in range(101)]}, "between 1 and 100"),
    ):
        with pytest.raises(InvalidUsage, match=message):
            npi_module._normalize_npi_batch_request(invalid_request_body)
    monkeypatch.setattr(npi_module, "NPI_BATCH_MAX_SIZE", 2)
    with pytest.raises(InvalidUsage, match="between 1 and 2"):
        npi_module._normalize_npi_batch_request({"npis": ["1234567890", "1098765432", "1987654321"]})

    for invalid_request_body, message in (
        (None, "JSON object"),
        ({"npis": ["1234567890"], "unknown": True}, "unsupported batch field"),
        ({"npis": ["1234567890"], "address_limit": True}, "address_limit"),
        ({"npis": ["1234567890"], "include_sources": "true"}, "include_sources"),
    ):
        with pytest.raises(InvalidUsage, match=message):
            npi_module._normalize_npi_batch_request(invalid_request_body)


def _ranked_batch_addresses(npi):
    return [
        {
            "npi": npi,
            "type": "primary",
            "first_line": f"{index} Main Street",
            "city_name": "Chicago",
            "state_name": "IL",
            "postal_code": "60601",
            "country_code": "US",
            "_base_row_identities": [f"location:address-{index}"],
        }
        for index in range(1, 4)
    ]


@pytest.mark.asyncio
async def test_batch_uses_set_maps_once_and_preserves_partial_result_order(monkeypatch):
    found_npi = 1234567890
    missing_npi = 1098765432
    address_map = {
        **_ranked_batch_addresses(found_npi)[0],
        "state_code": "IL",
        "address_key": "00000000-0000-0000-0000-000000000001",
        "address_sources": ["nppes"],
    }
    identity_map = AsyncMock(return_value={found_npi: {"npi": found_npi}})
    candidate_map = AsyncMock(return_value={found_npi: [address_map]})
    overlay_map = AsyncMock(return_value={})
    hydration_map = AsyncMock(return_value={found_npi: [address_map]})
    names_map = AsyncMock(return_value={found_npi: []})
    enrichment_map = AsyncMock(return_value={})

    monkeypatch.setattr(npi_module, "_build_npi_identity_details_map", identity_map)
    monkeypatch.setattr(npi_module, "_fetch_npi_location_candidates_map", candidate_map)
    monkeypatch.setattr(npi_module, "_fetch_provider_directory_address_overlay_map", overlay_map)
    monkeypatch.setattr(npi_module, "_fetch_npi_address_rows_map", hydration_map)
    monkeypatch.setattr(npi_module, "_fetch_other_names_map", names_map)
    monkeypatch.setattr(npi_module, "_fetch_provider_enrichment_summary_map", enrichment_map)
    monkeypatch.setattr(npi_module, "_apply_location_statuses", AsyncMock())
    monkeypatch.setattr(
        npi_module,
        "_attach_provider_directory_source_details",
        AsyncMock(),
    )
    monkeypatch.setattr(npi_module, "_request_session", lambda _request: None)

    request = SimpleNamespace(
        args={},
        json={
            "npis": [str(found_npi), str(missing_npi)],
            "include_sources": True,
            "address_limit": 5,
        },
    )
    operation_response = await npi_module.get_npi_batch(request)
    response_map = json.loads(operation_response.body)

    assert [provider_result["npi"] for provider_result in response_map["items"]] == [found_npi, missing_npi]
    assert [provider_result["status"] for provider_result in response_map["items"]] == [200, 404]
    assert response_map["found"] == 1
    assert response_map["not_found"] == 1
    assert response_map["meta"]["max_batch_size"] == npi_module.NPI_BATCH_MAX_SIZE
    assert response_map["meta"]["view"] == "summary"
    assert response_map["meta"]["elapsed_ms"] >= 0
    assert response_map["items"][0]["provider"]["address_pagination"] == {
        "limit": 5,
        "offset": 0,
        "returned": 1,
        "total": 1,
        "has_more": False,
    }
    for batch_mock in (identity_map, candidate_map, overlay_map, hydration_map, names_map, enrichment_map):
        assert batch_mock.await_count == 1


@pytest.mark.asyncio
async def test_batch_address_offset_slices_and_paginates(monkeypatch):
    npi = 1234567890
    ranked_addresses = _ranked_batch_addresses(npi)
    hydration_map = AsyncMock(return_value={npi: [ranked_addresses[1]]})
    monkeypatch.setattr(npi_module, "_fetch_npi_address_rows_map", hydration_map)

    selected_by_npi = await npi_module._hydrate_npi_batch_addresses(
        [npi],
        {npi: ranked_addresses},
        address_limit=1,
        address_offset=1,
        include_sources=False,
        include_evidence=False,
        session=None,
    )

    assert selected_by_npi[npi][0]["first_line"] == "2 Main Street"
    assert hydration_map.await_args.kwargs["address_row_identities"] == ["location:address-2"]
    provider_result, was_found = npi_module._npi_batch_provider_result(
        npi,
        {"npi": npi},
        ranked_addresses,
        selected_by_npi[npi],
        [],
        None,
        {
            "address_limit": 1,
            "address_offset": 1,
            "include_sources": False,
            "include_evidence": False,
        },
    )
    assert was_found is True
    assert provider_result["provider"]["address_pagination"] == {
        "limit": 1,
        "offset": 1,
        "returned": 1,
        "total": 3,
        "has_more": True,
    }


@pytest.mark.asyncio
async def test_batch_skips_hydration_without_base_identities(monkeypatch):
    npi = 1234567890
    hydration_map = AsyncMock()
    monkeypatch.setattr(npi_module, "_fetch_npi_address_rows_map", hydration_map)
    overlay_address_map = {
        **_ranked_batch_addresses(npi)[0],
        "_base_row_identities": [],
    }
    selected_by_npi = await npi_module._hydrate_npi_batch_addresses(
        [npi],
        {npi: [overlay_address_map]},
        address_limit=1,
        address_offset=0,
        include_sources=False,
        include_evidence=False,
        session=None,
    )
    assert selected_by_npi[npi] == [overlay_address_map]
    hydration_map.assert_not_awaited()


@pytest.mark.asyncio
async def test_batch_identity_map_uses_one_sorted_set_query():
    first_npi = 1234567890
    second_npi = 1098765432

    def identity_row(npi):
        return [npi if column.key == "npi" else None for column in npi_module._npi_serving_columns()] + [[], []]

    session = SimpleNamespace(
        execute=AsyncMock(return_value=_ResultRows([identity_row(first_npi), identity_row(second_npi)]))
    )

    identity_map = await npi_module._build_npi_identity_details_map(
        [first_npi, second_npi, first_npi],
        session=session,
    )

    assert set(identity_map) == {first_npi, second_npi}
    assert identity_map[first_npi]["npi"] == first_npi
    assert identity_map[second_npi]["taxonomy_list"] == []
    statement = session.execute.await_args.args[0]
    assert "search_taxonomy_codes" not in str(statement)
    statement_params = statement.compile().params.values()
    assert [second_npi, first_npi] in statement_params


@pytest.mark.asyncio
async def test_batch_location_maps_group_two_npis_in_one_query_each(monkeypatch):
    first_location_map = unified_location_mapping()
    second_location_map = {
        **first_location_map,
        "inferred_npi": 1098765432,
        "location_key": "synthetic-location-2",
    }
    monkeypatch.setattr(
        npi_module,
        "_address_serving_model",
        AsyncMock(return_value=npi_module.EntityAddressUnified),
    )
    monkeypatch.setattr(
        npi_module,
        "_table_columns",
        AsyncMock(return_value=set(first_location_map)),
    )
    execute_stmt = AsyncMock(
        side_effect=[
            _ResultRows([first_location_map, second_location_map]),
            _ResultRows([first_location_map, second_location_map]),
        ]
    )
    monkeypatch.setattr(npi_module, "_execute_stmt", execute_stmt)

    npis = [1234567890, 1098765432, 1234567890]
    candidate_map = await npi_module._fetch_npi_location_candidates_map(npis)
    hydration_map = await npi_module._fetch_npi_address_rows_map(
        npis,
        include_evidence=True,
        address_row_identities=[
            "location:synthetic-location-1",
            "location:synthetic-location-2",
        ],
    )

    assert set(candidate_map) == {1234567890, 1098765432}
    assert candidate_map[1098765432][0]["npi"] == 1098765432
    assert hydration_map[1234567890][0]["source_record_ids"] == (first_location_map["source_record_ids"])
    assert hydration_map[1098765432][0]["_base_row_identities"] == ["location:synthetic-location-2"]
    assert execute_stmt.await_count == 2
    for call in execute_stmt.await_args_list:
        assert [1098765432, 1234567890] in call.args[0].compile().params.values()


@pytest.mark.asyncio
async def test_batch_overlay_map_groups_npis_and_passes_shared_filters(monkeypatch):
    first_npi = 1234567890
    second_npi = 1098765432
    overlay_result = _ResultRows(
        [
            {"npi": first_npi, "first_line": "1 Main Street"},
            {"npi": second_npi, "first_line": "2 Main Street"},
            {"npi": None, "first_line": "Unassigned"},
        ]
    )
    monkeypatch.setattr(
        npi_module,
        "_is_table_available",
        AsyncMock(return_value=True),
    )
    monkeypatch.setattr(
        npi_module,
        "_table_columns",
        AsyncMock(return_value={"lat", "long"}),
    )
    execute_stmt = AsyncMock(return_value=overlay_result)
    monkeypatch.setattr(npi_module, "_execute_stmt", execute_stmt)
    session = object()

    overlay_map = await npi_module._fetch_provider_directory_address_overlay_map(
        [first_npi, second_npi, first_npi],
        address_key="synthetic-address-key",
        address_site_key="synthetic-site-key",
        session=session,
    )

    assert overlay_map == {
        first_npi: [{"npi": first_npi, "first_line": "1 Main Street"}],
        second_npi: [{"npi": second_npi, "first_line": "2 Main Street"}],
    }
    query_call = execute_stmt.await_args
    assert "overlay.npi = ANY(:npis)" in str(query_call.args[0])
    assert query_call.kwargs == {
        "session": session,
        "params": {
            "npis": [second_npi, first_npi],
            "address_key": "synthetic-address-key",
            "address_site_key": "synthetic-site-key",
        },
    }


@pytest.mark.parametrize("native_query", [None, [], "", True])
def test_extended_batch_query_requires_an_object(native_query):
    with pytest.raises(InvalidUsage, match="native_query must be an object"):
        provider_batch.parse_native_batch_query(native_query)


@pytest.mark.parametrize("field", ["include_sources", "include_evidence"])
@pytest.mark.parametrize("batch_value", [False, True])
def test_batch_source_controls_must_agree_before_reading(field, batch_value):
    params = npi_module._normalize_npi_batch_request({"npis": [9000000000], field: batch_value})
    disagreeing = provider_batch.parse_native_batch_query({field: str(not batch_value).lower()})
    with pytest.raises(InvalidUsage, match="source options must agree"):
        provider_batch._batch_shape_params(params, disagreeing)
    agreeing = provider_batch.parse_native_batch_query({field: str(batch_value).lower()})
    assert provider_batch._batch_shape_params(params, agreeing)[field] is batch_value


@pytest.mark.parametrize("address_limit", [0, "all", 6, 20, 1000])
def test_batch_premise_limit_cannot_exceed_existing_group_bound(address_limit):
    params = npi_module._normalize_npi_batch_request({"npis": [9000000000], "address_limit": address_limit})
    args = provider_batch.parse_native_batch_query({"address_grouping": "premise"})
    with pytest.raises(InvalidUsage, match="premise address_limit must be between 1 and 5"):
        provider_batch._batch_shape_params(params, args)


@pytest.mark.asyncio
async def test_empty_native_batch_preserves_missing_status_without_eligibility_or_hydration(monkeypatch):
    identities = [9000000001, 9000000000]
    state = provider_batch._NativeBatchState(
        {identity: None for identity in identities}, {identity: [] for identity in identities}
    )
    monkeypatch.setattr(provider_batch, "_prepare_native_batch", AsyncMock(return_value=state))
    eligibility = AsyncMock(side_effect=AssertionError("missing identities need no native search"))
    hydration = AsyncMock(side_effect=AssertionError("missing identities need no address hydration"))
    monkeypatch.setattr(npi_module, "list_providers", eligibility)
    monkeypatch.setattr(npi_module, "_hydrate_npi_batch_addresses", hydration)
    payload = await provider_batch.read_native_batch(
        SimpleNamespace(),
        npi_module._normalize_npi_batch_request({"npis": identities}),
        native_args=provider_batch.parse_native_batch_query({"q": "Synthetic"}),
        session=object(),
    )
    assert payload["requested"] == payload["not_found"] == 2 and payload["found"] == 0
    assert [(item["npi"], item["status"]) for item in payload["items"]] == [(identity, 404) for identity in identities]
    assert payload["pagination"]["total"] == 0 and not payload["pagination"]["has_more"]
    eligibility.assert_not_awaited()
    hydration.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [400, 503])
async def test_native_batch_never_reclassifies_upstream_failure_as_a_missing_provider(monkeypatch, status):
    native = AsyncMock(return_value=SimpleNamespace(status=status))
    monkeypatch.setattr(npi_module, "list_providers", native)
    with pytest.raises(CustomImportReadUnavailableError, match="eligibility is unavailable"):
        await provider_batch._batch_eligible_npis(
            SimpleNamespace(),
            provider_batch.parse_native_batch_query({"q": "Synthetic"}),
            None,
            [9000000000],
            [9000000000],
            object(),
        )
    native.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("order_by,message", [("other", "must be npi or relevance"), ("relevance", "requires q")])
async def test_identity_only_batch_does_not_silently_accept_invalid_native_order(monkeypatch, order_by, message):
    native = AsyncMock(side_effect=AssertionError("identity-only sorting needs no search"))
    monkeypatch.setattr(npi_module, "list_providers", native)
    with pytest.raises(InvalidUsage, match=message):
        await provider_batch._batch_eligible_npis(
            SimpleNamespace(),
            provider_batch.parse_native_batch_query({"order_by": order_by}),
            None,
            [9000000000],
            [9000000000],
            object(),
        )
    native.assert_not_awaited()


@pytest.mark.asyncio
async def test_native_batch_card_keeps_the_exact_eligibility_witness(monkeypatch):
    identity = 9000000000
    state = provider_batch._NativeBatchState({identity: {"npi": identity}}, {identity: []})
    witness_by_field = {"npi": identity, "provider_first_name": "Synthetic", "native_marker": "selected"}
    monkeypatch.setattr(provider_batch, "_prepare_native_batch", AsyncMock(return_value=state))
    native = AsyncMock(
        return_value=SimpleNamespace(
            status=200, body=json.dumps({"rows": [witness_by_field], "total": 1, "total_source": "computed"}).encode()
        )
    )
    monkeypatch.setattr(npi_module, "list_providers", native)
    monkeypatch.setattr(npi_module, "_hydrate_npi_batch_addresses", AsyncMock(return_value={identity: []}))
    monkeypatch.setattr(npi_module, "_fetch_other_names_map", AsyncMock(return_value={identity: []}))
    monkeypatch.setattr(npi_module, "_fetch_provider_enrichment_summary_map", AsyncMock(return_value={}))
    payload = await provider_batch.read_native_batch(
        SimpleNamespace(),
        npi_module._normalize_npi_batch_request({"npis": [identity]}),
        native_args=provider_batch.parse_native_batch_query({"q": "Synthetic", "view": "card"}),
        session=object(),
    )
    assert payload["items"][0]["provider"] == witness_by_field and payload["found"] == 1
    native.assert_awaited_once()


@pytest.mark.asyncio
async def test_selected_native_batch_identity_must_still_be_available_at_hydration(monkeypatch):
    identity = 9000000000
    state = provider_batch._NativeBatchState({identity: None}, {identity: []})
    monkeypatch.setattr(npi_module, "_hydrate_npi_batch_addresses", AsyncMock(return_value={identity: []}))
    monkeypatch.setattr(npi_module, "_fetch_other_names_map", AsyncMock(return_value={identity: []}))
    monkeypatch.setattr(npi_module, "_fetch_provider_enrichment_summary_map", AsyncMock(return_value={}))
    with pytest.raises(CustomImportReadUnavailableError, match="provider is unavailable"):
        await provider_batch._hydrate_native_batch(
            [identity],
            state,
            npi_module._normalize_npi_batch_request({"npis": [identity]}),
            provider_batch.parse_native_batch_query({}),
            object(),
        )


@pytest.mark.parametrize("address_limit", [1, 5])
def test_batch_premise_accepts_both_existing_group_limit_boundaries(address_limit):
    params = npi_module._normalize_npi_batch_request({"npis": [9000000000], "address_limit": address_limit})
    args = provider_batch.parse_native_batch_query({"address_grouping": "premise"})
    assert provider_batch._batch_shape_params(params, args)["address_limit"] == address_limit


@pytest.mark.parametrize("raw_limit,expected", [(0, 0), ("all", 0), (" ALL ", 0), (21, 21), (1000, 1000)])
def test_batch_address_limit_accepts_normal_public_exact_provider_values(raw_limit, expected):
    params = npi_module._normalize_npi_batch_request({"npis": [9000000000], "address_limit": raw_limit})
    assert params["address_limit"] == expected


@pytest.mark.parametrize("raw_limit", [-1, 1001, True, None, 1.5, "0", "1000", "invalid"])
def test_batch_address_limit_preserves_public_and_json_type_bounds(raw_limit):
    with pytest.raises(InvalidUsage, match="address_limit"):
        npi_module._normalize_npi_batch_request({"npis": [9000000000], "address_limit": raw_limit})


@pytest.mark.asyncio
@pytest.mark.parametrize("composed", [False, True])
@pytest.mark.parametrize("address_limit", [0, 1000])
async def test_batch_large_address_pages_hydrate_every_selected_provider_address(monkeypatch, composed, address_limit):
    identities = [9000000000, 9000000001]
    addresses_by_npi = {
        identity: [
            {
                **_ranked_batch_addresses(identity)[0],
                "first_line": f"{index} Example Avenue",
                "_base_row_identities": [f"location:{identity}-{index}"],
            }
            for index in range(1100 if identity == identities[0] else 3)
        ]
        for identity in identities
    }
    details_by_npi = {identity: {"npi": identity} for identity in identities}
    monkeypatch.setattr(npi_module, "_build_npi_identity_details_map", AsyncMock(return_value=details_by_npi))
    monkeypatch.setattr(npi_module, "_rank_npi_batch_addresses", AsyncMock(return_value=addresses_by_npi))
    hydration = AsyncMock(return_value=addresses_by_npi)
    monkeypatch.setattr(npi_module, "_fetch_npi_address_rows_map", hydration)
    monkeypatch.setattr(npi_module, "_fetch_other_names_map", AsyncMock(return_value={}))
    monkeypatch.setattr(npi_module, "_fetch_provider_enrichment_summary_map", AsyncMock(return_value={}))
    batch_params_by_field = {
        "npis": identities,
        "address_limit": address_limit,
        "address_offset": 0,
        "include_sources": False,
        "include_evidence": False,
    }
    if composed:
        state = provider_batch._NativeBatchState(details_by_npi, addresses_by_npi)
        provider_items = await provider_batch._hydrate_native_batch(
            identities, state, batch_params_by_field, provider_batch.parse_native_batch_query({}), object()
        )
    else:
        provider_items = (await provider_batch.build_native_batch_payload(batch_params_by_field, session=object()))[
            "items"
        ]
    expected_counts = [1100 if address_limit == 0 else 1000, 3]
    assert [len(provider_item["provider"]["address_list"]) for provider_item in provider_items] == expected_counts
    assert hydration.await_count == 1
    assert len(hydration.await_args.kwargs["address_row_identities"]) == sum(expected_counts)
    for provider_item, count in zip(provider_items, expected_counts, strict=True):
        identity = provider_item["npi"]
        provider = provider_item["provider"]
        assert [address["first_line"] for address in provider["address_list"]] == [
            address["first_line"] for address in addresses_by_npi[identity][:count]
        ]
        assert provider["address_pagination"] == {
            "limit": address_limit or None,
            "offset": 0,
            "returned": count,
            "total": len(addresses_by_npi[identity]),
            "has_more": count < len(addresses_by_npi[identity]),
        }
