# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Managed E&M orchestration and SQL contracts; database execution is separate."""

import json
import sys
from types import ModuleType, SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from api import network_address_scope as address_scope
from api import plan_pricing_em_distance as distance
from api import plan_pricing_em_managed as managed
from api import ptg2_serving
from api.network_read_execution import NetworkReadStatements, install_network_read_inventory
from api.plan_pricing_projection_contract import PlanPricingProjectionUnavailable
from api.plan_release_serving import PlanReleaseServingSelection, PlanReleaseSnapshotBinding
from api.ptg2_serving import PTG2LocationScopeError
from process import network_em_provider_inputs as input_contract
from process.network_compact_layout import CompactAddressRelation
from tests.network_read_execution_support import with_read_identity_receipts


@pytest.fixture
def held(monkeypatch):
    """Mock independent custody resolvers while exercising the actual bounded reader."""
    bindings = tuple(
        PlanReleaseSnapshotBinding(
            ordinal, f"snapshot_{ordinal}", f"source_{ordinal}", "example_plan", "group", "in_network", True
        )
        for ordinal in (3, 11, 29)
    )
    selection = PlanReleaseServingSelection(
        "hpserve_example", "hprelease_example", "hpplan_example", None, "2026-08", "published", "a" * 64, bindings
    )
    scope = SimpleNamespace(
        manifest=SimpleNamespace(
            schema_name="network_candidate_example",
            approved_custom_revision=17,
            compact_layout=None,
            address_table_oid=99,
        ),
        network_ids=None,
        legacy_checksums=None,
        excluded_network_ids=(7,),
        address_predicate_sql=address_scope._default_address_predicate(),
        read_statements=NetworkReadStatements(["a" * 64, None, (7,), "b" * 64]),
    )
    inputs = _retained_inputs()
    state = SimpleNamespace(
        admitted=bindings,
        bindings=bindings,
        selection=selection,
        inputs=inputs,
        scope=scope,
        args={"code": "99213", "view": "card", "order_by": "distance", "zip5": "60611"},
        pagination=SimpleNamespace(limit=2, offset=0, page=1),
        events=[],
        windows=[],
        observations=[(2, 2)],
        metadata_mutation=lambda rows: rows,
        envelope_mutation=lambda envelope: envelope,
    )

    _mock_authority_resolvers(state, monkeypatch)
    token = address_scope._SCOPE.set(scope)
    try:
        yield state
    finally:
        address_scope._SCOPE.reset(token)


def _mock_authority_resolvers(state, monkeypatch):
    async def admit(_session, actual):
        assert actual is state.selection and actual.bindings is state.bindings
        state.events.append("admission")
        return state.admitted

    async def artifact(actual_session, actual, *, api_read=False):
        assert actual_session is state.session and actual is state.selection and api_read is True
        state.events.append("artifact")
        await install_network_read_inventory(actual_session, (CompactAddressRelation("held_rates", "rate", 100),))
        return SimpleNamespace(rates_relation='"held_rates"."rate"')

    async def resolve_inputs(actual_session, manifest):
        assert actual_session is state.session and manifest is state.scope.manifest
        state.events.append("inputs")
        await install_network_read_inventory(
            actual_session,
            tuple(
                CompactAddressRelation(item.schema_name, item.table_name, item.relation_oid)
                for item in state.inputs.relations
            ),
        )
        return state.inputs

    state.session = _reader_session(state)
    attribution = ModuleType("api.plan_pricing_em_attribution")
    attribution.ATTRIBUTION_CONTRACT = "example_attribution_v1"
    attribution.resolve_em_attribution = AsyncMock(side_effect=artifact)
    provider_inputs = ModuleType("process.network_em_provider_inputs")
    provider_inputs.resolve_em_provider_inputs_in_session = AsyncMock(side_effect=resolve_inputs)
    provider_inputs.em_provider_geo_reads_sql = input_contract.em_provider_geo_reads_sql
    monkeypatch.setitem(sys.modules, attribution.__name__, attribution)
    monkeypatch.setitem(sys.modules, provider_inputs.__name__, provider_inputs)
    monkeypatch.setattr(managed, "admitted_plan_release_bindings", AsyncMock(side_effect=admit))
    state.attribution = attribution.resolve_em_attribution
    state.resolve_inputs = provider_inputs.resolve_em_provider_inputs_in_session


def _retained_inputs():
    roles = (
        "doctor_clinician_address",
        "geo_zip_lookup",
        "mrf_address",
        "npi",
        "npi_address",
        "npi_taxonomy",
        "nucc_taxonomy",
        "tiger.zcta5",
        "tiger.zip_state",
    )
    return input_contract.EMProviderInputs(
        tuple(
            input_contract.EMProviderInputRelation(
                role, "held_example", role.replace(".", "_"), "example_generation", index + 1, index + 20
            )
            for index, role in enumerate(roles)
        )
    )


def _reader_session(state):
    async def execute(statement, parameters):
        state.events.append("page")
        state.windows.append((str(statement), parameters))
        candidate_count, unique_count = state.observations.pop(0)
        counts_by_field = {"candidate_count": candidate_count, "unique_count": unique_count, "projection_ready": True}
        rows = [
            {
                **counts_by_field,
                "npi": 1003000000 + index,
                "distance_miles": index // 2,
                "city": "SELECTED OFFICE",
                "state": "IL",
                "zip5": "60611",
            }
            for index in range(parameters["offset"], min(unique_count, parameters["offset"] + parameters["page_limit"]))
        ] or [{**counts_by_field, "npi": None}]
        return SimpleNamespace(mappings=lambda: SimpleNamespace(all=lambda: rows))

    async def fetchrow(query, npis, ordinals, code_index):
        state.events.append("completion")
        assert ordinals == [binding.binding_ordinal for binding in state.admitted]
        rows = state.metadata_mutation(
            [
                {
                    "npi": npi,
                    "provider_name": "Example Provider",
                    "minimum_rates": ["10"] * 6,
                    "maximum_rates": ["20"] * 6,
                    "rate_counts": [2] * 6,
                }
                for npi in npis
            ]
        )
        encoded = json.dumps(rows)
        return state.envelope_mutation({"count": len(rows), "bytes": len(encoded), "rows_json": encoded})

    state.driver = SimpleNamespace(fetchrow=AsyncMock(side_effect=fetchrow))
    transaction = object()
    return with_read_identity_receipts(
        SimpleNamespace(
            execute=AsyncMock(side_effect=execute),
            get_transaction=lambda: transaction,
            info={},
        ),
        driver=state.driver,
    )


async def _search(held):
    return await distance.search_plan_pricing_em_distance(held.session, held.selection, held.args, held.pagination)


@pytest.mark.parametrize("admitted_indices", [(0, 1, 2), (1, 2), ()])
async def test_full_selection_is_admitted_before_any_provider_query(held, admitted_indices):
    held.admitted = tuple(held.bindings[index] for index in admitted_indices)
    response = await _search(held)
    assert held.events[:3] == ["admission", "artifact", "inputs"]
    assert held.selection.bindings is held.bindings
    assert response["binding_set_digest"] == held.selection.binding_set_digest
    assert response["plan_release_id"] == held.selection.plan_release_id
    assert response["resolved_snapshot_ids"] == sorted(binding.snapshot_id for binding in held.admitted)
    assert response["query"]["snapshots"] == [
        {
            "source_key": binding.source_key,
            "snapshot_id": binding.snapshot_id,
            "plan_id": binding.plan_id,
            "plan_market_type": binding.plan_market_type,
        }
        for binding in held.admitted
    ]
    if held.admitted:
        assert held.events == ["admission", "artifact", "inputs", "page", "completion"]
        assert held.windows[0][1]["allowed_binding_ordinals"] == [binding.binding_ordinal for binding in held.admitted]
    else:
        held.session.execute.assert_not_awaited()
        held.driver.fetchrow.assert_not_awaited()
        assert response["items"] == []
        assert response["pagination"] == {
            "total": 0,
            "total_is_exact": True,
            "total_lower_bound": 0,
            "limit": 2,
            "offset": 0,
            "page": 1,
            "has_more": False,
        }


@pytest.mark.parametrize("stage", ["admission", "artifact", "inputs"])
async def test_missing_or_stale_authority_never_falls_back(held, monkeypatch, stage):
    target = {
        "admission": managed.admitted_plan_release_bindings,
        "artifact": held.attribution,
        "inputs": held.resolve_inputs,
    }[stage]
    target.side_effect = PlanPricingProjectionUnavailable("unavailable")
    fallback = AsyncMock(side_effect=AssertionError("legacy fallback"))
    monkeypatch.setattr(distance, "_PAGE_SQL", "LEGACY MUST NOT RUN")
    monkeypatch.setattr(managed, "_complete_page", fallback)
    monkeypatch.setattr(ptg2_serving, "search_plan_pricing_projection", fallback)
    with pytest.raises(PlanPricingProjectionUnavailable):
        await ptg2_serving._search_selected_plan_release(held.session, held.args, held.pagination, held.selection)
    held.session.execute.assert_not_awaited()
    held.driver.fetchrow.assert_not_awaited()
    fallback.assert_not_awaited()


async def test_scalar_counts_and_selected_office_survive_metadata_completion(held):
    held.observations = [(128, 3)]
    response = await _search(held)
    assert [item["npi"] for item in response["items"]] == [1003000000, 1003000001]
    assert [item["distance_miles"] for item in response["items"]] == [0, 0]
    assert all(
        (item["city"], item["state"], item["zip5"]) == ("SELECTED OFFICE", "IL", "60611") for item in response["items"]
    )
    assert all(item["minimum_negotiated_rate"] == 10 and item["rate_count"] == 2 for item in response["items"])
    assert response["pagination"] == {
        "total": 3,
        "total_is_exact": False,
        "total_lower_bound": 3,
        "limit": 2,
        "offset": 0,
        "page": 1,
        "has_more": True,
    }
    query, npis, ordinals, code_index = held.driver.fetchrow.await_args.args
    assert len(npis) == 3 and ordinals == [3, 11, 29] and code_index == 3
    assert "count<=5000 AND bytes<=8388608" in query
    assert "rate.code_index=$3::smallint" in query and "rate.binding_ordinal=ANY($2::integer[])" in query
    assert "entity_address_unified" not in query


@pytest.mark.parametrize("system", ["CPT", "HCPCS"])
async def test_managed_code_systems_share_the_same_fixed_slot(held, system):
    held.args["code_system"] = system
    assert (await _search(held))["query"]["code_system"] == system
    assert held.windows[0][1]["code_index"] == 3


@pytest.mark.parametrize("mutation", ["duplicate", "missing", "unexpected", "short_rates", "missing_rate"])
async def test_incomplete_or_multiple_completion_rows_fail_closed(held, mutation):
    def alter(rows):
        if mutation == "duplicate":
            rows.append(dict(rows[0]))
        elif mutation == "missing":
            rows.pop()
        elif mutation == "unexpected":
            rows[0]["npi"] = 9999999999
        elif mutation == "short_rates":
            rows[0]["rate_counts"] = [1]
        else:
            rows[0]["minimum_rates"][3] = None
        return rows

    held.metadata_mutation = alter
    with pytest.raises(PlanPricingProjectionUnavailable, match="completion"):
        await _search(held)
    assert held.session.execute.await_count == 1


@pytest.mark.parametrize(
    "envelope",
    [
        {"count": 5001, "bytes": 1, "rows_json": None},
        {"count": 1, "bytes": 8388609, "rows_json": None},
        {"count": 1, "bytes": 1, "rows_json": None},
    ],
)
async def test_completion_bounds_are_not_converted_to_empty_success(held, envelope):
    held.envelope_mutation = lambda _original: envelope
    with pytest.raises(PlanPricingProjectionUnavailable, match="completion"):
        await _search(held)


async def test_managed_window_grows_to_the_existing_cap_without_fallback(held):
    held.observations = [(window, 1) for window in (128, 256, 512, 1024, 2048, 4096, 8192)]
    with pytest.raises(PTG2LocationScopeError):
        await _search(held)
    assert [parameters["candidate_limit"] for _, parameters in held.windows] == [128, 256, 512, 1024, 2048, 4096, 8192]
    held.driver.fetchrow.assert_not_awaited()


async def test_completion_rejects_more_than_101_provider_identities(held):
    rows = [{"npi": index} for index in range(102)]
    with pytest.raises(PlanPricingProjectionUnavailable, match="page bound"):
        await managed._complete_page(held.session, held.inputs, "rate", rows, (3,), 3)
    held.driver.fetchrow.assert_not_awaited()


def test_candidate_sql_filters_source_and_held_geo_before_the_knn_limit(held):
    sql = managed._page_sql(held.inputs, '"held_rates"."rate"')
    limit = sql.index("LIMIT :candidate_limit")
    assert sql.index("rate.binding_ordinal=ANY(CAST(:allowed_binding_ordinals AS integer[]))") < limit
    assert sql.index("rate.code_index=:code_index") < limit
    assert sql.index("row_origin IN ('base','ptg_overlay')") < limit
    assert sql.index('"held_example"."tiger_zcta5"') < limit
    assert '"network_candidate_example".entity_address_unified' in sql
    assert (
        "public.Geography(public.ST_MakePoint((addr.long)::double precision,(addr.lat)::double precision)) <-> center.point"
        in sql
    )
    assert "PARTITION BY npi ORDER BY distance_miles,address_type_rank,address_checksum,location_key" in sql
    assert "LIMIT :page_limit OFFSET :offset" in sql
    assert "SELECT count(*) FROM nearest" in sql and "SELECT count(*) FROM projected" in sql
    assert "geo_assurance_version" not in sql


@pytest.mark.parametrize(
    "override",
    [
        {"radius_miles": "26"},
        {"radius_miles": "-1"},
        {"limit": "101"},
        {"offset": "199", "limit": "2"},
        {"include_sources": True},
        {"order": "desc"},
    ],
)
async def test_closed_request_rejected_before_managed_admission(held, override):
    held.args.update(override)
    assert await _search(held) is None
    managed.admitted_plan_release_bindings.assert_not_awaited()
    held.session.execute.assert_not_awaited()


async def test_extra_parameters_cannot_override_page_or_radius(held):
    with pytest.raises(ValueError, match="bounded request"):
        await distance._progressive_page_rows(
            held.session,
            held.selection,
            held.args,
            held.pagination,
            ("60611", None, None),
            3,
            extra_parameters={"offset": 199},
        )
    held.session.execute.assert_not_awaited()


async def test_managed_empty_page_preserves_scalar_total_at_nonzero_offset(held):
    held.pagination = SimpleNamespace(limit=2, offset=5, page=3)
    held.observations = [(3, 3)]
    response = await _search(held)
    assert response["items"] == []
    assert response["pagination"]["total"] == 3
    assert response["pagination"]["total_is_exact"] is True
    assert response["pagination"]["has_more"] is False
    held.driver.fetchrow.assert_not_awaited()
    assert held.windows[0][1]["offset"] == 5


async def test_duplicate_heavy_windows_expand_until_unique_page_is_proven(held):
    held.observations = [(128, 1), (256, 2), (320, 3)]
    response = await _search(held)
    assert [parameters["candidate_limit"] for _, parameters in held.windows] == [128, 256, 512]
    assert response["pagination"]["total"] == 3
    assert response["pagination"]["total_is_exact"] is True
    assert response["pagination"]["has_more"] is True
    assert len(response["items"]) == 2
    assert held.driver.fetchrow.await_count == 1


async def test_unready_managed_page_is_unavailable_instead_of_legacy_fallback(held):
    held.session.execute.side_effect = None
    held.session.execute.return_value = SimpleNamespace(
        mappings=lambda: SimpleNamespace(all=lambda: [{"projection_ready": False}])
    )
    with pytest.raises(PlanPricingProjectionUnavailable, match="page is unavailable"):
        await _search(held)
    held.driver.fetchrow.assert_not_awaited()


async def test_direct_managed_read_requires_retained_scope(held, monkeypatch):
    monkeypatch.setattr(managed, "current_network_address_scope", lambda: None)
    with pytest.raises(PlanPricingProjectionUnavailable, match="retained network scope"):
        await managed.search_managed_em_distance(
            held.session, held.selection, held.args, held.pagination, "60611", None, None, 3
        )
    managed.admitted_plan_release_bindings.assert_not_awaited()
    held.session.execute.assert_not_awaited()


@pytest.mark.parametrize("limit,offset,radius,window", [(1, 0, "0", 128), (100, 100, "25", 804)])
async def test_bounded_request_parameters_reach_the_managed_query_unchanged(held, limit, offset, radius, window):
    held.args.update(limit=str(limit), offset=str(offset), zip_radius_miles=radius)
    held.pagination = SimpleNamespace(limit=limit, offset=offset, page=1)
    held.observations = [(0, 0)]
    response = await _search(held)
    parameters = held.windows[0][1]
    assert parameters["page_limit"] == limit + 1
    assert parameters["offset"] == offset and parameters["candidate_limit"] == window
    assert parameters["radius_meters"] == float(radius) * 1609.344
    assert parameters["_network_excluded_ids"] == [7] and parameters["_network_approved_revision"] == 17
    assert response["pagination"]["limit"] == limit and response["pagination"]["offset"] == offset


async def test_legacy_em_page_keeps_original_checksum_scope_and_admission(held):
    held.scope.network_ids = (42,)
    held.scope.excluded_network_ids = None
    held.scope.legacy_checksums = (84,)
    result = await _search(held)
    assert len(result["items"]) == 2
    statement, parameters_by_name = held.windows[0]
    assert "plans_network_array && CAST(:_legacy_network_checksums AS INTEGER[])" in statement
    assert "canonical_network_ids &&" not in statement
    assert parameters_by_name["_legacy_network_checksums"] == [84]
    assert parameters_by_name["_canonical_network_ids"] == [42]
    assert parameters_by_name["allowed_binding_ordinals"] == [3, 11, 29]
