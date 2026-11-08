# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Complete-code fallback keeps its physical capability local to each reader."""

import hashlib
from types import SimpleNamespace
from unittest.mock import AsyncMock

import orjson
import pytest

from api import plan_pricing_code_read as code_read
from api import plan_pricing_projection_read as projection
from api import plan_pricing_state_scan as scan
from api import ptg_provider_membership_build as membership
from api.endpoint.pagination import PaginationParams
from api.plan_pricing_code_contract import CONTRACT
from api.plan_pricing_projection_contract import (
    PlanPricingProjectionUnavailable,
    code_projection_scope,
    current_code_projection,
    table,
)
from tests.test_plan_pricing_code_projection import _capability, _selection
from tests.test_plan_pricing_state_scan import _args, _keyring, _Session
from tests.test_ptg_membership_code_factors import _inverse


@pytest.mark.asyncio
async def test_state_scan_resolves_complete_code_and_restores_outer_capability(monkeypatch):
    selection = _selection()
    capability = _capability()
    outer = _capability("reference_family_archive_00000000000000000000000000000002")
    resolve = AsyncMock(return_value=capability)
    monkeypatch.setattr(code_read, "resolve_code_pricing", resolve)
    monkeypatch.setattr(scan, "current_network_address_scope", lambda: None)
    monkeypatch.setattr(scan, "_cursor_keyring", _keyring)
    arguments_by_name = _args(code="27447", plan_release_id=selection.plan_release_id)
    session = _Session([], [])
    with code_projection_scope(outer):
        response = await scan.search_plan_pricing_state_scan(
            session, selection, arguments_by_name, PaginationParams(page=1, limit=2, offset=0, source="test")
        )
        assert current_code_projection() is outer
    resolve.assert_awaited_once_with(session, selection, arguments_by_name)
    assert response["items"] == [] and response["query"]["projection_contract"] == CONTRACT
    assert response["pagination"]["has_more"] is False
    assert len(session.calls) == 1
    sql, parameters_by_name = session.calls[0]
    assert "plan_pricing_code_provider_state" in sql
    assert capability.found[0].schema_name in sql and outer.found[0].schema_name not in sql
    assert parameters_by_name["projection_id"] == capability.projection_id
    assert selection.pricing_projection_id is None and current_code_projection() is None


@pytest.mark.asyncio
async def test_aggregate_fallback_reaches_packed_reader_with_exact_code_context(monkeypatch):
    selection = _selection()
    capability = _capability()
    outer = _capability("reference_family_archive_00000000000000000000000000000002")
    resolve = AsyncMock(return_value=capability)
    monkeypatch.setattr(code_read, "resolve_code_pricing", resolve)
    monkeypatch.setattr(projection, "current_network_address_scope", lambda: None)
    monkeypatch.setattr(projection, "geo_cells", AsyncMock(return_value=["60601"]))
    session = object()
    arguments_by_name = {"code_system": "CPT", "code": "27447", "zip5": "60601", "include_providers": "false"}
    pagination = SimpleNamespace(limit=2, offset=0, page=1)

    async def read_aggregate_page(actual_session, request, cells, actual_arguments, actual_pagination):
        assert actual_session is session and actual_arguments is arguments_by_name and actual_pagination is pagination
        assert cells == ["60601"] and (request.code_system, request.code) == ("CPT", "27447")
        assert request.contract == CONTRACT and request.projection_id == capability.projection_id
        assert current_code_projection() is capability
        assert capability.found[0].schema_name in table("plan_pricing_aggregate_pack")
        return [orjson.Fragment(b'{"geo_cell":"60601","rate_count":2}')], 1

    read = AsyncMock(side_effect=read_aggregate_page)
    monkeypatch.setattr(projection, "read_aggregate_pack_page", read)
    with code_projection_scope(outer):
        response = await projection.search_plan_pricing_projection(session, selection, arguments_by_name, pagination)
        assert current_code_projection() is outer
    resolve.assert_awaited_once_with(session, selection, arguments_by_name)
    read.assert_awaited_once()
    response_by_field = orjson.loads(orjson.dumps(response))
    assert response_by_field["items"] == [{"geo_cell": "60601", "rate_count": 2}]
    assert response_by_field["query"]["projection_contract"] == CONTRACT
    assert response_by_field["pagination"]["total"] == 1
    assert selection.pricing_projection_id is None and current_code_projection() is None


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ("state", "aggregate"))
@pytest.mark.parametrize("failure_stage", ("resolution", "read"))
async def test_unavailable_capability_never_escapes_into_ordinary_reader(monkeypatch, kind, failure_stage):
    selection = _selection()
    capability = _capability()
    outer = _capability("reference_family_archive_00000000000000000000000000000002")
    failure = PlanPricingProjectionUnavailable("sealed capability unavailable")
    resolve = AsyncMock(return_value=capability, side_effect=failure if failure_stage == "resolution" else None)
    monkeypatch.setattr(code_read, "resolve_code_pricing", resolve)
    monkeypatch.setattr(scan, "current_network_address_scope", lambda: None)
    monkeypatch.setattr(projection, "current_network_address_scope", lambda: None)
    monkeypatch.setattr(scan, "_cursor_keyring", _keyring)
    execute = AsyncMock(side_effect=failure)
    session = SimpleNamespace(execute=execute)
    read = AsyncMock(side_effect=failure)
    monkeypatch.setattr(projection, "read_aggregate_pack_page", read)
    geo = AsyncMock(return_value=["60601"])
    monkeypatch.setattr(projection, "geo_cells", geo)
    with code_projection_scope(outer):
        with pytest.raises(PlanPricingProjectionUnavailable) as caught:
            if kind == "state":
                await scan.search_plan_pricing_state_scan(
                    session,
                    selection,
                    _args(code="27447", plan_release_id=selection.plan_release_id),
                    PaginationParams(page=1, limit=2, offset=0, source="test"),
                )
            else:
                await projection.search_plan_pricing_projection(
                    session,
                    selection,
                    {"code_system": "CPT", "code": "27447", "zip5": "60601", "include_providers": "false"},
                    SimpleNamespace(limit=2, offset=0, page=1),
                )
        assert caught.value is failure and current_code_projection() is outer
    assert resolve.await_count == 1 and current_code_projection() is None
    if failure_stage == "resolution":
        execute.assert_not_awaited()
        read.assert_not_awaited()
        geo.assert_not_awaited()
    elif kind == "state":
        execute.assert_awaited_once()
        read.assert_not_awaited()
    else:
        read.assert_awaited_once()
        execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ("state", "aggregate"))
async def test_absent_capability_keeps_existing_reader_refusal_or_optional_fallback(monkeypatch, kind):
    selection = _selection()
    resolve = AsyncMock(return_value=None)
    monkeypatch.setattr(code_read, "resolve_code_pricing", resolve)
    execute = AsyncMock()
    session = SimpleNamespace(execute=execute)
    if kind == "state":
        with pytest.raises(PlanPricingProjectionUnavailable, match="no ready"):
            await scan.search_plan_pricing_state_scan(
                session,
                selection,
                _args(code="27447", plan_release_id=selection.plan_release_id),
                PaginationParams(page=1, limit=2, offset=0, source="test"),
            )
    else:
        assert (
            await projection.search_plan_pricing_projection(
                session,
                selection,
                {"code_system": "CPT", "code": "27447", "include_providers": "false"},
                SimpleNamespace(limit=2, offset=0, page=1),
            )
            is None
        )
    resolve.assert_awaited_once()
    execute.assert_not_awaited()
    assert current_code_projection() is None


@pytest.mark.asyncio
@pytest.mark.parametrize("representation", ("pattern_v1", "direct_v1"))
async def test_code_factor_owner_bound_refuses_before_inverse_reads_or_copy(monkeypatch, representation):
    root, lookup, copy, _, _ = _inverse(monkeypatch, representation)
    manifest = AsyncMock(return_value=SimpleNamespace(owner_count=membership.MAX_BUILD_OWNERS + 1))
    monkeypatch.setattr(membership, "load_v4_relation_manifest", manifest)
    with pytest.raises(ValueError, match="bounded build"):
        await membership._copy_code_factor(
            object(), object(), root, object(), (10,), hashlib.sha256(), edge_allowance=1
        )
    manifest.assert_awaited_once()
    lookup.assert_not_awaited()
    copy.assert_not_awaited()


@pytest.mark.asyncio
async def test_code_factor_copy_flush_preserves_every_edge_without_reusing_mutated_batch(monkeypatch):
    factors = tuple(range(5001))
    root, lookup, copy, relation, inverse = _inverse(monkeypatch, members={10: factors})
    manifests_by_relation = {
        relation: SimpleNamespace(owner_base=0, owner_count=5001, logical_member_count=5001),
        inverse: SimpleNamespace(owner_base=10, owner_count=1, logical_member_count=5001),
    }
    monkeypatch.setattr(
        membership,
        "load_v4_relation_manifest",
        AsyncMock(side_effect=lambda _session, **kw: manifests_by_relation[kw["relation"]]),
    )
    result = await membership._copy_code_factor(
        object(), object(), root, object(), (10,), hashlib.sha256(), edge_allowance=5001
    )
    copied_batches = [call.args[3] for call in copy.await_args_list]
    assert [len(rows) for rows in copied_batches] == [5000, 1]
    assert copied_batches[0] is not copied_batches[1]
    assert [row for rows in copied_batches for row in rows] == [(factor, 10) for factor in factors]
    assert all(call.args[2] is membership.PTGMembershipSecond for call in copy.await_args_list)
    assert result["edge_count"] == 5001
    lookup.assert_awaited_once()
