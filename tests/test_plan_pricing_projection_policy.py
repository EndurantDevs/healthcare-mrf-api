# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Projection reads admit original sources before data and public metadata."""

import asyncio
import json
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import orjson
import pytest

from api import plan_pricing_projection_read as reader
from tests.ptg2_factorized_card_support import factorized_selection


def _prepared(monkeypatch, positions, *, contract=reader.PROJECTION_CONTRACT):
    selection = factorized_selection(binding_count=3, contract=contract)
    admitted_bindings = tuple(selection.bindings[index] for index in positions)
    admission = AsyncMock(return_value=admitted_bindings)
    monkeypatch.setattr(reader, "admitted_plan_release_bindings", admission)
    monkeypatch.setattr(reader, "current_network_address_scope", lambda: object())
    geo = AsyncMock(return_value=["60601"])
    monkeypatch.setattr(reader, "geo_cells", geo)
    return SimpleNamespace(selection=selection, admitted=admitted_bindings, admission=admission, geo=geo)


def _args(**changes):
    return {"code_system": "CPT", "code": "27447", "zip5": "60601", "include_providers": "false", **changes}


@pytest.mark.asyncio
@pytest.mark.parametrize("positions", [(), (0,), (0, 2), (0, 1, 2)])
@pytest.mark.parametrize("offset", [0, 25])
async def test_admission_precedes_aggregate_reads_and_metadata_is_independent_of_page(monkeypatch, positions, offset):
    prepared = _prepared(monkeypatch, positions)
    session = AsyncMock()

    async def read(actual_session, request, cells, args, pagination, **options):
        prepared.admission.assert_awaited_once_with(session, prepared.selection)
        assert actual_session is session and cells == ["60601"]
        assert request.projection_id == prepared.selection.pricing_projection_id
        assert options == ({"binding_ordinals": positions} if len(positions) < 3 else {})
        return ([] if offset else [orjson.Fragment(b'{"geo_cell":"60601"}')]), 1

    aggregate = AsyncMock(side_effect=read)
    monkeypatch.setattr(reader, "read_aggregate_pack_page", aggregate)
    response = await reader.search_plan_pricing_projection(
        session, prepared.selection, _args(), SimpleNamespace(limit=1, offset=offset, page=1)
    )
    prepared.admission.assert_awaited_once_with(session, prepared.selection)
    assert aggregate.await_count == bool(positions)
    assert response["in_network_snapshot_ids"] == sorted(binding.snapshot_id for binding in prepared.admitted)
    assert response["binding_set_digest"] == prepared.selection.binding_set_digest
    assert len(prepared.selection.bindings) == 3
    assert response["pagination"]["total"] == bool(positions)
    assert bool(response["items"]) == bool(positions and offset == 0)
    encoded = json.dumps(orjson.loads(orjson.dumps(response)))
    for binding in prepared.selection.bindings:
        if binding not in prepared.admitted:
            assert binding.snapshot_id not in encoded
            assert binding.source_key not in encoded
            assert binding.plan_id not in encoded
    session.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("include_providers", ["true", "false"])
async def test_partial_legacy_projection_returns_to_attributed_native_reader(monkeypatch, include_providers):
    prepared = _prepared(monkeypatch, (0,), contract=reader.LEGACY_PROJECTION_CONTRACT)
    read = AsyncMock()
    monkeypatch.setattr(reader, "_read_page", read)
    assert (
        await reader.search_plan_pricing_projection(
            AsyncMock(),
            prepared.selection,
            _args(view="card", include_providers=include_providers),
            SimpleNamespace(limit=1, offset=0, page=1),
        )
        is None
    )
    prepared.admission.assert_awaited_once()
    read.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("positions", [(0, 1, 2), (0, 2)])
async def test_mixed_role_admission_preserves_packed_reads_and_subset_bounds(monkeypatch, positions):
    """Allowed amounts cannot turn complete pricing admission into a partial scan."""
    from api import plan_pricing_projection_aggregate_read as aggregate
    from tests.test_plan_pricing_projection_v3_read import PROJECTION_ID, _pack_row, _record, _Result

    prepared = _prepared(monkeypatch, positions)
    allowed = replace(
        prepared.selection.bindings[0], binding_ordinal=3, role="allowed_amounts", snapshot_id="allowed-snapshot"
    )
    selection = replace(
        prepared.selection,
        bindings=(*prepared.selection.bindings, allowed),
        pricing_projection_id=PROJECTION_ID,
    )
    statements = []

    async def execute(statement, parameters):
        statements.append((str(statement), parameters))
        if "profile_limit" in parameters:
            assert statement.compile().params["binding_ordinals"] == list(positions)
            return _Result(
                [
                    dict(
                        profile_count=1,
                        membership_count=aggregate._MAX_PARTIAL_MEMBERSHIPS + 1,
                        rate_value_count=1,
                        result_byte_count=0,
                        profiles_valid=True,
                    )
                ]
            )
        return _Result([_pack_row(_record("60601"))])

    session = SimpleNamespace(execute=AsyncMock(side_effect=execute))
    request = reader.search_plan_pricing_projection(
        session, selection, _args(), SimpleNamespace(limit=1, offset=0, page=1)
    )
    if len(positions) == 3:
        response = await request
        assert orjson.loads(orjson.dumps(response))["items"][0]["geo_cell"] == "60601"
        assert response["in_network_snapshot_ids"] == sorted(binding.snapshot_id for binding in prepared.admitted)
        assert "plan_pricing_aggregate_pack" in statements[0][0] and "profile_limit" not in statements[0][1]
    else:
        with pytest.raises(reader.PlanPricingProjectionUnsupported, match="native work bound"):
            await request
        assert "plan_pricing_rate_profile" in statements[0][0]
    assert len(statements) == 1 and selection.bindings[-1] is allowed
    prepared.admission.assert_awaited_once_with(session, selection)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [reader.PlanPricingProjectionUnavailable("unavailable"), asyncio.CancelledError()])
async def test_admission_refusal_and_cancellation_precede_geo_and_pricing(monkeypatch, failure):
    prepared = _prepared(monkeypatch, (0,))
    prepared.admission.side_effect = failure
    aggregate = AsyncMock()
    monkeypatch.setattr(reader, "read_aggregate_pack_page", aggregate)
    with pytest.raises(type(failure)) as caught:
        await reader.search_plan_pricing_projection(
            AsyncMock(), prepared.selection, _args(), SimpleNamespace(limit=1, offset=0, page=1)
        )
    assert caught.value is failure
    prepared.geo.assert_not_awaited()
    aggregate.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("stage", ["geo", "pricing"])
async def test_managed_capacity_refusal_does_not_become_unfiltered_fallback(monkeypatch, stage):
    prepared = _prepared(monkeypatch, (0, 1, 2))
    failure = reader.PlanPricingProjectionUnsupported("bounded request")
    if stage == "geo":
        prepared.geo.side_effect = failure
    else:
        monkeypatch.setattr(reader, "read_aggregate_pack_page", AsyncMock(side_effect=failure))
    with pytest.raises(reader.PlanPricingProjectionUnsupported) as caught:
        await reader.search_plan_pricing_projection(
            AsyncMock(), prepared.selection, _args(), SimpleNamespace(limit=1, offset=0, page=1)
        )
    assert caught.value is failure


@pytest.mark.asyncio
async def test_unmanaged_aggregate_keeps_existing_optional_fallback(monkeypatch):
    prepared = _prepared(monkeypatch, ())
    monkeypatch.setattr(reader, "current_network_address_scope", lambda: None)
    prepared.geo.side_effect = reader.PlanPricingProjectionUnsupported("bounded request")
    assert (
        await reader.search_plan_pricing_projection(
            AsyncMock(), prepared.selection, _args(), SimpleNamespace(limit=1, offset=0, page=1)
        )
        is None
    )
    prepared.admission.assert_not_awaited()


@pytest.mark.asyncio
async def test_factorized_cards_keep_their_own_source_admission(monkeypatch):
    prepared = _prepared(monkeypatch, ())
    assert (
        await reader.search_plan_pricing_projection(
            AsyncMock(),
            prepared.selection,
            _args(view="card", include_providers="true"),
            SimpleNamespace(limit=1, offset=0, page=1),
        )
        is None
    )
    prepared.admission.assert_not_awaited()
    prepared.geo.assert_not_awaited()
